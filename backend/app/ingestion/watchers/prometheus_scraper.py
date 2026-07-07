"""
Prometheus Scraper
Периодически опрашивает Prometheus Exporter N.I.N.A.
Устраняет Упрощение #8: парсит ВСЕ метрики оборудования и погоды.

ИСПРАВЛЕНО: Добавлена защита от спама логов при недоступности Prometheus.
- Throttling: ошибки логируются не чаще 1 раза в минуту
- Уровень DEBUG для стандартных ошибок соединения
- Информативное сообщение при восстановлении соединения
"""

import asyncio
import logging
import httpx
from datetime import datetime, timedelta
from app.core.config import settings
from app.core.events import event_bus
from app.ingestion.parsers.prometheus_metrics import parse_prometheus_text

logger = logging.getLogger("PrometheusScraper")


class PrometheusScraper:
    """
    Периодически опрашивает Prometheus Exporter N.I.N.A.
    Устраняет Упрощение #8: парсит ВСЕ метрики оборудования и погоды.
    """

    def __init__(self, interval_sec: float = 3.0):
        self.url = f"{settings.network.prometheus_url}/metrics"
        self.interval = interval_sec
        self._running = False
        self._task: asyncio.Task = None

        # ИСПРАВЛЕНО: Трекинг состояния для защиты от спама
        self._last_error_time: datetime = None
        self._consecutive_errors: int = 0
        self._error_log_interval = timedelta(
            minutes=1
        )  # Логировать ошибку раз в минуту
        self._was_connected: bool = False  # Для логирования восстановления

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._scrape_loop())
        logger.info(
            f"Prometheus Scraper started (interval={self.interval}s, url={self.url})"
        )

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _scrape_loop(self):
        async with httpx.AsyncClient(timeout=5.0) as client:
            while self._running:
                try:
                    response = await client.get(self.url)

                    if response.status_code == 200:
                        # Успешный запрос - сбрасываем счетчик ошибок
                        if self._consecutive_errors > 0:
                            logger.info(
                                f"✅ Prometheus connection восстановлено после "
                                f"{self._consecutive_errors} ошибок"
                            )
                            self._consecutive_errors = 0
                            self._last_error_time = None

                        self._was_connected = True
                        metrics = parse_prometheus_text(response.text)
                        await event_bus.publish(
                            "PROMETHEUS_UPDATE", metrics.model_dump()
                        )
                    else:
                        self._log_error_throttled(
                            f"Prometheus returned статус {response.status_code}",
                            level="WARNING",
                        )

                except httpx.ConnectError:
                    # N.I.N.A. закрыта или Prometheus Exporter плагин отключен
                    self._log_error_throttled(
                        "Prometheus exporter недоступен (N.I.N.A. закрыта или плагин отключен)",
                        level="DEBUG",
                    )

                except httpx.ReadError:
                    # "Server disconnected without sending a response"
                    self._log_error_throttled(
                        "Prometheus разорвал соединение (ReadError)", level="DEBUG"
                    )

                except httpx.RemoteProtocolError:
                    # Сервер закрыл соединение преждевременно
                    self._log_error_throttled(
                        "Prometheus закрыл соединение преждевременно", level="DEBUG"
                    )

                except httpx.TimeoutException:
                    self._log_error_throttled("Prometheus timeout (5s)", level="DEBUG")

                except httpx.HTTPError as e:
                    # Любая другая HTTP ошибка
                    self._log_error_throttled(
                        f"HTTP ошибка Prometheus: {type(e).__name__}", level="DEBUG"
                    )

                except Exception as e:
                    # Неожиданные ошибки (не httpx)
                    self._log_error_throttled(
                        f"Неожиданная ошибка Prometheus: {type(e).__name__}: {e}",
                        level="WARNING",
                    )

                await asyncio.sleep(self.interval)

    def _log_error_throttled(self, message: str, level: str = "DEBUG"):
        """
        Логирует ошибки с throttling для предотвращения спама.

        Args:
            message: Текст ошибки
            level: Уровень логирования (DEBUG, WARNING, ERROR)
        """
        now = datetime.now()
        self._consecutive_errors += 1

        # Первая ошибка или прошло достаточно времени
        should_log = (
            self._last_error_time is None
            or (now - self._last_error_time) >= self._error_log_interval
        )

        if should_log:
            # Добавляем счетчик если ошибок много
            if self._consecutive_errors > 1:
                message += f" ({self._consecutive_errors} ошибок подряд)"

            if level == "DEBUG":
                logger.debug(message)
            elif level == "WARNING":
                logger.warning(message)
            elif level == "ERROR":
                logger.error(message)
            else:
                logger.info(message)

            self._last_error_time = now
