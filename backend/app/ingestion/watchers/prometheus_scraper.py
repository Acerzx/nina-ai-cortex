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

from datetime import datetime, timedelta
from app.core.config import settings
from app.core.events import event_bus
from app.ingestion.parsers.prometheus_metrics import parse_prometheus_text
from app.core.http_client import http_client_manager

logger = logging.getLogger("PrometheusScraper")


class PrometheusScraper:
    """
    Периодически опрашивает Prometheus Exporter N.I.N.A.
    Устраняет Упрощение #8: парсит ВСЕ метрики оборудования и погоды.
    """

    def __init__(self, interval_sec: Optional[float] = None):
        # ИСПРАВЛЕНО (v4.0 — проблема #42): интервал читается из конфига
        if interval_sec is not None:
            self.interval = interval_sec
        else:
            # Читаем из settings.data_sources.metrics_poll_interval
            try:
                ds_cfg = getattr(settings, "data_sources", None)
                if ds_cfg:
                    self.interval = getattr(ds_cfg, "metrics_poll_interval", 3.0)
                else:
                    self.interval = 3.0
            except Exception as e:
                logger.debug(f"Could not load metrics_poll_interval: {e}")
                self.interval = 3.0

        # Валидация интервала
        if self.interval < 1.0:
            logger.warning(
                f"⚠️ Prometheus scrape interval {self.interval}s is too small, "
                f"setting to 1.0s minimum"
            )
            self.interval = 1.0
        elif self.interval > 60.0:
            logger.warning(
                f"⚠️ Prometheus scrape interval {self.interval}s is too large, "
                f"setting to 60.0s maximum"
            )
            self.interval = 60.0

        self.url = f"{settings.network.prometheus_url}/metrics"
        self._running = False
        self._task: asyncio.Task = None

        # Трекинг состояния для защиты от спама
        self._last_error_time: datetime = None
        self._consecutive_errors: int = 0
        self._error_log_interval = timedelta(minutes=1)
        self._was_connected: bool = False

        logger.info(
            f"📊 PrometheusScraper initialized "
            f"(interval: {self.interval}s, url: {self.url})"
        )

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
        """
        Основной цикл опроса Prometheus.

        ИСПРАВЛЕНО (С-15 + Н-10): использует http_client_manager для connection pooling.
        Клиент для Prometheus кэшируется по ключу "prometheus:{url}" и переиспользуется
        между всеми итерациями loop. Таймауты из settings.http_client.prometheus.
        """
        # ИСПРАВЛЕНО (С-15): Получаем клиент через менеджер один раз
        # service="prometheus" — использует конфигурацию из settings.http_client.prometheus
        # (timeout=5.0s, max_connections=5, max_keepalive=3)
        client = await http_client_manager.get_client(
            base_url=self.url,
            service="prometheus",
        )

        while self._running:
            try:
                response = await client.get(self.url)
                if response.status_code == 200:
                    # Успешный запрос - сбрасываем счетчик ошибок
                    if self._consecutive_errors > 0:
                        logger.info(
                            f"✅ Prometheus подключение восстановлено после "
                            f"{self._consecutive_errors} ошибок"
                        )
                        self._consecutive_errors = 0
                        self._last_error_time = None
                        self._was_connected = True

                    metrics = parse_prometheus_text(response.text)
                    await event_bus.publish("PROMETHEUS_UPDATE", metrics.model_dump())
                else:
                    self._log_error_throttled(
                        f"Prometheus вернула статус {response.status_code}",
                        level="WARNING",
                    )

            except Exception as e:
                # Обработка всех ошибок подключения
                error_type = type(e).__name__

                if "Connect" in error_type:
                    # N.I.N.A. закрыта или Prometheus Exporter плагин отключен
                    self._log_error_throttled(
                        "Prometheus exporter недоступен (N.I.N.A. закрыта или плагин отключен)",
                        level="DEBUG",
                    )
                elif "Read" in error_type:
                    # "Server disconnected without sending a response"
                    self._log_error_throttled(
                        "Prometheus разорвал соединение (ReadError)", level="DEBUG"
                    )
                elif "RemoteProtocol" in error_type:
                    # Сервер закрыл соединение преждевременно
                    self._log_error_throttled(
                        "Prometheus закрыл соединение преждевременно", level="DEBUG"
                    )
                elif "Timeout" in error_type:
                    self._log_error_throttled("Prometheus timeout", level="DEBUG")
                elif "HTTP" in error_type:
                    # Любая другая HTTP ошибка
                    self._log_error_throttled(
                        f"HTTP ошибка Prometheus: {error_type}", level="DEBUG"
                    )
                else:
                    # Неожиданные ошибки
                    self._log_error_throttled(
                        f"Неожиданная ошибка Prometheus: {error_type}: {e}",
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
