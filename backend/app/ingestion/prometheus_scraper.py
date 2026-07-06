import asyncio
import logging
import re
import aiohttp
from typing import Dict, Any
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class PrometheusScraper:
    """
    Асинхронно опрашивает Prometheus Exporter N.I.N.A.
    """

    def __init__(self):
        self.settings = get_settings()
        self.url = self.settings.network.prometheus_url
        self.running = False
        self.metrics: Dict[str, Any] = {}
        self.last_error: str = ""
        self.successful_scrapes = 0
        self.failed_scrapes = 0

        # Regex для парсинга Prometheus text format
        self.metric_pattern = re.compile(
            r"^([a-zA-Z0-9_]+)(?:\{(.*)\})?\s+([0-9eE\+\-\.]+)$"
        )

    async def start(self):
        self.running = True
        logger.info(f"📊 Starting Prometheus Scraper on {self.url}")

        # Проверка доступности при старте
        await self._check_connectivity()

        while self.running:
            try:
                await self._scrape()
                await asyncio.sleep(2)  # Опрашиваем каждые 2 секунды
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug(f"Scrape loop error: {e}")
                await asyncio.sleep(5)

    async def _check_connectivity(self):
        """Проверяет доступность Prometheus endpoint при старте."""
        try:
            timeout = aiohttp.ClientTimeout(total=5)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.url) as response:
                    if response.status == 200:
                        text = await response.text()
                        lines = [
                            l for l in text.splitlines() if l and not l.startswith("#")
                        ]
                        logger.info(
                            f"✅ Prometheus endpoint is accessible ({len(lines)} metrics found)"
                        )
                        self._parse_metrics(text)  # Парсим сразу
                    else:
                        logger.warning(
                            f"⚠️ Prometheus returned status {response.status}"
                        )
        except Exception as e:
            logger.error(f"❌ Cannot connect to Prometheus at {self.url}: {e}")
            logger.error(
                f"   Убедитесь, что плагин 'Prometheus Exporter' запущен в N.I.N.A."
            )
            logger.error(
                f"   Попробуйте изменить URL в settings.yaml на http://127.0.0.1:9876/metrics"
            )

    async def _scrape(self):
        """Выполняет один цикл скрейпинга."""
        try:
            timeout = aiohttp.ClientTimeout(total=3)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(self.url) as response:
                    if response.status == 200:
                        text = await response.text()
                        self._parse_metrics(text)
                        self.successful_scrapes += 1

                        # Логируем редко, чтобы не спамить
                        if self.successful_scrapes % 30 == 0:
                            logger.info(
                                f"📊 Prometheus: {len(self.metrics)} metrics active"
                            )
                    else:
                        self.failed_scrapes += 1

        except aiohttp.ClientError as e:
            # Тихий режим при ошибках сети
            self.failed_scrapes += 1
            if self.failed_scrapes == 1:
                logger.warning(f"⚠️ Prometheus connection lost: {e}")
        except Exception as e:
            self.failed_scrapes += 1

    def _parse_metrics(self, text: str):
        """Парсит Prometheus text format в плоский словарь."""
        new_metrics = {}

        for line in text.splitlines():
            if line.startswith("#") or not line.strip():
                continue

            match = self.metric_pattern.match(line)
            if match:
                name = match.group(1)
                labels_str = match.group(2)
                value_str = match.group(3)

                try:
                    value = float(value_str)
                except ValueError:
                    continue

                if labels_str:
                    key = f"{name}{{{labels_str}}}"
                else:
                    key = name

                new_metrics[key] = value

        self.metrics = new_metrics

    def get_metrics(self) -> Dict[str, Any]:
        return self.metrics

    def stop(self):
        self.running = False
