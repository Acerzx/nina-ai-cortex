import asyncio
import logging
import httpx
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
                        metrics = parse_prometheus_text(response.text)
                        await event_bus.publish(
                            "PROMETHEUS_UPDATE", metrics.model_dump()
                        )
                    else:
                        logger.warning(
                            f"Prometheus returned status {response.status_code}"
                        )
                except httpx.ConnectError:
                    logger.debug(
                        "Prometheus exporter not available (N.I.N.A. closed or plugin disabled)"
                    )
                except Exception as e:
                    logger.error(f"Error scraping Prometheus: {e}")

                await asyncio.sleep(self.interval)
