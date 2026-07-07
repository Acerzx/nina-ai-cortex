"""
InfluxDB Subscriber
Подписчик на события для выполнения Flux-запросов по требованию.

ИСПРАВЛЕНО: Убран async with для QueryApiAsync (не поддерживает context manager).
"""

import logging
import asyncio
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("InfluxSubscriber")


class InfluxDBSubscriber:
    """
    Подписчик на события InfluxDB.
    Предоставляет возможность выполнять Flux-запросы по требованию
    через событие QUERY_INFLUX.
    """

    def __init__(self):
        self.client = None
        self.org = settings.influxdb.org
        self.bucket = settings.influxdb.bucket

    async def start(self):
        try:
            self.client = InfluxDBClientAsync(
                url=settings.influxdb.url, token=settings.influxdb.token, org=self.org
            )
            # Проверяем подключение
            ready = await self.client.ping()
            if ready:
                logger.info("✅ InfluxDB Subscriber connected")
            else:
                logger.warning("⚠️ InfluxDB ping returned False")

            event_bus.subscribe("QUERY_INFLUX", self._handle_query)
        except Exception as e:
            logger.error(f"❌ InfluxDB connection failed: {e}")
            logger.error("   Убедитесь, что InfluxDB запущен: docker compose up -d")

    async def _handle_query(self, data: dict):
        """
        Обрабатывает запрос на выполнение Flux-запроса.

        Ожидаемый формат data:
        {
            "query": "from(bucket: ...) |> ...",
            "callback": async_function(tables)
        }
        """
        query = data.get("query")
        callback = data.get("callback")

        if not query or not callback:
            logger.warning("QUERY_INFLUX: missing query or callback")
            return

        if not self.client:
            logger.error("QUERY_INFLUX: client not connected")
            return

        try:
            # ИСПРАВЛЕНО: НЕ используем async with
            query_api = self.client.query_api()
            tables = await query_api.query(query)
            await callback(tables)
        except Exception as e:
            logger.error(f"❌ Influx query failed: {type(e).__name__}: {e}")

    async def stop(self):
        if self.client:
            try:
                await self.client.close()
                logger.info("✅ InfluxDB Subscriber client closed")
            except Exception as e:
                logger.debug(f"Error closing InfluxDB subscriber client: {e}")
            finally:
                self.client = None
