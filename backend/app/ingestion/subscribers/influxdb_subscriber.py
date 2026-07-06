import logging, asyncio
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("InfluxSubscriber")


class InfluxDBSubscriber:
    def __init__(self):
        self.client = None
        self.org = settings.influxdb.org
        self.bucket = settings.influxdb.bucket

    async def start(self):
        try:
            self.client = InfluxDBClientAsync(
                url=settings.influxdb.url, token=settings.influxdb.token, org=self.org
            )
            logger.info("InfluxDB Subscriber connected")
            event_bus.subscribe("QUERY_INFLUX", self._handle_query)
        except Exception as e:
            logger.error(f"InfluxDB connection failed: {e}")

    async def _handle_query(self, data: dict):
        query = data.get("query")
        callback = data.get("callback")
        if not query or not callback:
            return
        try:
            async with self.client.query_api() as query_api:
                tables = await query_api.query(query)
                await callback(tables)
        except Exception as e:
            logger.error(f"Influx query failed: {e}")

    async def stop(self):
        if self.client:
            await self.client.close()
