"""
InfluxDB Metrics Provider
Основной источник метрик обсерватории через InfluxDB Exporter.
Устраняет зависимость от Prometheus Exporter.
ИСПРАВЛЕНО:
- Убран async with для QueryApiAsync (не поддерживает context manager)
- Добавлена защита от None значений
- Оптимизированы Flux queries
- v4.1: Раскомментирован FLUX_QUERIES (был закомментирован в тройных кавычках)
ИСПРАВЛЕНО (P1):
- Переключение с 7 отдельных FLUX_QUERIES на единый UNIFIED_FLUX_QUERY
- Уменьшение round-trip к InfluxDB с 7 до 1
- Ожидаемый выигрыш: 4.9с → 1.2с (ускорение в 4 раза)
- FLUX_QUERIES сохранён как fallback
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("InfluxDBMetricsProvider")


class InfluxDBMetricsProvider:
    """
    Поставщик метрик из InfluxDB.
    Заменяет Prometheus Scraper как основной источник метрик.
    InfluxDB Exporter записывает метрики N.I.N.A. в InfluxDB,
    откуда мы их читаем через Flux queries.

    ИСПРАВЛЕНО (P1):
    - Единый UNIFIED_FLUX_QUERY вместо 7 отдельных
    - Fallback на FLUX_QUERIES при ошибке unified query
    """

    # ========================================================================
    # UNIFIED FLUX QUERY (P1: один запрос вместо 7)
    # ========================================================================
    UNIFIED_FLUX_QUERY = """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) =>
    r._measurement == "nina_camera" or
    r._measurement == "nina_focuser" or
    r._measurement == "nina_guider" or
    r._measurement == "nina_telescope" or
    r._measurement == "nina_rotator" or
    r._measurement == "nina_weather" or
    r._measurement == "nina_image" or
    r._measurement == "nina_sequence"
  )
  |> last()
  |> group()
"""

    # ========================================================================
    # FALLBACK: Раздельные запросы (используются если unified не работает)
    # ========================================================================
    FLUX_QUERIES = {
        "camera": """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "nina_camera")
  |> last()
  |> group()
""",
        "focuser": """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "nina_focuser")
  |> last()
  |> group()
""",
        "guider": """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "nina_guider")
  |> last()
  |> group()
""",
        "telescope": """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "nina_telescope")
  |> last()
  |> group()
""",
        "weather": """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "nina_weather")
  |> last()
  |> group()
""",
        "image": """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "nina_image")
  |> last()
  |> group()
""",
        "sequence": """
from(bucket: "{bucket}")
  |> range(start: -5m)
  |> filter(fn: (r) => r._measurement == "nina_sequence")
  |> last()
  |> group()
""",
    }

    # Маппинг InfluxDB полей на внутренние имена метрик
    METRICS_MAPPING = {
        # Camera
        ("nina_camera", "temperature"): "camera_temp",
        ("nina_camera", "cooler_power"): "camera_cooler_power",
        # Focuser
        ("nina_focuser", "position"): "focuser_position",
        ("nina_focuser", "temperature"): "focuser_temp",
        # Guider (PHD2)
        ("nina_guider", "rms_ra"): "guider_rms_ra",
        ("nina_guider", "rms_dec"): "guider_rms_dec",
        ("nina_guider", "rms_total"): "guider_rms_total",
        # Mount
        ("nina_telescope", "altitude"): "mount_altitude",
        ("nina_telescope", "azimuth"): "mount_azimuth",
        ("nina_telescope", "tracking"): "mount_tracking",
        # Rotator
        ("nina_rotator", "mechanical_position"): "rotator_angle",
        # Weather
        ("nina_weather", "temperature"): "wx_temperature",
        ("nina_weather", "humidity"): "wx_humidity",
        ("nina_weather", "dewpoint"): "wx_dewpoint",
        ("nina_weather", "cloud_cover"): "wx_cloud_cover",
        ("nina_weather", "wind_speed"): "wx_wind_speed",
        ("nina_weather", "wind_gust"): "wx_wind_gust",
        ("nina_weather", "wind_direction"): "wx_wind_direction",
        ("nina_weather", "pressure"): "wx_pressure",
        # Image Quality
        ("nina_image", "hfr"): "image_hfr",
        ("nina_image", "fwhm"): "image_fwhm",
        ("nina_image", "stars"): "image_stars",
        ("nina_image", "median"): "image_median",
        ("nina_image", "eccentricity"): "image_eccentricity",
        # Sequence
        ("nina_sequence", "running"): "sequence_running",
    }

    # История метрик (единая query для всех)
    HISTORY_FLUX_QUERY = """
from(bucket: "{bucket}")
  |> range(start: -1h)
  |> filter(fn: (r) =>
    r._measurement == "nina_camera" and r._field == "temperature" or
    r._measurement == "nina_image" and r._field == "hfr" or
    r._measurement == "nina_image" and r._field == "fwhm" or
    r._measurement == "nina_guider" and r._field == "rms_ra" or
    r._measurement == "nina_guider" and r._field == "rms_dec" or
    r._measurement == "nina_weather" and r._field == "wind_speed"
  )
  |> aggregateWindow(every: 30s, fn: mean, createEmpty: false)
  |> yield(name: "mean")
"""

    HISTORY_MAPPING = {
        ("nina_camera", "temperature"): "temperature_history",
        ("nina_image", "hfr"): "hfr_history",
        ("nina_image", "fwhm"): "fwhm_history",
        ("nina_guider", "rms_ra"): "rms_ra_history",
        ("nina_guider", "rms_dec"): "rms_dec_history",
        ("nina_weather", "wind_speed"): "wind_speed_history",
    }

    def __init__(self, query_interval: float = 3.0):
        self.bucket = settings.influxdb.bucket
        self.org = settings.influxdb.org
        self.url = settings.influxdb.url
        self.token = settings.influxdb.token
        self.query_interval = query_interval

        self._client: Optional[InfluxDBClientAsync] = None
        self._running = False
        self._task: Optional[asyncio.Task] = None

        self._history_cache: Dict[str, List[float]] = {}
        self._last_error_time: Optional[datetime] = None
        self._consecutive_errors: int = 0
        self._error_log_interval = timedelta(minutes=1)

        self._reconnect_attempts = 0
        self._max_reconnect_delay = 60.0
        self._base_reconnect_delay = 2.0

        self._last_metrics_count: int = 0
        self._successful_queries: int = 0

        # ИСПРАВЛЕНО (P1): Флаг использования unified query
        self._use_unified_query: bool = True
        self._unified_query_failures: int = 0
        self._unified_failure_threshold: int = 3  # После 3 неудач — fallback

        logger.info(
            f"📊 InfluxDB Metrics Provider initialized "
            f"(unified_query={self._use_unified_query}, "
            f"interval={self.query_interval}s)"
        )

    async def start(self):
        """Запускает provider."""
        self._running = True

        try:
            self._client = InfluxDBClientAsync(
                url=self.url, token=self.token, org=self.org
            )
            ready = await self._client.ping()
            if ready:
                logger.info(f"✅ InfluxDB Metrics Provider connected to {self.url}")
            else:
                logger.warning(f"⚠️ InfluxDB ping returned False, but continuing...")
        except Exception as e:
            logger.error(f"❌ Failed to connect to InfluxDB: {e}")
            logger.error("   Убедитесь, что InfluxDB запущен: docker compose up -d")

        self._task = asyncio.create_task(self._query_loop())
        logger.info(
            f"📊 InfluxDB Metrics Provider started (interval={self.query_interval}s)"
        )

    async def stop(self):
        """Останавливает provider и корректно закрывает клиент."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        if self._client is not None:
            try:
                await self._client.close()
                logger.info("✅ InfluxDB client closed")
            except Exception as e:
                logger.debug(f"Error closing InfluxDB client: {e}")
            finally:
                self._client = None

        logger.info("🛑 InfluxDB Metrics Provider stopped")

    async def _query_loop(self):
        """Основной цикл опроса InfluxDB."""
        while self._running:
            try:
                if self._client is None:
                    await self._try_reconnect()
                    await asyncio.sleep(self.query_interval)
                    continue

                metrics = await self._fetch_current_metrics()

                if metrics:
                    if self._consecutive_errors > 0:
                        logger.info(
                            f"✅ InfluxDB connection восстановлено после "
                            f"{self._consecutive_errors} ошибок"
                        )
                        self._consecutive_errors = 0
                        self._last_error_time = None

                    self._successful_queries += 1

                    await event_bus.publish("INFLUXDB_UPDATE", metrics)

                    if self._successful_queries % 20 == 0:
                        self._log_metrics_summary(metrics)

                # Периодически запрашиваем историю (каждые 20 успешных запросов)
                if self._successful_queries % 20 == 0 and self._successful_queries > 0:
                    await self._fetch_history_metrics()

            except Exception as e:
                self._log_error_throttled(
                    f"Ошибка запроса к InfluxDB: {type(e).__name__}: {e}",
                    level="WARNING",
                )
                self._client = None

            await asyncio.sleep(self.query_interval)

    async def _try_reconnect(self):
        """Переподключение к InfluxDB с exponential backoff."""
        if self._client is not None:
            try:
                await self._client.close()
                logger.debug("Old InfluxDB client closed before reconnect")
            except Exception as e:
                logger.debug(f"Error closing old InfluxDB client: {e}")
            finally:
                self._client = None

        import random

        exponential_delay = self._base_reconnect_delay * (2**self._reconnect_attempts)
        jitter = random.uniform(0, self._base_reconnect_delay * 0.5)
        delay = min(exponential_delay + jitter, self._max_reconnect_delay)

        logger.info(
            f"🔄 Attempting to reconnect to InfluxDB in {delay:.1f}s... "
            f"(attempt {self._reconnect_attempts + 1})"
        )
        await asyncio.sleep(delay)

        try:
            new_client = InfluxDBClientAsync(
                url=self.url, token=self.token, org=self.org
            )
            ready = await new_client.ping()

            if ready:
                self._client = new_client
                logger.info(
                    f"✅ Reconnected to InfluxDB successfully "
                    f"(after {self._reconnect_attempts} attempts)"
                )
                self._reconnect_attempts = 0
            else:
                try:
                    await new_client.close()
                except Exception:
                    pass
                logger.warning("⚠️ InfluxDB ping returned False after reconnect")
                self._client = None
                self._reconnect_attempts += 1

        except Exception as e:
            logger.debug(f"Reconnect failed: {e}")
            self._client = None
            self._reconnect_attempts += 1

    async def _fetch_current_metrics(self) -> Dict[str, Any]:
        """
        Запрашивает текущие метрики из InfluxDB.

        ИСПРАВЛЕНО (P1):
        - Primary: UNIFIED_FLUX_QUERY (один запрос, все метрики)
        - Fallback: FLUX_QUERIES (7 отдельных запросов) при ошибке unified
        """
        if not self._client:
            return {}

        query_api = self._client.query_api()

        # === P1: Пробуем unified query сначала ===
        if self._use_unified_query:
            try:
                metrics = await self._fetch_via_unified_query(query_api)
                if metrics:
                    # Сбрасываем счётчик неудач при успехе
                    self._unified_query_failures = 0
                    self._last_metrics_count = len(metrics)
                    return metrics
            except Exception as e:
                self._unified_query_failures += 1
                logger.debug(
                    f"Unified query failed "
                    f"({self._unified_query_failures}/"
                    f"{self._unified_failure_threshold}): {e}"
                )

                # После нескольких неудач — переключаемся на fallback
                if self._unified_query_failures >= self._unified_failure_threshold:
                    logger.warning(
                        f"⚠️ Unified query failed {self._unified_query_failures} "
                        f"times, switching to separate queries (fallback)"
                    )
                    self._use_unified_query = False

        # === Fallback: Раздельные запросы ===
        return await self._fetch_via_separate_queries(query_api)

    async def _fetch_via_unified_query(self, query_api) -> Dict[str, Any]:
        """
        P1: Единый запрос ко всем метрикам.
        Один round-trip к InfluxDB вместо 7.
        """
        query = self.UNIFIED_FLUX_QUERY.format(bucket=self.bucket)
        tables = await query_api.query(query)

        if not tables:
            return {}

        return self._parse_unified_results(tables)

    def _parse_unified_results(self, tables) -> Dict[str, Any]:
        """
        Парсит результаты unified query.
        Каждая запись содержит _measurement и _field для маппинга.
        """
        metrics: Dict[str, Any] = {}

        for table in tables:
            for record in table.records:
                # Извлекаем measurement и field из записи
                measurement = record.get_measurement()
                field = record.get_field()
                value = record.get_value()

                if value is None:
                    continue

                # Маппинг через METRICS_MAPPING
                internal_name = self.METRICS_MAPPING.get((measurement, field))
                if not internal_name:
                    continue

                try:
                    if isinstance(value, bool):
                        metrics[internal_name] = value
                    else:
                        metrics[internal_name] = float(value)
                except (ValueError, TypeError):
                    metrics[internal_name] = value

        return metrics

    async def _fetch_via_separate_queries(self, query_api) -> Dict[str, Any]:
        """
        Fallback: 7 раздельных запросов (используется при ошибке unified).
        """
        metrics: Dict[str, Any] = {}

        try:
            tasks = []
            for group_name, query_template in self.FLUX_QUERIES.items():
                query = query_template.format(bucket=self.bucket)
                tasks.append(self._execute_query(query_api, query))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Exception):
                    logger.debug(f"Query failed: {result}")
                    continue

                tables = result
                if not tables:
                    continue

                for table in tables:
                    for record in table.records:
                        measurement = record.get_measurement()
                        field = record.get_field()
                        value = record.get_value()

                        if value is None:
                            continue

                        internal_name = self.METRICS_MAPPING.get((measurement, field))
                        if not internal_name:
                            continue

                        try:
                            if isinstance(value, bool):
                                metrics[internal_name] = value
                            else:
                                metrics[internal_name] = float(value)
                        except (ValueError, TypeError):
                            metrics[internal_name] = value

        except Exception as e:
            raise

        self._last_metrics_count = len(metrics)
        return metrics

    async def _execute_query(self, query_api, query: str):
        """Выполняет один Flux запрос."""
        return await query_api.query(query)

    async def _fetch_history_metrics(self):
        """Запрашивает историю метрик для трендового анализа."""
        if not self._client:
            return

        query_api = self._client.query_api()

        try:
            query = self.HISTORY_FLUX_QUERY.format(bucket=self.bucket)
            tables = await query_api.query(query)

            if not tables:
                return

            grouped: Dict[tuple, List[float]] = {}

            for table in tables:
                for record in table.records:
                    measurement = record.get_measurement()
                    field = record.get_field()
                    value = record.get_value()

                    if value is None:
                        continue

                    try:
                        float_value = float(value)
                        key = (measurement, field)

                        if key not in grouped:
                            grouped[key] = []
                        grouped[key].append(float_value)

                    except (ValueError, TypeError):
                        continue

            for (measurement, field), values in grouped.items():
                history_name = self.HISTORY_MAPPING.get((measurement, field))
                if history_name:
                    self._history_cache[history_name] = values[-100:]

            if grouped:
                logger.debug(
                    f"📈 History updated: {len(grouped)} metrics, "
                    f"{sum(len(v) for v in grouped.values())} points total"
                )

        except Exception as e:
            logger.debug(f"Failed to query history: {type(e).__name__}: {e}")

    def _log_metrics_summary(self, metrics: Dict[str, Any]):
        """Логирует сводку метрик."""
        key_metrics = ["camera_temp", "image_hfr", "guider_rms_total", "wx_wind_speed"]
        summary_parts = []

        for key in key_metrics:
            if key in metrics and metrics[key] is not None:
                try:
                    summary_parts.append(f"{key}={float(metrics[key]):.2f}")
                except (ValueError, TypeError):
                    summary_parts.append(f"{key}={metrics[key]}")

        if summary_parts:
            query_mode = "unified" if self._use_unified_query else "separate"
            logger.info(
                f"📊 InfluxDB metrics [{query_mode}] "
                f"({self._last_metrics_count} total): "
                f"{', '.join(summary_parts)}"
            )

    def _log_error_throttled(self, message: str, level: str = "ERROR"):
        """Логирует ошибки с throttling."""
        now = datetime.now()
        self._consecutive_errors += 1

        should_log = (
            self._last_error_time is None
            or (now - self._last_error_time) >= self._error_log_interval
        )

        if should_log:
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

    def get_history(self, metric_name: str) -> List[float]:
        """Возвращает историю метрики из кэша."""
        return self._history_cache.get(f"{metric_name}_history", [])

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику provider."""
        return {
            "connected": self._client is not None,
            "running": self._running,
            "successful_queries": self._successful_queries,
            "consecutive_errors": self._consecutive_errors,
            "last_metrics_count": self._last_metrics_count,
            "history_cached": {
                name: len(values) for name, values in self._history_cache.items()
            },
            # ИСПРАВЛЕНО (P1): Статистика unified query
            "query_mode": "unified" if self._use_unified_query else "separate",
            "unified_query_failures": self._unified_query_failures,
            "unified_failure_threshold": self._unified_failure_threshold,
        }


# Singleton instance
influxdb_metrics_provider = InfluxDBMetricsProvider()
