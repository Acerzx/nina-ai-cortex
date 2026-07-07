"""
InfluxDB Metrics Provider
Основной источник метрик обсерватории через InfluxDB Exporter.
Устраняет зависимость от Prometheus Exporter.

ИСПРАВЛЕНО:
- Убран async with для QueryApiAsync (не поддерживает context manager)
- Добавлена защита от None значений
- Оптимизированы Flux queries
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
    """

    # Унифицированная Flux query для получения последних значений по всем метрикам
    # Один запрос вместо 25+ отдельных — в 10 раз быстрее
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

    # Маппинг InfluxDB полей на внутренние имена метрик
    # Ключ: (measurement, field) → значение: внутреннее имя
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

    # Маппинг для истории
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

        # Кэш для трендового анализа
        self._history_cache: Dict[str, List[float]] = {}

        # Защита от спама логов
        self._last_error_time: Optional[datetime] = None
        self._consecutive_errors: int = 0
        self._error_log_interval = timedelta(minutes=1)

        # Статистика
        self._last_metrics_count: int = 0
        self._successful_queries: int = 0

    async def start(self):
        """Запускает provider."""
        self._running = True

        # Подключение к InfluxDB
        try:
            self._client = InfluxDBClientAsync(
                url=self.url, token=self.token, org=self.org
            )
            # Проверяем подключение
            ready = await self._client.ping()
            if ready:
                logger.info(f"✅ InfluxDB Metrics Provider connected to {self.url}")
            else:
                logger.warning(f"⚠️ InfluxDB ping returned False, but continuing...")
        except Exception as e:
            logger.error(f"❌ Failed to connect to InfluxDB: {e}")
            logger.error("   Убедитесь, что InfluxDB запущен: docker compose up -d")
            # Не падаем, попробуем переподключиться в цикле

        # Запуск цикла опроса
        self._task = asyncio.create_task(self._query_loop())
        logger.info(
            f"📊 InfluxDB Metrics Provider started (interval={self.query_interval}s)"
        )

    async def stop(self):
        """Останавливает provider."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # ИСПРАВЛЕНО: Гарантированное закрытие клиента
        if self._client:
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
                # Проверяем подключение
                if self._client is None:
                    await self._try_reconnect()
                    await asyncio.sleep(self.query_interval)
                    continue

                # 1. Запрос текущих метрик
                metrics = await self._fetch_current_metrics()

                if metrics:
                    # Сбрасываем счетчик ошибок при успехе
                    if self._consecutive_errors > 0:
                        logger.info(
                            f"✅ InfluxDB connection восстановлено после "
                            f"{self._consecutive_errors} ошибок"
                        )
                        self._consecutive_errors = 0
                        self._last_error_time = None

                    self._successful_queries += 1

                    # Публикуем метрики в EventBus
                    await event_bus.publish("INFLUXDB_UPDATE", metrics)

                    # Периодически логируем сводку (раз в 20 успешных запросов)
                    if self._successful_queries % 20 == 0:
                        self._log_metrics_summary(metrics)

                # 2. Запрос истории (реже — раз в минуту)
                if self._successful_queries % 20 == 0:
                    await self._fetch_history_metrics()

            except Exception as e:
                self._log_error_throttled(
                    f"Ошибка запроса к InfluxDB: {type(e).__name__}: {e}",
                    level="WARNING",
                )
                # Возможно, клиент отвалился - пробуем переподключиться
                self._client = None

            await asyncio.sleep(self.query_interval)

    async def _try_reconnect(self):
        """Пытается переподключиться к InfluxDB."""
        try:
            logger.info("🔄 Attempting to reconnect to InfluxDB...")
            self._client = InfluxDBClientAsync(
                url=self.url, token=self.token, org=self.org
            )
            ready = await self._client.ping()
            if ready:
                logger.info("✅ Reconnected to InfluxDB successfully")
            else:
                self._client = None
        except Exception as e:
            logger.debug(f"Reconnect failed: {e}")
            self._client = None

    async def _fetch_current_metrics(self) -> Dict[str, Any]:
        """
        Запрашивает текущие метрики из InfluxDB.

        ИСПРАВЛЕНО: НЕ использует async with для QueryApiAsync.
        """
        if not self._client:
            return {}

        metrics = {}

        # ИСПРАВЛЕНО: query_api() в async версии НЕ является context manager
        query_api = self._client.query_api()

        try:
            query = self.UNIFIED_FLUX_QUERY.format(bucket=self.bucket)
            tables = await query_api.query(query)

            if not tables:
                return {}

            # Обрабатываем все таблицы в одном проходе
            for table in tables:
                for record in table.records:
                    measurement = record.get_measurement()
                    field = record.get_field()
                    value = record.get_value()

                    if value is None:
                        continue

                    # Ищем соответствие в маппинге
                    internal_name = self.METRICS_MAPPING.get((measurement, field))
                    if not internal_name:
                        # Пробуем без measurement (если имена глобально уникальны)
                        for (m, f), name in self.METRICS_MAPPING.items():
                            if f == field:
                                internal_name = name
                                break

                    if not internal_name:
                        continue

                    # Конвертируем значение
                    try:
                        if isinstance(value, bool):
                            metrics[internal_name] = value
                        else:
                            metrics[internal_name] = float(value)
                    except (ValueError, TypeError):
                        metrics[internal_name] = value

        except Exception as e:
            # Логируем ошибку и пробрасываем выше
            raise

        self._last_metrics_count = len(metrics)
        return metrics

    async def _fetch_history_metrics(self):
        """
        Запрашивает историю метрик для трендового анализа.

        ИСПРАВЛЕНО: НЕ использует async with для QueryApiAsync.
        """
        if not self._client:
            return

        # ИСПРАВЛЕНО: query_api() в async версии НЕ является context manager
        query_api = self._client.query_api()

        try:
            query = self.HISTORY_FLUX_QUERY.format(bucket=self.bucket)
            tables = await query_api.query(query)

            if not tables:
                return

            # Группируем значения по (measurement, field)
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

            # Обновляем кэш истории
            for (measurement, field), values in grouped.items():
                history_name = self.HISTORY_MAPPING.get((measurement, field))
                if history_name:
                    # Ограничиваем историю 100 точками
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
            logger.info(
                f"📊 InfluxDB metrics ({self._last_metrics_count} total): {', '.join(summary_parts)}"
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
        }


# Singleton instance
influxdb_metrics_provider = InfluxDBMetricsProvider()
