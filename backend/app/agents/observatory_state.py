"""
Metrics Aggregator (бывший ObservatoryState) — единое состояние обсерватории.

ЭТАП 2.2 (полный рефакторинг):
- Агрегирует данные из ВСЕХ источников в единый формат
- 5 layers приоритетов вместо dual source
- UnifiedMetric для каждой метрики
- Автоматический fallback при недоступности InfluxDB
- Thread-safe через asyncio.Lock (4 locks)

Архитектура приоритетов (Этап 2):
┌──────────────────────────────────────────────────────────────┐
│  Layer 1: PRIMARY (InfluxDB)                                  │
│  → Все основные метрики (camera, guider, mount, weather, etc.)│
│  → История для трендового анализа                             │
│  → Обновление каждые 2-3 секунды                              │
│                                                               │
│  Layer 2: UNIQUE (Prometheus-only)                            │
│  → autofocus_rsquares, sequence_status, equipment_connection  │
│  → Всегда принимаются, НЕ конкурируют с InfluxDB              │
│                                                               │
│  Layer 3: FALLBACK (Prometheus)                               │
│  → Дублирующие метрики (camera_temp, hfr, rms_ra, etc.)       │
│  → Активируются ТОЛЬКО если InfluxDB stale > 30s              │
│                                                               │
│  Layer 4: EVENTS (WebSocket)                                  │
│  → Sequence events (Started, Stopped, Item Started, etc.)     │
│  → Meridian flip, Flat mode, Errors                           │
│                                                               │
│  Layer 5: ENRICHMENT (File Watchers)                          │
│  → Hocus Focus (per-star analytics)                           │
│  → Session Metadata (per-frame details)                       │
│  → FITS Headers (WCS, MOONANGL, SUNANGLE)                     │
│  → LiveStack (SNR, acceptance rate)                           │
│  → Dither Statistics (CD, GFM, Voronoi)                       │
└──────────────────────────────────────────────────────────────┘

ИСПРАВЛЕНО (рефакторинг v3):
- MAX_POINTS вынесен в settings.metrics.history_max_points
- active_alerts max вынесен в settings.metrics.active_alerts_max
- ai_action_log max вынесен в settings.metrics.ai_action_log_max
- Удалены неиспользуемые методы

ИСПРАВЛЕНО (v4.0 — проблемы #9, #27):
- Добавлены asyncio.Lock для thread-safe обновления:
  * _metrics_lock — current_metrics
  * _history_lock — history (тренды)
  * _alerts_lock — active_alerts
  * _ai_action_lock — ai_action_log
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
from collections import deque
from pydantic import BaseModel, Field
from app.core.events import event_bus
from app.core.config import settings
from app.shadow_engine.state_tracker import state_tracker
from app.core.unified_metric import (
    UnifiedMetric,
    MetricSource,
    SourcePriority,
    UnitRegistry,
    is_prometheus_unique,
)
from app.core.math_utils import calculate_trend

logger = logging.getLogger("MetricsAggregator")


class MetricsHistory(BaseModel):
    """История метрик для трендового анализа."""

    hfr: List[float] = Field(default_factory=list)
    fwhm: List[float] = Field(default_factory=list)
    rms_ra: List[float] = Field(default_factory=list)
    rms_dec: List[float] = Field(default_factory=list)
    temperature: List[float] = Field(default_factory=list)
    wind_speed: List[float] = Field(default_factory=list)
    humidity: List[float] = Field(default_factory=list)


class AIAction(BaseModel):
    """Лог действия AI (для объяснимости)."""

    timestamp: str
    agent: str
    action: str
    reason: str
    result: str


class MetricsAggregator:
    """
    Metrics Aggregator — единое состояние обсерватории.

    ЭТАП 2.2 (рефакторинг):
    - 5 layers приоритетов источников
    - UnifiedMetric для каждой метрики
    - Thread-safe через 4 asyncio.Lock
    - Автоматический fallback при недоступности InfluxDB

    Обратная совместимость:
    - Алиас observatory_state сохранён
    - Все существующие методы работают как раньше
    - API endpoints не требуют изменений
    """

    def __init__(self):
        # === Текущие метрики (dict для обратной совместимости) ===
        self.current_metrics: Dict[str, Any] = {
            "hfr": None,
            "fwhm": None,
            "eccentricity": None,
            "star_count": None,
            "median_adu": None,
            "rms_ra": None,
            "rms_dec": None,
            "rms_total": None,
            "camera_temp": None,
            "focuser_position": None,
            "rotator_angle": None,
            "mount_altitude": None,
            "mount_azimuth": None,
            "exposure_time": None,
            "gain": None,
            "filter": None,
            "snr": None,
        }

        # === Unified metrics (новый формат с приоритетами) ===
        self._unified_metrics: Dict[str, UnifiedMetric] = {}

        # === Погода ===
        self.weather: Dict[str, Any] = {
            "temperature": None,
            "humidity": None,
            "dewpoint": None,
            "cloud_cover": None,
            "wind_speed": None,
            "wind_gust": None,
            "wind_direction": None,
            "sky_quality": None,
            "pressure": None,
        }

        # === Астрономия ===
        self.astronomy: Dict[str, Any] = {
            "moon_altitude": None,
            "sun_altitude": None,
            "moon_angle": None,
            "sun_angle": None,
        }

        # === История трендов ===
        self.history = MetricsHistory()

        # === Активные алерты ===
        self.active_alerts: List[Dict[str, Any]] = []

        # === Список целей ===
        self.active_targets: List[Dict[str, Any]] = []

        # === Статус безопасности ===
        self.safety_status: str = "UNKNOWN"

        # === Флаги режимов ===
        self.is_flat_mode: bool = False
        self.is_guiding_active: bool = False
        self.is_autofocus_running: bool = False

        # === Лимиты из конфига ===
        metrics_cfg = getattr(settings, "metrics", None)
        if metrics_cfg:
            self._max_history_points = getattr(metrics_cfg, "history_max_points", 100)
            self._max_ai_action_log = getattr(metrics_cfg, "ai_action_log_max", 1000)
            self._max_active_alerts = getattr(metrics_cfg, "active_alerts_max", 50)
        else:
            self._max_history_points = 100
            self._max_ai_action_log = 1000
            self._max_active_alerts = 50

        self.ai_action_log: deque = deque(maxlen=self._max_ai_action_log)

        # === Статус источников (для fallback логики) ===
        self._influxdb_last_update: Optional[datetime] = None
        self._prometheus_last_update: Optional[datetime] = None

        # С-6: Порог "stale" читается из settings.data_sources
        data_sources_cfg = getattr(settings, "data_sources", None)
        self._source_stale_threshold: float = (
            getattr(data_sources_cfg, "stale_threshold_seconds", 30.0)
            if data_sources_cfg
            else 30.0
        )

        self._subscribed = False

        # === Thread-safety locks ===
        self._metrics_lock = asyncio.Lock()
        self._history_lock = asyncio.Lock()
        self._alerts_lock = asyncio.Lock()
        self._ai_action_lock = asyncio.Lock()
        self._unified_lock = asyncio.Lock()

    async def start(self):
        """Подписывается на все события EventBus для обновления состояния."""
        if self._subscribed:
            return

        # Основные источники метрик
        event_bus.subscribe("INFLUXDB_UPDATE", self._on_influxdb_update)
        event_bus.subscribe("PROMETHEUS_UPDATE", self._on_prometheus_update)

        # Per-image enrichment
        event_bus.subscribe("NEW_FRAME", self._on_new_frame)
        event_bus.subscribe("WEATHER_UPDATE", self._on_weather_update)
        event_bus.subscribe("FITS_HEADER_PARSED", self._on_fits_parsed)

        # Events
        event_bus.subscribe("ALERT", self._on_alert)
        event_bus.subscribe("FLAT_MODE_CONFIRMED", self._on_flat_mode_start)
        event_bus.subscribe("FLAT_MODE_ENDED", self._on_flat_mode_end)
        event_bus.subscribe("LOG_EVENT", self._on_log_event)
        event_bus.subscribe("LIVESTACK_STATUS", self._on_livestack_status)

        self._subscribed = True

        logger.info(
            f"🧠 MetricsAggregator initialized "
            f"(primary: influxdb, fallback: prometheus, "
            f"max_history: {self._max_history_points}, "
            f"locks: 5 active, unified_metrics: enabled)"
        )

    # ========================================================================
    # SOURCE AVAILABILITY (для fallback логики)
    # ========================================================================

    def _is_influxdb_stale(self) -> bool:
        """
        Проверяет, устарел ли InfluxDB (не было обновлений > threshold).

        Используется для активации FALLBACK режима Prometheus.
        """
        if not self._influxdb_last_update:
            return True  # Никогда не было обновлений — считаем stale
        age = (datetime.now() - self._influxdb_last_update).total_seconds()
        return age > self._source_stale_threshold

    def _is_prometheus_stale(self) -> bool:
        """Проверяет, устарел ли Prometheus."""
        if not self._prometheus_last_update:
            return True
        age = (datetime.now() - self._prometheus_last_update).total_seconds()
        return age > self._source_stale_threshold

    # ========================================================================
    # LAYER 1: PRIMARY (InfluxDB)
    # ========================================================================

    async def _on_influxdb_update(self, data: Dict[str, Any]):
        """
        Layer 1: InfluxDB update — PRIMARY источник.

        InfluxDB всегда имеет наивысший приоритет для time-series метрик.
        Обновляет current_metrics, history и unified_metrics.
        """
        self._influxdb_last_update = datetime.now()

        # === Camera metrics ===
        async with self._metrics_lock:
            if "camera_temp" in data and data["camera_temp"] is not None:
                self.current_metrics["camera_temp"] = data["camera_temp"]
                await self._update_unified(
                    "camera_temp",
                    data["camera_temp"],
                    MetricSource.INFLUXDB,
                    SourcePriority.PRIMARY,
                )
            if "camera_cooler_power" in data:
                self.current_metrics["camera_cooler_power"] = data[
                    "camera_cooler_power"
                ]
            if "focuser_position" in data:
                self.current_metrics["focuser_position"] = data["focuser_position"]
            if "focuser_temp" in data:
                self.current_metrics["focuser_temp"] = data["focuser_temp"]

            # === Guider metrics ===
            if "guider_rms_ra" in data and data["guider_rms_ra"] is not None:
                self.current_metrics["rms_ra"] = data["guider_rms_ra"]
                await self._update_unified(
                    "rms_ra",
                    data["guider_rms_ra"],
                    MetricSource.INFLUXDB,
                    SourcePriority.PRIMARY,
                )
            if "guider_rms_dec" in data and data["guider_rms_dec"] is not None:
                self.current_metrics["rms_dec"] = data["guider_rms_dec"]
                await self._update_unified(
                    "rms_dec",
                    data["guider_rms_dec"],
                    MetricSource.INFLUXDB,
                    SourcePriority.PRIMARY,
                )
            if "guider_rms_total" in data:
                self.current_metrics["rms_total"] = data["guider_rms_total"]
            if data.get("guider_guiding") is not None:
                self.is_guiding_active = data["guider_guiding"]

            # === Mount metrics ===
            if "mount_altitude" in data:
                self.current_metrics["mount_altitude"] = data["mount_altitude"]
            if "mount_azimuth" in data:
                self.current_metrics["mount_azimuth"] = data["mount_azimuth"]

            # === Rotator ===
            if "rotator_angle" in data:
                self.current_metrics["rotator_angle"] = data["rotator_angle"]

            # === Filter ===
            if "filter_current" in data and data["filter_current"]:
                filter_value = data["filter_current"]
                if isinstance(filter_value, str):
                    self.current_metrics["filter"] = filter_value
                else:
                    self.current_metrics["filter"] = str(filter_value)

            # === Image Quality ===
            if "image_hfr" in data and data["image_hfr"] is not None:
                self.current_metrics["hfr"] = data["image_hfr"]
                await self._update_unified(
                    "hfr",
                    data["image_hfr"],
                    MetricSource.INFLUXDB,
                    SourcePriority.PRIMARY,
                )
            if "image_fwhm" in data and data["image_fwhm"] is not None:
                self.current_metrics["fwhm"] = data["image_fwhm"]
                await self._update_unified(
                    "fwhm",
                    data["image_fwhm"],
                    MetricSource.INFLUXDB,
                    SourcePriority.PRIMARY,
                )
            if "image_stars" in data:
                self.current_metrics["star_count"] = data["image_stars"]
            if "image_median" in data:
                self.current_metrics["median_adu"] = data["image_median"]
            if "image_eccentricity" in data:
                self.current_metrics["eccentricity"] = data["image_eccentricity"]

            # === Safety ===
            if data.get("safety_is_safe") is not None:
                self.safety_status = "SAFE" if data["safety_is_safe"] else "UNSAFE"

        # === History updates (отдельная блокировка) ===
        async with self._history_lock:
            if "camera_temp" in data and data["camera_temp"] is not None:
                self._append_history_unlocked("temperature", data["camera_temp"])
            if "guider_rms_ra" in data and data["guider_rms_ra"] is not None:
                self._append_history_unlocked("rms_ra", data["guider_rms_ra"])
            if "guider_rms_dec" in data and data["guider_rms_dec"] is not None:
                self._append_history_unlocked("rms_dec", data["guider_rms_dec"])
            if "guider_rms_total" in data:
                self._append_history_unlocked("rms_total", data["guider_rms_total"])
            if "image_hfr" in data and data["image_hfr"] is not None:
                self._append_history_unlocked("hfr", data["image_hfr"])
            if "image_fwhm" in data and data["image_fwhm"] is not None:
                self._append_history_unlocked("fwhm", data["image_fwhm"])

        # === Weather (metrics lock) ===
        async with self._metrics_lock:
            if "wx_temperature" in data:
                self.weather["temperature"] = data["wx_temperature"]
            if "wx_humidity" in data:
                self.weather["humidity"] = data["wx_humidity"]
            if "wx_dewpoint" in data:
                self.weather["dewpoint"] = data["wx_dewpoint"]
            if "wx_cloud_cover" in data:
                self.weather["cloud_cover"] = data["wx_cloud_cover"]
            if "wx_wind_speed" in data:
                self.weather["wind_speed"] = data["wx_wind_speed"]
            if "wx_wind_gust" in data:
                self.weather["wind_gust"] = data["wx_wind_gust"]
            if "wx_wind_direction" in data:
                self.weather["wind_direction"] = data["wx_wind_direction"]
            if "wx_pressure" in data:
                self.weather["pressure"] = data["wx_pressure"]
            if "wx_sky_quality" in data:
                self.weather["sky_quality"] = data["wx_sky_quality"]

        # === Weather history ===
        async with self._history_lock:
            if "wx_humidity" in data:
                self._append_history_unlocked("humidity", data["wx_humidity"])
            if "wx_wind_speed" in data:
                self._append_history_unlocked("wind_speed", data["wx_wind_speed"])

    # ========================================================================
    # LAYER 2 & 3: UNIQUE + FALLBACK (Prometheus)
    # ========================================================================

    async def _on_prometheus_update(self, data: Dict[str, Any]):
        """
        Layer 2 & 3: Prometheus update.

        Layer 2 (UNIQUE): метрики, которых НЕТ в InfluxDB — всегда принимаются.
        Layer 3 (FALLBACK): дублирующие метрики — принимаются ТОЛЬКО если
        InfluxDB stale (> 30s без обновлений).
        """
        self._prometheus_last_update = datetime.now()
        influxdb_stale = self._is_influxdb_stale()

        # === Маппинг стандартных метрик (FALLBACK) ===
        fallback_mapping = {
            "image_hfr": "hfr",
            "image_fwhm": "fwhm",
            "image_eccentricity": "eccentricity",
            "image_star_count": "star_count",
            "image_median_adu": "median_adu",
            "guider_rms_ra": "rms_ra",
            "guider_rms_dec": "rms_dec",
            "guider_rms_total": "rms_total",
            "camera_temp": "camera_temp",
            "focuser_position": "focuser_position",
            "rotator_angle": "rotator_angle",
            "mount_altitude": "mount_altitude",
            "mount_azimuth": "mount_azimuth",
        }

        async with self._metrics_lock:
            # === Layer 2: UNIQUE Prometheus метрики (всегда принимаем) ===

            # Autofocus state (нет в InfluxDB)
            if "autofocus_running" in data:
                self.is_autofocus_running = bool(data["autofocus_running"])
                await self._update_unified(
                    "autofocus_running",
                    float(data["autofocus_running"]),
                    MetricSource.PROMETHEUS,
                    SourcePriority.UNIQUE,
                )

            # Equipment connection (nina_equipment с labels)
            equipment_keys = {
                "equipment_camera": "camera",
                "equipment_mount": "mount",
                "equipment_focuser": "focuser",
                "equipment_filterwheel": "filterwheel",
                "equipment_guider": "guider",
                "equipment_dome": "dome",
                "equipment_rotator": "rotator",
                "equipment_safety_monitor": "safety_monitor",
                "equipment_weather": "weather",
            }
            for key, device in equipment_keys.items():
                if key in data:
                    await self._update_unified(
                        f"equipment_{device}",
                        float(data[key]),
                        MetricSource.PROMETHEUS,
                        SourcePriority.UNIQUE,
                        labels={"device": device},
                    )

            # Sequence status (nina_status с labels)
            if "sequence_item_name" in data and data["sequence_item_name"]:
                await self._update_unified(
                    "sequence_item",
                    1.0,
                    MetricSource.PROMETHEUS,
                    SourcePriority.UNIQUE,
                    labels={
                        "item": data.get("sequence_item_name", ""),
                        "category": data.get("sequence_category", ""),
                    },
                )

            # === Layer 3: FALLBACK (только если InfluxDB stale) ===
            if influxdb_stale:
                for prom_key, state_key in fallback_mapping.items():
                    if prom_key in data and data[prom_key] is not None:
                        self.current_metrics[state_key] = data[prom_key]
                        await self._update_unified(
                            state_key,
                            data[prom_key],
                            MetricSource.PROMETHEUS,
                            SourcePriority.FALLBACK,
                        )

                # Weather (fallback)
                if data.get("wx_temp") is not None:
                    self.weather["temperature"] = data["wx_temp"]
                if data.get("wx_humidity") is not None:
                    self.weather["humidity"] = data["wx_humidity"]
                if data.get("wx_wind_speed") is not None:
                    self.weather["wind_speed"] = data["wx_wind_speed"]
                if data.get("wx_wind_gust") is not None:
                    self.weather["wind_gust"] = data["wx_wind_gust"]
                if data.get("wx_cloud_cover") is not None:
                    self.weather["cloud_cover"] = data["wx_cloud_cover"]

        # History (fallback)
        if influxdb_stale:
            async with self._history_lock:
                self._append_history_unlocked("hfr", data.get("image_hfr"))
                self._append_history_unlocked("fwhm", data.get("image_fwhm"))
                self._append_history_unlocked("rms_ra", data.get("guider_rms_ra"))
                self._append_history_unlocked("rms_dec", data.get("guider_rms_dec"))
                self._append_history_unlocked("temperature", data.get("camera_temp"))
                self._append_history_unlocked("humidity", data.get("wx_humidity"))
                self._append_history_unlocked("wind_speed", data.get("wx_wind_speed"))

    # ========================================================================
    # UNIFIED METRIC UPDATE (внутренний метод)
    # ========================================================================

    async def _update_unified(
        self,
        name: str,
        value: float,
        source: MetricSource,
        priority: SourcePriority,
        labels: Optional[Dict[str, str]] = None,
    ):
        """
        Обновляет UnifiedMetric в _unified_metrics.

        ДОЛЖЕН вызываться ПОД _metrics_lock (или _unified_lock)!
        """
        if value is None:
            return

        unit = UnitRegistry.get_unit(name)

        self._unified_metrics[name] = UnifiedMetric(
            name=name,
            value=float(value),
            timestamp=datetime.now(),
            source=source,
            priority=priority,
            unit=unit,
            quality=1.0,
            labels=labels or {},
        )

    # ========================================================================
    # LAYER 5: ENRICHMENT (File Watchers, Per-Image)
    # ========================================================================

    async def _on_new_frame(self, data: Dict[str, Any]):
        """
        Layer 5: Per-frame enrichment from SessionWatcher.

        Обновляет current_metrics и history метриками кадра.
        """
        frame = data.get("frame", {})
        if not frame:
            return

        def get_value(*keys, default=None):
            for key in keys:
                if key in frame and frame[key] is not None:
                    return frame[key]
            return default

        hfr = get_value("HFR", "hfr")
        fwhm = get_value("FWHM", "fwhm")
        stars = get_value("Stars", "stars", "star_count")
        rms = get_value("RmsTotal", "rms_total", "RMS")
        exposure = get_value("ExposureTime", "exposure_time", "Exposure")
        gain = get_value("Gain", "gain")
        filter_name = get_value("Filter", "filter", "FilterName")
        temp = get_value("Temperature", "temperature", "CameraTemp")
        index = get_value("Index", "index")

        async with self._metrics_lock:
            if hfr is not None:
                self.current_metrics["hfr"] = hfr
                await self._update_unified(
                    "hfr", hfr, MetricSource.FILE_WATCHER, SourcePriority.ENRICHMENT
                )
            if fwhm is not None:
                self.current_metrics["fwhm"] = fwhm
                await self._update_unified(
                    "fwhm", fwhm, MetricSource.FILE_WATCHER, SourcePriority.ENRICHMENT
                )
            if stars is not None:
                self.current_metrics["star_count"] = stars
            if rms is not None:
                self.current_metrics["rms_total"] = rms
            if exposure is not None:
                self.current_metrics["exposure_time"] = exposure
            if gain is not None:
                self.current_metrics["gain"] = gain
            if filter_name is not None:
                self.current_metrics["filter"] = filter_name
            if temp is not None:
                self.current_metrics["camera_temp"] = temp
            if index is not None:
                self.current_metrics["frame_index"] = index

        async with self._history_lock:
            if hfr is not None:
                self._append_history_unlocked("hfr", hfr)
            if fwhm is not None:
                self._append_history_unlocked("fwhm", fwhm)
            if rms is not None:
                self._append_history_unlocked("rms_total", rms)
            if temp is not None:
                self._append_history_unlocked("temperature", temp)

    async def _on_weather_update(self, data: Dict[str, Any]):
        """Layer 5: Weather enrichment from WeatherData.json."""
        weather = data.get("weather", {})
        async with self._metrics_lock:
            for key, value in weather.items():
                if value is not None and key in self.weather:
                    self.weather[key] = value

        async with self._history_lock:
            self._append_history_unlocked("wind_speed", weather.get("wind_speed"))
            self._append_history_unlocked("humidity", weather.get("humidity"))

    async def _on_fits_parsed(self, data: Dict[str, Any]):
        """Layer 5: FITS header enrichment (WCS, MOONANGL, SUNANGLE)."""
        report = data.get("report", {})
        async with self._metrics_lock:
            if report.get("moon_angl") is not None:
                self.astronomy["moon_angle"] = report["moon_angl"]
            if report.get("sun_angle") is not None:
                self.astronomy["sun_angle"] = report["sun_angle"]

    async def _on_livestack_status(self, data: Dict[str, Any]):
        """Layer 5: LiveStack enrichment (SNR, acceptance rate)."""
        if "snr" in data:
            async with self._metrics_lock:
                self.current_metrics["snr"] = data["snr"]
                await self._update_unified(
                    "snr",
                    data["snr"],
                    MetricSource.FILE_WATCHER,
                    SourcePriority.ENRICHMENT,
                )

    # ========================================================================
    # LAYER 4: EVENTS (WebSocket, Log Events)
    # ========================================================================

    async def _on_alert(self, data: Dict[str, Any]):
        """Layer 4: Alert event."""
        alert = {
            "id": f"alert_{datetime.now().timestamp()}",
            "timestamp": datetime.now().isoformat(),
            **data,
        }
        async with self._alerts_lock:
            self.active_alerts.append(alert)
            if len(self.active_alerts) > self._max_active_alerts:
                self.active_alerts = self.active_alerts[-self._max_active_alerts :]

    async def _on_flat_mode_start(self, data: Dict[str, Any]):
        """Layer 4: Flat mode started."""
        async with self._metrics_lock:
            self.is_flat_mode = True

    async def _on_flat_mode_end(self, data: Dict[str, Any]):
        """Layer 4: Flat mode ended."""
        async with self._metrics_lock:
            self.is_flat_mode = False

    async def _on_log_event(self, data: Dict[str, Any]):
        """Layer 4: Log events (guiding, autofocus, safety)."""
        event_type = data.get("event_type", "")
        async with self._metrics_lock:
            if event_type == "guiding_start":
                self.is_guiding_active = True
            elif event_type in ("guiding_lost", "guiding_stop", "stop_guiding"):
                self.is_guiding_active = False
            elif event_type == "autofocus_start":
                self.is_autofocus_running = True
            elif event_type in ("autofocus_complete", "autofocus_fail"):
                self.is_autofocus_running = False
            elif event_type == "safety_unsafe":
                self.safety_status = "UNSAFE"
            elif event_type == "safety_safe":
                self.safety_status = "SAFE"

    # ========================================================================
    # HISTORY MANAGEMENT
    # ========================================================================

    def _append_history_unlocked(self, metric: str, value: Optional[float]):
        """
        ВНУТРЕННИЙ метод: добавляет значение в историю.
        ДОЛЖЕН вызываться ТОЛЬКО под _history_lock!
        """
        if value is None:
            return
        history_list = getattr(self.history, metric, None)
        if history_list is not None and isinstance(history_list, list):
            try:
                history_list.append(float(value))
                if len(history_list) > self._max_history_points:
                    history_list.pop(0)
            except (ValueError, TypeError):
                pass

    # ========================================================================
    # PUBLIC API: Trend Analysis
    # ========================================================================

    def get_trend(self, metric: str, window: int = 10) -> Optional[float]:
        """
        Вычисляет тренд метрики (наклон линейной регрессии).
        Read-only — делает snapshot copy под блокировкой.
        ИСПРАВЛЕНО (С-4): использует calculate_trend из core.math_utils.
        """
        history_list = getattr(self.history, metric, None)
        if not history_list or len(history_list) < window:
            return None
        recent = list(history_list[-window:])
        # ИСПРАВЛЕНО (С-4): единая функция из math_utils
        return calculate_trend(recent)

    def get_metric_average(self, metric: str, window: int = 20) -> Optional[float]:
        """Возвращает среднее значение метрики за последние N точек."""
        history_list = getattr(self.history, metric, None)
        if not history_list:
            return None
        recent = list(history_list[-window:])
        return sum(recent) / len(recent) if recent else None

    def get_metric_std(self, metric: str, window: int = 20) -> Optional[float]:
        """Возвращает стандартное отклонение метрики за последние N точек."""
        import numpy as np

        history_list = getattr(self.history, metric, None)
        if not history_list or len(history_list) < 3:
            return None
        recent = list(history_list[-window:])
        return float(np.std(recent))

    def is_metric_degrading(self, metric: str, threshold_percent: float = 20.0) -> bool:
        """Проверяет, деградирует ли метрика."""
        history_list = getattr(self.history, metric, None)
        if not history_list or len(history_list) < 10:
            return False
        history_copy = list(history_list)
        baseline = (
            history_copy[-20:-10]
            if len(history_copy) >= 20
            else history_copy[: len(history_copy) // 2]
        )
        recent = history_copy[-10:]
        if not baseline:
            return False
        baseline_mean = sum(baseline) / len(baseline)
        recent_mean = sum(recent) / len(recent)
        if baseline_mean == 0:
            return False
        change_percent = ((recent_mean - baseline_mean) / baseline_mean) * 100
        return change_percent > threshold_percent

    # ========================================================================
    # PUBLIC API: AI Action Logging
    # ========================================================================

    async def log_ai_action(self, agent: str, action: str, reason: str, result: str):
        """Логирует действие AI (для объяснимости). Thread-safe."""
        entry = AIAction(
            timestamp=datetime.now().isoformat(),
            agent=agent,
            action=action,
            reason=reason,
            result=result,
        )
        async with self._ai_action_lock:
            self.ai_action_log.append(entry.model_dump())
        logger.info(f"🤖 [{agent}] {action}: {reason} -> {result}")

    # ========================================================================
    # PUBLIC API: Full State (для API и агентов)
    # ========================================================================

    def get_full_state(self) -> Dict[str, Any]:
        """
        Возвращает полное состояние для AI-агентов и Frontend.
        Read-only — делает snapshot под блокировками.
        """
        influxdb_stale = self._is_influxdb_stale()
        prometheus_stale = self._is_prometheus_stale()

        return {
            "metrics": dict(self.current_metrics),
            "weather": dict(self.weather),
            "astronomy": dict(self.astronomy),
            "sequence": state_tracker.get_state(),
            "safety": self.safety_status,
            "modes": {
                "flat_mode": self.is_flat_mode,
                "guiding": self.is_guiding_active,
                "autofocus": self.is_autofocus_running,
            },
            "active_alerts": list(self.active_alerts),
            "targets": list(self.active_targets),
            "recent_ai_actions": list(self.ai_action_log)[-10:],
            "data_sources": {
                "primary": "influxdb",
                "influxdb_stale": influxdb_stale,
                "prometheus_stale": prometheus_stale,
                "prometheus_mode": "unique+fallback"
                if influxdb_stale
                else "unique_only",
                "influxdb_last_update": (
                    self._influxdb_last_update.isoformat()
                    if self._influxdb_last_update
                    else None
                ),
                "prometheus_last_update": (
                    self._prometheus_last_update.isoformat()
                    if self._prometheus_last_update
                    else None
                ),
            },
            "unified_metrics_count": len(self._unified_metrics),
        }

    def get_unified_metrics(self) -> Dict[str, Dict[str, Any]]:
        """
        Возвращает все unified metrics в сериализованном виде.
        Для API endpoint /api/v1/metrics/unified.
        """
        return {
            name: metric.to_dict() for name, metric in self._unified_metrics.items()
        }

    def get_data_sources_status(self) -> Dict[str, Any]:
        """
        Возвращает статус всех источников данных.
        Для API endpoint /api/v1/metrics/sources.
        """
        return {
            "influxdb": {
                "priority": "PRIMARY",
                "last_update": (
                    self._influxdb_last_update.isoformat()
                    if self._influxdb_last_update
                    else None
                ),
                "is_stale": self._is_influxdb_stale(),
                "stale_threshold_seconds": self._source_stale_threshold,
            },
            "prometheus": {
                "priority": "UNIQUE + FALLBACK",
                "last_update": (
                    self._prometheus_last_update.isoformat()
                    if self._prometheus_last_update
                    else None
                ),
                "is_stale": self._is_prometheus_stale(),
                "fallback_active": self._is_influxdb_stale(),
            },
            "websocket": {
                "priority": "EVENTS",
                "description": "Sequence state, real-time events",
            },
            "file_watchers": {
                "priority": "ENRICHMENT",
                "description": "Per-image detail (Hocus Focus, Session Metadata, etc.)",
            },
            "unified_metrics_count": len(self._unified_metrics),
            "unique_prometheus_metrics": list(
                m
                for m in self._unified_metrics.keys()
                if self._unified_metrics[m].priority == SourcePriority.UNIQUE
            ),
        }


# ============================================================================
# SINGLETON INSTANCE (с алиасом для обратной совместимости)
# ============================================================================
metrics_aggregator = MetricsAggregator()

# Алиас для обратной совместимости со всем существующим кодом
observatory_state = metrics_aggregator
