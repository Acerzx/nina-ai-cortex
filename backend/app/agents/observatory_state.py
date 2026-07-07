"""
ObservatoryState — единое состояние обсерватории.
Агрегирует данные из всех источников (EventBus, Prometheus, InfluxDB, Shadow Engine)
и предоставляет API для Multi-Agent Swarm (LangGraph).

Архитектура источников метрик:
- InfluxDB (основной): через INFLUXDB_UPDATE от InfluxDBMetricsProvider
- Prometheus (резервный): через PROMETHEUS_UPDATE от PrometheusScraper
- Session Metadata: через NEW_FRAME от SessionWatcher
- Weather: через WEATHER_UPDATE от WeatherData watcher
- FITS Headers: через FITS_HEADER_PARSED от FITSHeaderScanner
- Safety/Events: через LOG_EVENT от LogTailer
"""

import logging
import asyncio
from typing import Dict, Any, List, Optional, ClassVar
from datetime import datetime
from collections import deque
from pydantic import BaseModel, Field
from app.core.events import event_bus
from app.shadow_engine.state_tracker import state_tracker

logger = logging.getLogger("ObservatoryState")


class MetricsHistory(BaseModel):
    """История метрик для трендового анализа."""

    hfr: List[float] = Field(default_factory=list)
    fwhm: List[float] = Field(default_factory=list)
    rms_ra: List[float] = Field(default_factory=list)
    rms_dec: List[float] = Field(default_factory=list)
    temperature: List[float] = Field(default_factory=list)
    wind_speed: List[float] = Field(default_factory=list)
    humidity: List[float] = Field(default_factory=list)

    # ClassVar указывает Pydantic, что это константа класса
    MAX_POINTS: ClassVar[int] = 100


class AIAction(BaseModel):
    """Лог действия AI (для объяснимости)."""

    timestamp: str
    agent: str
    action: str
    reason: str
    result: str


class ObservatoryState:
    """
    Единое состояние обсерватории.
    Все AI-агенты обращаются к этому объекту для получения актуальных данных.
    """

    def __init__(self):
        # === Текущие метрики (обновляются из Prometheus/Session Metadata/InfluxDB) ===
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

        # === История действий AI (для объяснимости) ===
        self.ai_action_log: deque = deque(maxlen=1000)

        # === Список целей (из Dynamic Sequencer / Target Scheduler) ===
        self.active_targets: List[Dict[str, Any]] = []

        # === Статус безопасности ===
        self.safety_status: str = "UNKNOWN"  # SAFE, UNSAFE, UNKNOWN

        # === Флаги режимов ===
        self.is_flat_mode: bool = False
        self.is_guiding_active: bool = False
        self.is_autofocus_running: bool = False

        # === Источники данных ===
        self._influxdb_active: bool = False
        self._prometheus_active: bool = False

        self._subscribed = False

    async def start(self):
        """Подписывается на все события EventBus для обновления состояния."""
        if self._subscribed:
            return

        # === ОСНОВНОЙ ИСТОЧНИК: InfluxDB ===
        event_bus.subscribe("INFLUXDB_UPDATE", self._on_influxdb_update)

        # === РЕЗЕРВНЫЙ ИСТОЧНИК: Prometheus ===
        event_bus.subscribe("PROMETHEUS_UPDATE", self._on_prometheus_update)

        # Session Metadata (новые кадры)
        event_bus.subscribe("NEW_FRAME", self._on_new_frame)

        # Погода
        event_bus.subscribe("WEATHER_UPDATE", self._on_weather_update)

        # FITS Headers (астрономия)
        event_bus.subscribe("FITS_HEADER_PARSED", self._on_fits_parsed)

        # Алерты от Watcher агента
        event_bus.subscribe("ALERT", self._on_alert)

        # Режимы
        event_bus.subscribe("FLAT_MODE_CONFIRMED", self._on_flat_mode_start)
        event_bus.subscribe("FLAT_MODE_ENDED", self._on_flat_mode_end)

        # События гида и безопасности из логов
        event_bus.subscribe("LOG_EVENT", self._on_log_event)

        # LiveStack статус
        event_bus.subscribe("LIVESTACK_STATUS", self._on_livestack_status)

        self._subscribed = True
        logger.info("🧠 ObservatoryState initialized and subscribed to events")

    async def _on_influxdb_update(self, data: Dict[str, Any]):
        """
        Обновление метрик из InfluxDB (ОСНОВНОЙ ИСТОЧНИК).

        Данные приходят от InfluxDBMetricsProvider, который выполняет
        Flux queries к InfluxDB Exporter (daleghent plugin).
        """
        self._influxdb_active = True

        # === Camera ===
        if "camera_temp" in data and data["camera_temp"] is not None:
            self.current_metrics["camera_temp"] = data["camera_temp"]
            self._append_history("temperature", data["camera_temp"])

        if "camera_cooler_power" in data:
            self.current_metrics["camera_cooler_power"] = data["camera_cooler_power"]

        # === Focuser ===
        if "focuser_position" in data:
            self.current_metrics["focuser_position"] = data["focuser_position"]

        if "focuser_temp" in data:
            self.current_metrics["focuser_temp"] = data["focuser_temp"]

        # === Guider (PHD2) ===
        if "guider_rms_ra" in data and data["guider_rms_ra"] is not None:
            self.current_metrics["rms_ra"] = data["guider_rms_ra"]
            self._append_history("rms_ra", data["guider_rms_ra"])

        if "guider_rms_dec" in data and data["guider_rms_dec"] is not None:
            self.current_metrics["rms_dec"] = data["guider_rms_dec"]
            self._append_history("rms_dec", data["guider_rms_dec"])

        if "guider_rms_total" in data:
            self.current_metrics["rms_total"] = data["guider_rms_total"]

        if data.get("guider_guiding") is not None:
            self.is_guiding_active = data["guider_guiding"]

        # === Mount ===
        if "mount_altitude" in data:
            self.current_metrics["mount_altitude"] = data["mount_altitude"]

        if "mount_azimuth" in data:
            self.current_metrics["mount_azimuth"] = data["mount_azimuth"]

        # === Rotator ===
        if "rotator_angle" in data:
            self.current_metrics["rotator_angle"] = data["rotator_angle"]

        # === Filter ===
        if "filter_current" in data and data["filter_current"]:
            self.current_metrics["filter"] = data["filter_current"]

        # === Weather ===
        if "wx_temperature" in data:
            self.weather["temperature"] = data["wx_temperature"]

        if "wx_humidity" in data:
            self.weather["humidity"] = data["wx_humidity"]
            self._append_history("humidity", data["wx_humidity"])

        if "wx_dewpoint" in data:
            self.weather["dewpoint"] = data["wx_dewpoint"]

        if "wx_cloud_cover" in data:
            self.weather["cloud_cover"] = data["wx_cloud_cover"]

        if "wx_wind_speed" in data:
            self.weather["wind_speed"] = data["wx_wind_speed"]
            self._append_history("wind_speed", data["wx_wind_speed"])

        if "wx_wind_gust" in data:
            self.weather["wind_gust"] = data["wx_wind_gust"]

        if "wx_wind_direction" in data:
            self.weather["wind_direction"] = data["wx_wind_direction"]

        if "wx_pressure" in data:
            self.weather["pressure"] = data["wx_pressure"]

        if "wx_sky_quality" in data:
            self.weather["sky_quality"] = data["wx_sky_quality"]

        # === Image Quality (from ImageSaved event) ===
        if "image_hfr" in data and data["image_hfr"] is not None:
            self.current_metrics["hfr"] = data["image_hfr"]
            self._append_history("hfr", data["image_hfr"])

        if "image_fwhm" in data and data["image_fwhm"] is not None:
            self.current_metrics["fwhm"] = data["image_fwhm"]
            self._append_history("fwhm", data["image_fwhm"])

        if "image_stars" in data:
            self.current_metrics["star_count"] = data["image_stars"]

        if "image_median" in data:
            self.current_metrics["median_adu"] = data["image_median"]

        if "image_eccentricity" in data:
            self.current_metrics["eccentricity"] = data["image_eccentricity"]

        # === Safety ===
        if data.get("safety_is_safe") is not None:
            self.safety_status = "SAFE" if data["safety_is_safe"] else "UNSAFE"

    async def _on_prometheus_update(self, data: Dict[str, Any]):
        """
        Обновление метрик из Prometheus (РЕЗЕРВНЫЙ ИСТОЧНИК).

        Используется только если InfluxDB недоступен.
        Если InfluxDB активен, Prometheus данные игнорируются
        чтобы избежать дублирования и конфликтов.
        """
        # Если InfluxDB работает, игнорируем Prometheus
        if self._influxdb_active:
            self._prometheus_active = False
            return

        self._prometheus_active = True

        # Маппинг Prometheus метрик на внутренние имена
        mapping = {
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

        for prom_key, state_key in mapping.items():
            if prom_key in data and data[prom_key] is not None:
                self.current_metrics[state_key] = data[prom_key]

        # Обновление истории
        self._append_history("hfr", data.get("image_hfr"))
        self._append_history("fwhm", data.get("image_fwhm"))
        self._append_history("rms_ra", data.get("guider_rms_ra"))
        self._append_history("rms_dec", data.get("guider_rms_dec"))
        self._append_history("temperature", data.get("camera_temp"))

        # Погода из Prometheus (wx_* префикс)
        if data.get("wx_temp") is not None:
            self.weather["temperature"] = data["wx_temp"]
        if data.get("wx_humidity") is not None:
            self.weather["humidity"] = data["wx_humidity"]
            self._append_history("humidity", data["wx_humidity"])
        if data.get("wx_wind_speed") is not None:
            self.weather["wind_speed"] = data["wx_wind_speed"]
            self._append_history("wind_speed", data["wx_wind_speed"])
        if data.get("wx_wind_gust") is not None:
            self.weather["wind_gust"] = data["wx_wind_gust"]
        if data.get("wx_cloud_cover") is not None:
            self.weather["cloud_cover"] = data["wx_cloud_cover"]

    async def _on_new_frame(self, data: Dict[str, Any]):
        """Обработка нового кадра из Session Metadata."""
        frame = data.get("frame", {})

        if frame:
            if frame.get("hfr"):
                self._append_history("hfr", frame["hfr"])
                self.current_metrics["hfr"] = frame["hfr"]

            if frame.get("fwhm"):
                self._append_history("fwhm", frame["fwhm"])
                self.current_metrics["fwhm"] = frame["fwhm"]

            # Обновляем текущие метрики из кадра
            if frame.get("exposure_time"):
                self.current_metrics["exposure_time"] = frame["exposure_time"]
            if frame.get("gain"):
                self.current_metrics["gain"] = frame["gain"]
            if frame.get("filter"):
                self.current_metrics["filter"] = frame["filter"]
            if frame.get("stars"):
                self.current_metrics["star_count"] = frame["stars"]

    async def _on_weather_update(self, data: Dict[str, Any]):
        """Обновление погоды из WeatherData.json."""
        weather = data.get("weather", {})

        for key, value in weather.items():
            if value is not None and key in self.weather:
                self.weather[key] = value

        self._append_history("wind_speed", weather.get("wind_speed"))
        self._append_history("humidity", weather.get("humidity"))

    async def _on_fits_parsed(self, data: Dict[str, Any]):
        """Обновление астрономических данных из FITS."""
        report = data.get("report", {})

        if report.get("moon_angl") is not None:
            self.astronomy["moon_angle"] = report["moon_angl"]
        if report.get("sun_angle") is not None:
            self.astronomy["sun_angle"] = report["sun_angle"]

    async def _on_alert(self, data: Dict[str, Any]):
        """Добавление нового алерта."""
        alert = {
            "id": f"alert_{datetime.now().timestamp()}",
            "timestamp": datetime.now().isoformat(),
            **data,
        }

        self.active_alerts.append(alert)

        # Ограничиваем количество активных алертов
        if len(self.active_alerts) > 50:
            self.active_alerts = self.active_alerts[-50:]

    async def _on_flat_mode_start(self, data: Dict[str, Any]):
        """Начало FLAT_MODE."""
        self.is_flat_mode = True

    async def _on_flat_mode_end(self, data: Dict[str, Any]):
        """Окончание FLAT_MODE."""
        self.is_flat_mode = False

    async def _on_log_event(self, data: Dict[str, Any]):
        """Отслеживание состояний из логов."""
        event_type = data.get("event_type", "")

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

    async def _on_livestack_status(self, data: Dict[str, Any]):
        """Обновление статуса LiveStack."""
        if "snr" in data:
            self.current_metrics["snr"] = data["snr"]

    def _append_history(self, metric: str, value: Optional[float]):
        """Добавляет значение в историю метрики."""
        if value is None:
            return

        history_list = getattr(self.history, metric, None)
        if history_list is not None and isinstance(history_list, list):
            try:
                history_list.append(float(value))

                # Обрезаем историю до MAX_POINTS
                if len(history_list) > self.history.MAX_POINTS:
                    history_list.pop(0)
            except (ValueError, TypeError):
                pass

    def log_ai_action(self, agent: str, action: str, reason: str, result: str):
        """Логирует действие AI (для объяснимости)."""
        entry = AIAction(
            timestamp=datetime.now().isoformat(),
            agent=agent,
            action=action,
            reason=reason,
            result=result,
        )
        self.ai_action_log.append(entry.model_dump())
        logger.info(f"🤖 [{agent}] {action}: {reason} -> {result}")

    def get_trend(self, metric: str, window: int = 10) -> Optional[float]:
        """
        Вычисляет тренд метрики (наклон линейной регрессии).
        Положительный = рост, отрицательный = падение.
        """
        history_list = getattr(self.history, metric, None)

        if not history_list or len(history_list) < window:
            return None

        recent = history_list[-window:]
        n = len(recent)
        x_mean = (n - 1) / 2
        y_mean = sum(recent) / n

        numerator = sum((i - x_mean) * (recent[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return 0.0

        return numerator / denominator

    def get_metric_average(self, metric: str, window: int = 20) -> Optional[float]:
        """Возвращает среднее значение метрики за последние N точек."""
        history_list = getattr(self.history, metric, None)
        if not history_list:
            return None

        recent = history_list[-window:]
        return sum(recent) / len(recent) if recent else None

    def get_metric_std(self, metric: str, window: int = 20) -> Optional[float]:
        """Возвращает стандартное отклонение метрики за последние N точек."""
        import numpy as np

        history_list = getattr(self.history, metric, None)
        if not history_list or len(history_list) < 3:
            return None

        recent = history_list[-window:]
        return float(np.std(recent))

    def is_metric_degrading(self, metric: str, threshold_percent: float = 20.0) -> bool:
        """
        Проверяет, деградирует ли метрика (растет ли она сверх порога).
        Полезно для HFR, FWHM, RMS.
        """
        history_list = getattr(self.history, metric, None)
        if not history_list or len(history_list) < 10:
            return False

        baseline = (
            history_list[-20:-10]
            if len(history_list) >= 20
            else history_list[: len(history_list) // 2]
        )
        recent = history_list[-10:]

        if not baseline:
            return False

        baseline_mean = sum(baseline) / len(baseline)
        recent_mean = sum(recent) / len(recent)

        if baseline_mean == 0:
            return False

        change_percent = ((recent_mean - baseline_mean) / baseline_mean) * 100
        return change_percent > threshold_percent

    def clear_resolved_alerts(self):
        """Очищает алерты, которые были решены."""
        # Оставляем только CRITICAL для аудита
        self.active_alerts = [
            a for a in self.active_alerts if a.get("level") == "CRITICAL"
        ]

    def get_session_summary(self) -> Dict[str, Any]:
        """Возвращает краткую сводку текущей сессии для LLM."""
        return {
            "target": self.active_targets[0].get("name")
            if self.active_targets
            else "Unknown",
            "safety_status": self.safety_status,
            "is_guiding": self.is_guiding_active,
            "is_autofocus_running": self.is_autofocus_running,
            "current_hfr": self.current_metrics.get("hfr"),
            "current_rms_total": self.current_metrics.get("rms_total"),
            "wind_speed": self.weather.get("wind_speed"),
            "cloud_cover": self.weather.get("cloud_cover"),
            "active_alerts_count": len(self.active_alerts),
            "data_source": "influxdb"
            if self._influxdb_active
            else ("prometheus" if self._prometheus_active else "none"),
        }

    def get_full_state(self) -> Dict[str, Any]:
        """Возвращает полное состояние для AI-агентов и Frontend."""
        return {
            "metrics": self.current_metrics,
            "weather": self.weather,
            "astronomy": self.astronomy,
            "sequence": state_tracker.get_state(),
            "safety": self.safety_status,
            "modes": {
                "flat_mode": self.is_flat_mode,
                "guiding": self.is_guiding_active,
                "autofocus": self.is_autofocus_running,
            },
            "active_alerts": self.active_alerts,
            "targets": self.active_targets,
            "recent_ai_actions": list(self.ai_action_log)[-10:],
            "data_sources": {
                "influxdb_active": self._influxdb_active,
                "prometheus_active": self._prometheus_active,
            },
        }


# Singleton instance
observatory_state = ObservatoryState()
