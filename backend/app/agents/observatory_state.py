import logging
import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
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

    # Максимум 100 точек для каждой метрики
    MAX_POINTS = 100


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
    Устраняет Упрощение #17.

    Все AI-агенты обращаются к этому объекту для получения актуальных данных.
    """

    def __init__(self):
        # === Текущие метрики (обновляются из Prometheus/Session Metadata) ===
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

        # === Состояние секвенсора (из Shadow Engine) ===
        # Делегируем к state_tracker.state

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

        self._subscribed = False

    async def start(self):
        """Подписывается на все события EventBus для обновления состояния."""
        if self._subscribed:
            return

        # Prometheus метрики
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
        event_bus.subscribe("FLAT_MODE_START", self._on_flat_mode_start)
        event_bus.subscribe("FLAT_MODE_END", self._on_flat_mode_end)

        # События гида
        event_bus.subscribe("LOG_EVENT", self._on_log_event)

        self._subscribed = True
        logger.info("🧠 ObservatoryState initialized and subscribed to events")

    async def _on_prometheus_update(self, data: Dict[str, Any]):
        """Обновление метрик из Prometheus."""
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

    async def _on_new_frame(self, data: Dict[str, Any]):
        """Обработка нового кадра из Session Metadata."""
        frame = data.get("frame", {})
        if frame:
            if frame.get("hfr"):
                self._append_history("hfr", frame["hfr"])
            if frame.get("fwhm"):
                self._append_history("fwhm", frame["fwhm"])

    async def _on_weather_update(self, data: Dict[str, Any]):
        """Обновление погоды."""
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
        self.is_flat_mode = True

    async def _on_flat_mode_end(self, data: Dict[str, Any]):
        self.is_flat_mode = False

    async def _on_log_event(self, data: Dict[str, Any]):
        """Отслеживание состояний из логов."""
        event_type = data.get("event_type", "")

        if event_type == "guiding_start":
            self.is_guiding_active = True
        elif event_type == "guiding_lost" or event_type == "guiding_stop":
            self.is_guiding_active = False
        elif event_type == "autofocus_start":
            self.is_autofocus_running = True
        elif event_type in ("autofocus_complete", "autofocus_fail"):
            self.is_autofocus_running = False
        elif event_type == "safety_unsafe":
            self.safety_status = "UNSAFE"
        elif event_type == "safety_safe":
            self.safety_status = "SAFE"

    def _append_history(self, metric: str, value: Optional[float]):
        """Добавляет значение в историю метрики."""
        if value is None:
            return

        history_list = getattr(self.history, metric, None)
        if history_list is not None:
            history_list.append(value)
            if len(history_list) > self.history.MAX_POINTS:
                history_list.pop(0)

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

    def get_full_state(self) -> Dict[str, Any]:
        """Возвращает полное состояние для AI-агентов."""
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
        }


observatory_state = ObservatoryState()
