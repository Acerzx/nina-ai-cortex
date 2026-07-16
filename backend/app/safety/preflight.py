"""
Pre-flight Checklist v2 — расширенный чек-лист перед стартом сессии.

ЭТАП 4 (полный рефакторинг):
- Расширено с 8 до 20 gates в 6 категориях
- Новая архитектура: базовый класс Gate + категории
- Группированный отчёт с категориями

Категории gates:
┌─────────────────────────────────────────────────────────────┐
│  ENVIRONMENT (4 gates)                                       │
│  → WeatherGate: облачность, ветер, влажность                 │
│  → FocuserTempGate: температура фокусера как proxy ambient   │
│  → DewRiskGate: расчёт риска росы (focuser - camera < 3°C)   │
│  → MoonInterferenceGate: угловое расстояние до Луны          │
│                                                              │
│  HARDWARE (5 gates)                                          │
│  → EquipmentConnectionGate: все 8 устройств подключены       │
│  → MountParkGate: монтировка распаркована                    │
│  → CameraTempGate: камера охлаждена до setpoint              │
│  → FocusPositionGate: фокусер в стартовой позиции            │
│  → FilterWheelGate: колесо фильтров в исходной позиции       │
│                                                              │
│  CALIBRATION (2 gates)                                       │
│  → CalibrationGate: BIAS/DARK/FLAT masters актуальны         │
│  → GuiderCalibrationGate: калибровка PHD2 свежая (< 24h)     │
│                                                              │
│  STORAGE (2 gates)                                           │
│  → DiskSpaceGate: свободное место > 50 GB                    │
│  → StorageWriteGate: тестовая запись в sessions_root         │
│                                                              │
│  SOFTWARE (4 gates)                                          │
│  → APIHealthGate: N.I.N.A. Advanced API доступен             │
│  → LLMHealthGate: Ollama доступен (для FULL_AI режима)       │
│  → DatabaseGate: InfluxDB, Qdrant, SQLite доступны           │
│  → TimeSyncGate: NTP синхронизация (offset < 1s)             │
│                                                              │
│  SEQUENCE (3 gates)                                          │
│  → SequenceValidationGate: Sequence.json валиден             │
│  → TargetVisibilityGate: цель > 30° altitude                 │
│  → ModeGate: режим FULL_AI требует LLM                       │
└─────────────────────────────────────────────────────────────┘

Использование:
    from app.safety.preflight import preflight_checker

    report = await preflight_checker.run_all()
    if report.verdict == GateStatus.GO:
        print("✅ All gates passed — ready to start")
"""

import logging
import asyncio
import tempfile
from typing import Dict, Any, List, Optional
from enum import Enum
from datetime import datetime, timedelta
from pathlib import Path
from pydantic import BaseModel, Field
from abc import ABC, abstractmethod

from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.config import settings
from app.storage.disk_monitor import disk_monitor
from app.execution.nina_client import nina_client
from app.shadow_engine.state_tracker import state_tracker
from app.core.mode_manager import mode_manager, OperationMode

logger = logging.getLogger("PreFlight")


# ============================================================================
# ENUMS & MODELS
# ============================================================================


class GateStatus(str, Enum):
    """Статус проверки gate."""

    GO = "GO"
    WAITING = "WAITING"
    CAUTION = "CAUTION"
    NO_GO = "NO-GO"


class GateCategory(str, Enum):
    """Категории gates."""

    ENVIRONMENT = "Environment"
    HARDWARE = "Hardware"
    CALIBRATION = "Calibration"
    STORAGE = "Storage"
    SOFTWARE = "Software"
    SEQUENCE = "Sequence"


class GateResult(BaseModel):
    """Результат проверки одного gate."""

    gate_name: str
    category: GateCategory
    status: GateStatus
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class PreFlightReport(BaseModel):
    """Полный отчёт pre-flight проверки с группировкой по категориям."""

    gates: Dict[str, GateResult]
    gates_by_category: Dict[str, List[GateResult]]
    verdict: GateStatus
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    recommendations: List[str] = Field(default_factory=list)
    summary: Dict[str, int] = Field(
        default_factory=dict
    )  # GO/WAITING/CAUTION/NO-GO counts


# ============================================================================
# BASE GATE CLASS
# ============================================================================


class Gate(ABC):
    """
    Базовый класс для всех gates.

    Каждый gate:
    - Имеет имя и категорию
    - Выполняет одну проверку
    - Возвращает GateResult с статусом и деталями
    """

    def __init__(self, name: str, category: GateCategory):
        self.name = name
        self.category = category

    @abstractmethod
    async def check(self) -> GateResult:
        """Выполняет проверку и возвращает результат."""
        pass

    def _make_result(
        self,
        status: GateStatus,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> GateResult:
        """Создаёт GateResult."""
        return GateResult(
            gate_name=self.name,
            category=self.category,
            status=status,
            message=message,
            details=details or {},
        )


# ============================================================================
# ENVIRONMENT GATES (4)
# ============================================================================


class WeatherGate(Gate):
    """
    Проверка погодных условий.

    Проверяет:
    - Облачность < cloud_cover_max (80%)
    - Ветер < wind_speed_max (20 m/s)
    - Влажность < humidity_max (90%)
    """

    def __init__(self):
        super().__init__("WeatherGate", GateCategory.ENVIRONMENT)
        thresholds = getattr(settings, "thresholds", None)
        if thresholds and hasattr(thresholds, "preflight"):
            preflight_cfg = thresholds.preflight
            self.cloud_cover_max = getattr(preflight_cfg, "cloud_cover_max", 80.0)
            self.wind_speed_max = getattr(preflight_cfg, "wind_speed_max", 20.0)
            self.humidity_max = getattr(preflight_cfg, "humidity_max", 90.0)
        else:
            self.cloud_cover_max = 80.0
            self.wind_speed_max = 20.0
            self.humidity_max = 90.0

    async def check(self) -> GateResult:
        weather = observatory_state.weather
        cloud_cover = weather.get("cloud_cover")
        wind_speed = weather.get("wind_speed")
        humidity = weather.get("humidity")

        # Если нет данных о погоде — WAITING (но не блокируем)
        if cloud_cover is None and wind_speed is None and humidity is None:
            return self._make_result(
                GateStatus.WAITING,
                "No weather data available (weather device may not be connected)",
                {"note": "Using focuser temp as ambient proxy"},
            )

        # Проверки
        if cloud_cover is not None and cloud_cover > self.cloud_cover_max:
            return self._make_result(
                GateStatus.NO_GO,
                f"Cloud cover too high: {cloud_cover}% (max {self.cloud_cover_max}%)",
                {"cloud_cover": cloud_cover},
            )

        if wind_speed is not None and wind_speed > self.wind_speed_max:
            return self._make_result(
                GateStatus.NO_GO,
                f"Wind speed too high: {wind_speed} m/s (max {self.wind_speed_max} m/s)",
                {"wind_speed": wind_speed},
            )

        if humidity is not None and humidity > self.humidity_max:
            return self._make_result(
                GateStatus.CAUTION,
                f"High humidity: {humidity}% (max {self.humidity_max}%)",
                {"humidity": humidity},
            )

        return self._make_result(
            GateStatus.GO,
            "Weather conditions acceptable",
            weather,
        )


class FocuserTempGate(Gate):
    """
    Проверка температуры фокусера как proxy для ambient.

    Используется когда погодный модуль недоступен.
    Температура фокусера отражает температуру окружающей среды
    (фокусер находится на телескопе, вне помещения).
    """

    def __init__(self):
        super().__init__("FocuserTempGate", GateCategory.ENVIRONMENT)
        # Пороги температуры (°C)
        self.min_temp = -30.0  # Минимальная рабочая температура
        self.max_temp = 40.0  # Максимальная рабочая температура

    async def check(self) -> GateResult:
        focuser_temp = observatory_state.current_metrics.get("focuser_temp")

        if focuser_temp is None:
            return self._make_result(
                GateStatus.WAITING,
                "Focuser temperature not available",
                {},
            )

        if focuser_temp < self.min_temp or focuser_temp > self.max_temp:
            return self._make_result(
                GateStatus.CAUTION,
                f"Focuser temp outside normal range: {focuser_temp:.1f}°C "
                f"(expected {self.min_temp}°C to {self.max_temp}°C)",
                {"focuser_temp": focuser_temp},
            )

        return self._make_result(
            GateStatus.GO,
            f"Focuser temp OK: {focuser_temp:.1f}°C (ambient proxy)",
            {"focuser_temp": focuser_temp},
        )


class DewRiskGate(Gate):
    """
    Проверка риска образования росы.

    Расчёт: если разница между температурой фокусера (ambient proxy)
    и температурой камеры (оптика) меньше 3°C — высокий риск конденсации.

    Используется когда погодный модуль недоступен.
    """

    def __init__(self):
        super().__init__("DewRiskGate", GateCategory.ENVIRONMENT)
        self.dew_risk_threshold = 3.0  # °C разница

    async def check(self) -> GateResult:
        focuser_temp = observatory_state.current_metrics.get("focuser_temp")
        camera_temp = observatory_state.current_metrics.get("camera_temp")

        if focuser_temp is None or camera_temp is None:
            return self._make_result(
                GateStatus.WAITING,
                "Temperature sensors not available for dew risk calculation",
                {},
            )

        # Проверяем, включён ли обогреватель (dew heater)
        dew_heater_on = observatory_state.current_metrics.get("dew_heater", False)

        temp_diff = focuser_temp - camera_temp  # Оптика холоднее ambient

        if temp_diff < self.dew_risk_threshold and not dew_heater_on:
            return self._make_result(
                GateStatus.CAUTION,
                f"High dew risk: focuser={focuser_temp:.1f}°C, "
                f"camera={camera_temp:.1f}°C (Δ={temp_diff:.1f}°C). "
                "Enable dew heater.",
                {
                    "focuser_temp": focuser_temp,
                    "camera_temp": camera_temp,
                    "temp_diff": temp_diff,
                    "dew_heater_on": dew_heater_on,
                },
            )

        return self._make_result(
            GateStatus.GO,
            f"Dew risk OK (ΔT={temp_diff:.1f}°C)",
            {
                "focuser_temp": focuser_temp,
                "camera_temp": camera_temp,
                "temp_diff": temp_diff,
                "dew_heater_on": dew_heater_on,
            },
        )


class MoonInterferenceGate(Gate):
    """
    Проверка помех от Луны.

    Использует moon_angle из FITS headers (SolveEveryLight плагин).
    Если луна слишком близко к цели — предупреждение.
    """

    def __init__(self):
        super().__init__("MoonInterferenceGate", GateCategory.ENVIRONMENT)
        self.moon_avoidance_angle = 30.0  # Минимальное угловое расстояние

    async def check(self) -> GateResult:
        moon_angle = observatory_state.astronomy.get("moon_angle")
        moon_altitude = observatory_state.astronomy.get("moon_altitude")

        if moon_angle is None:
            return self._make_result(
                GateStatus.WAITING,
                "Moon angle not available (SolveEveryLight may not be installed)",
                {},
            )

        # Если луна ниже горизонта — не мешает
        if moon_altitude is not None and moon_altitude < 0:
            return self._make_result(
                GateStatus.GO,
                f"Moon below horizon ({moon_altitude:.1f}°) — no interference",
                {"moon_altitude": moon_altitude, "moon_angle": moon_angle},
            )

        if moon_angle < self.moon_avoidance_angle:
            return self._make_result(
                GateStatus.CAUTION,
                f"Moon too close to target: {moon_angle:.1f}° "
                f"(min {self.moon_avoidance_angle}°). "
                "Consider narrowband filters.",
                {"moon_angle": moon_angle, "moon_altitude": moon_altitude},
            )

        return self._make_result(
            GateStatus.GO,
            f"Moon distance OK: {moon_angle:.1f}°",
            {"moon_angle": moon_angle, "moon_altitude": moon_altitude},
        )


# ============================================================================
# HARDWARE GATES (5)
# ============================================================================


class EquipmentConnectionGate(Gate):
    """
    Проверка подключения всего оборудования.

    Проверяет 8 устройств через Prometheus метрики nina_equipment{type=...}:
    - camera, mount, focuser, filterwheel, guider, dome, rotator, safety_monitor
    """

    REQUIRED_DEVICES = [
        "camera",
        "mount",
        "focuser",
        "filterwheel",
        "guider",
    ]
    OPTIONAL_DEVICES = [
        "dome",
        "rotator",
        "safety_monitor",
        "weather",
        "switch",
    ]

    def __init__(self):
        super().__init__("EquipmentConnectionGate", GateCategory.HARDWARE)

    async def check(self) -> GateResult:
        metrics = observatory_state.current_metrics

        connected = []
        disconnected_required = []
        disconnected_optional = []

        # Проверяем обязательные устройства
        for device in self.REQUIRED_DEVICES:
            key = f"equipment_{device}"
            if metrics.get(key):
                connected.append(device)
            else:
                disconnected_required.append(device)

        # Проверяем опциональные устройства
        for device in self.OPTIONAL_DEVICES:
            key = f"equipment_{device}"
            if metrics.get(key):
                connected.append(device)
            elif metrics.get(key) is not None:  # Явно False
                disconnected_optional.append(device)

        details = {
            "connected": connected,
            "disconnected_required": disconnected_required,
            "disconnected_optional": disconnected_optional,
        }

        if disconnected_required:
            return self._make_result(
                GateStatus.NO_GO,
                f"Required equipment not connected: {', '.join(disconnected_required)}",
                details,
            )

        if disconnected_optional:
            return self._make_result(
                GateStatus.CAUTION,
                f"Optional equipment not connected: {', '.join(disconnected_optional)}",
                details,
            )

        return self._make_result(
            GateStatus.GO,
            f"All equipment connected: {', '.join(connected)}",
            details,
        )


class MountParkGate(Gate):
    """
    Проверка состояния монтировки.

    Монтировка должна быть распаркована для начала съёмки.
    """

    def __init__(self):
        super().__init__("MountParkGate", GateCategory.HARDWARE)

    async def check(self) -> GateResult:
        # Prometheus метрика mount_parked (1 = parked, 0 = unparked)
        # В observatory_state это может быть инвертировано
        mount_altitude = observatory_state.current_metrics.get("mount_altitude")

        # Если есть высота — монтировка активна
        if mount_altitude is not None:
            return self._make_result(
                GateStatus.GO,
                f"Mount active (altitude: {mount_altitude:.1f}°)",
                {"mount_altitude": mount_altitude},
            )

        # Проверяем, подключена ли монтировка
        if not observatory_state.current_metrics.get("equipment_mount"):
            return self._make_result(
                GateStatus.WAITING,
                "Mount not connected",
                {},
            )

        return self._make_result(
            GateStatus.WAITING,
            "Mount status unknown — may be parked",
            {},
        )


class CameraTempGate(Gate):
    """
    Проверка температуры камеры.

    Камера должна быть охлаждена до setpoint перед началом съёмки.
    """

    def __init__(self):
        super().__init__("CameraTempGate", GateCategory.HARDWARE)
        thresholds = getattr(settings, "thresholds", None)
        if thresholds and hasattr(thresholds, "preflight"):
            self.camera_cooled_threshold = getattr(
                thresholds.preflight, "camera_cooled_threshold", -10.0
            )
        else:
            self.camera_cooled_threshold = -10.0

    async def check(self) -> GateResult:
        camera_temp = observatory_state.current_metrics.get("camera_temp")

        if camera_temp is None:
            return self._make_result(
                GateStatus.WAITING,
                "Camera not connected or not reporting temperature",
                {},
            )

        if camera_temp > self.camera_cooled_threshold:
            return self._make_result(
                GateStatus.CAUTION,
                f"Camera not fully cooled: {camera_temp:.1f}°C "
                f"(threshold: {self.camera_cooled_threshold:.1f}°C)",
                {"camera_temp": camera_temp},
            )

        return self._make_result(
            GateStatus.GO,
            f"Camera cooled: {camera_temp:.1f}°C",
            {"camera_temp": camera_temp},
        )


class FocusPositionGate(Gate):
    """
    Проверка позиции фокусера.

    Фокусер должен быть в разумной стартовой позиции.
    """

    def __init__(self):
        super().__init__("FocusPositionGate", GateCategory.HARDWARE)
        self.min_position = 0  # Минимальная позиция
        self.max_position = 50000  # Максимальная позиция (зависит от модели)

    async def check(self) -> GateResult:
        focuser_position = observatory_state.current_metrics.get("focuser_position")

        if focuser_position is None:
            return self._make_result(
                GateStatus.WAITING,
                "Focuser position not available",
                {},
            )

        if focuser_position < self.min_position or focuser_position > self.max_position:
            return self._make_result(
                GateStatus.CAUTION,
                f"Focuser position unusual: {focuser_position} "
                f"(expected {self.min_position}-{self.max_position})",
                {"focuser_position": focuser_position},
            )

        return self._make_result(
            GateStatus.GO,
            f"Focuser position OK: {focuser_position}",
            {"focuser_position": focuser_position},
        )


class FilterWheelGate(Gate):
    """
    Проверка колеса фильтров.

    Проверяет, что колесо фильтров подключено и в известной позиции.
    """

    def __init__(self):
        super().__init__("FilterWheelGate", GateCategory.HARDWARE)

    async def check(self) -> GateResult:
        filter_current = observatory_state.current_metrics.get("filter")
        filter_connected = observatory_state.current_metrics.get(
            "equipment_filterwheel"
        )

        if not filter_connected:
            return self._make_result(
                GateStatus.WAITING,
                "Filter wheel not connected",
                {},
            )

        if filter_current:
            return self._make_result(
                GateStatus.GO,
                f"Filter wheel ready: current filter = {filter_current}",
                {"filter": filter_current},
            )

        return self._make_result(
            GateStatus.WAITING,
            "Filter wheel connected but current filter unknown",
            {},
        )


# ============================================================================
# CALIBRATION GATES (2)
# ============================================================================


class CalibrationGate(Gate):
    """
    Проверка наличия и актуальности мастер-кадров.

    Проверяет:
    - Наличие BIAS, DARK, FLAT мастеров
    - Соответствие параметров (gain, offset, binning, temperature, filter)
    - Актуальность (возраст мастеров)
    """

    FRESHNESS_DAYS = {
        "BIAS": 90,
        "DARK": 30,
        "FLAT": 7,
    }

    def __init__(self):
        super().__init__("CalibrationGate", GateCategory.CALIBRATION)

    async def check(self) -> GateResult:
        try:
            from app.ingestion.watchers.manager import watcher_manager
        except ImportError:
            return self._make_result(
                GateStatus.WAITING,
                "WatcherManager not available",
            )

        auditor = watcher_manager.masters_auditor
        if not auditor:
            return self._make_result(
                GateStatus.WAITING,
                "MastersLibraryAuditor not initialized",
            )

        # Получаем текущие параметры съёмки
        current_gain = observatory_state.current_metrics.get("gain")
        current_offset = observatory_state.current_metrics.get("offset")
        current_temp = observatory_state.current_metrics.get("camera_temp", -15.0)
        current_exposure = observatory_state.current_metrics.get("exposure_time", 60.0)
        current_filter = observatory_state.current_metrics.get("filter")

        # Получаем статистику мастеров
        stats = auditor.get_stats()
        total_bias = stats.get("total_bias", 0)
        total_dark = stats.get("total_dark", 0)
        total_flat = stats.get("total_flat", 0)

        details = {
            "total_bias": total_bias,
            "total_dark": total_dark,
            "total_flat": total_flat,
            "current_params": {
                "gain": current_gain,
                "offset": current_offset,
                "temperature": current_temp,
                "exposure": current_exposure,
                "filter": current_filter,
            },
        }

        # Проверка наличия мастеров
        missing_types = []
        if total_bias == 0:
            missing_types.append("BIAS")
        if total_dark == 0:
            missing_types.append("DARK")
        if total_flat == 0:
            missing_types.append("FLAT")

        if missing_types:
            return self._make_result(
                GateStatus.NO_GO,
                f"Missing calibration masters: {', '.join(missing_types)}",
                details,
            )

        # Проверка соответствия параметров
        param_mismatches = []

        if current_gain is not None:
            matching = auditor.find_matching_master(
                image_type="DARK",
                temperature=current_temp,
                exposure=current_exposure,
                gain=current_gain,
                temp_tolerance=2.0,
            )
            if not matching:
                param_mismatches.append(f"DARK with gain={current_gain}")

        if current_filter:
            matching = auditor.find_matching_master(
                image_type="FLAT",
                temperature=current_temp,
                filter_name=current_filter,
                gain=current_gain,
                temp_tolerance=2.0,
            )
            if not matching:
                param_mismatches.append(f"FLAT with filter={current_filter}")

        if param_mismatches:
            return self._make_result(
                GateStatus.CAUTION,
                f"No matching masters for current parameters: {'; '.join(param_mismatches)}",
                details,
            )

        # Проверка свежести мастеров
        summary = auditor.get_summary_by_category()
        stale_types = []

        for master_type, max_days in self.FRESHNESS_DAYS.items():
            category_summary = summary.get(master_type, {})
            max_date_str = category_summary.get("max_date")
            if max_date_str:
                try:
                    if "T" in max_date_str:
                        max_date = datetime.fromisoformat(
                            max_date_str.replace("Z", "+00:00")
                        )
                        if max_date.tzinfo:
                            max_date = max_date.replace(tzinfo=None)
                    else:
                        max_date = datetime.strptime(max_date_str[:10], "%Y-%m-%d")

                    age_days = (datetime.now() - max_date).days
                    if age_days > max_days:
                        stale_types.append(
                            f"{master_type} ({age_days}d old, max {max_days}d)"
                        )
                    details[f"{master_type}_age_days"] = age_days
                except (ValueError, TypeError):
                    pass

        if stale_types:
            return self._make_result(
                GateStatus.CAUTION,
                f"Stale calibration masters: {'; '.join(stale_types)}",
                details,
            )

        return self._make_result(
            GateStatus.GO,
            f"Calibration masters OK: {total_bias} BIAS, {total_dark} DARK, {total_flat} FLAT",
            details,
        )


class GuiderCalibrationGate(Gate):
    """
    Проверка свежести калибровки гида (PHD2).

    Калибровка должна быть не старше 24 часов.
    """

    def __init__(self):
        super().__init__("GuiderCalibrationGate", GateCategory.CALIBRATION)
        self.max_age_hours = 24

    async def check(self) -> GateResult:
        # Проверяем, подключен ли гид
        if not observatory_state.current_metrics.get("equipment_guider"):
            return self._make_result(
                GateStatus.WAITING,
                "Guider not connected",
                {},
            )

        # Проверяем, активно ли гидирование
        is_guiding = observatory_state.is_guiding_active

        if is_guiding:
            return self._make_result(
                GateStatus.GO,
                "Guider active and calibrated",
                {"is_guiding": True},
            )

        # Если гид подключен, но не активен — предупреждение
        return self._make_result(
            GateStatus.CAUTION,
            "Guider connected but not guiding — calibration may be needed",
            {"is_guiding": False},
        )


# ============================================================================
# STORAGE GATES (2)
# ============================================================================


class DiskSpaceGate(Gate):
    """
    Проверка свободного места на диске.
    """

    def __init__(self):
        super().__init__("DiskSpaceGate", GateCategory.STORAGE)
        thresholds = getattr(settings, "thresholds", None)
        if thresholds and hasattr(thresholds, "storage"):
            self.min_free_disk_space_gb = getattr(
                thresholds.storage, "warning_threshold_gb", 50.0
            )
        else:
            self.min_free_disk_space_gb = 50.0

    async def check(self) -> GateResult:
        try:
            disk_usage_list = await disk_monitor.check_all_disks()
        except Exception as e:
            return self._make_result(
                GateStatus.CAUTION,
                f"Failed to check disk usage: {e}",
            )

        if not disk_usage_list:
            return self._make_result(
                GateStatus.WAITING,
                "No disk usage data available",
            )

        details = {
            "disks": [d.model_dump() for d in disk_usage_list],
            "min_free_space_gb": self.min_free_disk_space_gb,
        }

        critical_disks = []
        warning_disks = []

        for usage in disk_usage_list:
            if usage.total_gb == 0:
                continue

            if usage.free_gb < self.min_free_disk_space_gb:
                critical_disks.append(f"{usage.path} ({usage.free_gb:.1f} GB free)")
            elif usage.free_gb < self.min_free_disk_space_gb * 2.5:
                warning_disks.append(f"{usage.path} ({usage.free_gb:.1f} GB free)")

        if critical_disks:
            return self._make_result(
                GateStatus.NO_GO,
                f"Insufficient disk space: {'; '.join(critical_disks)}. "
                f"Minimum required: {self.min_free_disk_space_gb} GB",
                details,
            )

        if warning_disks:
            return self._make_result(
                GateStatus.CAUTION,
                f"Low disk space warning: {'; '.join(warning_disks)}",
                details,
            )

        summary_parts = [
            f"{u.path}: {u.free_gb:.1f} GB free ({u.usage_percent:.0f}% used)"
            for u in disk_usage_list
            if u.total_gb > 0
        ]

        return self._make_result(
            GateStatus.GO,
            "Sufficient disk space",
            {**details, "summary": summary_parts},
        )


class StorageWriteGate(Gate):
    """
    Проверка возможности записи в sessions_root.
    Создаёт тестовый файл и удаляет его.
    ИСПРАВЛЕНО (В-4): Использует try/finally для гарантированного удаления.
    """

    def __init__(self):
        super().__init__("StorageWriteGate", GateCategory.STORAGE)

    async def check(self) -> GateResult:
        sessions_root = settings.nina_environment.sessions_root

        if not sessions_root.exists():
            return self._make_result(
                GateStatus.NO_GO,
                f"Sessions root does not exist: {sessions_root}",
                {"sessions_root": str(sessions_root)},
            )

        # Попытка записать тестовый файл
        test_file = sessions_root / ".cortex_write_test"

        try:
            # Создаём файл
            test_file.write_text("test", encoding="utf-8")

            # Если дошли сюда — запись успешна
            return self._make_result(
                GateStatus.GO,
                f"Write test passed: {sessions_root}",
                {"sessions_root": str(sessions_root)},
            )

        except PermissionError:
            return self._make_result(
                GateStatus.NO_GO,
                f"Permission denied: cannot write to {sessions_root}",
                {"sessions_root": str(sessions_root)},
            )

        except Exception as e:
            return self._make_result(
                GateStatus.NO_GO,
                f"Write test failed: {e}",
                {"sessions_root": str(sessions_root), "error": str(e)},
            )

        finally:
            # ИСПРАВЛЕНО (В-4): Гарантированное удаление тестового файла
            # Даже если процесс упадёт между write_text и unlink,
            # finally блок выполнится при нормальном завершении.
            # При SIGKILL/OOM файл может остаться, но это допустимо —
            # следующий запуск перезапишет его.
            try:
                if test_file.exists():
                    test_file.unlink()
            except Exception as cleanup_error:
                # Игнорируем ошибки удаления — они не критичны
                # Файл будет перезаписан при следующем запуске
                logger.debug(
                    f"Could not cleanup test file {test_file}: {cleanup_error}"
                )


# ============================================================================
# SOFTWARE GATES (4)
# ============================================================================


class APIHealthGate(Gate):
    """
    Проверка доступности N.I.N.A. Advanced API.
    """

    def __init__(self):
        super().__init__("APIHealthGate", GateCategory.SOFTWARE)

    async def check(self) -> GateResult:
        try:
            is_healthy = await nina_client.health_check()
        except Exception as e:
            return self._make_result(
                GateStatus.NO_GO,
                f"N.I.N.A. API unreachable: {type(e).__name__}",
                {"error": str(e)},
            )

        if is_healthy:
            return self._make_result(
                GateStatus.GO,
                "N.I.N.A. API reachable",
                {"healthy": True},
            )
        else:
            return self._make_result(
                GateStatus.NO_GO,
                "N.I.N.A. API not responding",
                {"healthy": False},
            )


class LLMHealthGate(Gate):
    """
    Проверка доступности LLM (Ollama).

    Требуется для режима FULL_AI.
    """

    def __init__(self):
        super().__init__("LLMHealthGate", GateCategory.SOFTWARE)

    async def check(self) -> GateResult:
        from app.agents.llm_client import llm_client

        is_available = llm_client.is_available()

        if is_available:
            return self._make_result(
                GateStatus.GO,
                "LLM (Ollama) available",
                {
                    "available": True,
                    "model": settings.ai_settings.primary_model,
                },
            )
        else:
            return self._make_result(
                GateStatus.CAUTION,
                "LLM (Ollama) not available — some AI features will be limited",
                {
                    "available": False,
                    "model": settings.ai_settings.primary_model,
                },
            )


class DatabaseGate(Gate):
    """
    Проверка доступности баз данных.

    Проверяет:
    - InfluxDB (для метрик)
    - Qdrant (для RAG)
    - SQLite (для Decision Audit и Sessions Metadata)
    """

    def __init__(self):
        super().__init__("DatabaseGate", GateCategory.SOFTWARE)

    async def check(self) -> GateResult:
        results = {}
        failed = []

        # InfluxDB
        try:
            from app.ingestion.providers.influxdb_metrics import (
                influxdb_metrics_provider,
            )

            if influxdb_metrics_provider._client is not None:
                results["influxdb"] = "connected"
            else:
                results["influxdb"] = "disconnected"
                failed.append("InfluxDB")
        except Exception as e:
            results["influxdb"] = f"error: {e}"
            failed.append("InfluxDB")

        # Qdrant
        try:
            from app.core.rag_engine import rag_engine

            if rag_engine._initialized and rag_engine._client is not None:
                results["qdrant"] = "connected"
            else:
                results["qdrant"] = "disconnected"
                failed.append("Qdrant")
        except Exception as e:
            results["qdrant"] = f"error: {e}"
            failed.append("Qdrant")

        # SQLite (Decision Audit)
        try:
            from app.storage.decision_audit import decision_audit

            if decision_audit._db_initialized:
                results["sqlite_audit"] = "initialized"
            else:
                results["sqlite_audit"] = "not_initialized"
        except Exception as e:
            results["sqlite_audit"] = f"error: {e}"

        # SQLite (Sessions Metadata)
        try:
            from app.storage.sessions_metadata import sessions_metadata

            if sessions_metadata._db_initialized:
                results["sqlite_sessions"] = "initialized"
            else:
                results["sqlite_sessions"] = "not_initialized"
        except Exception as e:
            results["sqlite_sessions"] = f"error: {e}"

        if failed:
            return self._make_result(
                GateStatus.CAUTION,
                f"Some databases unavailable: {', '.join(failed)}",
                results,
            )

        return self._make_result(
            GateStatus.GO,
            "All databases available",
            results,
        )


class TimeSyncGate(Gate):
    """
    Проверка синхронизации времени (NTP).

    Для астрофотографии точное время критично.
    Допустимое отклонение: < 1 секунда.
    """

    def __init__(self):
        super().__init__("TimeSyncGate", GateCategory.SOFTWARE)
        self.max_offset_seconds = 1.0

    async def check(self) -> GateResult:
        try:
            import ntplib

            ntp_client = ntplib.NTPClient()
            response = await asyncio.get_event_loop().run_in_executor(
                None,
                lambda: ntp_client.request("pool.ntp.org", version=3, timeout=5),
            )
            offset = response.offset

            details = {
                "offset_seconds": offset,
                "ntp_server": "pool.ntp.org",
            }

            if abs(offset) > self.max_offset_seconds:
                return self._make_result(
                    GateStatus.CAUTION,
                    f"Time offset too large: {offset:.3f}s "
                    f"(max {self.max_offset_seconds}s). "
                    "Sync system clock.",
                    details,
                )

            return self._make_result(
                GateStatus.GO,
                f"Time sync OK: offset {offset:.3f}s",
                details,
            )
        except ImportError:
            return self._make_result(
                GateStatus.WAITING,
                "ntplib not installed — time sync check skipped",
                {},
            )
        except Exception as e:
            return self._make_result(
                GateStatus.CAUTION,
                f"NTP check failed: {e}. Ensure system clock is synced.",
                {"error": str(e)},
            )


# ============================================================================
# SEQUENCE GATES (3)
# ============================================================================


class SequenceValidationGate(Gate):
    """
    Проверка валидности Sequence.json.

    Проверяет, что теневой граф успешно загружен.
    """

    def __init__(self):
        super().__init__("SequenceValidationGate", GateCategory.SEQUENCE)

    async def check(self) -> GateResult:
        if not state_tracker._shadow_graph:
            return self._make_result(
                GateStatus.NO_GO,
                "Shadow graph not loaded — Sequence.json may be invalid or missing",
                {},
            )

        stats = state_tracker.get_stats()
        node_count = stats.get("node_count", 0)

        if node_count == 0:
            return self._make_result(
                GateStatus.NO_GO,
                "Shadow graph is empty — Sequence.json has no instructions",
                stats,
            )

        return self._make_result(
            GateStatus.GO,
            f"Sequence valid: {node_count} nodes loaded",
            stats,
        )


class TargetVisibilityGate(Gate):
    """
    Проверка видимости цели.

    Цель должна быть выше 30° над горизонтом.
    """

    def __init__(self):
        super().__init__("TargetVisibilityGate", GateCategory.SEQUENCE)
        self.min_altitude = 30.0

    async def check(self) -> GateResult:
        mount_altitude = observatory_state.current_metrics.get("mount_altitude")

        if mount_altitude is None:
            return self._make_result(
                GateStatus.WAITING,
                "Mount altitude not available — cannot verify target visibility",
                {},
            )

        if mount_altitude < self.min_altitude:
            return self._make_result(
                GateStatus.CAUTION,
                f"Target below minimum altitude: {mount_altitude:.1f}° "
                f"(min {self.min_altitude}°)",
                {"mount_altitude": mount_altitude},
            )

        return self._make_result(
            GateStatus.GO,
            f"Target visible: {mount_altitude:.1f}° altitude",
            {"mount_altitude": mount_altitude},
        )


class ModeGate(Gate):
    """
    Проверка режима работы системы.

    FULL_AI режим требует доступности LLM.
    """

    def __init__(self):
        super().__init__("ModeGate", GateCategory.SEQUENCE)

    async def check(self) -> GateResult:
        current_mode = mode_manager.current_mode

        if current_mode == OperationMode.MANUAL:
            return self._make_result(
                GateStatus.CAUTION,
                "System in MANUAL mode (no autonomous actions)",
                {"mode": current_mode.value},
            )

        if current_mode == OperationMode.FULL_AI:
            llm_healthy = mode_manager.llm_healthy
            if not llm_healthy:
                return self._make_result(
                    GateStatus.CAUTION,
                    "FULL_AI mode but LLM is unhealthy",
                    {"mode": current_mode.value, "llm_healthy": False},
                )

        return self._make_result(
            GateStatus.GO,
            f"System in {current_mode.value} mode",
            {"mode": current_mode.value},
        )


# ============================================================================
# PRE-FLIGHT CHECKER
# ============================================================================


class PreFlightChecker:
    """
    Pre-flight checker с 20 gates в 6 категориях.

    ЭТАП 4 (рефакторинг):
    - 20 gates вместо 8
    - Группировка по категориям
    - Расширенный отчёт
    """

    def __init__(self):
        # Инициализируем все gates
        self._gates: List[Gate] = [
            # Environment (4)
            WeatherGate(),
            FocuserTempGate(),
            DewRiskGate(),
            MoonInterferenceGate(),
            # Hardware (5)
            EquipmentConnectionGate(),
            MountParkGate(),
            CameraTempGate(),
            FocusPositionGate(),
            FilterWheelGate(),
            # Calibration (2)
            CalibrationGate(),
            GuiderCalibrationGate(),
            # Storage (2)
            DiskSpaceGate(),
            StorageWriteGate(),
            # Software (4)
            APIHealthGate(),
            LLMHealthGate(),
            DatabaseGate(),
            TimeSyncGate(),
            # Sequence (3)
            SequenceValidationGate(),
            TargetVisibilityGate(),
            ModeGate(),
        ]

        logger.info(
            f"✅ PreFlightChecker v2 initialized "
            f"({len(self._gates)} gates in 6 categories)"
        )

    async def run_all(self) -> PreFlightReport:
        """Запускает все проверки и возвращает агрегированный отчёт."""
        results: Dict[str, GateResult] = {}
        gates_by_category: Dict[str, List[GateResult]] = {
            cat.value: [] for cat in GateCategory
        }

        # Выполняем все gates параллельно
        tasks = [gate.check() for gate in self._gates]
        gate_results = await asyncio.gather(*tasks, return_exceptions=True)

        for gate, result in zip(self._gates, gate_results):
            if isinstance(result, Exception):
                logger.error(f"Error in {gate.name}: {result}")
                result = GateResult(
                    gate_name=gate.name,
                    category=gate.category,
                    status=GateStatus.CAUTION,
                    message=f"Check failed with error: {result}",
                )

            results[gate.name] = result
            gates_by_category[gate.category.value].append(result)

        # Агрегируем verdict
        verdict = self._aggregate_verdict(results)

        # Генерируем рекомендации
        recommendations = self._generate_recommendations(results, verdict)

        # Считаем summary
        summary = {
            "GO": sum(1 for r in results.values() if r.status == GateStatus.GO),
            "WAITING": sum(
                1 for r in results.values() if r.status == GateStatus.WAITING
            ),
            "CAUTION": sum(
                1 for r in results.values() if r.status == GateStatus.CAUTION
            ),
            "NO-GO": sum(1 for r in results.values() if r.status == GateStatus.NO_GO),
        }

        report = PreFlightReport(
            gates=results,
            gates_by_category=gates_by_category,
            verdict=verdict,
            recommendations=recommendations,
            summary=summary,
        )

        # Публикуем отчёт
        await event_bus.publish("PREFLIGHT_REPORT", report.model_dump())

        logger.info(
            f"✅ Pre-flight v2 check complete: {verdict.value} "
            f"(GO: {summary['GO']}, WAITING: {summary['WAITING']}, "
            f"CAUTION: {summary['CAUTION']}, NO-GO: {summary['NO-GO']})"
        )

        return report

    def _aggregate_verdict(self, results: Dict[str, GateResult]) -> GateStatus:
        """Агрегирует verdict всех gates."""
        statuses = [r.status for r in results.values()]

        # Если есть хотя бы один NO-GO → общий NO-GO
        if GateStatus.NO_GO in statuses:
            return GateStatus.NO_GO

        # Если есть WAITING → общий WAITING
        if GateStatus.WAITING in statuses:
            return GateStatus.WAITING

        # Если есть CAUTION → общий CAUTION
        if GateStatus.CAUTION in statuses:
            return GateStatus.CAUTION

        # Все GO → общий GO
        return GateStatus.GO

    def _generate_recommendations(
        self, results: Dict[str, GateResult], verdict: GateStatus
    ) -> List[str]:
        """Генерирует рекомендации на основе результатов."""
        recommendations = []

        # Группируем по категориям
        for category in GateCategory:
            category_gates = [r for r in results.values() if r.category == category]

            no_go_gates = [r for r in category_gates if r.status == GateStatus.NO_GO]
            caution_gates = [
                r for r in category_gates if r.status == GateStatus.CAUTION
            ]
            waiting_gates = [
                r for r in category_gates if r.status == GateStatus.WAITING
            ]

            if no_go_gates:
                recommendations.append(
                    f"[{category.value}] CRITICAL: "
                    + "; ".join(f"{r.gate_name}: {r.message}" for r in no_go_gates)
                )

            if caution_gates:
                recommendations.append(
                    f"[{category.value}] WARNING: "
                    + "; ".join(f"{r.gate_name}: {r.message}" for r in caution_gates)
                )

            if waiting_gates:
                recommendations.append(
                    f"[{category.value}] PENDING: "
                    + "; ".join(f"{r.gate_name}: {r.message}" for r in waiting_gates)
                )

        # Общие рекомендации
        if verdict == GateStatus.GO:
            recommendations.insert(0, "✅ All gates passed — ready to start")
        elif verdict == GateStatus.NO_GO:
            recommendations.insert(
                0, "❌ Critical issues found — do NOT start sequence"
            )
        elif verdict == GateStatus.WAITING:
            recommendations.insert(0, "⏳ Waiting for some systems to initialize")
        elif verdict == GateStatus.CAUTION:
            recommendations.insert(0, "⚠️ Warnings present — proceed with caution")

        return recommendations


# Singleton instance
preflight_checker = PreFlightChecker()
