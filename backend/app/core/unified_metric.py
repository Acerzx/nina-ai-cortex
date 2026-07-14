"""
Unified Metric — единый формат метрик для Metrics Aggregator.

ЭТАП 2.1 (новая модель):
- Все метрики из разных источников приводятся к единому формату
- Чёткие приоритеты источников данных
- Единицы измерения валидируются через UnitRegistry
- Quality score отражает надёжность данных
- Stale detection для автоматического переключения источников

Архитектура приоритетов:
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: PRIMARY (InfluxDB)                                 │
│  → Time-series, история, основная масса метрик               │
│  → Обновление каждые 2-3 секунды                             │
│                                                              │
│  LAYER 2: UNIQUE (Prometheus-only)                           │
│  → Метрики, которых НЕТ в InfluxDB:                          │
│    * autofocus_rsquares (R² кривых AF)                       │
│    * sequence_status (nina_status с labels)                  │
│    * equipment_connection (nina_equipment с labels)          │
│  → Всегда принимаются, не конкурируют с InfluxDB             │
│                                                              │
│  LAYER 3: FALLBACK (Prometheus)                              │
│  → Дублирующие InfluxDB метрики                              │
│  → Используются ТОЛЬКО если InfluxDB недоступен > 30s        │
│                                                              │
│  LAYER 4: EVENTS (WebSocket N.I.N.A.)                        │
│  → Sequence state, real-time события                         │
│  → Обновление флагов: is_running, is_flat_mode, etc.         │
│                                                              │
│  LAYER 5: ENRICHMENT (File Watchers)                         │
│  → Per-image детализация: Hocus Focus, Session Metadata,     │
│    FITS headers, LiveStack, Dither Statistics                │
│  → Дополняют, но не заменяют основные метрики                │
└─────────────────────────────────────────────────────────────┘

Использование:
    from app.core.unified_metric import (
        UnifiedMetric, MetricSource, SourcePriority, UnitRegistry
    )

    metric = UnifiedMetric(
        name="hfr",
        value=2.31,
        source=MetricSource.INFLUXDB,
        priority=SourcePriority.PRIMARY,
    )
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum


class MetricSource(str, Enum):
    """Источник метрики."""

    INFLUXDB = "influxdb"
    PROMETHEUS = "prometheus"
    WEBSOCKET = "websocket"
    FILE_WATCHER = "file_watcher"
    MANUAL = "manual"


class SourcePriority(str, Enum):
    """
    Приоритет источника данных.

    Определяет, как метрика взаимодействует с другими источниками:
    - PRIMARY: основной источник, всегда перезаписывает
    - UNIQUE: уникальные метрики, не конкурируют с PRIMARY
    - FALLBACK: резервный источник, активен при недоступности PRIMARY
    - EVENTS: событийные данные, обновляют только флаги
    - ENRICHMENT: детализация, дополняет основные метрики
    """

    PRIMARY = "primary"
    UNIQUE = "unique"
    FALLBACK = "fallback"
    EVENTS = "events"
    ENRICHMENT = "enrichment"


@dataclass
class UnifiedMetric:
    """
    Единый формат метрики.

    Все метрики из всех источников приводятся к этой модели,
    что обеспечивает консистентность и упрощает работу агентов.

    Attributes:
        name: Имя метрики (например, "hfr", "rms_ra", "camera_temp")
        value: Числовое значение
        timestamp: Время измерения
        source: Источник данных
        priority: Приоритет источника
        unit: Единица измерения (pixels, arcsec, celsius, etc.)
        quality: Оценка надёжности данных (0.0 - 1.0)
        labels: Дополнительные метки (filter, camera_name, etc.)
    """

    name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.now)
    source: MetricSource = MetricSource.INFLUXDB
    priority: SourcePriority = SourcePriority.PRIMARY
    unit: str = ""
    quality: float = 1.0
    labels: Dict[str, str] = field(default_factory=dict)

    def is_stale(self, max_age_seconds: float = 30.0) -> bool:
        """
        Проверяет, устарела ли метрика.

        Используется для автоматического переключения на FALLBACK
        когда PRIMARY источник недоступен.

        Args:
            max_age_seconds: Максимальный возраст в секундах

        Returns:
            True если метрика устарела
        """
        age = (datetime.now() - self.timestamp).total_seconds()
        return age > max_age_seconds

    def age_seconds(self) -> float:
        """Возвращает возраст метрики в секундах."""
        return (datetime.now() - self.timestamp).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация для API."""
        return {
            "name": self.name,
            "value": self.value,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source.value,
            "priority": self.priority.value,
            "unit": self.unit,
            "quality": self.quality,
            "labels": self.labels,
            "age_seconds": round(self.age_seconds(), 2),
            "is_stale": self.is_stale(),
        }


class UnitRegistry:
    """
    Реестр единиц измерения для метрик.

    Используется для:
    - Валидации единиц измерения при создании UnifiedMetric
    - Документирования метрик
    - Конвертации единиц (future scope)
    """

    UNITS: Dict[str, Dict[str, str]] = {
        # === Изображение ===
        "hfr": {"unit": "pixels", "description": "Half Flux Radius"},
        "fwhm": {"unit": "pixels", "description": "Full Width Half Maximum"},
        "eccentricity": {"unit": "ratio", "description": "Star eccentricity (0-1)"},
        "star_count": {"unit": "count", "description": "Detected stars"},
        "median_adu": {"unit": "adu", "description": "Median pixel value"},
        "snr": {"unit": "ratio", "description": "Signal-to-Noise Ratio"},
        # === Гидирование ===
        "rms_ra": {"unit": "arcsec", "description": "RMS RA guiding error"},
        "rms_dec": {"unit": "arcsec", "description": "RMS Dec guiding error"},
        "rms_total": {"unit": "arcsec", "description": "Total RMS guiding error"},
        # === Оборудование ===
        "camera_temp": {"unit": "celsius", "description": "Sensor temperature"},
        "camera_cooler_power": {"unit": "percent", "description": "Cooler power level"},
        "focuser_position": {"unit": "steps", "description": "Focuser position"},
        "focuser_temp": {"unit": "celsius", "description": "Focuser temperature"},
        "rotator_angle": {"unit": "degrees", "description": "Rotator sky angle"},
        "mount_altitude": {"unit": "degrees", "description": "Mount altitude"},
        "mount_azimuth": {"unit": "degrees", "description": "Mount azimuth"},
        # === Погода ===
        "wx_temperature": {"unit": "celsius", "description": "Ambient temperature"},
        "wx_humidity": {"unit": "percent", "description": "Relative humidity"},
        "wx_dewpoint": {"unit": "celsius", "description": "Dewpoint temperature"},
        "wx_cloud_cover": {"unit": "percent", "description": "Cloud cover"},
        "wx_wind_speed": {"unit": "m/s", "description": "Wind speed"},
        "wx_wind_gust": {"unit": "m/s", "description": "Wind gust speed"},
        "wx_wind_direction": {"unit": "degrees", "description": "Wind direction"},
        "wx_pressure": {"unit": "hPa", "description": "Air pressure"},
        "wx_sky_quality": {"unit": "mag/arcsec²", "description": "Sky quality"},
        # === Экспозиция ===
        "exposure_time": {"unit": "seconds", "description": "Exposure time"},
        "gain": {"unit": "gain", "description": "Camera gain"},
        "filter": {"unit": "name", "description": "Current filter name"},
        # === Prometheus-unique ===
        "autofocus_rsquares": {"unit": "ratio", "description": "R² of AF curve fits"},
        "sequence_item": {"unit": "name", "description": "Current sequence item"},
        "equipment_status": {
            "unit": "bool",
            "description": "Equipment connection state",
        },
    }

    @classmethod
    def get_unit(cls, metric_name: str) -> str:
        """Возвращает единицу измерения для метрики."""
        info = cls.UNITS.get(metric_name)
        return info["unit"] if info else "unknown"

    @classmethod
    def get_description(cls, metric_name: str) -> str:
        """Возвращает описание метрики."""
        info = cls.UNITS.get(metric_name)
        return info["description"] if info else ""

    @classmethod
    def is_known(cls, metric_name: str) -> bool:
        """Проверяет, известна ли метрика в реестре."""
        return metric_name in cls.UNITS

    @classmethod
    def get_all_units(cls) -> Dict[str, Dict[str, str]]:
        """Возвращает все зарегистрированные единицы."""
        return dict(cls.UNITS)


# Константы приоритетов источников (для быстрой проверки)
PROMETHEUS_UNIQUE_METRICS = frozenset(
    {
        # Autofocus quality (нет в InfluxDB Exporter)
        "autofocus_rsquares",
        "autofocus_running",
        "autofocus_success_total",
        "autofocus_failure_total",
        # Sequence state machine (nina_status с labels)
        "sequence_item",
        "sequence_category",
        "sequence_started_total",
        "sequence_completed_total",
        # Equipment connection (nina_equipment с labels)
        "equipment_camera",
        "equipment_mount",
        "equipment_focuser",
        "equipment_filterwheel",
        "equipment_guider",
        "equipment_dome",
        "equipment_rotator",
        "equipment_flat_device",
        "equipment_safety_monitor",
        "equipment_weather",
        "equipment_switch",
    }
)


def is_prometheus_unique(metric_name: str) -> bool:
    """Проверяет, является ли метрика уникальной для Prometheus."""
    return metric_name in PROMETHEUS_UNIQUE_METRICS
