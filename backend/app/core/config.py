"""
Конфигурация N.I.N.A. AI Cortex
Загружает settings.yaml и переменные окружения.
Все пути параметризированы — система работает на любом ПК.

ИСПРАВЛЕНО (рефакторинг v3):
- Удалены: HomeAssistantConfig, PluginsStatus, AuthConfig, SecurityConfig
- Добавлены: OpenAPIConfig, StorageThresholds, MetricsConfig, SimulationConfig, ExecutionConfig
- Удален model_name из AISettings (deprecated)
- Упрощен CORSConfig для локального использования
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

logger = logging.getLogger("Config")


# ============================================================================
# БАЗОВЫЕ МОДЕЛИ КОНФИГУРАЦИИ
# ============================================================================


class NinaEnvironment(BaseModel):
    """Пути к файлам и папкам N.I.N.A."""

    appdata_root: Path
    sessions_root: Path
    masters_root: Path
    profiles_dir: Path
    sequence_template: Path
    logs_dir: Path
    plugins_dir: Path


class NetworkConfig(BaseModel):
    """Сетевые подключения."""

    nina_api_host: str
    nina_ws_url: str
    prometheus_url: str


class InfluxDBConfig(BaseModel):
    """Настройки InfluxDB 2.x."""

    url: str
    token: str
    org: str
    bucket: str


class QdrantConfig(BaseModel):
    """Настройки Qdrant для RAG-системы."""

    url: str = "http://localhost:6333"
    collection_name: str = "nina_sessions"
    embedding_model: str = "nomic-embed-text"


class WSBroadcastConfig(BaseModel):
    """Настройки WebSocket Broadcasting для Frontend."""

    enabled: bool = True
    path: str = "/ws"


class AISettings(BaseModel):
    """Настройки AI (Ollama, RAG)."""

    ollama_host: str
    primary_model: str = "gemma4:31b-cloud"
    fallback_model: str = "gemma4:e4b"
    rag_db_path: Path


class HALConfig(BaseModel):
    """Hardware Abstraction Layer — финальная валидация команд."""

    enabled: bool = True
    min_altitude_limit: float = 15.0


class WatchersConfig(BaseModel):
    """Настройки вотчеров."""

    debounce_seconds: float = 1.5
    ai_weather_status_file: Optional[str] = None
    dither_statistics_path: Optional[str] = None
    guiding_analyzer_path: Optional[str] = None
    dynamic_sequencer_path: Optional[str] = None


class LoggingConfig(BaseModel):
    """Настройки логирования."""

    level: str = "INFO"
    format: str = "json"


# ============================================================================
# УПРОЩЕННЫЕ МОДЕЛИ (локальное использование)
# ============================================================================


class CORSConfig(BaseModel):
    """
    Настройки CORS (Cross-Origin Resource Sharing).
    Упрощено для локального использования.
    """

    enabled: bool = True
    allowed_origins: List[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",
            "http://localhost:5173",
            "http://localhost:8080",
            "http://127.0.0.1:3000",
            "http://127.0.0.1:5173",
        ]
    )
    allow_credentials: bool = True
    allowed_methods: List[str] = Field(
        default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    allowed_headers: List[str] = Field(
        default_factory=lambda: ["Content-Type", "X-Request-ID"]
    )
    max_age: int = 3600


# ============================================================================
# НОВЫЕ МОДЕЛИ (OpenAPI, Storage, Metrics, Simulation, Execution)
# ============================================================================


class OpenAPIConfig(BaseModel):
    """
    Настройки OpenAPI спецификации N.I.N.A. Advanced API.
    Используется для динамической генерации триггеров и валидации.
    """

    spec_path: str = "config/nina_api_spec.json"
    auto_load: bool = True
    cache_enabled: bool = True


class StorageThresholds(BaseModel):
    """Пороговые значения для Disk Monitor."""

    warning_threshold_gb: float = 50.0
    critical_threshold_gb: float = 20.0
    retention_keep_last_days: int = 30
    retention_max_records: int = 100000


class MetricsConfig(BaseModel):
    """Настройки метрик и истории."""

    history_max_points: int = 100
    ai_action_log_max: int = 1000
    active_alerts_max: int = 50
    update_interval: float = 3.0


class SimulationConfig(BaseModel):
    """Настройки режима симуляции (FakeNina, FakePhd2)."""

    flush_every_frames: int = 10
    flush_every_seconds: float = 30.0
    frame_delay_seconds: float = 2.0


class ExecutionConfig(BaseModel):
    """
    Конфигурация Execution Layer.
    Содержит маппинги для trigger_emulator и agent_aliases.
    """

    agent_aliases: Dict[str, str] = Field(
        default_factory=lambda: {
            "autofocus": "autofocus",
            "dither": "guider_start",
            "guider_calibration": "guider_calibrate",
            "phd2_settle": "guider_start",
            "emergency_park": "mount_park",
        }
    )
    trigger_patterns: Optional[Dict[str, Dict[str, Any]]] = None


class DecisionAuditConfig(BaseModel):
    """Конфигурация политики хранения решений."""

    keep_last_days: int = 90
    max_records: int = 100000
    archive_before_delete: bool = True
    archive_path: str = "./data/decision_archives"
    auto_cleanup_enabled: bool = True
    auto_cleanup_interval_hours: int = 24


# ============================================================================
# МОДЕЛИ ДЛЯ ПОРОГОВЫХ ЗНАЧЕНИЙ
# ============================================================================


class WatcherThresholds(BaseModel):
    """Пороговые значения для Watcher Agent."""

    hfr_increase_percent: float = 30.0
    fwhm_increase_percent: float = 30.0
    rms_ra_critical: float = 2.0
    rms_dec_critical: float = 2.0
    temperature_deviation: float = 2.0
    wind_speed_warning: float = 15.0
    wind_gust_critical: float = 20.0
    z_score_threshold: float = 3.0
    min_history_points: int = 5
    anomaly_cooldown_seconds: int = 300


class CalibratorThresholds(BaseModel):
    """Пороговые значения для Calibrator Agent."""

    bias_freshness_days: int = 90
    dark_freshness_days: int = 30
    flat_freshness_days: int = 7
    temperature_tolerance: float = 2.0
    alert_cooldown_seconds: int = 600


class PreflightThresholds(BaseModel):
    """Пороговые значения для Preflight gates."""

    cloud_cover_max: float = 80.0
    wind_speed_max: float = 20.0
    humidity_max: float = 90.0
    min_free_disk_space_gb: float = 50.0
    camera_cooled_threshold: float = -10.0


class StrategistThresholds(BaseModel):
    """Пороговые значения для Strategist Agent."""

    snr_target: float = 20.0
    hfr_target: float = 2.5
    fwhm_target: float = 3.0
    acceptance_rate_target: float = 0.90
    autofocus_interval_normal: int = 60
    autofocus_interval_frequent: int = 30
    autofocus_interval_emergency: int = 15
    hfr_degradation_threshold: float = 0.05
    min_proposal_interval_seconds: int = 600
    wind_load_warning: float = 10.0


class GuardianThresholds(BaseModel):
    """Пороговые значения для Guardian Agent."""

    wind_speed_park: float = 20.0
    cloud_cover_pause: float = 80.0
    humidity_warning: float = 90.0
    rms_recalibration: float = 3.0
    temperature_alarm: float = 5.0


class ThresholdsConfig(BaseModel):
    """Конфигурация всех пороговых значений."""

    watcher: WatcherThresholds = Field(default_factory=WatcherThresholds)
    calibrator: CalibratorThresholds = Field(default_factory=CalibratorThresholds)
    preflight: PreflightThresholds = Field(default_factory=PreflightThresholds)
    strategist: StrategistThresholds = Field(default_factory=StrategistThresholds)
    guardian: GuardianThresholds = Field(default_factory=GuardianThresholds)
    storage: StorageThresholds = Field(default_factory=StorageThresholds)


class DataSourcesConfig(BaseModel):
    """Конфигурация источников данных."""

    primary_metrics_source: str = "influxdb"
    enable_fallback_source: bool = True
    metrics_poll_interval: float = 3.0


# ============================================================================
# КОРНЕВАЯ МОДЕЛЬ SETTINGS
# ============================================================================


class Settings(BaseSettings):
    """Корневая модель конфигурации."""

    # Оригинальные секции
    nina_environment: NinaEnvironment
    network: NetworkConfig
    influxdb: InfluxDBConfig
    qdrant: QdrantConfig = Field(default_factory=QdrantConfig)
    ws_broadcast: WSBroadcastConfig = Field(default_factory=WSBroadcastConfig)
    ai_settings: AISettings
    hal: HALConfig = Field(default_factory=HALConfig)
    watchers: WatchersConfig = Field(default_factory=WatchersConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    # Упрощенные секции
    cors: CORSConfig = Field(default_factory=CORSConfig)

    # Пороговые значения и источники
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)
    data_sources: DataSourcesConfig = Field(default_factory=DataSourcesConfig)

    # Новые секции (рефакторинг v3)
    openapi: OpenAPIConfig = Field(default_factory=OpenAPIConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    simulation: SimulationConfig = Field(default_factory=SimulationConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    decision_audit: DecisionAuditConfig = Field(default_factory=DecisionAuditConfig)

    @field_validator("influxdb", mode="before")
    def resolve_env_vars(cls, value):
        """Заменяет ${VAR_NAME} на значения из переменных окружения."""
        if isinstance(value, dict):
            token = value.get("token")
            if (
                isinstance(token, str)
                and token.startswith("${")
                and token.endswith("}")
            ):
                env_var = token[2:-1]
                value["token"] = os.getenv(env_var, "default_token")
        return value

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "allow"


# ============================================================================
# ФУНКЦИИ ЗАГРУЗКИ И ВАЛИДАЦИИ
# ============================================================================


def load_settings() -> Settings:
    """
    Загружает settings.yaml и накладывает переменные окружения.
    Возвращает валидированный Settings объект.
    ИСПРАВЛЕНО (audit 8.1): добавлена валидация критических путей.
    ИСПРАВЛЕНО: явная загрузка .env через python-dotenv ДО создания Settings,
    чтобы os.getenv() работал в field_validator.
    """
    config_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "config"
        / "settings.yaml"
    )

    if not config_path.exists():
        logger.error(f"❌ Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    # ИСПРАВЛЕНО: Загружаем .env в os.environ ДО создания Settings.
    # pydantic-settings читает .env только для полей модели,
    # но field_validator использует os.getenv(), который не знает о .env.
    # Ищем .env в backend/ директории
    backend_dir = Path(__file__).resolve().parent.parent.parent
    env_path = backend_dir / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
        logger.info(f"✅ Loaded environment from {env_path}")
    else:
        # Пробуем корень проекта
        root_env = backend_dir.parent / ".env"
        if root_env.exists():
            load_dotenv(root_env, override=False)
            logger.info(f"✅ Loaded environment from {root_env}")
        else:
            logger.warning(
                "⚠️ .env file not found. "
                f"Searched: {env_path}, {root_env}. "
                "Using OS environment variables only."
            )

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        settings_obj = Settings(**yaml_data)

        # ИСПРАВЛЕНО (audit 8.1): Валидация критических путей
        _validate_critical_paths(settings_obj)

        logger.info(f"✅ Configuration loaded from {config_path}")
        return settings_obj

    except Exception as e:
        logger.error(f"❌ Failed to load configuration: {e}")
        raise


def _validate_critical_paths(settings_obj: Settings) -> None:
    """Проверяет существование критических путей при старте."""
    env = settings_obj.nina_environment
    warnings = []

    # Критические пути (без них система не работает)
    critical_paths = {
        "appdata_root": env.appdata_root,
        "sessions_root": env.sessions_root,
    }

    for name, path in critical_paths.items():
        if not path.exists():
            warnings.append(f"⚠️ Critical path does not exist: {name} = {path}")

    # Опциональные пути (только warning)
    optional_paths = {
        "masters_root": env.masters_root,
        "profiles_dir": env.profiles_dir,
        "logs_dir": env.logs_dir,
        "plugins_dir": env.plugins_dir,
    }

    for name, path in optional_paths.items():
        if not path.exists():
            logger.debug(f"Optional path does not exist: {name} = {path}")

    for warning in warnings:
        logger.warning(warning)

    # В production требуем наличия всех критических путей
    if warnings and os.getenv("ENVIRONMENT") == "production":
        raise RuntimeError(f"Critical paths missing in production: {warnings}")


# ============================================================================
# SINGLETON
# ============================================================================

_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """Возвращает глобальный экземпляр Settings."""
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


# Инициализация при импорте (для совместимости)
settings = get_settings()
