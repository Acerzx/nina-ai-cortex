"""
Конфигурация N.I.N.A. AI Cortex
Загружает settings.yaml и переменные окружения.
Все пути параметризированы — система работает на любом ПК.

ИСПРАВЛЕНО (audit F2, C4, 7.2, 8.1):
- Добавлены модели для CORS, Authentication, Thresholds, DataSources, Security
- Добавлена валидация критических путей при старте
- Магические числа вынесены в ThresholdsConfig
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

logger = logging.getLogger("Config")


# ============================================================================
# БАЗОВЫЕ МОДЕЛИ КОНФИГУРАЦИИ (оригинальные)
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
    # [DEPRECATED] Оставлено для обратной совместимости
    model_name: str = "gemma4:31b-cloud"
    # Основная модель (облачная) — используется LLM Provider
    primary_model: str = "gemma4:31b-cloud"
    # Fallback модель (локальная) — используется при недоступности primary
    fallback_model: str = "gemma4:e4b"
    rag_db_path: Path


class HomeAssistantConfig(BaseModel):
    """Интеграция с Home Assistant."""

    enabled: bool = False
    url: str = "http://localhost:8123"
    token: str = ""


class HALConfig(BaseModel):
    """Hardware Abstraction Layer — финальная валидация команд."""

    enabled: bool = True
    min_altitude_limit: float = 15.0  # Жесткий лимит высоты (градусы)


class WatchersConfig(BaseModel):
    """Настройки вотчеров."""

    debounce_seconds: float = 1.5
    ai_weather_status_file: Optional[str] = None
    dither_statistics_path: Optional[str] = None
    guiding_analyzer_path: Optional[str] = None
    dynamic_sequencer_path: Optional[str] = None


class PluginsStatus(BaseModel):
    """Статус отсутствующих плагинов (Graceful Degradation)."""

    dither_inject: str = "NOT_INSTALLED"
    guider_calibration: str = "NOT_INSTALLED"


class LoggingConfig(BaseModel):
    """Настройки логирования."""

    level: str = "INFO"
    format: str = "json"


# ============================================================================
# НОВЫЕ МОДЕЛИ ДЛЯ БЕЗОПАСНОСТИ (audit F2, C4)
# ============================================================================


class CORSConfig(BaseModel):
    """
    Настройки CORS (Cross-Origin Resource Sharing).

    ИСПРАВЛЕНО (audit F2): allow_origins=["*"] заменён на whitelist.
    """

    enabled: bool = True
    allowed_origins: List[str] = Field(
        default_factory=lambda: [
            "http://localhost:3000",  # Vue dev server
            "http://localhost:5173",  # Vite dev server
            "http://localhost:8080",  # Alternative frontend
            "http://127.0.0.1:3000",
            "http://127.0.0.1:5173",
        ]
    )
    allow_credentials: bool = True
    allowed_methods: List[str] = Field(
        default_factory=lambda: ["GET", "POST", "PUT", "DELETE", "OPTIONS"]
    )
    allowed_headers: List[str] = Field(
        default_factory=lambda: [
            "Authorization",
            "Content-Type",
            "X-API-Key",
            "X-Request-ID",
        ]
    )
    max_age: int = 3600  # Preflight cache (seconds)


class AuthConfig(BaseModel):
    """
    Настройки аутентификации и авторизации.

    ИСПРАВЛЕНО (audit C4): добавлена централизованная конфигурация auth.
    """

    enabled: bool = True
    # Время жизни JWT access token (минуты)
    access_token_expire_minutes: int = 60 * 24
    # Rate limiting: максимум запросов в минуту с одного IP
    rate_limit_per_minute: int = 120
    # Rate limiting: максимум запросов к LLM endpoints в минуту
    llm_rate_limit_per_minute: int = 20
    # Rate limiting: максимум запросов к trigger endpoints в минуту
    trigger_rate_limit_per_minute: int = 30
    # Публичные paths (не требуют auth)
    public_paths: List[str] = Field(
        default_factory=lambda: [
            "/",
            "/docs",
            "/openapi.json",
            "/redoc",
            "/health",
            "/metrics",
        ]
    )


class SecurityConfig(BaseModel):
    """
    Настройки безопасности (маскирование логов, защищённые параметры).

    ИСПРАВЛЕНО (audit 11.1): централизованная конфигурация для маскирования
    чувствительных данных в логах.
    """

    # Паттерны имен переменных, значения которых маскируются в логах
    sensitive_patterns: List[str] = Field(
        default_factory=lambda: [
            "token",
            "password",
            "passwd",
            "secret",
            "api_key",
            "apikey",
            "api-key",
            "private_key",
            "privatekey",
            "credentials",
            "auth",
            "bearer",
            "access_key",
            "accesskey",
            "secret_key",
            "secretkey",
        ]
    )


# ============================================================================
# НОВЫЕ МОДЕЛИ ДЛЯ ПОРОГОВЫХ ЗНАЧЕНИЙ (audit 7.2)
# ============================================================================


class WatcherThresholds(BaseModel):
    """Пороговые значения для Watcher Agent."""

    hfr_increase_percent: float = 30.0
    rms_ra_critical: float = 2.0
    rms_dec_critical: float = 2.0
    temperature_deviation: float = 2.0
    wind_speed_warning: float = 15.0
    wind_gust_critical: float = 20.0
    z_score_threshold: float = 3.0
    min_history_points: int = 5
    # FWHM пороги
    fwhm_increase_percent: float = 30.0
    # Алерт cooldown
    anomaly_cooldown_seconds: int = 300


class CalibratorThresholds(BaseModel):
    """Пороговые значения для Calibrator Agent."""

    bias_freshness_days: int = 90
    dark_freshness_days: int = 30
    flat_freshness_days: int = 7
    temperature_tolerance: float = 2.0
    # Cooldown для алертов
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
    # Интервалы автофокуса (минуты)
    autofocus_interval_normal: int = 60
    autofocus_interval_frequent: int = 30
    autofocus_interval_emergency: int = 15
    # Порог деградации HFR (пикселей/кадр)
    hfr_degradation_threshold: float = 0.05
    # Минимальный интервал между предложениями оптимизаций (сек)
    min_proposal_interval_seconds: int = 600
    # Ветровая нагрузка
    wind_load_warning: float = 10.0


class GuardianThresholds(BaseModel):
    """Пороговые значения для Guardian Agent."""

    wind_speed_park: float = 20.0
    cloud_cover_pause: float = 80.0
    humidity_warning: float = 90.0
    rms_recalibration: float = 3.0
    temperature_alarm: float = 5.0


class ThresholdsConfig(BaseModel):
    """
    Конфигурация всех пороговых значений.

    ИСПРАВЛЕНО (audit 7.2): все магические числа вынесены в конфиг.
    """

    watcher: WatcherThresholds = Field(default_factory=WatcherThresholds)
    calibrator: CalibratorThresholds = Field(default_factory=CalibratorThresholds)
    preflight: PreflightThresholds = Field(default_factory=PreflightThresholds)
    strategist: StrategistThresholds = Field(default_factory=StrategistThresholds)
    guardian: GuardianThresholds = Field(default_factory=GuardianThresholds)


class DataSourcesConfig(BaseModel):
    """
    Конфигурация источников данных.

    ИСПРАВЛЕНО (audit 9.1): централизованный выбор основного источника метрик
    для предотвращения дублирования InfluxDB/Prometheus.
    """

    # Основной источник метрик: "influxdb" или "prometheus"
    primary_metrics_source: str = "influxdb"
    # Включить резервный источник (для отказоустойчивости)
    enable_fallback_source: bool = True
    # Интервал опроса метрик (секунды)
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
    home_assistant: HomeAssistantConfig = Field(default_factory=HomeAssistantConfig)
    hal: HALConfig = Field(default_factory=HALConfig)
    watchers: WatchersConfig = Field(default_factory=WatchersConfig)
    plugins_status: PluginsStatus = Field(default_factory=PluginsStatus)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)

    # Новые секции (audit F2, C4, 7.2, 9.1, 11.1)
    cors: CORSConfig = Field(default_factory=CORSConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)
    data_sources: DataSourcesConfig = Field(default_factory=DataSourcesConfig)

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

    @field_validator("home_assistant", mode="before")
    def resolve_ha_token(cls, value):
        """Заменяет ${HA_TOKEN} на значение из env."""
        if isinstance(value, dict):
            token = value.get("token")
            if (
                isinstance(token, str)
                and token.startswith("${")
                and token.endswith("}")
            ):
                env_var = token[2:-1]
                value["token"] = os.getenv(env_var, "")
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
    """
    config_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "config"
        / "settings.yaml"
    )
    if not config_path.exists():
        logger.error(f"❌ Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

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
    """
    Проверяет существование критических путей при старте.

    ИСПРАВЛЕНО (audit 8.1): явная валидация вместо падения в рантайме.
    """
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

# Глобальный экземпляр настроек (Singleton)
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Возвращает глобальный экземпляр Settings.
    Используется в sequence_parser.py и других модулях.
    """
    global _settings
    if _settings is None:
        _settings = load_settings()
    return _settings


# Инициализация при импорте (для совместимости)
settings = get_settings()
