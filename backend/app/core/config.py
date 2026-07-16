"""
Конфигурация N.I.N.A. AI Cortex
Загружает settings.yaml и переменные окружения.
Все пути параметризированы — система работает на любом ПК.

ИСПРАВЛЕНО (рефакторинг v3):
- Удалены: HomeAssistantConfig, PluginsStatus, AuthConfig, SecurityConfig
- Добавлены: OpenAPIConfig, StorageThresholds, MetricsConfig, SimulationConfig, ExecutionConfig
- Удален model_name из AISettings (deprecated)
- Упрощен CORSConfig для локального использования

ИСПРАВЛЕНО (v4.2 — полная централизация):
- Добавлены ВСЕ секции из settings.yaml:
  DiskMonitorConfig, PredictiveHALConfig, RAGConfig, RAGUpdaterConfig,
  MetricsMonitorConfig, PythonBridgeConfig, GlobalVarInjectorConfig,
  DecisionAnalyzerConfig, ShadowVisualizerConfig, EmbeddingsConfig,
  TriggersConfig, ExtendedHALConfig
- Все настраиваемые параметры вынесены в единый конфиг
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


class SessionMetadataConfig(BaseModel):
    """Настройки Session Metadata файлов."""

    files: List[str] = Field(
        default_factory=lambda: [
            "ImageMetaData.json",
            "AcquisitionDetails.json",
            "WeatherData.json",
        ]
    )


class LoggingConfig(BaseModel):
    """Настройки логирования."""

    level: str = "INFO"
    format: str = "json"


# ============================================================================
# УПРОЩЕННЫЕ МОДЕЛИ (локальное использование)
# ============================================================================


class CORSConfig(BaseModel):
    """Настройки CORS (Cross-Origin Resource Sharing)."""

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
    """Настройки OpenAPI спецификации N.I.N.A. Advanced API."""

    spec_path: str = "config/nina_api_spec.json"
    auto_load: bool = True
    cache_enabled: bool = True


class StorageThresholds(BaseModel):
    """Пороговые значения для Disk Monitor."""

    warning_threshold_gb: float = 50.0
    critical_threshold_gb: float = 20.0
    retention_keep_last_days: int = 30
    retention_max_records: int = 100000
    retention_cleanup_interval_hours: int = 24


class MetricsConfig(BaseModel):
    """Настройки метрик и истории."""

    history_max_points: int = 100
    ai_action_log_max: int = 1000
    active_alerts_max: int = 50
    update_interval: float = 3.0
    # НОВОЕ (К-8): Параметры EventBus
    event_queue_maxsize: int = 10000
    event_stop_timeout_seconds: float = 5.0


class SimulationConfig(BaseModel):
    """Настройки режима симуляции (FakeNina, FakePhd2)."""

    flush_every_frames: int = 10
    flush_every_seconds: float = 30.0
    frame_delay_seconds: float = 2.0


class ExecutionConfig(BaseModel):
    """Конфигурация Execution Layer."""

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


class CalibrationFreshnessConfig(BaseModel):
    """
    Общие пороги свежести калибровочных мастеров.
    С-2: Единый источник правды для Calibrator и Pre-flight.
    Используется через YAML anchor &calibration_freshness.
    """

    bias_freshness_days: int = 90
    dark_freshness_days: int = 30
    flat_freshness_days: int = 7


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

    # С-2: Общие пороги свежести калибровок (источник правды)
    calibration_freshness: CalibrationFreshnessConfig = Field(
        default_factory=CalibrationFreshnessConfig
    )

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
    # С-6: Порог "InfluxDB stale" — если InfluxDB не обновлялся
    # дольше этого значения, активируется Prometheus FALLBACK
    stale_threshold_seconds: float = 30.0


# ============================================================================
# FEATURE FLAGS
# ============================================================================


class RAGFeatureFlags(BaseModel):
    """Feature flags для RAG."""

    auto_update_enabled: bool = False
    multimodal_enabled: bool = False
    sqlite_integration_enabled: bool = True


class HALFeatureFlags(BaseModel):
    """Feature flags для HAL (Predictive)."""

    predictive_enabled: bool = False
    confidence_threshold_critical: float = 0.95
    confidence_threshold_medium: float = 0.85
    confidence_threshold_low: float = 0.70


class AnalyticsFeatureFlags(BaseModel):
    """Feature flags для аналитики."""

    decision_analyzer_enabled: bool = True
    ml_parameter_optimizer: bool = False


class ShadowFeatureFlags(BaseModel):
    """Feature flags для Shadow Engine."""

    mermaid_export_enabled: bool = True
    d3_visualization_enabled: bool = False


class MetricsFeatureFlags(BaseModel):
    """Feature flags для метрик."""

    auto_source_selection: bool = True


class IntegrationsFeatureFlags(BaseModel):
    """Feature flags для интеграций."""

    siril_enabled: bool = False


class MLFeatureFlags(BaseModel):
    """Feature flags для ML."""

    rl_pipeline_enabled: bool = False


class FeatureFlagsConfig(BaseModel):
    """Полная конфигурация feature flags."""

    rag: RAGFeatureFlags = Field(default_factory=RAGFeatureFlags)
    hal: HALFeatureFlags = Field(default_factory=HALFeatureFlags)
    analytics: AnalyticsFeatureFlags = Field(default_factory=AnalyticsFeatureFlags)
    shadow: ShadowFeatureFlags = Field(default_factory=ShadowFeatureFlags)
    metrics: MetricsFeatureFlags = Field(default_factory=MetricsFeatureFlags)
    integrations: IntegrationsFeatureFlags = Field(
        default_factory=IntegrationsFeatureFlags
    )
    ml: MLFeatureFlags = Field(default_factory=MLFeatureFlags)


# ============================================================================
# SECURITY
# ============================================================================


class SecurityConfig(BaseModel):
    """Конфигурация безопасности (паттерны чувствительных данных)."""

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
# НОВОЕ v4.2: ДОПОЛНИТЕЛЬНЫЕ СЕКЦИИ КОНФИГУРАЦИИ
# ============================================================================


class DiskMonitorConfig(BaseModel):
    """Конфигурация Disk Monitor (только мониторинг, без удаления)."""

    check_interval_seconds: int = 3600
    auto_recommendations_enabled: bool = True
    min_session_size_gb: float = 0.1
    max_recommendations_per_check: int = 50


class PredictiveHALConfig(BaseModel):
    """Конфигурация Predictive HAL."""

    window_short: int = 10
    window_medium: int = 20
    window_long: int = 40
    prediction_horizon_minutes: float = 5.0
    min_points_for_prediction: int = 8
    prediction_cooldown_seconds: int = 300
    points_per_minute: float = 20.0
    warning_temperature_celsius: float = -5.0


class ChunkSizesConfig(BaseModel):
    """Размеры чанков для RAG."""

    documentation: int = 1000
    session: int = 500
    error_log: int = 300


class RAGConfig(BaseModel):
    """Конфигурация RAG Engine."""

    chunk_sizes: ChunkSizesConfig = Field(default_factory=ChunkSizesConfig)
    embedding_cache_max_size: int = 10000
    # ИСПРАВЛЕНО (С-1): Размер батча для upsert в Qdrant
    batch_upsert_size: int = 100


class RAGUpdaterConfig(BaseModel):
    """Конфигурация RAG Updater."""

    docs_dir: str = "./docs"
    doc_extensions: List[str] = Field(default_factory=lambda: [".md", ".txt", ".rst"])
    max_docs_per_run: int = 50


class MetricsMonitorConfig(BaseModel):
    """Конфигурация Metrics Source Monitor."""

    expected_metrics_count: int = 25
    history_size: int = 20


class PythonBridgeConfig(BaseModel):
    """Конфигурация Python Bridge (Safety)."""

    forbidden_substrings: List[str] = Field(
        default_factory=lambda: [
            "import os",
            "import sys",
            "import subprocess",
            "os.system",
            "os.popen",
            "subprocess.",
            "eval(",
            "exec(",
            "__import__",
            "open(",
            "System.IO.File",
            "System.Diagnostics.Process",
            "System.Net.WebClient",
            "System.Net.Http",
        ]
    )


class GlobalVarInjectorConfig(BaseModel):
    """Конфигурация Global Var Injector."""

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


class DecisionAnalyzerConfig(BaseModel):
    """Конфигурация Decision Analyzer."""

    low_success_rate_threshold: float = 0.6
    low_sample_size: int = 10
    high_confidence_wrong_threshold: float = 0.7
    known_agents: List[str] = Field(
        default_factory=lambda: [
            "Watcher",
            "Guardian",
            "Diagnostician",
            "Strategist",
            "Auditor",
            "Calibrator",
            "Copilot",
            "Orchestrator",
            "HybridLangGraphOrchestrator",
        ]
    )


class ShadowVisualizerConfig(BaseModel):
    """Конфигурация Shadow Visualizer."""

    critical_keywords: List[str] = Field(
        default_factory=lambda: ["shutdown", "park", "meridian", "flip"]
    )


class QualityWeightsConfig(BaseModel):
    """Веса факторов качества (С-10: единый модуль)."""

    hfr_weight: float = 0.30
    eccentricity_weight: float = 0.20
    acceptance_rate_weight: float = 0.15
    rms_weight: float = 0.15
    hfr_trend_weight: float = 0.10
    problems_weight: float = 0.10


class QualityThresholdsConfig(BaseModel):
    """Пороговые значения для расчёта quality score (С-10)."""

    # HFR (pixels)
    hfr_excellent: float = 2.0
    hfr_good: float = 2.5
    hfr_acceptable: float = 3.0
    hfr_poor: float = 3.5
    # Eccentricity (0-1)
    eccentricity_excellent: float = 0.3
    eccentricity_good: float = 0.4
    eccentricity_acceptable: float = 0.5
    eccentricity_poor: float = 0.7
    # Acceptance Rate (0-1)
    acceptance_excellent: float = 0.95
    acceptance_good: float = 0.90
    acceptance_acceptable: float = 0.80
    acceptance_poor: float = 0.70
    # RMS Total (arcsec)
    rms_excellent: float = 1.0
    rms_good: float = 1.5
    rms_acceptable: float = 2.0
    rms_poor: float = 3.0
    # HFR Trend (pixels/frame)
    hfr_trend_degrading: float = 0.05
    hfr_trend_stable: float = 0.02
    hfr_trend_improving: float = -0.02
    # Problems count
    problems_few: int = 2
    problems_many: int = 5


class EmbeddingsConfig(BaseModel):
    """Конфигурация Embeddings."""

    model: str = "nomic-embed-text"
    cache_file: str = "./data/embeddings_cache.pkl"
    dimension: int = 768


class HttpClientServiceConfig(BaseModel):
    """Конфигурация HTTP клиента для одного сервиса."""

    timeout_seconds: float = 30.0
    max_connections: int = 20
    max_keepalive: int = 10
    keepalive_expiry: int = 30


class HttpClientConfig(BaseModel):
    """
    Конфигурация HttpClientManager (С-15).
    Архитектура: отдельный httpx.AsyncClient на каждый base_url.
    """

    # Глобальные дефолты
    default_timeout_seconds: float = 30.0
    default_max_connections: int = 20
    default_max_keepalive: int = 10
    default_keepalive_expiry: int = 30

    # Конфигурации по сервисам
    nina: HttpClientServiceConfig = Field(
        default_factory=lambda: HttpClientServiceConfig(
            timeout_seconds=10.0,
            max_connections=20,
            max_keepalive=10,
            keepalive_expiry=30,
        )
    )
    ollama: HttpClientServiceConfig = Field(
        default_factory=lambda: HttpClientServiceConfig(
            timeout_seconds=30.0,
            max_connections=10,
            max_keepalive=5,
            keepalive_expiry=30,
        )
    )
    prometheus: HttpClientServiceConfig = Field(
        default_factory=lambda: HttpClientServiceConfig(
            timeout_seconds=5.0,
            max_connections=5,
            max_keepalive=3,
            keepalive_expiry=30,
        )
    )
    embeddings: HttpClientServiceConfig = Field(
        default_factory=lambda: HttpClientServiceConfig(
            timeout_seconds=30.0,
            max_connections=5,
            max_keepalive=3,
            keepalive_expiry=30,
        )
    )


class TriggersConfig(BaseModel):
    """Конфигурация Trigger Emulator."""

    protected_params: List[str] = Field(
        default_factory=lambda: ["cancel", "skipValidation"]
    )
    patterns: Optional[Dict[str, Dict[str, Any]]] = None


class ExtendedHALConfig(BaseModel):
    """Расширенная конфигурация HAL."""

    critical_instruction_types: List[str] = Field(
        default_factory=lambda: [
            "ShutdownPcInstruction",
            "ShutdownNina",
            "MeridianFlipInstruction",
            "ParkScopeInstruction",
            "TwoPointPolarAlignmentSequenceItem",
            "CenterAfterDriftInstruction",
            "CenterInstruction",
            "SlewScopeInstruction",
            "SlewScopeToAltAzInstruction",
        ]
    )
    safe_during_critical: List[str] = Field(
        default_factory=lambda: [
            "InterruptWhenRMSAbove",
            "RestartWhenSaturated",
        ]
    )


class LangGraphTracingConfig(BaseModel):
    """Конфигурация tracing для LangGraph workflows (Спринт 4)."""

    # Включить OpenTelemetry spans для каждого узла
    node_spans_enabled: bool = True
    # Включить логирование решений через orchestrator
    decision_logging_enabled: bool = True
    # Префикс для имён spans
    span_prefix: str = "langgraph"
    # Включать ли контекст observatory_state в атрибуты span
    include_observatory_state: bool = False
    # Максимальная длина rationale в логах
    max_rationale_length: int = 200


class TracingConfig(BaseModel):
    """Конфигурация OpenTelemetry distributed tracing."""

    enabled: bool = False
    exporter: str = "otlp"  # "otlp" | "console" | "none"
    otlp_endpoint: str = "http://localhost:4317"
    service_name: str = "nina-ai-cortex"
    service_version: str = "5.0.0"
    sample_rate: float = 1.0  # 0.0-1.0 (доля трассируемых запросов)
    console_export: bool = False  # Дублировать spans в консоль
    instrument_fastapi: bool = True
    instrument_httpx: bool = True


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
    session_metadata: SessionMetadataConfig = Field(
        default_factory=SessionMetadataConfig
    )
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

    # Feature flags
    feature_flags: FeatureFlagsConfig = Field(default_factory=FeatureFlagsConfig)

    # Security
    security: SecurityConfig = Field(default_factory=SecurityConfig)

    # ========================================================================
    # НОВОЕ v4.2: Дополнительные секции
    # ========================================================================
    disk_monitor: DiskMonitorConfig = Field(default_factory=DiskMonitorConfig)
    predictive_hal: PredictiveHALConfig = Field(default_factory=PredictiveHALConfig)
    rag: RAGConfig = Field(default_factory=RAGConfig)
    rag_updater: RAGUpdaterConfig = Field(default_factory=RAGUpdaterConfig)
    metrics_monitor: MetricsMonitorConfig = Field(default_factory=MetricsMonitorConfig)
    python_bridge: PythonBridgeConfig = Field(default_factory=PythonBridgeConfig)
    global_var_injector: GlobalVarInjectorConfig = Field(
        default_factory=GlobalVarInjectorConfig
    )
    decision_analyzer: DecisionAnalyzerConfig = Field(
        default_factory=DecisionAnalyzerConfig
    )
    shadow_visualizer: ShadowVisualizerConfig = Field(
        default_factory=ShadowVisualizerConfig
    )
    embeddings: EmbeddingsConfig = Field(default_factory=EmbeddingsConfig)

    # HTTP Client Manager (С-15)
    http_client: HttpClientConfig = Field(default_factory=HttpClientConfig)

    # Quality Score (С-10: единый модуль расчёта)
    quality_weights: QualityWeightsConfig = Field(default_factory=QualityWeightsConfig)
    quality_thresholds: QualityThresholdsConfig = Field(
        default_factory=QualityThresholdsConfig
    )

    triggers: TriggersConfig = Field(default_factory=TriggersConfig)
    hal_config: ExtendedHALConfig = Field(default_factory=ExtendedHALConfig)
    # LangGraph tracing (Спринт 4)
    langgraph_tracing: LangGraphTracingConfig = Field(
        default_factory=LangGraphTracingConfig
    )
    # OpenTelemetry tracing (Спринт 4)
    tracing: TracingConfig = Field(default_factory=TracingConfig)

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
    """Загружает settings.yaml и накладывает переменные окружения."""
    config_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "config"
        / "settings.yaml"
    )

    if not config_path.exists():
        logger.error(f"❌ Configuration file not found: {config_path}")
        raise FileNotFoundError(f"Configuration file not found at {config_path}")

    # Загружаем .env в os.environ ДО создания Settings
    backend_dir = Path(__file__).resolve().parent.parent.parent
    env_path = backend_dir / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
        logger.info(f"✅ Loaded environment from {env_path}")
    else:
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

        # Валидация критических путей
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

    critical_paths = {
        "appdata_root": env.appdata_root,
        "sessions_root": env.sessions_root,
    }
    for name, path in critical_paths.items():
        if not path.exists():
            warnings.append(f"⚠️ Critical path does not exist: {name} = {path}")

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
