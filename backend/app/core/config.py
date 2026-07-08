"""
Конфигурация N.I.N.A. AI Cortex
Загружает settings.yaml и переменные окружения.
Все пути параметризированы — система работает на любом ПК.

ИСПРАВЛЕНО (все аудиты):
- F2, C4, C5: CORS, Auth, Security
- 7.2: Thresholds
- 8.1: Path validation
- 9.1: Data sources
- 11.1: Sensitive patterns
- 14: Decision audit retention
- P0, P1: Secure memory, JWT
- P2: Circuit Breaker, Retry, LLM connection
- P3: ПОЛНОЕ устранение хардкода — все константы агентов и модулей
  вынесены в конфигурацию
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Optional, List, Dict
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

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


class HomeAssistantConfig(BaseModel):
    """Интеграция с Home Assistant."""

    enabled: bool = False
    url: str = "http://localhost:8123"
    token: str = ""


class HALConfig(BaseModel):
    """Hardware Abstraction Layer — финальная валидация команд."""

    enabled: bool = True
    min_altitude_limit: float = 15.0


class WatchersConfig(BaseModel):
    """Настройки файловых вотчеров."""

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
# БЕЗОПАСНОСТЬ
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
        default_factory=lambda: [
            "Authorization",
            "Content-Type",
            "X-API-Key",
            "X-Request-ID",
        ]
    )
    max_age: int = 3600


class AuthConfig(BaseModel):
    """Настройки аутентификации и авторизации."""

    enabled: bool = True
    access_token_expire_minutes: int = 60 * 24
    rate_limit_per_minute: int = 120
    llm_rate_limit_per_minute: int = 20
    trigger_rate_limit_per_minute: int = 30
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
    # P3: вынесено из хардкода в auth.py
    algorithm: str = "HS256"
    jwt_token_length: int = 48
    api_key_length: int = 32
    min_secret_length: int = 32


class SecurityConfig(BaseModel):
    """Настройки безопасности (маскирование логов)."""

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
    mask_value: str = "***"


# ============================================================================
# ПОРОГОВЫЕ ЗНАЧЕНИЯ
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
    # P3: вынесено из хардкода в watcher_agent.py
    small_sample_threshold_percent: float = 50.0
    large_sample_no_zscore_threshold_percent: float = 60.0
    hfr_trend_window: int = 5
    baseline_window: int = 10
    min_history_for_full_detection: int = 5


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
    # P3: вынесено из хардкода в guardian_agent.py
    cloud_cover_critical: float = 95.0


class ThresholdsConfig(BaseModel):
    """Конфигурация всех пороговых значений."""

    watcher: WatcherThresholds = Field(default_factory=WatcherThresholds)
    calibrator: CalibratorThresholds = Field(default_factory=CalibratorThresholds)
    preflight: PreflightThresholds = Field(default_factory=PreflightThresholds)
    strategist: StrategistThresholds = Field(default_factory=StrategistThresholds)
    guardian: GuardianThresholds = Field(default_factory=GuardianThresholds)


class DataSourcesConfig(BaseModel):
    """Конфигурация источников данных."""

    primary_metrics_source: str = "influxdb"
    enable_fallback_source: bool = True
    metrics_poll_interval: float = 3.0


# ============================================================================
# P2: Circuit Breaker, Retry, LLM Connection
# ============================================================================


class CircuitBreakerConfig(BaseModel):
    """Конфигурация Circuit Breaker для HTTP-клиентов."""

    failure_threshold: int = 5
    recovery_timeout_seconds: float = 60.0
    half_open_max_calls: int = 1
    enabled: bool = True


class RetryConfig(BaseModel):
    """Конфигурация retry-логики для HTTP-клиентов."""

    max_retries: int = 3
    base_delay_seconds: float = 1.0
    max_delay_seconds: float = 10.0
    enabled: bool = True


class LLMConnectionConfig(BaseModel):
    """Конфигурация HTTP-соединений для LLM-провайдера."""

    max_connections: int = 10
    max_keepalive_connections: int = 5
    keepalive_expiry: int = 30


# ============================================================================
# AI НАСТРОЙКИ
# ============================================================================


class AISettings(BaseModel):
    """Настройки AI (Ollama, RAG, LLM)."""

    ollama_host: str
    model_name: str = "gemma4:31b-cloud"  # [DEPRECATED]
    primary_model: str = "gemma4:31b-cloud"
    fallback_model: str = "gemma4:e4b"
    rag_db_path: Path
    primary_timeout: float = 30.0
    fallback_timeout: float = 15.0
    max_tokens: int = 1500
    temperature: float = 0.3
    fallback_enabled: bool = True
    connection: LLMConnectionConfig = Field(default_factory=LLMConnectionConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)


# ============================================================================
# P3: AGENTS CONFIG
# ============================================================================


class AuditorQualityScoreConfig(BaseModel):
    """Пороги для расчёта quality score (Auditor Agent)."""

    hfr_penalty_high: float = 2.0
    hfr_penalty_medium: float = 1.0
    hfr_threshold_high: float = 3.0
    hfr_threshold_medium: float = 2.5
    fwhm_penalty_high: float = 2.0
    fwhm_penalty_medium: float = 1.0
    fwhm_threshold_high: float = 4.0
    fwhm_threshold_medium: float = 3.0
    rms_penalty: float = 1.0
    rms_threshold: float = 1.5
    acceptance_bonus_threshold: float = 0.95
    acceptance_penalty_threshold: float = 0.80
    acceptance_bonus: float = 1.0
    acceptance_penalty: float = 1.0
    default_acceptance_rate_estimate: float = 0.90
    base_score: float = 10.0


class AuditorRecommendationsConfig(BaseModel):
    """Пороги для генерации рекомендаций (Auditor Agent)."""

    hfr_warning_threshold: float = 2.5
    rms_warning_threshold: float = 1.5
    wind_warning_threshold: float = 10.0


class AuditorConfig(BaseModel):
    """Конфигурация Auditor Agent."""

    quality_score: AuditorQualityScoreConfig = Field(
        default_factory=AuditorQualityScoreConfig
    )
    recommendations: AuditorRecommendationsConfig = Field(
        default_factory=AuditorRecommendationsConfig
    )
    llm_max_tokens: int = 800
    llm_temperature: float = 0.3


class DiagnosticianCorrelationThresholds(BaseModel):
    """Пороги корреляции для Diagnostician Agent."""

    strong: float = 0.7
    moderate: float = 0.5
    weak: float = 0.3


class DiagnosticianConfidenceConfig(BaseModel):
    """Уверенность в диагностике (Diagnostician Agent)."""

    temperature_correlation: float = 0.85
    wind_correlation: float = 0.80
    rms_correlation: float = 0.75
    heuristic_hfr_drift: float = 0.6
    heuristic_wind_load: float = 0.75
    llm_boost: float = 0.15
    llm_max: float = 0.95
    default_confidence: float = 0.5
    unknown_root_cause: str = "Неизвестная причина"


class DiagnosticianConfig(BaseModel):
    """Конфигурация Diagnostician Agent."""

    correlation_thresholds: DiagnosticianCorrelationThresholds = Field(
        default_factory=DiagnosticianCorrelationThresholds
    )
    min_sample_size: int = 10
    confidence: DiagnosticianConfidenceConfig = Field(
        default_factory=DiagnosticianConfidenceConfig
    )
    llm_max_tokens: int = 400
    llm_temperature: float = 0.2
    similar_cases_top_k: int = 5
    max_recommendations: int = 5


class CopilotTimeoutsConfig(BaseModel):
    """Таймауты для интерактивных инструкций (Copilot Agent)."""

    messagebox: int = 300
    two_point_alignment: int = 600
    oag_focus: int = 900
    filter_selector: int = 120


class CopilotConfig(BaseModel):
    """Конфигурация Copilot Agent."""

    timeouts: CopilotTimeoutsConfig = Field(default_factory=CopilotTimeoutsConfig)
    rag_max_tokens: int = 1000


class SchedulerScoringConfig(BaseModel):
    """Параметры скоринга целей (Scheduler Agent)."""

    priority_max_score: float = 40.0
    priority_multiplier: float = 4.0
    altitude_max_score: float = 30.0
    altitude_normalizer: float = 2.0
    moon_distance_max_score: float = 20.0
    moon_distance_normalizer: float = 1.0
    progress_max_score: float = 10.0
    below_horizon_penalty: float = 50.0
    near_moon_penalty: float = 10.0
    narrowband_moon_safe_filters: List[str] = Field(
        default_factory=lambda: ["Ha", "OIII", "SII"]
    )


class SchedulerDefaultsConfig(BaseModel):
    """Значения по умолчанию (Scheduler Agent)."""

    altitude: float = 45.0
    moon_distance: float = 90.0


class SchedulerConfig(BaseModel):
    """Конфигурация Scheduler Agent."""

    min_altitude: float = 30.0
    moon_avoidance_angle: float = 30.0
    scoring: SchedulerScoringConfig = Field(default_factory=SchedulerScoringConfig)
    defaults: SchedulerDefaultsConfig = Field(default_factory=SchedulerDefaultsConfig)


class StrategistExposureLimitsConfig(BaseModel):
    """Пределы оптимизации экспозиции (Strategist Agent)."""

    min: float = 30.0
    max: float = 300.0


class StrategistWindAnalysisConfig(BaseModel):
    """Параметры анализа ветровой нагрузки (Strategist Agent)."""

    angle_threshold: int = 180
    windward_threshold: int = 90
    angle_complement: int = 360


class StrategistExtraConfig(BaseModel):
    """Дополнительная конфигурация Strategist Agent."""

    exposure_limits: StrategistExposureLimitsConfig = Field(
        default_factory=StrategistExposureLimitsConfig
    )
    exposure_change_threshold: float = 0.1
    snr_low_threshold_ratio: float = 0.8
    wind_analysis: StrategistWindAnalysisConfig = Field(
        default_factory=StrategistWindAnalysisConfig
    )
    trend_window: int = 10


class WatcherExtraConfig(BaseModel):
    """Дополнительная конфигурация Watcher Agent (уже часть WatcherThresholds)."""

    # Алиас для обратной совместимости
    pass


# ============================================================================
# P3: CORE MODULES CONFIG
# ============================================================================


class EmbeddingsConfig(BaseModel):
    """Конфигурация Embeddings."""

    dimension: int = 768
    cache_file: str = "./data/embeddings_cache.pkl"
    timeout: float = 30.0
    cache_save_interval: int = 100
    model_name: str = "nomic-embed-text"


class ModeManagerConfig(BaseModel):
    """Конфигурация Mode Manager."""

    health_check_interval: int = 30
    error_retry_delay: int = 10
    health_check_timeout: float = 5.0
    status_log_interval: int = 300


class RAGChunkSizesConfig(BaseModel):
    """Размеры чанков для разных типов документов."""

    documentation: int = 1000
    session: int = 500
    error_log: int = 300


class RAGSearchConfig(BaseModel):
    """Параметры поиска в RAG."""

    default_top_k: int = 10
    max_context_multiplier: int = 4


class RAGExtraConfig(BaseModel):
    """Дополнительная конфигурация RAG Engine."""

    chunk_sizes: RAGChunkSizesConfig = Field(default_factory=RAGChunkSizesConfig)
    chunk_overlap_ratio: float = 0.25
    search: RAGSearchConfig = Field(default_factory=RAGSearchConfig)
    embedding_cache_max_size: int = 10000
    http_timeout: float = 30.0


class WSBroadcastExtraConfig(BaseModel):
    """Дополнительная конфигурация WS Broadcast."""

    heartbeat_interval: int = 30


class ObservatoryStateTrendAnalysisConfig(BaseModel):
    """Параметры анализа трендов (ObservatoryState)."""

    default_window: int = 10
    average_window: int = 20
    std_window: int = 20
    min_points_for_std: int = 3
    degradation_threshold_percent: float = 20.0
    baseline_slice_start: int = -20
    baseline_slice_end: int = -10
    recent_slice: int = -10


class ObservatoryStateConfig(BaseModel):
    """Конфигурация ObservatoryState."""

    max_history_points: int = 100
    source_timeout: float = 30.0
    max_active_alerts: int = 50
    ai_action_log_size: int = 1000
    trend_analysis: ObservatoryStateTrendAnalysisConfig = Field(
        default_factory=ObservatoryStateTrendAnalysisConfig
    )
    default_camera_temp: float = -15.0
    default_exposure_time: float = 60.0
    default_gain: int = 85


class BaseAgentConfig(BaseModel):
    """Конфигурация BaseAgent."""

    decision_log_max_size: int = 1000
    recent_decisions_default: int = 10


class OrchestratorExtraConfig(BaseModel):
    """Дополнительная конфигурация Orchestrator."""

    decision_queue_timeout: float = 1.0
    error_retry_delay: float = 1.0
    decisions_memory_limit: int = 1000


class HALExtraConfig(BaseModel):
    """Дополнительная конфигурация HAL."""

    default_altitude: float = 90.0


class HocusFocusParserConfig(BaseModel):
    """Конфигурация парсера Hocus Focus."""

    z_threshold: float = 3.0
    eccentricity_threshold: float = 0.85


class PythonBridgeExtraConfig(BaseModel):
    """Дополнительная конфигурация Python Bridge."""

    max_message_length: int = 500
    max_log_length: int = 1000


class SafetyInterceptorExtraConfig(BaseModel):
    """Дополнительная конфигурация Safety Interceptor."""

    shutdown_delay_minutes: int = 10


class PreflightFreshnessDaysConfig(BaseModel):
    """Свежесть мастеров для Preflight (дни)."""

    BIAS: int = 90
    DARK: int = 30
    FLAT: int = 7


class PreflightExtraConfig(BaseModel):
    """Дополнительная конфигурация Preflight."""

    freshness_days: PreflightFreshnessDaysConfig = Field(
        default_factory=PreflightFreshnessDaysConfig
    )


class WebSocketClientConfig(BaseModel):
    """Конфигурация WebSocket Client."""

    reconnect_delay: float = 5.0
    ping_interval: int = 20
    ping_timeout: int = 10


class LogTailerConfig(BaseModel):
    """Конфигурация Log Tailer."""

    poll_interval: float = 0.5
    rotation_check_interval: float = 2.0


class PrometheusScraperConfig(BaseModel):
    """Конфигурация Prometheus Scraper."""

    interval_seconds: float = 3.0
    timeout: float = 5.0
    error_log_interval_minutes: int = 1


class InfluxDBExtraConfig(BaseModel):
    """Дополнительная конфигурация InfluxDB Provider."""

    query_interval: float = 3.0
    error_log_interval_minutes: int = 1
    history_max_points: int = 100
    progress_report_interval: int = 20


class MastersAuditorConfig(BaseModel):
    """Конфигурация Masters Auditor."""

    progress_report_interval: int = 50


# ============================================================================
# EXECUTION LAYER
# ============================================================================


class ParameterRange(BaseModel):
    """Диапазон допустимых значений для параметра оборудования."""

    min: Optional[float] = None
    max: Optional[float] = None
    description: str = ""


class TriggerExecutionConfig(BaseModel):
    """Конфигурация Trigger Emulator."""

    protected_params: List[str] = Field(
        default_factory=lambda: ["cancel", "skipValidation"]
    )
    parameter_ranges: Dict[str, ParameterRange] = Field(
        default_factory=lambda: {
            "temperature": ParameterRange(
                min=-40.0, max=30.0, description="Температура камеры (°C)"
            ),
            "minutes": ParameterRange(
                min=0, max=120, description="Время охлаждения/нагрева (мин)"
            ),
            "brightness": ParameterRange(
                min=0, max=100, description="Яркость плоской панели"
            ),
            "count": ParameterRange(min=1, max=100, description="Количество кадров"),
            "exposureTime": ParameterRange(
                min=0.001, max=3600.0, description="Экспозиция (сек)"
            ),
            "minExposure": ParameterRange(
                min=0.0, max=300.0, description="Мин. экспозиция (сек)"
            ),
            "maxExposure": ParameterRange(
                min=0.0, max=600.0, description="Макс. экспозиция (сек)"
            ),
            "azimuth": ParameterRange(
                min=0.0, max=360.0, description="Азимут купола (°)"
            ),
            "ra": ParameterRange(min=0.0, max=360.0, description="RA координата (°)"),
            "dec": ParameterRange(
                min=-90.0, max=90.0, description="Dec координата (°)"
            ),
            "position": ParameterRange(
                min=0.0, max=360.0, description="Позиция ротатора (°)"
            ),
            "rotationAngle": ParameterRange(
                min=-360.0, max=360.0, description="Угол поворота (°)"
            ),
            "histogramMean": ParameterRange(
                min=0.0, max=1.0, description="Среднее гистограммы"
            ),
            "meanTolerance": ParameterRange(
                min=0.0, max=1.0, description="Допуск среднего"
            ),
            "gain": ParameterRange(min=0, max=10000, description="Gain камеры"),
            "offset": ParameterRange(min=-1000, max=10000, description="Offset камеры"),
            "filterId": ParameterRange(min=0, max=20, description="ID фильтра"),
        }
    )
    history_max_size: int = 100
    request_timeout: float = 30.0


class DiskMonitorConfig(BaseModel):
    """Конфигурация Disk Monitor."""

    warning_threshold_gb: float = 50.0
    critical_threshold_gb: float = 20.0


# ============================================================================
# SIMULATION
# ============================================================================


class SimulationDefaultMetrics(BaseModel):
    """Начальные значения метрик для симуляции."""

    hfr: float = 2.5
    fwhm: float = 3.0
    eccentricity: float = 0.35
    star_count: int = 150
    median_adu: int = 15000
    rms_ra: float = 0.8
    rms_dec: float = 0.9
    rms_total: float = 1.2
    camera_temp: float = -14.8
    focuser_position: int = 6931
    rotator_angle: float = 180.0
    mount_altitude: float = 45.0
    mount_azimuth: float = 90.0


class SimulationDefaultParams(BaseModel):
    """Параметры симуляции по умолчанию."""

    target: str = "M31"
    filter: str = "Ha"
    exposure_time: float = 60.0
    gain: int = 85
    temperature_setpoint: float = -15.0


class SimulationNoiseConfig(BaseModel):
    """Параметры шума для симуляции."""

    hfr_std: float = 0.05
    fwhm_std: float = 0.05
    rms_ra_std: float = 0.02
    rms_dec_std: float = 0.02
    frame_hfr_std: float = 0.1
    frame_fwhm_std: float = 0.1
    frame_stars_std: float = 10.0
    frame_rms_std: float = 0.05
    temperature_drift_factor: float = 0.1
    temperature_noise_std: float = 0.02


class SimulationLimitsConfig(BaseModel):
    """Пределы значений для симуляции."""

    hfr_min: float = 1.5
    hfr_max: float = 5.0
    fwhm_min: float = 2.0
    fwhm_max: float = 6.0
    rms_ra_min: float = 0.3
    rms_ra_max: float = 3.0
    rms_dec_min: float = 0.3
    rms_dec_max: float = 3.0


class SimulationAnomaliesConfig(BaseModel):
    """Параметры аномалий для симуляции."""

    hfr_spike: float = 2.0
    rms_spike: float = 1.5
    temp_drift: float = 3.0


class SimulationAutofocusImprovementConfig(BaseModel):
    """Улучшение после автофокуса."""

    hfr_reduction: float = 0.5
    fwhm_reduction: float = 0.5
    hfr_floor: float = 1.8
    fwhm_floor: float = 2.2


class SimulationConfig(BaseModel):
    """Конфигурация симуляторов."""

    flush_every_frames: int = 10
    flush_every_seconds: float = 30.0
    frame_interval_seconds: float = 2.0
    autofocus_duration_seconds: float = 10.0
    meridian_flip_duration_seconds: float = 30.0
    metrics_interval_seconds: float = 3.0
    default_metrics: SimulationDefaultMetrics = Field(
        default_factory=SimulationDefaultMetrics
    )
    default_params: SimulationDefaultParams = Field(
        default_factory=SimulationDefaultParams
    )
    noise: SimulationNoiseConfig = Field(default_factory=SimulationNoiseConfig)
    limits: SimulationLimitsConfig = Field(default_factory=SimulationLimitsConfig)
    anomalies: SimulationAnomaliesConfig = Field(
        default_factory=SimulationAnomaliesConfig
    )
    autofocus_improvement: SimulationAutofocusImprovementConfig = Field(
        default_factory=SimulationAutofocusImprovementConfig
    )


class SimulationPHD2Config(BaseModel):
    """Конфигурация симулятора PHD2."""

    initial_rms_ra: float = 0.8
    initial_rms_dec: float = 0.9
    initial_rms_total: float = 1.2
    noise_rms_ra_std: float = 0.02
    noise_rms_dec_std: float = 0.02
    rms_min: float = 0.3
    rms_max: float = 3.0
    metrics_interval_seconds: float = 2.0
    guiding_error_spike: float = 2.0


class NinaAPIClientConfig(BaseModel):
    """Конфигурация HTTP-клиента для N.I.N.A. Advanced API."""

    request_timeout: float = 10.0
    max_connections: int = 20
    max_keepalive_connections: int = 10
    keepalive_expiry: int = 30
    retry: RetryConfig = Field(default_factory=RetryConfig)
    circuit_breaker: CircuitBreakerConfig = Field(default_factory=CircuitBreakerConfig)


class DecisionAuditConfig(BaseModel):
    """Конфигурация Decision Audit Trail."""

    keep_last_days: int = 90
    max_records: int = 100000
    archive_before_delete: bool = True
    archive_path: str = "./data/decision_archives"
    auto_cleanup_enabled: bool = True
    auto_cleanup_interval_hours: int = 24


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

    # Безопасность
    cors: CORSConfig = Field(default_factory=CORSConfig)
    auth: AuthConfig = Field(default_factory=AuthConfig)
    security: SecurityConfig = Field(default_factory=SecurityConfig)
    thresholds: ThresholdsConfig = Field(default_factory=ThresholdsConfig)
    data_sources: DataSourcesConfig = Field(default_factory=DataSourcesConfig)
    decision_audit: DecisionAuditConfig = Field(default_factory=DecisionAuditConfig)

    # Execution Layer
    nina_api_client: NinaAPIClientConfig = Field(default_factory=NinaAPIClientConfig)
    execution: TriggerExecutionConfig = Field(default_factory=TriggerExecutionConfig)
    storage_disk_monitor: DiskMonitorConfig = Field(default_factory=DiskMonitorConfig)

    # Simulation
    simulation: SimulationConfig = Field(default_factory=SimulationConfig)
    simulation_phd2: SimulationPHD2Config = Field(default_factory=SimulationPHD2Config)

    # P3: Agents
    auditor: AuditorConfig = Field(default_factory=AuditorConfig)
    diagnostician: DiagnosticianConfig = Field(default_factory=DiagnosticianConfig)
    copilot: CopilotConfig = Field(default_factory=CopilotConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    strategist_extra: StrategistExtraConfig = Field(
        default_factory=StrategistExtraConfig
    )

    # P3: Core modules
    embeddings: EmbeddingsConfig = Field(default_factory=EmbeddingsConfig)
    mode_manager: ModeManagerConfig = Field(default_factory=ModeManagerConfig)
    rag_extra: RAGExtraConfig = Field(default_factory=RAGExtraConfig)
    ws_broadcast_extra: WSBroadcastExtraConfig = Field(
        default_factory=WSBroadcastExtraConfig
    )
    observatory_state: ObservatoryStateConfig = Field(
        default_factory=ObservatoryStateConfig
    )
    base_agent: BaseAgentConfig = Field(default_factory=BaseAgentConfig)
    orchestrator_extra: OrchestratorExtraConfig = Field(
        default_factory=OrchestratorExtraConfig
    )
    hal_extra: HALExtraConfig = Field(default_factory=HALExtraConfig)
    hocus_focus_parser: HocusFocusParserConfig = Field(
        default_factory=HocusFocusParserConfig
    )
    python_bridge_extra: PythonBridgeExtraConfig = Field(
        default_factory=PythonBridgeExtraConfig
    )
    safety_interceptor_extra: SafetyInterceptorExtraConfig = Field(
        default_factory=SafetyInterceptorExtraConfig
    )
    preflight_extra: PreflightExtraConfig = Field(default_factory=PreflightExtraConfig)
    websocket_client: WebSocketClientConfig = Field(
        default_factory=WebSocketClientConfig
    )
    log_tailer: LogTailerConfig = Field(default_factory=LogTailerConfig)
    prometheus_extra: PrometheusScraperConfig = Field(
        default_factory=PrometheusScraperConfig
    )
    influxdb_extra: InfluxDBExtraConfig = Field(default_factory=InfluxDBExtraConfig)
    masters_auditor: MastersAuditorConfig = Field(default_factory=MastersAuditorConfig)

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
    """Загружает settings.yaml и накладывает переменные окружения."""
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


settings = get_settings()
