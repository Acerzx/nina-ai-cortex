"""
Конфигурация N.I.N.A. AI Cortex
Загружает settings.yaml и переменные окружения.
Все пути параметризированы — система работает на любом ПК.
"""

import os
import yaml
import logging
from pathlib import Path
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

logger = logging.getLogger("Config")


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
    model_name: str
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


class Settings(BaseSettings):
    """Корневая модель конфигурации."""

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


def load_settings() -> Settings:
    """
    Загружает settings.yaml и накладывает переменные окружения.
    Возвращает валидированный Settings объект.
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

        settings = Settings(**yaml_data)
        logger.info(f"✅ Configuration loaded from {config_path}")
        return settings
    except Exception as e:
        logger.error(f"❌ Failed to load configuration: {e}")
        raise


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
