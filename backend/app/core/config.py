import os
import yaml
from pathlib import Path
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings


class NinaEnvironment(BaseModel):
    appdata_root: Path
    sessions_root: Path
    masters_root: Path
    profiles_dir: Path
    sequence_template: Path
    logs_dir: Path
    plugins_dir: Path


class NetworkConfig(BaseModel):
    nina_api_host: str
    nina_ws_url: str
    prometheus_url: str


class InfluxDBConfig(BaseModel):
    url: str
    token: str
    org: str
    bucket: str


class AISettings(BaseModel):
    ollama_host: str
    model_name: str
    rag_db_path: Path


class HomeAssistantConfig(BaseModel):
    enabled: bool = False
    url: str = "http://localhost:8123"
    token: str = ""


class HALConfig(BaseModel):
    enabled: bool = True
    min_altitude_limit: float = 15.0


class WatchersConfig(BaseModel):
    debounce_seconds: float = 1.5
    ai_weather_status_file: Optional[str] = None


class PluginsStatus(BaseModel):
    dither_inject: str = "NOT_INSTALLED"
    guider_calibration: str = "NOT_INSTALLED"


class Settings(BaseSettings):
    nina_environment: NinaEnvironment
    network: NetworkConfig
    influxdb: InfluxDBConfig
    ai_settings: AISettings
    home_assistant: HomeAssistantConfig = Field(default_factory=HomeAssistantConfig)
    hal: HALConfig = Field(default_factory=HALConfig)
    watchers: WatchersConfig = Field(default_factory=WatchersConfig)
    plugins_status: PluginsStatus = Field(default_factory=PluginsStatus)

    @field_validator("influxdb", mode="before")
    def resolve_env_vars(cls, value):
        if isinstance(value, dict):
            token = value.get("token")
            if isinstance(token, str) and token.startswith("${"):
                value["token"] = os.getenv(token[2:-1], "default_token")
        return value

    class Config:
        env_file = ".env"


def load_settings() -> Settings:
    config_path = (
        Path(__file__).resolve().parent.parent.parent.parent
        / "config"
        / "settings.yaml"
    )
    with open(config_path, "r", encoding="utf-8") as f:
        return Settings(**yaml.safe_load(f))


settings = load_settings()
