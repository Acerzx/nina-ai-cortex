from pydantic import BaseModel
import yaml
from pathlib import Path
from functools import lru_cache


class NinaEnvironment(BaseModel):
    appdata_root: str
    user_profile: str
    sessions_root: str
    masters_root: str
    profiles_dir: str
    sequence_template: str
    logs_dir: str


class NetworkSettings(BaseModel):
    nina_api_host: str
    nina_ws_url: str
    prometheus_url: str


class AISettings(BaseModel):
    ollama_host: str
    model_name: str


class LoggingSettings(BaseModel):
    level: str = "INFO"


class Settings(BaseModel):
    nina_environment: NinaEnvironment
    network: NetworkSettings
    ai_settings: AISettings
    logging: LoggingSettings


@lru_cache()
def get_settings() -> Settings:
    # Ищем settings.yaml относительно корня проекта (на 2 уровня выше backend/)
    # backend/app/core/config.py -> нужно подняться на 3 уровня вверх
    current_file = Path(__file__).resolve()
    project_root = current_file.parent.parent.parent.parent

    config_path = project_root / "config" / "settings.yaml"

    if not config_path.exists():
        raise FileNotFoundError(
            f"Configuration file not found at {config_path}\n"
            f"Expected location: {project_root / 'config' / 'settings.yaml'}"
        )

    with open(config_path, "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    return Settings(**config_dict)
