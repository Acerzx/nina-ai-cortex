import json, logging
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.config import settings

logger = logging.getLogger("AIWeatherWatcher")


class AIWeatherWatcher(BaseFileWatcher):
    def __init__(self, registry):
        status_file = settings.watchers.ai_weather_status_file
        path = (
            Path(status_file).parent
            if status_file
            else settings.nina_environment.appdata_root / "AIWeather"
        )
        super().__init__(path, ["status.json"], registry)

    async def process_file(self, path: Path):
        if path.name == "status.json":
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                await event_bus.publish("AI_WEATHER_STATUS", data)
            except Exception as e:
                logger.error(f"AI Weather error: {e}")
