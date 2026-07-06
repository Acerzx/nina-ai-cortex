import logging
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.session_metadata import (
    ImageMetaData,
    AcquisitionDetails,
    WeatherData,
)

logger = logging.getLogger("SessionWatcher")


class SessionWatcher(BaseFileWatcher):
    def __init__(self, registry):
        super().__init__(
            settings.nina_environment.sessions_root,
            ["ImageMetaData.json", "AcquisitionDetails.json", "WeatherData.json"],
            registry,
        )
        self._last_idx = {}
        self._is_flat_mode = False

    async def process_file(self, path: Path):
        session_id = path.parent.name
        if path.name == "ImageMetaData.json":
            data = ImageMetaData.from_json_file(str(path))
            last = self._last_idx.get(session_id, -1)
            new_frames = [f for f in data.frames if f.index > last]
            if not new_frames:
                return
            self._last_idx[session_id] = max(f.index for f in new_frames)
            for frame in new_frames:
                is_flat = frame.image_type.upper() == "FLAT"
                if is_flat and not self._is_flat_mode:
                    self._is_flat_mode = True
                    await event_bus.publish(
                        "FLAT_MODE_CONFIRMED", {"session_id": session_id}
                    )
                elif not is_flat and self._is_flat_mode:
                    self._is_flat_mode = False
                    await event_bus.publish(
                        "FLAT_MODE_ENDED", {"session_id": session_id}
                    )
                await event_bus.publish(
                    "NEW_FRAME",
                    {
                        "session_id": session_id,
                        "is_flat": is_flat,
                        "frame": frame.model_dump(),
                    },
                )
        elif path.name == "AcquisitionDetails.json":
            await event_bus.publish(
                "SESSION_DETAILS_UPDATE",
                {
                    "session_id": session_id,
                    "details": AcquisitionDetails.from_json_file(
                        str(path)
                    ).model_dump(),
                },
            )
        elif path.name == "WeatherData.json":
            await event_bus.publish(
                "WEATHER_UPDATE",
                {
                    "session_id": session_id,
                    "weather": WeatherData.from_json_file(str(path)).model_dump(),
                },
            )
