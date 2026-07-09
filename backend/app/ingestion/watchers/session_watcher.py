"""
Session Watcher
Мониторит папку сессий N.I.N.A. на предмет изменений в файлах Session Metadata.
Устраняет Упрощение #1: обрабатывает ВСЕ 3 файла (ImageMetaData, AcquisitionDetails, WeatherData).
"""

import logging
import aiofiles
from pathlib import Path
from typing import Dict, Any

from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.session_metadata import (
    ImageMetaData,
    ImageFrame,
    AcquisitionDetails,
    WeatherData,
)
from app.core.config import settings
from app.core.capability_registry import CapabilityRegistry

from app.storage.sessions_metadata import sessions_metadata, SessionRecord

logger = logging.getLogger("SessionWatcher")


class SessionWatcher(BaseFileWatcher):
    """
    Мониторит папку сессий N.I.N.A. на предмет изменений в файлах Session Metadata.
    Устраняет Упрощение #1: обрабатывает ВСЕ 3 файла, а не только ImageMetaData.

    Ключевые возможности:
    - Отслеживание дельты (новых кадров) для защиты от дублей
    - Автоматическое определение FLAT_MODE
    - Публикация событий NEW_FRAME, SESSION_DETAILS_UPDATE, WEATHER_UPDATE
    """

    def __init__(self, registry: CapabilityRegistry):
        # Мониторим корень сессий рекурсивно
        super().__init__(
            watch_path=settings.nina_environment.sessions_root,
            target_files=settings.watchers.session_metadata.files
            if hasattr(settings.watchers, "session_metadata")
            else ["ImageMetaData.json", "AcquisitionDetails.json", "WeatherData.json"],
            registry=registry,
        )
        # Кэш последних состояний для вычисления дельты (защита от дублей)
        self._last_image_index: Dict[str, int] = {}
        self._is_flat_mode: bool = False

    async def process_file(self, path: Path) -> None:
        """Обработка измененного файла Session Metadata."""
        filename = path.name
        session_id = path.parent.name  # Имя папки сессии как ID

        if filename == "ImageMetaData.json":
            await self._process_image_metadata(path, session_id)
        elif filename == "AcquisitionDetails.json":
            await self._process_acquisition_details(path, session_id)
        elif filename == "WeatherData.json":
            await self._process_weather_data(path, session_id)

    async def _process_image_metadata(self, path: Path, session_id: str):
        """Обработка ImageMetaData.json — метаданные каждого кадра."""
        data = ImageMetaData.from_json_file(str(path))
        if not data.frames:
            return

        last_idx = self._last_image_index.get(session_id, -1)
        new_frames = [f for f in data.frames if f.index > last_idx]
        if not new_frames:
            return

        # Обновляем кэш
        self._last_image_index[session_id] = max(f.index for f in new_frames)

        # === НОВОЕ (v4.0): Создание сессии при первом кадре ===
        existing_session = await sessions_metadata.get_session(session_id)
        if not existing_session and new_frames:
            # Первая встреча этой сессии — создаём запись
            first_frame = new_frames[0]
            try:
                new_session = SessionRecord(
                    session_id=session_id,
                    target_name=session_id.split("_")[0]
                    if "_" in session_id
                    else session_id,
                    filter_name=first_frame.filter_name,
                    exposure_time=first_frame.exposure_time,
                    gain=first_frame.gain,
                    temperature_setpoint=first_frame.temperature,
                    start_time=datetime.now().isoformat(),
                )
                await sessions_metadata.create_session(new_session)
            except Exception as e:
                logger.debug(f"Could not create session record: {e}")

        for frame in new_frames:
            # FLAT_MODE detection (существующая логика)
            is_flat = frame.image_type.upper() == "FLAT"
            if is_flat and not self._is_flat_mode:
                logger.info(f"🟦 FLAT_MODE activated for session {session_id}")
                self._is_flat_mode = True
                await event_bus.publish(
                    "FLAT_MODE_CONFIRMED", {"session_id": session_id}
                )
            elif not is_flat and self._is_flat_mode:
                logger.info(f"🟩 FLAT_MODE deactivated for session {session_id}")
                self._is_flat_mode = False
                await event_bus.publish("FLAT_MODE_ENDED", {"session_id": session_id})

            payload = {
                "session_id": session_id,
                "is_flat": is_flat,
                "frame": frame.model_dump(),
            }
            await event_bus.publish("NEW_FRAME", payload)

            # === НОВОЕ (v4.0): Логирование кадра в sessions_metadata ===
            if not is_flat:  # Логируем только LIGHT кадры
                try:
                    await sessions_metadata.log_frame_from_dict(
                        session_id=session_id,
                        frame_data=frame.model_dump(),
                    )
                except Exception as e:
                    logger.debug(f"Could not log frame to sessions_metadata: {e}")

            logger.info(
                f"Processed new frame #{frame.index} ({frame.image_type}) for {session_id}"
            )

    async def _process_acquisition_details(self, path: Path, session_id: str):
        """
        Обработка AcquisitionDetails.json — общая информация о сессии.
        ИСПРАВЛЕНО (v4.0 — проблема #14): aiofiles для асинхронного чтения.
        """
        try:
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                content = await f.read()
                raw_data = json.loads(content)

            data = AcquisitionDetails(**raw_data)
        except Exception as e:
            logger.error(f"Failed to parse AcquisitionDetails: {e}")
            return

        payload = {
            "session_id": session_id,
            "details": data.model_dump(exclude_none=True),
        }
        await event_bus.publish("SESSION_DETAILS_UPDATE", payload)
        logger.info(f"Updated acquisition details for {session_id}")

    async def _process_weather_data(self, path: Path, session_id: str):
        """
        Обработка WeatherData.json — погодные условия.
        ИСПРАВЛЕНО (v4.0 — проблема #14): aiofiles для асинхронного чтения.
        """
        try:
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                content = await f.read()
                raw_data = json.loads(content)

            data = WeatherData(**raw_data)
        except Exception as e:
            logger.error(f"Failed to parse WeatherData: {e}")
            return

        payload = {
            "session_id": session_id,
            "weather": data.model_dump(exclude_none=True),
        }
        await event_bus.publish("WEATHER_UPDATE", payload)
        logger.debug(f"Updated weather data for {session_id}")
