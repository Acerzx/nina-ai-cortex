import asyncio
import json
import logging
from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class ImageMetadataHandler(FileSystemEventHandler):
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.loop = loop

    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith("ImageMetaData.json"):
            # Планируем асинхронную обработку в основном event loop
            asyncio.run_coroutine_threadsafe(
                self.process_metadata(event.src_path), self.loop
            )

    async def process_metadata(self, filepath: str):
        try:
            # Небольшая задержка, чтобы N.I.N.A. успела закрыть файл после записи
            await asyncio.sleep(0.5)

            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)

            # data может быть списком (все кадры сессии) или одним объектом
            # Session Metadata обычно обновляет список
            if isinstance(data, list) and len(data) > 0:
                latest_frame = data[-1]
                hfr = latest_frame.get("HFR", "N/A")
                stars = latest_frame.get("Stars", "N/A")
                filter_name = latest_frame.get("Filter", "N/A")

                logger.info(
                    f"📸 NEW FRAME DETECTED | Filter: {filter_name} | HFR: {hfr} | Stars: {stars}"
                )
                # Здесь мы будем пушить это состояние в Redis / InfluxDB / WebSocket

        except Exception as e:
            logger.debug(f"Could not parse {filepath}: {e}")


class SessionWatcher:
    def __init__(self, loop: asyncio.AbstractEventLoop):
        self.settings = get_settings()
        self.sessions_root = Path(self.settings.nina_environment.sessions_root)
        self.loop = loop
        self.observer = Observer()

    async def start(self):
        if not self.sessions_root.exists():
            logger.warning(f"⚠️ Sessions root not found: {self.sessions_root}")
            return

        logger.info(f"📂 Starting SessionWatcher on {self.sessions_root}")
        event_handler = ImageMetadataHandler(self.loop)

        # Рекурсивно мониторим все подпапки (Target/Date/Light)
        self.observer.schedule(event_handler, str(self.sessions_root), recursive=True)
        self.observer.start()

        try:
            while True:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.observer.stop()
        self.observer.join()
