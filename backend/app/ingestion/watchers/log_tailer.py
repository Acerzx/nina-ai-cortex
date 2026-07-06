import asyncio
import logging
from pathlib import Path
from typing import Optional
import aiofiles
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent

from app.core.config import settings
from app.core.events import event_bus
from app.ingestion.parsers.log_patterns import classify_log_line

logger = logging.getLogger("LogTailer")


class LogFileHandler(FileSystemEventHandler):
    """Следит за появлением новых лог-файлов N.I.N.A."""

    def __init__(self, callback):
        self.callback = callback

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory and event.src_path.endswith(".log"):
            asyncio.run_coroutine_threadsafe(
                self.callback(Path(event.src_path)), asyncio.get_running_loop()
            )


class LogTailer:
    """
    Читает самый свежий лог N.I.N.A. в реальном времени (tail -f).
    Устраняет Упрощение #9: полный паттерн-матчинг всех критических событий.
    """

    def __init__(self):
        self.logs_dir = settings.nina_environment.logs_dir
        self._active_log: Optional[Path] = None
        self._file_position = 0
        self._running = False
        self._task: asyncio.Task = None
        self._observer = Observer()

    async def start(self):
        self._running = True
        # Находим самый свежий .log файл
        self._active_log = self._find_latest_log()
        if self._active_log:
            self._file_position = self._active_log.stat().st_size
            logger.info(f"LogTailer attached to: {self._active_log.name}")

        # Watchdog для детекции ротации логов (N.I.N.A. создает новый лог при каждом запуске)
        handler = LogFileHandler(self._handle_new_log)
        self._observer.schedule(handler, str(self.logs_dir), recursive=False)
        self._observer.start()

        self._task = asyncio.create_task(self._tail_loop())

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
        self._observer.stop()
        self._observer.join()

    def _find_latest_log(self) -> Optional[Path]:
        logs = list(self.logs_dir.glob("*.log"))
        if not logs:
            return None
        return max(logs, key=lambda p: p.stat().st_mtime)

    async def _handle_new_log(self, new_log: Path):
        """Переключается на новый лог-файл при ротации"""
        if new_log != self._active_log:
            logger.info(f"Log rotation detected. Switching to: {new_log.name}")
            self._active_log = new_log
            self._file_position = 0

    async def _tail_loop(self):
        while self._running:
            if not self._active_log or not self._active_log.exists():
                await asyncio.sleep(2)
                continue

            try:
                async with aiofiles.open(
                    self._active_log, mode="r", encoding="utf-8", errors="ignore"
                ) as f:
                    await f.seek(self._file_position)
                    lines = await f.readlines()
                    if lines:
                        self._file_position = await f.tell()
                        for line in lines:
                            event = classify_log_line(line)
                            if event and event.event_type != "generic":
                                await event_bus.publish("LOG_EVENT", event.model_dump())
                                if event.level in ("ERROR", "FATAL"):
                                    await event_bus.publish(
                                        "LOG_ERROR", event.model_dump()
                                    )
            except Exception as e:
                logger.error(f"Error tailing log: {e}")

            await asyncio.sleep(0.5)  # Читаем 2 раза в секунду
