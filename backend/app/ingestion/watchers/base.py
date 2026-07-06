import asyncio
import logging
from pathlib import Path
from typing import Callable, Awaitable
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from app.core.config import settings
from app.core.events import event_bus
from app.core.capability_registry import CapabilityRegistry

logger = logging.getLogger("BaseWatcher")


class AsyncDebouncedEventHandler(FileSystemEventHandler):
    def __init__(self, loop, callback, target_files, debounce):
        self.loop, self.callback, self.target_files, self.debounce = (
            loop,
            callback,
            target_files,
            debounce,
        )
        self._pending = {}

    def on_modified(self, event: FileSystemEvent):
        if event.is_directory:
            return
        path = Path(event.src_path)
        if (
            self.target_files
            and path.name not in self.target_files
            and path.suffix not in self.target_files
        ):
            return
        if path.name in self._pending:
            self._pending[path.name].cancel()
        self._pending[path.name] = self.loop.call_later(
            self.debounce, asyncio.create_task, self._safe_cb(path)
        )

    async def _safe_cb(self, path: Path):
        try:
            await self.callback(path)
        except Exception as e:
            logger.error(f"Error processing {path}: {e}")


class BaseFileWatcher:
    def __init__(
        self, watch_path: Path, target_files: list, registry: CapabilityRegistry = None
    ):
        self.watch_path = watch_path
        self.target_files = target_files
        self.registry = registry
        self.observer = Observer()
        self.loop = asyncio.get_running_loop()
        self.handler = AsyncDebouncedEventHandler(
            self.loop,
            self.process_file,
            target_files,
            settings.watchers.debounce_seconds,
        )

    async def process_file(self, path: Path):
        raise NotImplementedError

    def start(self):
        self.watch_path.mkdir(parents=True, exist_ok=True)
        self.observer.schedule(self.handler, str(self.watch_path), recursive=True)
        self.observer.start()

    def stop(self):
        self.observer.stop()
        self.observer.join()
