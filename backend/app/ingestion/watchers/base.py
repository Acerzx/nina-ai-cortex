"""
Base File Watcher — базовый класс для всех файловых вотчеров.

ИСПРАВЛЕНО (audit 5.1): Сохранение ссылок на debounced задачи для предотвращения
их отмены сборщиком мусора.
"""

import asyncio
import logging
from pathlib import Path
from typing import Callable, Awaitable, Set
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from app.core.config import settings
from app.core.events import event_bus
from app.core.capability_registry import CapabilityRegistry

logger = logging.getLogger("BaseWatcher")


class AsyncDebouncedEventHandler(FileSystemEventHandler):
    """
    Обработчик файловых событий с debouncing.

    ИСПРАВЛЕНО (audit 5.1): Добавлен набор _active_tasks для хранения
    ссылок на все активные debounced задачи.
    """

    def __init__(self, loop, callback, target_files, debounce):
        self.loop = loop
        self.callback = callback
        self.target_files = target_files
        self.debounce = debounce

        # Debouncing state
        self._pending = {}

        # ИСПРАВЛЕНО (audit 5.1): Хранение ссылок на активные задачи
        self._active_tasks: Set[asyncio.Task] = set()

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

        # Отменяем предыдущую debounced задачу для этого файла
        if path.name in self._pending:
            self._pending[path.name].cancel()

        # Создаем новую debounced задачу
        self._pending[path.name] = self.loop.call_later(
            self.debounce, self._schedule_task, path
        )

    def _schedule_task(self, path: Path):
        """Создает задачу и сохраняет ссылку на неё."""
        task = asyncio.create_task(self._safe_cb(path))

        # ИСПРАВЛЕНО (audit 5.1): Сохраняем ссылку на задачу
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)

    async def _safe_cb(self, path: Path):
        """Безопасный вызов callback с обработкой ошибок."""
        try:
            await self.callback(path)
        except Exception as e:
            logger.error(f"Error processing {path}: {e}")
        finally:
            # Удаляем из pending после выполнения
            if path.name in self._pending:
                del self._pending[path.name]

            # ИСПРАВЛЕНО (v4.0 — проблема #51): удаляем задачу из active_tasks
            current_task = asyncio.current_task()
            if current_task:
                self._active_tasks.discard(current_task)

    def cancel_all_pending(self):
        """Отменяет все pending и active задачи."""
        # Отменяем pending задачи
        for timer in self._pending.values():
            timer.cancel()
        self._pending.clear()

        # Отменяем active задачи
        for task in list(self._active_tasks):
            if not task.done():
                task.cancel()

        # Ждем завершения
        if self._active_tasks:
            # Не можем использовать await здесь (синхронный метод)
            # Задачи будут отменены и завершатся асинхронно
            logger.debug(f"Cancelling {len(self._active_tasks)} active tasks")


class BaseFileWatcher:
    """
    Базовый класс для всех файловых вотчеров.
    Предоставляет общую логику мониторинга директорий.
    """

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
        """Метод для обработки файла (должен быть переопределен в наследниках)."""
        raise NotImplementedError

    def start(self):
        """Запускает мониторинг директории."""
        self.watch_path.mkdir(parents=True, exist_ok=True)
        self.observer.schedule(self.handler, str(self.watch_path), recursive=True)
        self.observer.start()

    def stop(self):
        """
        Останавливает мониторинг.

        ИСПРАВЛЕНО (audit 5.1): Отменяет все pending и active задачи.
        """
        # ИСПРАВЛЕНО (audit 5.1): Отменяем все задачи перед остановкой
        self.handler.cancel_all_pending()

        self.observer.stop()
        self.observer.join()
