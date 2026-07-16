"""
Base File Watcher — базовый класс для всех файловых вотчеров.
ИСПРАВЛЕНО (audit 5.1): Сохранение ссылок на debounced задачи для предотвращения
их отмены сборщиком мусора.
ИСПРАВЛЕНО (С-17): Добавлен threading.Lock для защиты _pending dict от
race condition между watchdog thread (on_modified) и asyncio event loop
(_safe_cb). Используется threading.Lock, а не asyncio.Lock, потому что
конфликт возникает МЕЖДУ потоками, а не внутри одного event loop.
ИСПРАВЛЕНО (К-5): Добавлен threading.Lock для защиты _active_tasks set от
race condition между watchdog thread (_schedule_task) и asyncio event loop
(add_done_callback, cancel_all_pending). Set в CPython не является
потокобезопасным для операций add/discard/iterate между потоками.
"""

import asyncio
import logging
import threading
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
    ИСПРАВЛЕНО (С-17): _pending dict защищён threading.Lock для
    thread-safe доступа между watchdog thread и asyncio event loop.
    ИСПРАВЛЕНО (К-5): _active_tasks set защищён threading.Lock для
    thread-safe доступа между watchdog thread (_schedule_task) и
    asyncio event loop (add_done_callback, cancel_all_pending).
    """

    def __init__(self, loop, callback, target_files, debounce):
        self.loop = loop
        self.callback = callback
        self.target_files = target_files
        self.debounce = debounce

        # Debouncing state
        self._pending = {}

        # ИСПРАВЛЕНО (С-17): Lock для thread-safe доступа к _pending
        # Критично: on_modified() вызывается из watchdog thread,
        # _safe_cb() — из asyncio event loop thread.
        # threading.Lock работает между потоками, asyncio.Lock — нет.
        self._pending_lock = threading.Lock()

        # ИСПРАВЛЕНО (audit 5.1): Хранение ссылок на активные задачи
        self._active_tasks: Set[asyncio.Task] = set()

        # ИСПРАВЛЕНО (К-5): Lock для thread-safe доступа к _active_tasks
        # Критично: _schedule_task() вызывается из watchdog thread
        # (через loop.call_later), add_done_callback и cancel_all_pending
        # вызываются из asyncio event loop thread.
        # Set в CPython не является потокобезопасным для операций
        # add/discard/iterate между потоками.
        self._active_tasks_lock = threading.Lock()

    def on_modified(self, event: FileSystemEvent):
        """
        Вызывается из WATCHDOG THREAD при модификации файла.
        ИСПРАВЛЕНО (С-17): Доступ к _pending защищён threading.Lock,
        чтобы избежать race condition с _safe_cb() из event loop.
        """
        if event.is_directory:
            return

        path = Path(event.src_path)
        if (
            self.target_files
            and path.name not in self.target_files
            and path.suffix not in self.target_files
        ):
            return

        # ИСПРАВЛЕНО (С-17): Защищённый доступ к _pending
        with self._pending_lock:
            # Отменяем предыдущую debounced задачу для этого файла
            if path.name in self._pending:
                self._pending[path.name].cancel()

            # Создаем новую debounced задачу
            self._pending[path.name] = self.loop.call_later(
                self.debounce, self._schedule_task, path
            )

    def _schedule_task(self, path: Path):
        """
        Создаёт задачу и сохраняет ссылку на неё.
        Вызывается из WATCHDOG THREAD через loop.call_later().

        ИСПРАВЛЕНО (К-5): Доступ к _active_tasks защищён threading.Lock.
        Раньше task.add() и add_done_callback вызывались без блокировки,
        что приводило к race condition с cancel_all_pending() из event loop.
        """
        task = asyncio.create_task(self._safe_cb(path))

        # ИСПРАВЛЕНО (К-5): Защищённое добавление задачи в set
        # _schedule_task вызывается из watchdog thread,
        # cancel_all_pending — из event loop thread
        with self._active_tasks_lock:
            self._active_tasks.add(task)

        # Используем lambda для безопасного удаления под lock
        task.add_done_callback(lambda t: self._remove_task(t))

    def _remove_task(self, task: asyncio.Task):
        """
        Удаляет задачу из _active_tasks после завершения.
        Вызывается из ASYNCIO EVENT LOOP THREAD через add_done_callback.

        ИСПРАВЛЕНО (К-5): Доступ к _active_tasks защищён threading.Lock.
        """
        with self._active_tasks_lock:
            self._active_tasks.discard(task)

    async def _safe_cb(self, path: Path):
        """
        Безопасный вызов callback с обработкой ошибок.
        Вызывается из ASYNCIO EVENT LOOP THREAD.
        ИСПРАВЛЕНО (С-17): Доступ к _pending защищён threading.Lock.
        """
        try:
            await self.callback(path)
        except Exception as e:
            logger.error(f"Error processing {path}: {e}")
        finally:
            # ИСПРАВЛЕНО (С-17): Защищённое удаление из _pending
            with self._pending_lock:
                if path.name in self._pending:
                    del self._pending[path.name]

            # ИСПРАВЛЕНО (v4.0 — проблема #51): удаляем задачу из active_tasks
            # через _remove_task (который использует _active_tasks_lock)
            current_task = asyncio.current_task()
            if current_task:
                self._remove_task(current_task)

    def cancel_all_pending(self):
        """
        Отменяет все pending и active задачи.
        Вызывается из asyncio event loop при остановке watcher'а.

        ИСПРАВЛЕНО (С-17): Доступ к _pending защищён threading.Lock.
        ИСПРАВЛЕНО (К-5): Доступ к _active_tasks защищён threading.Lock.

        Алгоритм:
        1. Snapshot pending timers под _pending_lock
        2. Отмена timers вне lock (быстрая операция)
        3. Snapshot active tasks под _active_tasks_lock
        4. Отмена tasks вне lock (быстрая операция)
        """
        # === Шаг 1: Snapshot pending timers под lock ===
        with self._pending_lock:
            pending_timers = list(self._pending.values())
            self._pending.clear()

        # Отмена timers вне lock (thread-safe operation)
        for timer in pending_timers:
            timer.cancel()

        # === Шаг 2: Snapshot active tasks под lock ===
        # ИСПРАВЛЕНО (К-5): защищённый snapshot
        with self._active_tasks_lock:
            tasks_to_cancel = [t for t in self._active_tasks if not t.done()]

        # Отмена tasks вне lock (asyncio task.cancel() — thread-safe)
        for task in tasks_to_cancel:
            task.cancel()

        # Логируем количество отменённых задач
        if tasks_to_cancel:
            logger.debug(
                f"Cancelling {len(tasks_to_cancel)} active tasks "
                f"and {len(pending_timers)} pending timers"
            )


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
