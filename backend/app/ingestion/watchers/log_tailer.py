"""
LogTailer — читает самый свежий лог N.I.N.A. в реальном времени (tail -f).
ИСПРАВЛЕНО (v4.0 — проблема #44):
- Использует watchdog для отслеживания изменений файла
- Читает только при получении уведомления о модификации
- Увеличен интервал ожидания до 2с при отсутствии изменений
ИСПРАВЛЕНО (v4.2):
- RuntimeWarning: coroutine 'LogTailer.notify_modification' was never awaited
- RuntimeError: no running event loop в watchdog callback
- Решение: сохраняем ссылку на event loop при старте
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
import aiofiles
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler, FileSystemEvent
from app.core.config import settings
from app.core.events import event_bus
from app.ingestion.parsers.log_patterns import classify_log_line

logger = logging.getLogger("LogTailer")


class LogTailer:
    """
    Читает самый свежий лог N.I.N.A. в реальном времени (tail -f).
    ИСПРАВЛЕНО (v4.2): Сохраняем event loop при старте для использования в watchdog thread.
    """

    def __init__(self):
        self.logs_dir = settings.nina_environment.logs_dir
        self._active_log: Optional[Path] = None
        self._file_position = 0
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._observer: Optional[Observer] = None
        self._has_new_data = asyncio.Event()
        self._last_modification_time: Optional[datetime] = None
        # ИСПРАВЛЕНО (v4.2): сохраняем ссылку на event loop
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    def _find_latest_log(self) -> Optional[Path]:
        """Находит самый свежий .log файл в директории логов."""
        if not self.logs_dir.exists():
            return None
        log_files = list(self.logs_dir.glob("*.log"))
        if not log_files:
            return None
        return max(log_files, key=lambda p: p.stat().st_mtime)

    async def start(self):
        """Запускает LogTailer."""
        self._running = True
        # ИСПРАВЛЕНО (v4.2): сохраняем ссылку на текущий event loop
        self._loop = asyncio.get_running_loop()

        self._active_log = self._find_latest_log()
        if self._active_log:
            self._file_position = self._active_log.stat().st_size
            self._last_modification_time = datetime.fromtimestamp(
                self._active_log.stat().st_mtime
            )
            logger.info(f"LogTailer attached to: {self._active_log.name}")

        # Создаём handler с ссылкой на tailer
        handler = LogFileEventHandler(self)

        self._observer = Observer()
        self._observer.schedule(handler, str(self.logs_dir), recursive=False)
        self._observer.start()

        self._task = asyncio.create_task(self._tail_loop())

    async def stop(self):
        """Останавливает LogTailer."""
        self._running = False

        if self._observer:
            self._observer.stop()
            self._observer.join()
            self._observer = None

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

        logger.info("🛑 LogTailer stopped")

    async def notify_modification(self, new_log: Path):
        """
        Вызывается watchdog при модификации файла.
        Устанавливает флаг для пробуждения _tail_loop.
        """
        if new_log == self._active_log:
            # Тот же файл — просто будим reader
            self._has_new_data.set()
        else:
            # Новый файл — переключаемся
            logger.info(f"Log rotation detected. Switching to: {new_log.name}")
            self._active_log = new_log
            self._file_position = 0
            self._last_modification_time = datetime.now()
            self._has_new_data.set()

    async def _tail_loop(self):
        """
        Событийно-управляемый цикл чтения.
        Ждёт уведомления от watchdog вместо фиксированного интервала.
        """
        while self._running:
            # Ждём либо уведомления о новых данных, либо таймаут 2 секунды
            try:
                await asyncio.wait_for(self._has_new_data.wait(), timeout=2.0)
                self._has_new_data.clear()
            except asyncio.TimeoutError:
                # Таймаут — проверяем файл всё равно
                pass

            if not self._active_log or not self._active_log.exists():
                # Пытаемся найти новый файл
                new_log = self._find_latest_log()
                if new_log and new_log != self._active_log:
                    self._active_log = new_log
                    self._file_position = 0
                else:
                    await asyncio.sleep(2.0)
                    continue


            # Проверяем, изменился ли файл
            try:
                current_size = self._active_log.stat().st_size
                
                # ИСПРАВЛЕНО (В-5): Детекция truncation (log rotation)
                # N.I.N.A. может ротировать логи: truncate файл и начать писать заново.
                # Если current_size < _file_position, значит файл был усечён.
                if current_size < self._file_position:
                    logger.info(
                        f"🔄 Log rotation detected: {self._active_log.name} "
                        f"truncated ({self._file_position} → {current_size} bytes). "
                        f"Resetting read position to 0."
                    )
                    self._file_position = 0
                
                if current_size == self._file_position:
                    # Файл не изменился — пропускаем
                    continue
            except OSError:
                continue

            # Читаем новые данные
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

            # Защита от зацикливания
            await asyncio.sleep(1.0)


class LogFileEventHandler(FileSystemEventHandler):
    """
    Отслеживает изменения в существующих лог-файлах.
    Уведомляет LogTailer при каждой модификации.
    ИСПРАВЛЕНО (v4.2): Использует сохранённый event loop из LogTailer.
    """

    def __init__(self, tailer: LogTailer):
        self.tailer = tailer

    def on_modified(self, event: FileSystemEvent):
        """Вызывается при модификации файла."""
        if not event.is_directory and event.src_path.endswith(".log"):
            # ИСПРАВЛЕНО (v4.2): используем сохранённый loop вместо get_running_loop()
            if self.tailer._loop is not None and not self.tailer._loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self.tailer.notify_modification(Path(event.src_path)),
                    self.tailer._loop,
                )

    def on_created(self, event: FileSystemEvent):
        """Вызывается при создании нового файла."""
        if not event.is_directory and event.src_path.endswith(".log"):
            # ИСПРАВЛЕНО (v4.2): используем сохранённый loop вместо get_running_loop()
            if self.tailer._loop is not None and not self.tailer._loop.is_closed():
                asyncio.run_coroutine_threadsafe(
                    self.tailer.notify_modification(Path(event.src_path)),
                    self.tailer._loop,
                )
