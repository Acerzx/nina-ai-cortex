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


# В классе LogTailer добавить файловый вотчер (после _observer.start()):


class LogTailer:
    """
    Читает самый свежий лог N.I.N.A. в реальном времени (tail -f).
    ИСПРАВЛЕНО (v4.0 — проблема #44):
    - Использует watchdog для отслеживания изменений файла
    - Читает только при получении уведомления о модификации
    - Увеличен интервал ожидания до 2с при отсутствии изменений
    """

    def __init__(self):
        self.logs_dir = settings.nina_environment.logs_dir
        self._active_log: Optional[Path] = None
        self._file_position = 0
        self._running = False
        self._task: asyncio.Task = None
        self._observer = Observer()
        # ИСПРАВЛЕНО (v4.0): флаг для уведомления о наличии новых данных
        self._has_new_data = asyncio.Event()
        self._last_modification_time: Optional[datetime] = None

    async def start(self):
        self._running = True
        # Находим самый свежий .log файл
        self._active_log = self._find_latest_log()
        if self._active_log:
            self._file_position = self._active_log.stat().st_size
            self._last_modification_time = datetime.fromtimestamp(
                self._active_log.stat().st_mtime
            )
            logger.info(f"LogTailer attached to: {self._active_log.name}")

        # Watchdog для детекции ротации логов И изменений текущего файла
        handler = LogFileEventHandler(self)
        self._observer.schedule(handler, str(self.logs_dir), recursive=False)
        self._observer.start()

        self._task = asyncio.create_task(self._tail_loop())

    async def notify_modification(self, new_log: Path):
        """
        ИСПРАВЛЕНО (v4.0): Вызывается watchdog при модификации файла.
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
        ИСПРАВЛЕНО (v4.0): Событийно-управляемый цикл чтения.
        Ждёт уведомления от watchdog вместо фиксированного интервала.
        """
        while self._running:
            # Ждём либо уведомления о новых данных, либо таймаут 2 секунды
            try:
                await asyncio.wait_for(self._has_new_data.wait(), timeout=2.0)
                self._has_new_data.clear()
            except asyncio.TimeoutError:
                # Таймаут — проверяем файл всё равно (для случая когда watchdog пропустил)
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
                # ИСПРАВЛЕНО: защита от зацикливания
                await asyncio.sleep(1.0)


class LogFileEventHandler(FileSystemEventHandler):
    """
    ИСПРАВЛЕНО (v4.0): Отслеживает изменения в существующих лог-файлах.
    Уведомляет LogTailer при каждой модификации.
    """

    def __init__(self, tailer: "LogTailer"):
        self.tailer = tailer

    def on_modified(self, event: FileSystemEvent):
        if not event.is_directory and event.src_path.endswith(".log"):
            # Уведомляем tailer асинхронно
            asyncio.run_coroutine_threadsafe(
                self.tailer.notify_modification(Path(event.src_path)),
                asyncio.get_running_loop(),
            )

    def on_created(self, event: FileSystemEvent):
        if not event.is_directory and event.src_path.endswith(".log"):
            # Новый лог-файл — переключаемся
            asyncio.run_coroutine_threadsafe(
                self.tailer.notify_modification(Path(event.src_path)),
                asyncio.get_running_loop(),
            )
