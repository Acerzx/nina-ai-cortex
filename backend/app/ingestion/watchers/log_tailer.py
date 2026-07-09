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
        """
        Основной цикл чтения лога.
        ИСПРАВЛЕНО (v4.0 — проблема #39): защита от зацикливания при ошибках.
        """
        consecutive_errors = 0
        max_consecutive_errors = 10
        error_cooldown = 5.0  # Увеличиваем задержку при ошибках

        while self._running:
            # Проверяем, существует ли активный лог
            if not self._active_log or not self._active_log.exists():
                # ИСПРАВЛЕНО: ищем новый лог вместо зацикливания
                logger.debug("Active log file not found, searching for new log...")
                new_log = self._find_latest_log()

                if new_log and new_log != self._active_log:
                    logger.info(f"Switching to new log file: {new_log.name}")
                    self._active_log = new_log
                    self._file_position = 0
                    consecutive_errors = 0  # Сбрасываем счётчик ошибок
                else:
                    # Нет доступных логов — ждём дольше
                    await asyncio.sleep(2.0)
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

                        # Успешное чтение — сбрасываем счётчик ошибок
                        consecutive_errors = 0

            except FileNotFoundError:
                # Файл был удалён (ротация логов)
                logger.info(f"Log file removed: {self._active_log.name}")
                self._active_log = None
                self._file_position = 0
                consecutive_errors = 0

            except PermissionError:
                consecutive_errors += 1
                logger.warning(
                    f"Permission denied reading {self._active_log.name} "
                    f"(errors: {consecutive_errors})"
                )

                if consecutive_errors >= max_consecutive_errors:
                    logger.error(
                        f"Too many consecutive errors ({consecutive_errors}), "
                        f"switching to new log file"
                    )
                    self._active_log = None
                    self._file_position = 0
                    consecutive_errors = 0

            except Exception as e:
                consecutive_errors += 1
                logger.error(f"Error tailing log (attempt {consecutive_errors}): {e}")

                # ИСПРАВЛЕНО: при слишком многих ошибках — переключаемся на новый лог
                if consecutive_errors >= max_consecutive_errors:
                    logger.error(
                        f"Too many consecutive errors ({consecutive_errors}), "
                        f"searching for new log file"
                    )
                    new_log = self._find_latest_log()

                    if new_log and new_log != self._active_log:
                        logger.info(f"Switching to new log: {new_log.name}")
                        self._active_log = new_log
                        self._file_position = 0
                        consecutive_errors = 0
                    else:
                        # Нет альтернативы — увеличиваем задержку
                        logger.warning(
                            "No alternative log file found, increasing delay"
                        )
                        await asyncio.sleep(error_cooldown * consecutive_errors)
                        continue

            # ИСПРАВЛЕНО: адаптивная задержка на основе количества ошибок
            if consecutive_errors > 0:
                # Увеличиваем задержку при ошибках
                delay = min(0.5 * (2 ** min(consecutive_errors, 5)), 10.0)
                await asyncio.sleep(delay)
            else:
                # Нормальная задержка
                await asyncio.sleep(0.5)
