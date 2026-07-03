import asyncio
import logging
from pathlib import Path
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class NinaLogTailer:
    def __init__(self):
        self.settings = get_settings()
        self.logs_dir = Path(self.settings.nina_environment.logs_dir)
        self.running = False
        self.current_log_path = None

    def find_latest_log(self) -> Path:
        """Находит самый свежий .log файл в папке Logs"""
        if not self.logs_dir.exists():
            return None

        # Ищем все .log файлы (N.I.N.A. создает их с таймштампом)
        logs = list(self.logs_dir.glob("*.log"))
        if not logs:
            return None

        # Сортируем по времени изменения, берем самый новый
        latest = sorted(logs, key=lambda p: p.stat().st_mtime, reverse=True)[0]
        return latest

    async def start(self):
        self.running = True
        logger.info(f"👁️ LogTailer initialized. Scanning: {self.logs_dir}")

        while self.running:
            latest_log = self.find_latest_log()

            if not latest_log:
                logger.warning(f"⚠️ No .log files found in {self.logs_dir}. Waiting...")
                await asyncio.sleep(10)
                continue

            if latest_log != self.current_log_path:
                logger.info(f"📄 Switching to latest log: {latest_log.name}")
                self.current_log_path = latest_log
                await self.tail_file(latest_log)

            await asyncio.sleep(2)  # Проверяем появление новых логов каждые 2 сек

    async def tail_file(self, file_path: Path):
        """Читает хвост конкретного файла"""
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                f.seek(0, 2)  # Переходим в конец файла

                while self.running and self.current_log_path == file_path:
                    line = f.readline()
                    if line:
                        await self.process_line(line)
                    else:
                        await asyncio.sleep(0.5)
        except Exception as e:
            logger.error(f"❌ Error reading log file {file_path}: {e}")

    async def process_line(self, line: str):
        """Фильтрует логи, чтобы не спамить в консоль"""
        line = line.strip()
        if not line:
            return

        # РЕАГИРУЕМ ТОЛЬКО НА КРИТИЧЕСКИЕ СОБЫТИЯ
        if "ERROR" in line or "FATAL" in line or "Exception" in line:
            logger.error(f"🚨 NINA ERROR: {line}")
        elif "Trigger" in line and "fired" in line.lower():
            logger.info(f"⚡ TRIGGER FIRED: {line}")
        elif "Unsafe" in line or "Safety Monitor" in line:
            logger.warning(f"🛡️ SAFETY EVENT: {line}")
        elif "Meridian Flip" in line and "Starting" in line:
            logger.info(f"🔄 MERIDIAN FLIP: {line}")
        elif "Download failed" in line or "USB" in line and "Timeout" in line:
            logger.error(f"🔌 CONNECTION ISSUE: {line}")

        # Все остальные INFO/DEBUG логи N.I.N.A. игнорируются для чистоты консоли
