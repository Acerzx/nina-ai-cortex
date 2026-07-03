import asyncio
import logging
from pathlib import Path
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class NinaLogTailer:
    def __init__(self):
        self.settings = get_settings()
        self.log_path = Path(self.settings.nina_environment.logs_dir) / "NINA.log"
        self.running = False

    async def start(self):
        if not self.log_path.exists():
            logger.warning(f"⚠️ NINA.log not found at {self.log_path}")
            return

        self.running = True
        logger.info(f"👁️ Starting LogTailer on {self.log_path.name}")

        # Открываем файл и переходим в конец
        with open(self.log_path, "r", encoding="utf-8") as f:
            f.seek(0, 2)  # End of file

            while self.running:
                line = f.readline()
                if line:
                    await self.process_line(line)
                else:
                    await asyncio.sleep(0.5)  # Ждем новые записи

    async def process_line(self, line: str):
        # Простейший роутинг ошибок и триггеров
        if "ERROR" in line or "FATAL" in line:
            logger.error(f"🚨 NINA ERROR: {line.strip()}")
            # Здесь будет отправка в AI-агента или WebSocket на Frontend
        elif "Trigger" in line and "fired" in line.lower():
            logger.info(f"⚡ TRIGGER FIRED: {line.strip()}")
        elif "Unsafe" in line or "Safety" in line:
            logger.warning(f"🛡️ SAFETY EVENT: {line.strip()}")

    def stop(self):
        self.running = False
