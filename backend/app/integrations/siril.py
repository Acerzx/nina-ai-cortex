"""
Siril Integration — интеграция с Siril для обработки изображений.
Заглушка для идеи 7: автоматизация preprocessing через SirilIC плагин.

Текущая реализация:
- SirilDetector: находит Siril в системе
- ManualSirilBridge: логирует инструкции вместо реального выполнения

Будущая реализация (когда SirilIC API стабилизируется):
- Subprocess management для Siril
- Автоматический запуск preprocessing workflow
- Мониторинг прогресса через SirilIC HTTP API

Feature flag: feature_flags.integrations.siril_enabled
"""

import logging
import shutil
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime

from app.core.config import settings

logger = logging.getLogger("SirilIntegration")


class SirilBridge(ABC):
    """Абстрактный класс для интеграции с Siril."""

    @abstractmethod
    async def is_available(self) -> bool:
        """Проверяет доступность Siril."""
        pass

    @abstractmethod
    async def preprocess_session(
        self,
        session_dir: Path,
    ) -> Dict[str, Any]:
        """Запускает preprocessing сессии в Siril."""
        pass

    @abstractmethod
    def get_siril_path(self) -> Optional[Path]:
        """Возвращает путь к Siril."""
        pass


class SirilDetector:
    """Детектор Siril в системе."""

    @staticmethod
    def find_siril() -> Optional[Path]:
        """Ищет Siril в стандартных путях."""
        import os

        # Windows
        if os.name == "nt":
            possible_paths = [
                Path("C:/Program Files/Siril/bin/siril.exe"),
                Path("C:/Program Files (x86)/Siril/bin/siril.exe"),
                Path(
                    os.path.expanduser("~/AppData/Local/Programs/Siril/bin/siril.exe")
                ),
            ]
            for path in possible_paths:
                if path.exists():
                    return path

        # Linux/macOS
        siril_which = shutil.which("siril")
        if siril_which:
            return Path(siril_which)

        return None

    @staticmethod
    def check_sirilic_plugin(siril_path: Path) -> bool:
        """Проверяет наличие SirilIC плагина."""
        if not siril_path:
            return False

        plugins_dir = siril_path.parent / "plugins"
        if not plugins_dir.exists():
            return False

        return any("sirilic" in p.name.lower() for p in plugins_dir.glob("*"))


class ManualSirilBridge(SirilBridge):
    """
    Ручной Siril bridge — логирует инструкции вместо выполнения.
    Используется когда Siril недоступен или integration отключена.
    """

    def __init__(self):
        self._siril_path = SirilDetector.find_siril()
        self._sirilic_available = (
            SirilDetector.check_sirilic_plugin(self._siril_path)
            if self._siril_path
            else False
        )

        self._stats = {
            "instructions_logged": 0,
        }

        logger.info(
            f"🎨 ManualSirilBridge initialized "
            f"(Siril found: {self._siril_path is not None}, "
            f"SirilIC: {self._sirilic_available})"
        )

    async def is_available(self) -> bool:
        return self._siril_path is not None and self._sirilic_available

    async def preprocess_session(
        self,
        session_dir: Path,
    ) -> Dict[str, Any]:
        """Логирует инструкцию для Siril preprocessing."""
        if not await self.is_available():
            return {
                "status": "unavailable",
                "message": "Siril or SirilIC not available",
                "instructions": self._generate_instructions(session_dir),
            }

        instructions = self._generate_instructions(session_dir)

        logger.info(f"📋 Siril preprocessing instructions for {session_dir.name}:")
        for i, instruction in enumerate(instructions, 1):
            logger.info(f"   {i}. {instruction}")

        self._stats["instructions_logged"] += 1

        return {
            "status": "instructions_generated",
            "message": "Manual mode — follow instructions in Siril",
            "instructions": instructions,
            "session_dir": str(session_dir),
        }

    def _generate_instructions(self, session_dir: Path) -> List[str]:
        """Генерирует пошаговые инструкции для Siril."""
        return [
            f"Откройте Siril",
            f"Перейдите в директорию: {session_dir}",
            f"Загрузите скрипт preprocessing (или используйте SirilIC)",
            f"Запустите калибровку (Bias, Dark, Flat)",
            f"Выполните дебайеринг (если OSC)",
            f"Запустите регистрацию звёзд",
            f"Выполните стеккирование",
            f"Сохраните результат как master_light.fit",
        ]

    def get_siril_path(self) -> Optional[Path]:
        return self._siril_path

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "siril_found": self._siril_path is not None,
            "siril_path": str(self._siril_path) if self._siril_path else None,
            "sirilic_available": self._sirilic_available,
        }


class SubprocessSirilBridgeStub(SirilBridge):
    """
    STUB для будущего subprocess-based Siril bridge.
    Не реализован — будет создан когда SirilIC API стабилизируется.
    """

    async def is_available(self) -> bool:
        return False

    async def preprocess_session(self, session_dir: Path) -> Dict[str, Any]:
        return {"status": "not_implemented"}

    def get_siril_path(self) -> Optional[Path]:
        return None


# ============================================================================
# FACTORY
# ============================================================================
def create_siril_bridge() -> SirilBridge:
    """Создаёт Siril bridge на основе feature flag."""
    siril_enabled = False
    try:
        ff = getattr(settings, "feature_flags", None)
        if ff:
            int_ff = getattr(ff, "integrations", None)
            if int_ff:
                siril_enabled = getattr(int_ff, "siril_enabled", False)
    except Exception:
        pass

    if siril_enabled:
        # В будущем здесь будет реальная реализация
        # if SirilDetector.find_siril():
        #     return SubprocessSirilBridge()
        pass

    return ManualSirilBridge()


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
siril_bridge = create_siril_bridge()
