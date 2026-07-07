"""
Mode Manager — управление режимами работы системы.
Обеспечивает graceful degradation при потере LLM API или других компонентов.
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from enum import Enum
from datetime import datetime
import httpx
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("ModeManager")


class OperationMode(Enum):
    """Режимы работы системы."""

    FULL_AI = "full_ai"  # Все агенты активны, LLM работает
    SAFE_AUTONOMOUS = "safe"  # Только Watcher + Guardian, без Strategist
    MANUAL = "manual"  # Только мониторинг, без автодействий
    SIMULATION = "simulation"  # Режим симуляции (Fake NINA/PHD2)


class ModeManager:
    """
    Менеджер режимов работы системы.

    Responsibilities:
    - Мониторинг здоровья LLM API (Ollama)
    - Автоматическое переключение в SAFE_AUTONOMOUS при потере LLM
    - Управление разрешениями для агентов в зависимости от режима
    - Публикация событий смены режима

    Режимы:
    - FULL_AI: все агенты активны, Strategist может оптимизировать параметры
    - SAFE_AUTONOMOUS: только Watcher и Guardian, без оптимизации
    - MANUAL: только мониторинг, без автодействий
    - SIMULATION: используется Fake NINA/PHD2 для тестов
    """

    def __init__(self):
        self.current_mode = OperationMode.FULL_AI
        self.llm_healthy = True
        self._health_check_task: Optional[asyncio.Task] = None
        self._running = False

        # Разрешения агентов для каждого режима
        self.agent_permissions = {
            OperationMode.FULL_AI: {
                "Watcher": True,
                "Guardian": True,
                "Diagnostician": True,
                "Strategist": True,
                "Scheduler": True,
                "Auditor": True,
                "Calibrator": True,
                "Copilot": True,
            },
            OperationMode.SAFE_AUTONOMOUS: {
                "Watcher": True,
                "Guardian": True,
                "Diagnostician": False,
                "Strategist": False,
                "Scheduler": False,
                "Auditor": False,
                "Calibrator": False,
                "Copilot": True,
            },
            OperationMode.MANUAL: {
                "Watcher": True,
                "Guardian": False,
                "Diagnostician": False,
                "Strategist": False,
                "Scheduler": False,
                "Auditor": False,
                "Calibrator": False,
                "Copilot": True,
            },
            OperationMode.SIMULATION: {
                "Watcher": True,
                "Guardian": True,
                "Diagnostician": True,
                "Strategist": True,
                "Scheduler": True,
                "Auditor": True,
                "Calibrator": True,
                "Copilot": True,
            },
        }

    async def start(self):
        """Запускает менеджер режимов."""
        if self._running:
            return

        self._running = True

        # Запускаем health check loop
        self._health_check_task = asyncio.create_task(self._health_check_loop())

        logger.info(
            f"✅ Mode Manager started (current mode: {self.current_mode.value})"
        )

    async def stop(self):
        """Останавливает менеджер режимов."""
        self._running = False

        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        logger.info("🛑 Mode Manager stopped")

    async def set_mode(self, mode: OperationMode, reason: str = "Manual override"):
        """Устанавливает режим работы системы."""
        old_mode = self.current_mode
        self.current_mode = mode

        logger.info(
            f"🔄 Mode changed: {old_mode.value} -> {mode.value} (reason: {reason})"
        )

        # Публикуем событие смены режима
        await event_bus.publish(
            "MODE_CHANGED",
            {
                "old_mode": old_mode.value,
                "new_mode": mode.value,
                "reason": reason,
                "timestamp": datetime.now().isoformat(),
            },
        )

        # Логируем в ObservatoryState
        from app.agents.observatory_state import observatory_state

        observatory_state.log_ai_action(
            agent="ModeManager",
            action=f"Mode changed to {mode.value}",
            reason=reason,
            result="Mode updated",
        )

    def is_agent_allowed(self, agent_name: str) -> bool:
        """Проверяет, разрешен ли агент в текущем режиме."""
        permissions = self.agent_permissions.get(self.current_mode, {})
        return permissions.get(agent_name, False)

    async def _health_check_loop(self):
        """Периодически проверяет здоровье LLM API."""
        while self._running:
            try:
                await self._check_llm_health()
                await asyncio.sleep(30)  # Проверка каждые 30 секунд

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(10)

    async def _check_llm_health(self):
        """Проверяет доступность LLM API (Ollama)."""
        try:
            ollama_host = settings.ai_settings.ollama_host

            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get(f"{ollama_host}/api/tags")

                is_healthy = response.status_code == 200

                if is_healthy != self.llm_healthy:
                    self.llm_healthy = is_healthy

                    if is_healthy:
                        logger.info("✅ LLM API recovered")

                        # Возвращаемся в FULL_AI если были в SAFE_AUTONOMOUS
                        if self.current_mode == OperationMode.SAFE_AUTONOMOUS:
                            await self.set_mode(
                                OperationMode.FULL_AI, reason="LLM API recovered"
                            )
                    else:
                        logger.warning("⚠️ LLM API unreachable")

                        # Переключаемся в SAFE_AUTONOMOUS если были в FULL_AI
                        if self.current_mode == OperationMode.FULL_AI:
                            await self.set_mode(
                                OperationMode.SAFE_AUTONOMOUS,
                                reason="LLM API unreachable",
                            )

                            # Публикуем алерт
                            await event_bus.publish(
                                "ALERT",
                                {
                                    "level": "WARNING",
                                    "message": "LLM API unreachable, switched to safe-autonomous mode",
                                    "agent": "ModeManager",
                                    "timestamp": datetime.now().isoformat(),
                                },
                            )

        except httpx.ConnectError:
            if self.llm_healthy:
                self.llm_healthy = False
                logger.warning("⚠️ Cannot connect to LLM API")

                if self.current_mode == OperationMode.FULL_AI:
                    await self.set_mode(
                        OperationMode.SAFE_AUTONOMOUS,
                        reason="LLM API connection failed",
                    )

        except Exception as e:
            logger.error(f"Error checking LLM health: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику Mode Manager."""
        return {
            "current_mode": self.current_mode.value,
            "llm_healthy": self.llm_healthy,
            "agent_permissions": self.agent_permissions.get(self.current_mode, {}),
        }


# Singleton instance
mode_manager = ModeManager()
