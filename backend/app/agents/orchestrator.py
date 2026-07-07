"""
Orchestrator — центральный координатор всех AI-агентов.
Управляет приоритетами, маршрутизацией задач и workflow.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from app.core.events import event_bus
from app.agents.observatory_state import observatory_state
from app.agents.base_agent import AgentDecision, AgentContext

logger = logging.getLogger("Orchestrator")


class OperationMode(Enum):
    """Режимы работы системы."""

    FULL_AI = "full_ai"  # Все агенты активны, LLM работает
    SAFE_AUTONOMOUS = "safe"  # Только Watcher + Guardian, без Strategist
    MANUAL = "manual"  # Только мониторинг, без автодействий
    SIMULATION = "simulation"  # Режим симуляции (Fake NINA/PHD2)


class Priority(Enum):
    """Приоритеты агентов."""

    CRITICAL = 1  # Safety (Guardian)
    HIGH = 2  # Quality (Watcher, Diagnostician)
    MEDIUM = 3  # Optimization (Strategist, Scheduler)
    LOW = 4  # Analysis (Auditor, Calibrator)
    INFO = 5  # Interactive (Copilot)


class Orchestrator:
    """
    Центральный координатор Multi-Agent Swarm.

    Архитектура (Orchestrator-Worker Pattern):
    - Маршрутизирует задачи между агентами
    - Управляет приоритетами (Safety > Quality > Optimization)
    - Логирует все решения в Decision Audit Trail
    - Реализует паттерн Supervisor

    Workflow:
    1. Scheduler строит план на ночь
    2. Watcher мониторит метрики в реальном времени
    3. При аномалии → Diagnostician анализирует причину
    4. Strategist предлагает оптимизацию
    5. Guardian проверяет безопасность
    6. Auditor генерирует Session Digest после завершения
    7. Calibrator управляет мастер-кадрами
    """

    def __init__(self):
        self.agents: Dict[str, Any] = {}
        self.mode = OperationMode.FULL_AI
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._decision_queue: asyncio.Queue = asyncio.Queue()
        self._decisions_log: List[AgentDecision] = []

        # Приоритеты агентов
        self.agent_priorities = {
            "Guardian": Priority.CRITICAL,
            "Watcher": Priority.HIGH,
            "Diagnostician": Priority.HIGH,
            "Strategist": Priority.MEDIUM,
            "Scheduler": Priority.MEDIUM,
            "Auditor": Priority.LOW,
            "Calibrator": Priority.LOW,
            "Copilot": Priority.INFO,
        }

    async def start(self):
        """Запускает Orchestrator."""
        if self._running:
            return

        self._running = True
        logger.info("🎯 Orchestrator started")

        # Подписываемся на события
        event_bus.subscribe("ALERT", self._handle_alert)
        event_bus.subscribe("NEW_FRAME", self._handle_new_frame)
        event_bus.subscribe("SEQUENCE_STARTED", self._handle_sequence_started)
        event_bus.subscribe("SEQUENCE_STOPPED", self._handle_sequence_stopped)

        # Запускаем main loop
        self._task = asyncio.create_task(self._main_loop())

    async def stop(self):
        """Останавливает Orchestrator."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        # Отписываемся от событий
        event_bus.unsubscribe("ALERT", self._handle_alert)
        event_bus.unsubscribe("NEW_FRAME", self._handle_new_frame)
        event_bus.unsubscribe("SEQUENCE_STARTED", self._handle_sequence_started)
        event_bus.unsubscribe("SEQUENCE_STOPPED", self._handle_sequence_stopped)

        logger.info("🎯 Orchestrator stopped")

    def register_agent(self, name: str, agent: Any):
        """Регистрирует агента в Orchestrator."""
        self.agents[name] = agent
        logger.info(f"✅ Agent '{name}' registered with Orchestrator")

    async def set_mode(self, mode: OperationMode):
        """Устанавливает режим работы системы."""
        old_mode = self.mode
        self.mode = mode

        logger.info(f"🔄 Mode changed: {old_mode.value} -> {mode.value}")

        # Публикуем событие смены режима
        await event_bus.publish(
            "MODE_CHANGED",
            {
                "old_mode": old_mode.value,
                "new_mode": mode.value,
                "timestamp": datetime.now().isoformat(),
            },
        )

    async def route_decision(self, decision: AgentDecision) -> bool:
        """
        Маршрутизирует решение на выполнение.
        Проверяет режим работы и приоритеты.
        """
        # Проверка режима работы
        if self.mode == OperationMode.MANUAL:
            logger.warning(f"🛑 Decision blocked: system in MANUAL mode")
            return False

        if self.mode == OperationMode.SAFE_AUTONOMOUS:
            # В safe mode разрешены только Guardian и Watcher
            if decision.agent not in ["Guardian", "Watcher"]:
                logger.warning(
                    f"🛑 Decision blocked: agent '{decision.agent}' "
                    f"not allowed in SAFE_AUTONOMOUS mode"
                )
                return False

        # Добавляем в очередь
        await self._decision_queue.put(decision)
        return True

    async def _main_loop(self):
        """Основной цикл обработки решений."""
        while self._running:
            try:
                # Получаем решение из очереди
                decision = await asyncio.wait_for(
                    self._decision_queue.get(), timeout=1.0
                )

                # Выполняем решение
                await self._execute_decision(decision)

                # Логируем решение
                self._decisions_log.append(decision)
                if len(self._decisions_log) > 10000:
                    self._decisions_log = self._decisions_log[-10000:]

            except asyncio.TimeoutError:
                # Очередь пуста, продолжаем
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                await asyncio.sleep(1.0)

    async def _execute_decision(self, decision: AgentDecision):
        """Выполняет решение агента."""
        agent = self.agents.get(decision.agent)
        if not agent:
            logger.error(f"Agent '{decision.agent}' not found")
            return

        logger.info(
            f"⚡ Executing decision from {decision.agent}: {decision.decision_type}"
        )

        try:
            success = await agent.execute(decision)

            # Обновляем outcome
            outcome = "SUCCESS" if success else "FAILED"
            await agent.update_outcome(decision, outcome)

        except Exception as e:
            logger.error(f"Error executing decision: {e}")
            await agent.update_outcome(decision, "FAILED")

    async def _handle_alert(self, data: Dict[str, Any]):
        """Обработка алертов."""
        level = data.get("level", "INFO")
        message = data.get("message", "")

        logger.info(f"🚨 Alert [{level}]: {message}")

        # Маршрутизация в зависимости от уровня
        if level == "CRITICAL":
            # Критические алерты → Guardian
            await self._route_to_agent("Guardian", "handle_critical_alert", data)
        elif level == "WARNING":
            # Предупреждения → Diagnostician
            await self._route_to_agent("Diagnostician", "analyze_warning", data)

    async def _handle_new_frame(self, data: Dict[str, Any]):
        """Обработка нового кадра."""
        # Watcher анализирует метрики
        await self._route_to_agent("Watcher", "analyze_frame", data)

    async def _handle_sequence_started(self, data: Dict[str, Any]):
        """Обработка начала секвенсора."""
        logger.info("🚀 Sequence started")

        # Scheduler обновляет план
        await self._route_to_agent("Scheduler", "on_sequence_started", data)

        # Watcher начинает мониторинг
        await self._route_to_agent("Watcher", "start_monitoring", data)

    async def _handle_sequence_stopped(self, data: Dict[str, Any]):
        """Обработка остановки секвенсора."""
        logger.info("🛑 Sequence stopped")

        # Auditor генерирует Session Digest
        await self._route_to_agent("Auditor", "generate_session_digest", data)

        # Watcher останавливает мониторинг
        await self._route_to_agent("Watcher", "stop_monitoring", data)

    async def _route_to_agent(self, agent_name: str, method: str, data: Dict[str, Any]):
        """Маршрутизирует задачу конкретному агенту."""
        agent = self.agents.get(agent_name)
        if not agent:
            logger.warning(f"Agent '{agent_name}' not found")
            return

        try:
            method_func = getattr(agent, method, None)
            if method_func and callable(method_func):
                await method_func(data)
            else:
                logger.warning(f"Method '{method}' not found in agent '{agent_name}'")
        except Exception as e:
            logger.error(f"Error routing to {agent_name}.{method}: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику Orchestrator."""
        return {
            "mode": self.mode.value,
            "running": self._running,
            "agents_registered": len(self.agents),
            "agents": [name for name in self.agents.keys()],
            "decisions_processed": len(self._decisions_log),
            "queue_size": self._decision_queue.qsize(),
        }

    def get_recent_decisions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Возвращает последние N решений."""
        return [d.model_dump() for d in self._decisions_log[-limit:]]


# Singleton instance
orchestrator = Orchestrator()
