"""
Orchestrator — центральный координатор всех AI-агентов.
Управляет приоритетами, маршрутизацией задач и workflow.

ИСПРАВЛЕНО (audit 10.2): _decisions_log теперь использует collections.deque
с ограничением maxlen=1000 для предотвращения утечки памяти.
Решения одновременно persist-ятся в Decision Audit Trail (SQLite).
"""

import asyncio
import logging
from collections import deque
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field
from app.core.events import event_bus
from app.agents.observatory_state import observatory_state
from app.agents.base_agent import AgentDecision, AgentContext
from app.storage.decision_audit import decision_audit, DecisionRecord

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
    - Логирует все решения в Decision Audit Trail (persistent)
    - Хранит ограниченную историю в памяти (deque maxlen=1000)
    - Реализует паттерн Supervisor

    ИСПРАВЛЕНО (audit 10.2): _decisions_log использует deque с maxlen=1000
    вместо list с ручной обрезкой до 10000. Это:
    - Предотвращает утечку памяти при длительной работе
    - Устраняет costly list re-creation (срез [-10000:])
    - O(1) амортизированное добавление вместо O(n)
    """

    # Максимальное количество решений в памяти (in-memory cache)
    DECISIONS_MEMORY_LIMIT: int = 1000

    def __init__(self):
        self.agents: Dict[str, Any] = {}
        self.mode = OperationMode.FULL_AI
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._decision_queue: asyncio.Queue = asyncio.Queue()

        # ИСПРАВЛЕНО (audit 10.2): используем deque с maxlen вместо list
        # deque автоматически удаляет самые старые элементы при превышении лимита
        self._decisions_log: deque = deque(maxlen=self.DECISIONS_MEMORY_LIMIT)

        # Счетчик всех обработанных решений (включая выгруженные из памяти)
        self._total_decisions_processed: int = 0

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

        # Запускаем main loop, сохраняя ссылку на задачу (audit 5.1)
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
        try:
            event_bus.unsubscribe("ALERT", self._handle_alert)
            event_bus.unsubscribe("NEW_FRAME", self._handle_new_frame)
            event_bus.unsubscribe("SEQUENCE_STARTED", self._handle_sequence_started)
            event_bus.unsubscribe("SEQUENCE_STOPPED", self._handle_sequence_stopped)
        except Exception as e:
            logger.debug(f"Error unsubscribing from events: {e}")

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
        """
        Основной цикл обработки решений.

        ИСПРАВЛЕНО (audit 10.2): решения добавляются в deque (O(1)),
        а также persist-ятся в Decision Audit Trail (SQLite) для
        долгосрочного хранения и анализа.
        """
        while self._running:
            try:
                # Получаем решение из очереди
                decision = await asyncio.wait_for(
                    self._decision_queue.get(), timeout=1.0
                )

                # Выполняем решение
                await self._execute_decision(decision)

                # 1. In-memory cache (deque автоматически удаляет старые)
                self._decisions_log.append(decision)
                self._total_decisions_processed += 1

                # 2. Persist в SQLite (для долгосрочного хранения и анализа)
                await self._persist_decision(decision)

            except asyncio.TimeoutError:
                # Очередь пуста, продолжаем
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}", exc_info=True)
                await asyncio.sleep(1.0)

    async def _persist_decision(self, decision: AgentDecision) -> None:
        """
        Persist решение в Decision Audit Trail (SQLite).
        Это обеспечивает долгосрочное хранение и возможность анализа
        решений постфактум (hindsight verdict).
        """
        try:
            record = DecisionRecord(
                timestamp=decision.timestamp,
                agent=decision.agent,
                decision_type=decision.decision_type,
                inputs=decision.inputs,
                outputs=decision.outputs,
                rationale=decision.rationale,
                confidence=decision.confidence,
                outcome=decision.outcome,
                hindsight_verdict=decision.hindsight_verdict,
                context={"mode": self.mode.value},
            )
            await decision_audit.log_decision(record)
        except Exception as e:
            logger.warning(
                f"Failed to persist decision to audit trail: {e}. "
                f"Decision still cached in memory."
            )

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

            # Обновляем outcome в Decision Audit Trail для hindsight
            if decision.id:
                try:
                    hindsight = "CORRECT" if success else "WRONG"
                    await decision_audit.update_outcome(decision.id, outcome, hindsight)
                except Exception as e:
                    logger.debug(f"Failed to update outcome in audit: {e}")

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
            "agents": list(self.agents.keys()),
            "decisions_in_memory": len(self._decisions_log),
            "decisions_memory_limit": self.DECISIONS_MEMORY_LIMIT,
            "decisions_total_processed": self._total_decisions_processed,
            "queue_size": self._decision_queue.qsize(),
        }

    def get_recent_decisions(self, limit: int = 20) -> List[Dict[str, Any]]:
        """
        Возвращает последние N решений из in-memory cache.

        Для получения полной истории решений использовать
        Decision Audit Trail через API /api/v1/audit/decisions
        """
        # deque поддерживает итерацию в обратном порядке
        recent = list(self._decisions_log)[-limit:]
        return [d.model_dump() for d in recent]

    async def get_historical_decisions(
        self,
        agent: Optional[str] = None,
        decision_type: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 50,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        """
        Получает решения из Decision Audit Trail (SQLite).
        Используется для анализа полной истории решений.
        """
        records = await decision_audit.get_decisions(
            agent=agent,
            decision_type=decision_type,
            session_id=session_id,
            limit=limit,
            offset=offset,
        )
        return [r.model_dump() for r in records]


# Singleton instance
orchestrator = Orchestrator()
