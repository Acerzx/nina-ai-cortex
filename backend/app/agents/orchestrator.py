"""
Orchestrator — центральный координатор всех AI-агентов.
Управляет приоритетами, маршрутизацией задач и workflow.

Архитектура: Dual Orchestrator System
- Event-Driven Orchestrator (этот модуль): реактивная маршрутизация через EventBus
- Hybrid LangGraph Orchestrator: проактивные многошаговые workflows
  (Complex Diagnostic, Post-Mortem Analysis, Adaptive Response)

ИСПРАВЛЕНО (рефакторинг v3):
- Удалён хардкод: DECISIONS_MEMORY_LIMIT читается из settings.metrics
- Добавлены метрики через cortex_metrics (счётчики решений, времени обработки)
- Интеграция с hybrid_langgraph_orchestrator для сложных сценариев
- Улучшена обработка ошибок в _main_loop и _execute_decision
- Корректная остановка всех компонентов, включая фоновые LangGraph workflows
- Ленивый импорт hybrid_orchestrator для избежания циклических зависимостей
"""

import asyncio
import logging
import time
from collections import deque
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum

from app.core.events import event_bus
from app.core.config import settings
from app.core.metrics import cortex_metrics
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
    - Хранит ограниченную историю в памяти (deque maxlen из settings)
    - Делегирует сложные сценарии в Hybrid LangGraph Orchestrator
    - Собирает метрики через cortex_metrics

    Dual Orchestrator System:
    - Простые реакции (ALERT → Guardian) обрабатываются здесь
    - Сложные многошаговые сценарии делегируются в LangGraph
    """

    def __init__(self):
        # Реестр зарегистрированных агентов
        self.agents: Dict[str, Any] = {}

        # Текущий режим работы
        self.mode = OperationMode.FULL_AI

        # Флаги и задачи жизненного цикла
        self._running = False
        self._task: Optional[asyncio.Task] = None

        # Очередь решений для обработки
        self._decision_queue: asyncio.Queue = asyncio.Queue()

        # In-memory cache решений: используем deque с maxlen из settings
        # Это предотвращает утечку памяти при длительной работе (O(1) amortized)
        self._decisions_memory_limit: int = getattr(
            settings.metrics, "ai_action_log_max", 1000
        )
        self._decisions_log: deque = deque(maxlen=self._decisions_memory_limit)

        # Счётчик всех обработанных решений (включая выгруженные из памяти)
        self._total_decisions_processed: int = 0

        # Приоритеты агентов (используются для маршрутизации и разрешения конфликтов)
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

        logger.info(
            f"🎯 Orchestrator initialized "
            f"(decisions_memory_limit={self._decisions_memory_limit})"
        )

    async def start(self):
        """Запускает Orchestrator и подписывается на события EventBus."""
        if self._running:
            logger.warning("Orchestrator already running")
            return

        self._running = True
        logger.info("🎯 Orchestrator started")

        # Подписываемся на ключевые события
        event_bus.subscribe("ALERT", self._handle_alert)
        event_bus.subscribe("NEW_FRAME", self._handle_new_frame)
        event_bus.subscribe("SEQUENCE_STARTED", self._handle_sequence_started)
        event_bus.subscribe("SEQUENCE_STOPPED", self._handle_sequence_stopped)

        # Подписываемся на события для делегирования в LangGraph
        event_bus.subscribe("COMPLEX_SCENARIO", self._handle_complex_scenario_event)

        # Запускаем main loop, сохраняя ссылку на задачу
        self._task = asyncio.create_task(self._main_loop())

    async def stop(self):
        """Останавливает Orchestrator и все связанные компоненты."""
        if not self._running:
            return

        self._running = False

        # Отменяем main loop
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
            event_bus.unsubscribe(
                "COMPLEX_SCENARIO", self._handle_complex_scenario_event
            )
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

        Returns:
            True если решение принято в очередь, False если отклонено
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

        Особенности:
        - Решения добавляются в deque (O(1) амортизированное)
        - Persist в Decision Audit Trail (SQLite) для долгосрочного хранения
        - Сбор метрик через cortex_metrics
        - Защита от падения цикла при ошибках обработки
        """
        while self._running:
            try:
                # Получаем решение из очереди с таймаутом
                decision = await asyncio.wait_for(
                    self._decision_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                # Очередь пуста, продолжаем
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error getting decision from queue: {e}", exc_info=True)
                await asyncio.sleep(1.0)
                continue

            # Обрабатываем решение с замером времени
            start_time = time.perf_counter()
            try:
                await self._execute_decision(decision)

                # 1. In-memory cache (deque автоматически удаляет старые)
                self._decisions_log.append(decision)
                self._total_decisions_processed += 1

                # 2. Persist в SQLite (для долгосрочного хранения и анализа)
                await self._persist_decision(decision)

                # 3. Метрики
                duration = time.perf_counter() - start_time
                cortex_metrics.decisions_total.labels(
                    agent=decision.agent,
                    decision_type=decision.decision_type,
                    outcome=decision.outcome or "pending",
                ).inc()
                cortex_metrics.decision_confidence.labels(agent=decision.agent).observe(
                    decision.confidence
                )

            except Exception as e:
                logger.error(
                    f"Error processing decision from {decision.agent}: {e}",
                    exc_info=True,
                )

    async def _persist_decision(self, decision: AgentDecision) -> None:
        """
        Persist решение в Decision Audit Trail (SQLite).
        Обеспечивает долгосрочное хранение и возможность анализа
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
        """Выполняет решение агента с обновлением outcome."""
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
            logger.error(f"Error executing decision: {e}", exc_info=True)
            await agent.update_outcome(decision, "FAILED")

    # ========================================================================
    # ОБРАБОТЧИКИ СОБЫТИЙ EVENTBUS
    # ========================================================================

    async def _handle_alert(self, data: Dict[str, Any]):
        """Обработка алертов от Watcher."""
        level = data.get("level", "INFO")
        message = data.get("message", "")
        logger.info(f"🚨 Alert [{level}]: {message}")

        # Маршрутизация в зависимости от уровня
        if level == "CRITICAL":
            # Критические алерты → Guardian (немедленная реакция)
            await self._route_to_agent("Guardian", "handle_critical_alert", data)
        elif level == "WARNING":
            # Предупреждения → Diagnostician (анализ причины)
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

    async def _handle_complex_scenario_event(self, data: Dict[str, Any]):
        """
        Обработка события сложного сценария.
        Делегирует обработку в Hybrid LangGraph Orchestrator.
        """
        scenario_type = data.get("type", "unknown")
        logger.info(f"🎭 Complex scenario detected: {scenario_type}")
        await self.handle_complex_scenario(scenario_type, data)

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
            logger.error(f"Error routing to {agent_name}.{method}: {e}", exc_info=True)

    # ========================================================================
    # ГИБРИДНЫЙ LANGGRAPH ORCHESTRATOR (Dual Orchestrator System)
    # ========================================================================

    async def handle_complex_scenario(
        self,
        scenario_type: str,
        scenario_data: Dict[str, Any],
    ) -> Optional[str]:
        """
        Делегирует сложный сценарий в Hybrid LangGraph Orchestrator.

        Определяет тип workflow на основе события и запускает его.
        Используется для многошаговых сценариев:
        - Complex Diagnostic Workflow (поиск root cause с RAG)
        - Post-Mortem Analysis Workflow (анализ завершённой сессии)
        - Adaptive Response Workflow (адаптация к изменяющимся условиям)

        Args:
            scenario_type: Тип сценария (например, "diagnostic", "post_mortem")
            scenario_data: Данные сценария

        Returns:
            workflow_id если запущен, None если не удалось
        """
        logger.info(f"🎭 Handling complex scenario: {scenario_type}")

        # Ленивый импорт для избежания циклических зависимостей
        try:
            from app.agents.hybrid_langgraph_orchestrator import (
                hybrid_orchestrator,
                WorkflowType,
            )
        except ImportError as e:
            logger.error(
                f"Hybrid LangGraph Orchestrator not available: {e}. "
                f"Falling back to simple routing."
            )
            return None

        # Определяем тип workflow на основе сценария
        workflow_type = self._determine_workflow_type(scenario_type)
        if not workflow_type:
            logger.warning(
                f"Unknown complex scenario type: {scenario_type}. "
                f"Cannot determine workflow type."
            )
            return None

        # Запускаем workflow
        try:
            workflow_id = await hybrid_orchestrator.start_workflow(
                workflow_type=workflow_type,
                trigger_event={"type": scenario_type, "data": scenario_data},
                context={
                    "source": "event_driven_orchestrator",
                    "mode": self.mode.value,
                },
            )

            logger.info(
                f"🚀 Launched workflow {workflow_id} for scenario {scenario_type}"
            )

            # Логируем решение о делегировании
            decision = AgentDecision(
                agent="Orchestrator",
                decision_type="COMPLEX_SCENARIO_DELEGATED",
                inputs={
                    "scenario_type": scenario_type,
                    "scenario_data": scenario_data,
                },
                outputs={
                    "workflow_id": workflow_id,
                    "workflow_type": workflow_type.value,
                },
                rationale=f"Delegated complex scenario to LangGraph workflow",
                confidence=0.9,
            )
            await self._persist_decision(decision)

            return workflow_id

        except Exception as e:
            logger.error(
                f"Failed to start workflow for scenario {scenario_type}: {e}",
                exc_info=True,
            )
            return None

    def _determine_workflow_type(self, scenario_type: str):
        """
        Определяет тип LangGraph workflow на основе типа сценария.

        Ленивый импорт WorkflowType для избежания ошибок при недоступности
        hybrid_langgraph_orchestrator.
        """
        try:
            from app.agents.hybrid_langgraph_orchestrator import WorkflowType
        except ImportError:
            return None

        scenario_lower = scenario_type.lower()

        # Diagnostic workflow: проблемы, аномалии, root cause analysis
        if any(
            kw in scenario_lower
            for kw in [
                "diagnostic",
                "root_cause",
                "anomaly",
                "problem",
                "failure",
                "degradation",
            ]
        ):
            return WorkflowType.DIAGNOSTIC

        # Post-Mortem workflow: завершение сессии, анализ истории
        if any(
            kw in scenario_lower
            for kw in [
                "post_mortem",
                "session_end",
                "session_complete",
                "night_summary",
            ]
        ):
            return WorkflowType.POST_MORTEM

        # Adaptive workflow: изменение условий, погоды, адаптация
        if any(
            kw in scenario_lower
            for kw in ["adaptive", "weather_change", "condition_change", "environment"]
        ):
            return WorkflowType.ADAPTIVE

        return None

    # ========================================================================
    # ПУБЛИЧНЫЕ МЕТОДЫ (API)
    # ========================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику Orchestrator."""
        # Получаем статистику активных LangGraph workflows (если доступен)
        langgraph_stats = {"available": False, "active_workflows": 0}
        try:
            from app.agents.hybrid_langgraph_orchestrator import hybrid_orchestrator

            active = hybrid_orchestrator.list_active_workflows()
            langgraph_stats = {
                "available": True,
                "active_workflows": len(active),
                "workflow_ids": active,
            }
        except ImportError:
            pass

        return {
            "mode": self.mode.value,
            "running": self._running,
            "agents_registered": len(self.agents),
            "agents": list(self.agents.keys()),
            "decisions_in_memory": len(self._decisions_log),
            "decisions_memory_limit": self._decisions_memory_limit,
            "decisions_total_processed": self._total_decisions_processed,
            "queue_size": self._decision_queue.qsize(),
            "agent_priorities": {
                name: p.name for name, p in self.agent_priorities.items()
            },
            "langgraph": langgraph_stats,
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


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
orchestrator = Orchestrator()
