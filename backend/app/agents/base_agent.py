"""
Base Agent — базовый класс для всех AI-агентов в Multi-Agent Swarm.
Обеспечивает единый интерфейс, логирование решений и интеграцию с ObservatoryState.
ИСПРАВЛЕНО (рефакторинг v3):
- Удалён get_recent_decisions() — дублирует функциональность Orchestrator
- get_stats() упрощён (без recent_decisions)
- Template Method pattern сохранён
ИСПРАВЛЕНО (В-2):
- log_decision() сохраняет ссылки на фоновые задачи в _background_tasks
- Предотвращает RuntimeWarning "Task was destroyed but it is pending"
- Добавлен метод wait_for_background_tasks() для graceful shutdown
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Set
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.observatory_state import observatory_state
from app.core.rag_engine import rag_engine

logger = logging.getLogger("BaseAgent")


class AgentDecision(BaseModel):
    """Структура решения агента (для Decision Audit Trail)."""

    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    agent: str
    decision_type: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)
    rationale: str
    confidence: float = Field(ge=0.0, le=1.0)
    outcome: Optional[str] = None
    hindsight_verdict: Optional[str] = None


class AgentContext(BaseModel):
    """Контекст, передаваемый агенту для принятия решения."""

    current_metrics: Dict[str, Any]
    weather: Dict[str, Any]
    astronomy: Dict[str, Any]
    sequence_state: Dict[str, Any]
    safety_status: str
    active_alerts: List[Dict[str, Any]]
    rag_context: Optional[str] = None
    custom_data: Dict[str, Any] = Field(default_factory=dict)


class BaseAgent(ABC):
    """
    Базовый класс для всех AI-агентов.

    Архитектурные принципы:
    - Все агенты обращаются к ObservatoryState для получения данных
    - Все решения логируются в Decision Audit Trail
    - Агенты используют RAG для получения контекста из истории
    - Приоритет: Safety > Quality > Optimization
    - История решений хранится в Orchestrator (единый источник правды)

    ИСПРАВЛЕНО (В-2):
    - Фоновые задачи сохраняются в _background_tasks
    - Предотвращает потерю задач из-за garbage collection
    """

    def __init__(self, name: str, role: str):
        self.name = name
        self.role = role
        self._decision_log: List[AgentDecision] = []
        self._last_action_time: Optional[datetime] = None
        self._is_running = False

        # ИСПРАВЛЕНО (В-2): Хранение ссылок на фоновые задачи
        # Предотвращает RuntimeWarning "Task was destroyed but it is pending"
        # в Python 3.12+ при высокой нагрузке
        self._background_tasks: Set[asyncio.Task] = set()

    async def initialize(self):
        """Инициализация агента (подписка на события, загрузка контекста)."""
        self._is_running = True
        logger.info(f"🤖 Agent '{self.name}' ({self.role}) initialized")

    async def shutdown(self):
        """
        Корректное завершение работы агента.
        ИСПРАВЛЕНО (В-2): Ожидает завершения всех фоновых задач.
        """
        self._is_running = False

        # Ждём завершения всех фоновых задач
        await self.wait_for_background_tasks()

        logger.info(f"🛑 Agent '{self.name}' shutdown")

    async def wait_for_background_tasks(self, timeout: float = 5.0) -> None:
        """
        Ожидает завершения всех фоновых задач с таймаутом.
        ИСПРАВЛЕНО (В-2): Гарантирует, что все log_ai_action вызовы завершатся.

        Args:
            timeout: Максимальное время ожидания (секунды)
        """
        if not self._background_tasks:
            return

        pending = [t for t in self._background_tasks if not t.done()]
        if not pending:
            return

        logger.debug(
            f"⏳ Waiting for {len(pending)} background tasks "
            f"of agent '{self.name}' (timeout: {timeout}s)..."
        )

        try:
            done, still_pending = await asyncio.wait(pending, timeout=timeout)

            # Отменяем задачи, которые не успели завершиться
            for task in still_pending:
                task.cancel()
                logger.warning(
                    f"⚠️ Background task of '{self.name}' cancelled (timeout exceeded)"
                )

            # Собираем результаты отменённых задач
            if still_pending:
                await asyncio.gather(*still_pending, return_exceptions=True)

            logger.debug(
                f"✅ Background tasks of '{self.name}' completed: "
                f"{len(done)} done, {len(still_pending)} cancelled"
            )
        except Exception as e:
            logger.error(f"❌ Error waiting for background tasks of '{self.name}': {e}")

    # ========================================================================
    # TEMPLATE METHOD: analyze()
    # ========================================================================

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        TEMPLATE METHOD: Анализирует контекст и принимает решение.

        Алгоритм:
        1. Валидация контекста (hook: _validate_context)
        2. Сбор дополнительных данных (hook: _gather_data)
        3. Анализ и принятие решения (hook: _make_decision)
        4. Логирование решения (hook: _log_decision)
        """
        if not await self._validate_context(context):
            logger.debug(f"{self.name}: Context validation failed, skipping")
            return None

        enriched_context = await self._gather_data(context)
        decision = await self._make_decision(enriched_context)

        if decision:
            await self._log_decision(decision)

        return decision

    # ========================================================================
    # TEMPLATE METHOD: execute()
    # ========================================================================

    async def execute(self, decision: AgentDecision) -> bool:
        """
        TEMPLATE METHOD: Выполняет принятое решение.

        Алгоритм:
        1. Валидация решения (hook: _validate_decision)
        2. Подготовка к выполнению (hook: _prepare_execution)
        3. Выполнение действия (hook: _perform_action)
        4. Постобработка (hook: _post_process)
        """
        if not await self._validate_decision(decision):
            logger.warning(f"{self.name}: Decision validation failed")
            return False

        await self._prepare_execution(decision)

        try:
            success = await self._perform_action(decision)
        except Exception as e:
            logger.error(f"{self.name}: Action failed with error: {e}")
            success = False

        await self._post_process(decision, success)

        return success

    # ========================================================================
    # HOOKS для analyze()
    # ========================================================================

    async def _validate_context(self, context: AgentContext) -> bool:
        """HOOK: Валидация контекста. По умолчанию True."""
        return True

    async def _gather_data(self, context: AgentContext) -> AgentContext:
        """HOOK: Сбор дополнительных данных. По умолчанию без изменений."""
        return context

    @abstractmethod
    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """HOOK (АБСТРАКТНЫЙ): Принятие решения. ДОЛЖЕН быть реализован."""
        pass

    async def _log_decision(self, decision: AgentDecision):
        """HOOK: Логирование решения. По умолчанию стандартное."""
        self.log_decision(decision)

    # ========================================================================
    # HOOKS для execute()
    # ========================================================================

    async def _validate_decision(self, decision: AgentDecision) -> bool:
        """HOOK: Валидация решения. По умолчанию True."""
        return True

    async def _prepare_execution(self, decision: AgentDecision):
        """HOOK: Подготовка выполнения. По умолчанию ничего."""
        pass

    @abstractmethod
    async def _perform_action(self, decision: AgentDecision) -> bool:
        """HOOK (АБСТРАКТНЫЙ): Выполнение действия. ДОЛЖЕН быть реализован."""
        pass

    async def _post_process(self, decision: AgentDecision, success: bool):
        """HOOK: Постобработка. По умолчанию ничего."""
        pass

    # ========================================================================
    # ОБЩИЕ МЕТОДЫ
    # ========================================================================

    async def get_rag_context(self, query: str, max_tokens: int = 2000) -> str:
        """Получает контекст из RAG для принятия решения."""
        try:
            context = await rag_engine.get_context(query=query, max_tokens=max_tokens)
            return context
        except Exception as e:
            logger.error(f"Failed to get RAG context: {e}")
            return "Контекст недоступен"

    def log_decision(self, decision: AgentDecision):
        """
        Логирует решение в Decision Audit Trail.

        ИСПРАВЛЕНО (v4.2): observatory_state.log_ai_action() — async метод,
        вызывается через asyncio.create_task() для fire-and-forget.

        ИСПРАВЛЕНО (В-2): Ссылки на задачи сохраняются в _background_tasks.
        Раньше задачи могли быть уничтожены сборщиком мусора до выполнения,
        что приводило к RuntimeWarning в Python 3.12+ и потере логов.
        """
        self._decision_log.append(decision)
        if len(self._decision_log) > 1000:
            self._decision_log = self._decision_log[-1000:]

        # ИСПРАВЛЕНО (v4.2): log_ai_action — это async метод
        # Вызываем через create_task для fire-and-forget
        try:
            loop = asyncio.get_running_loop()
            task = loop.create_task(
                observatory_state.log_ai_action(
                    agent=self.name,
                    action=decision.decision_type,
                    reason=decision.rationale,
                    result=f"Confidence: {decision.confidence:.2f}",
                )
            )

            # ИСПРАВЛЕНО (В-2): Сохраняем ссылку на задачу
            self._background_tasks.add(task)

            # Автоматически удаляем задачу из set после завершения
            task.add_done_callback(self._remove_background_task)

        except RuntimeError:
            # Нет запущенного event loop — пропускаем логирование
            pass

        logger.info(
            f"📝 [{self.name}] Decision: {decision.decision_type} "
            f"(confidence: {decision.confidence:.2f}) - {decision.rationale[:100]}"
        )

    def _remove_background_task(self, task: asyncio.Task) -> None:
        """
        Удаляет задачу из _background_tasks после завершения.
        ИСПРАВЛЕНО (В-2): Вызывается через add_done_callback.
        """
        self._background_tasks.discard(task)

        # Логируем исключения, если они были
        if not task.cancelled() and task.exception():
            logger.warning(
                f"⚠️ Background task of '{self.name}' failed: {task.exception()}"
            )

    async def update_outcome(self, decision: AgentDecision, outcome: str):
        """
        Обновляет outcome решения (для Hindsight Verdict).
        Вызывается после выполнения действия и оценки результата.
        """
        decision.outcome = outcome

        if outcome == "SUCCESS":
            decision.hindsight_verdict = "CORRECT"
        elif outcome == "FAILED":
            decision.hindsight_verdict = "WRONG"
        elif outcome == "PARTIAL":
            decision.hindsight_verdict = "SUBOPTIMAL"
        else:
            decision.hindsight_verdict = "UNKNOWN"

        logger.info(
            f"🔍 [{self.name}] Hindsight: {decision.decision_type} -> "
            f"{decision.hindsight_verdict} (outcome: {outcome})"
        )

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику агента.

        ИСПРАВЛЕНО (v3): Удалён recent_decisions — история решений
        хранится централизованно в Orchestrator и Decision Audit Trail.

        Для получения истории решений:
        - orchestrator.get_recent_decisions() — in-memory cache
        - /api/v1/audit/decisions — SQLite persistence

        ИСПРАВЛЕНО (В-2): Добавлена статистика по фоновым задачам.
        """
        active_tasks = len([t for t in self._background_tasks if not t.done()])

        return {
            "name": self.name,
            "role": self.role,
            "is_running": self._is_running,
            "total_decisions": len(self._decision_log),
            "last_action": (
                self._last_action_time.isoformat() if self._last_action_time else None
            ),
            "background_tasks": {
                "total": len(self._background_tasks),
                "active": active_tasks,
            },
        }
