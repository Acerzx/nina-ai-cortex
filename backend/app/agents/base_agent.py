"""
Base Agent — базовый класс для всех AI-агентов в Multi-Agent Swarm.
Обеспечивает единый интерфейс, логирование решений и интеграцию с ObservatoryState.

ИСПРАВЛЕНО (рефакторинг v3):
- Удалён get_recent_decisions() — дублирует функциональность Orchestrator
- get_stats() упрощён (без recent_decisions)
- Template Method pattern сохранён
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
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
    """

    def __init__(self, name: str, role: str):
        self.name = name
        self.role = role
        self._decision_log: List[AgentDecision] = []
        self._last_action_time: Optional[datetime] = None
        self._is_running = False

    async def initialize(self):
        """Инициализация агента (подписка на события, загрузка контекста)."""
        self._is_running = True
        logger.info(f"🤖 Agent '{self.name}' ({self.role}) initialized")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        self._is_running = False
        logger.info(f"🛑 Agent '{self.name}' shutdown")

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
        вызываем через asyncio.create_task() для fire-and-forget.
        """
        self._decision_log.append(decision)
        if len(self._decision_log) > 1000:
            self._decision_log = self._decision_log[-1000:]

        # ИСПРАВЛЕНО (v4.2): log_ai_action — это async метод
        # Вызываем через create_task для fire-and-forget
        try:
            loop = asyncio.get_running_loop()
            loop.create_task(
                observatory_state.log_ai_action(
                    agent=self.name,
                    action=decision.decision_type,
                    reason=decision.rationale,
                    result=f"Confidence: {decision.confidence:.2f}",
                )
            )
        except RuntimeError:
            # Нет запущенного event loop — пропускаем логирование
            pass

        logger.info(
            f"📝 [{self.name}] Decision: {decision.decision_type} "
            f"(confidence: {decision.confidence:.2f}) - {decision.rationale[:100]}"
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
        """
        return {
            "name": self.name,
            "role": self.role,
            "is_running": self._is_running,
            "total_decisions": len(self._decision_log),
            "last_action": (
                self._last_action_time.isoformat() if self._last_action_time else None
            ),
        }
