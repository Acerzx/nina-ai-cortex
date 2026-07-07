"""
Base Agent — базовый класс для всех AI-агентов в Multi-Agent Swarm.
Обеспечивает единый интерфейс, логирование решений и интеграцию с ObservatoryState.
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.observatory_state import observatory_state
from app.core.rag_engine import rag_engine
from app.core.events import event_bus

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
    outcome: Optional[str] = None  # Заполняется позже (SUCCESS/FAILED/SUBOPTIMAL)
    hindsight_verdict: Optional[str] = None  # CORRECT/WRONG/SUBOPTIMAL


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

    @abstractmethod
    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует контекст и принимает решение.

        Args:
            context: Текущий контекст обсерватории

        Returns:
            AgentDecision если решение принято, None если нет необходимости действовать
        """
        pass

    @abstractmethod
    async def execute(self, decision: AgentDecision) -> bool:
        """
        Выполняет принятое решение.

        Args:
            decision: Решение для выполнения

        Returns:
            True если выполнение успешно, False в противном случае
        """
        pass

    async def get_rag_context(self, query: str, max_tokens: int = 2000) -> str:
        """Получает контекст из RAG для принятия решения."""
        try:
            context = await rag_engine.get_context(query=query, max_tokens=max_tokens)
            return context
        except Exception as e:
            logger.error(f"Failed to get RAG context: {e}")
            return "Контекст недоступен"

    def log_decision(self, decision: AgentDecision):
        """Логирует решение в Decision Audit Trail."""
        self._decision_log.append(decision)

        # Ограничиваем размер лога
        if len(self._decision_log) > 1000:
            self._decision_log = self._decision_log[-1000:]

        # Логируем в ObservatoryState для объяснимости
        observatory_state.log_ai_action(
            agent=self.name,
            action=decision.decision_type,
            reason=decision.rationale,
            result=f"Confidence: {decision.confidence:.2f}",
        )

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

        # Автоматическая оценка hindsight verdict
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

    def get_recent_decisions(self, limit: int = 10) -> List[AgentDecision]:
        """Возвращает последние N решений агента."""
        return self._decision_log[-limit:]

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику агента."""
        return {
            "name": self.name,
            "role": self.role,
            "is_running": self._is_running,
            "total_decisions": len(self._decision_log),
            "last_action": self._last_action_time.isoformat()
            if self._last_action_time
            else None,
            "recent_decisions": [d.model_dump() for d in self.get_recent_decisions(5)],
        }
