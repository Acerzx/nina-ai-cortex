"""
Reinforcement Learning Pipeline — фундамент для будущего RL.
Заглушка для идеи 2: обучение агентов на истории решений.

Текущая реализация:
- Интерфейсы RewardFunction и PolicyTrainer
- HeuristicPolicy (заглушка, возвращает None)
- StatisticalAnalyzer (работает — см. decision_analyzer.py)

Будущая реализация (когда накопится достаточно данных):
- PyTorch/TensorFlow policy network
- Environment simulator (FakeNina + FakePhd2)
- Training loop на Decision Audit данных
- Model serving (ONNX)

Feature flag: feature_flags.ml.rl_pipeline_enabled
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List, Protocol
from datetime import datetime

from app.core.config import settings
from app.storage.decision_audit import DecisionRecord

logger = logging.getLogger("RLPipeline")


class RewardFunction(Protocol):
    """Протокол для функции вознаграждения."""

    def calculate(
        self,
        decision: DecisionRecord,
        outcome: str,
    ) -> float:
        """
        Вычисляет вознаграждение за решение.

        Args:
            decision: Запись о решении
            outcome: Исход (SUCCESS, FAILED, PARTIAL)

        Returns:
            Значение вознаграждения (-1.0 до 1.0)
        """
        ...


class SimpleRewardFunction:
    """
    Простая функция вознаграждения.
    +1 за SUCCESS, -1 за FAILED, 0 за PARTIAL.
    """

    def calculate(
        self,
        decision: DecisionRecord,
        outcome: str,
    ) -> float:
        if outcome == "SUCCESS":
            return 1.0 * decision.confidence
        elif outcome == "FAILED":
            return -1.0 * decision.confidence
        else:
            return 0.0


class PolicyTrainer(ABC):
    """Абстрактный класс для обучения политики."""

    @abstractmethod
    async def train(self, decisions: List[DecisionRecord]) -> bool:
        """Обучает политику на истории решений."""
        pass

    @abstractmethod
    async def suggest_action(
        self,
        state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Предлагает действие для данного состояния."""
        pass

    @abstractmethod
    def is_trained(self) -> bool:
        """Проверяет, обучена ли политика."""
        pass


class HeuristicPolicy(PolicyTrainer):
    """
    Заглушка политики — использует эвристики.
    Будет заменена на реальную RL-политику.
    """

    async def train(self, decisions: List[DecisionRecord]) -> bool:
        logger.debug("HeuristicPolicy.train() called (no-op)")
        return True

    async def suggest_action(
        self,
        state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        # Заглушка — возвращает None, чтобы агенты использовали свои логики
        return None

    def is_trained(self) -> bool:
        return True  # Всегда "готова"


class RLPipeline:
    """
    Главный класс RL pipeline.
    Координирует обучение и применение политик.
    """

    def __init__(self):
        self._enabled = self._load_enabled_flag()
        self._reward_function = SimpleRewardFunction()
        self._policy = HeuristicPolicy()

        self._stats = {
            "training_runs": 0,
            "suggestions_made": 0,
        }

        logger.info(
            f"🧠 RL Pipeline initialized (enabled: {self._enabled}, "
            f"policy: {self._policy.__class__.__name__})"
        )

    def _load_enabled_flag(self) -> bool:
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                ml_ff = getattr(ff, "ml", None)
                if ml_ff:
                    return getattr(ml_ff, "rl_pipeline_enabled", False)
        except Exception:
            pass
        return False

    async def train_on_history(self, days: int = 30) -> bool:
        """
        Обучает политику на истории решений.
        STUB — в будущем будет использовать PyTorch.
        """
        if not self._enabled:
            return False

        from app.storage.decision_audit import decision_audit

        decisions = await decision_audit.get_decisions(limit=10000)
        if not decisions:
            return False

        success = await self._policy.train(decisions)
        self._stats["training_runs"] += 1

        return success

    async def suggest_action(
        self,
        state: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """Предлагает действие на основе обученной политики."""
        if not self._enabled:
            return None

        action = await self._policy.suggest_action(state)
        if action:
            self._stats["suggestions_made"] += 1
        return action

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "enabled": self._enabled,
            "policy_type": self._policy.__class__.__name__,
            "policy_trained": self._policy.is_trained(),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
rl_pipeline = RLPipeline()
