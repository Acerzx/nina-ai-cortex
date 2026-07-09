"""
Parameter Optimizer — ML-модели для оптимизации параметров съёмки.
Заглушка для идеи 4: автоматическая настройка параметров через ML.

Текущая реализация:
- HeuristicParameterModel: использует формулы Strategist (SNR ~ sqrt(time))
- MLParameterModel (future): sklearn/PyTorch модель, обученная на истории сессий

Feature flag: feature_flags.analytics.ml_parameter_optimizer
"""

import logging
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass
from datetime import datetime

from app.core.config import settings

logger = logging.getLogger("ParameterOptimizer")


@dataclass
class ParameterSuggestion:
    """Предложение по оптимизации параметра."""

    parameter: str
    current_value: Any
    suggested_value: Any
    confidence: float
    model_name: str
    rationale: str
    timestamp: str = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()


class BaseParameterModel(ABC):
    """Базовый класс для всех ML-моделей оптимизации параметров."""

    @abstractmethod
    def name(self) -> str:
        """Имя модели."""
        pass

    @abstractmethod
    async def predict_exposure(
        self,
        conditions: Dict[str, Any],
    ) -> Optional[ParameterSuggestion]:
        """Предсказывает оптимальную экспозицию."""
        pass

    @abstractmethod
    async def predict_autofocus_interval(
        self,
        conditions: Dict[str, Any],
    ) -> Optional[ParameterSuggestion]:
        """Предсказывает оптимальный интервал автофокуса."""
        pass

    @abstractmethod
    def is_trained(self) -> bool:
        """Проверяет, обучена ли модель."""
        pass


class HeuristicParameterModel(BaseParameterModel):
    """
    Эвристическая модель — использует физические формулы Strategist.

    Формулы:
    - Экспозиция: new_time = old_time * (target_snr / current_snr)^2
    - Интервал автофокуса: зависит от тренда HFR
    """

    def name(self) -> str:
        return "heuristic"

    async def predict_exposure(
        self,
        conditions: Dict[str, Any],
    ) -> Optional[ParameterSuggestion]:
        """Предсказывает экспозицию на основе SNR."""
        current_snr = conditions.get("current_snr")
        current_exposure = conditions.get("current_exposure", 60.0)

        target_snr = getattr(settings.thresholds.strategist, "snr_target", 20.0)

        if current_snr is None or current_snr <= 0:
            return None

        if current_snr >= target_snr * 0.9:
            # SNR уже хорош
            return None

        # SNR ~ sqrt(time)
        ratio = target_snr / current_snr
        suggested = current_exposure * (ratio**2)

        # Ограничиваем разумными пределами
        suggested = max(10.0, min(600.0, suggested))

        return ParameterSuggestion(
            parameter="exposure_time",
            current_value=current_exposure,
            suggested_value=suggested,
            confidence=0.85,
            model_name=self.name(),
            rationale=(
                f"SNR ~ sqrt(time). Текущий SNR {current_snr:.1f}, "
                f"целевой {target_snr:.1f}."
            ),
        )

    async def predict_autofocus_interval(
        self,
        conditions: Dict[str, Any],
    ) -> Optional[ParameterSuggestion]:
        """Предсказывает интервал автофокуса на основе тренда HFR."""
        hfr_trend = conditions.get("hfr_trend")
        current_interval = conditions.get("current_interval", 60)

        if hfr_trend is None:
            return None

        degradation_threshold = getattr(
            settings.thresholds.strategist,
            "hfr_degradation_threshold",
            0.05,
        )

        if hfr_trend <= degradation_threshold:
            return None

        # Быстрая деградация → emergency interval
        if hfr_trend > degradation_threshold * 2:
            suggested = getattr(
                settings.thresholds.strategist,
                "autofocus_interval_emergency",
                15,
            )
        else:
            suggested = getattr(
                settings.thresholds.strategist,
                "autofocus_interval_frequent",
                30,
            )

        if suggested >= current_interval:
            return None

        return ParameterSuggestion(
            parameter="autofocus_interval",
            current_value=current_interval,
            suggested_value=suggested,
            confidence=0.80,
            model_name=self.name(),
            rationale=f"HFR растёт ({hfr_trend:.3f}/frame), нужен более частый автофокус",
        )

    def is_trained(self) -> bool:
        """Эвристика всегда 'обучена'."""
        return True


class MLParameterModelStub(BaseParameterModel):
    """
    STUB для будущей ML-модели.
    Возвращает None — будет заменена на sklearn/PyTorch реализацию.
    """

    def name(self) -> str:
        return "ml_stub"

    async def predict_exposure(
        self, conditions: Dict[str, Any]
    ) -> Optional[ParameterSuggestion]:
        logger.debug("ML parameter model stub called (not implemented)")
        return None

    async def predict_autofocus_interval(
        self, conditions: Dict[str, Any]
    ) -> Optional[ParameterSuggestion]:
        return None

    def is_trained(self) -> bool:
        return False


class ParameterOptimizer:
    """
    Главный класс оптимизатора параметров.
    Использует ансамбль моделей с fallback.

    Приоритет:
    1. ML модель (если обучена и включена)
    2. Heuristic модель (всегда доступна)
    """

    def __init__(self):
        self._heuristic = HeuristicParameterModel()
        self._ml_model: Optional[BaseParameterModel] = MLParameterModelStub()

        # Feature flag
        self._ml_enabled = self._load_ml_flag()

        self._stats = {
            "heuristic_calls": 0,
            "ml_calls": 0,
            "suggestions_generated": 0,
        }

        logger.info(
            f"🤖 Parameter Optimizer initialized "
            f"(ML enabled: {self._ml_enabled}, "
            f"ML trained: {self._ml_model.is_trained() if self._ml_model else False})"
        )

    def _load_ml_flag(self) -> bool:
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                analytics_ff = getattr(ff, "analytics", None)
                if analytics_ff:
                    return getattr(analytics_ff, "ml_parameter_optimizer", False)
        except Exception:
            pass
        return False

    async def suggest_exposure(
        self,
        conditions: Dict[str, Any],
    ) -> Optional[ParameterSuggestion]:
        """Предлагает оптимальную экспозицию."""
        # Пробуем ML модель если включена и обучена
        if self._ml_enabled and self._ml_model and self._ml_model.is_trained():
            self._stats["ml_calls"] += 1
            result = await self._ml_model.predict_exposure(conditions)
            if result:
                self._stats["suggestions_generated"] += 1
                return result

        # Fallback на эвристику
        self._stats["heuristic_calls"] += 1
        result = await self._heuristic.predict_exposure(conditions)
        if result:
            self._stats["suggestions_generated"] += 1
        return result

    async def suggest_autofocus_interval(
        self,
        conditions: Dict[str, Any],
    ) -> Optional[ParameterSuggestion]:
        """Предлагает оптимальный интервал автофокуса."""
        if self._ml_enabled and self._ml_model and self._ml_model.is_trained():
            self._stats["ml_calls"] += 1
            result = await self._ml_model.predict_autofocus_interval(conditions)
            if result:
                self._stats["suggestions_generated"] += 1
                return result

        self._stats["heuristic_calls"] += 1
        result = await self._heuristic.predict_autofocus_interval(conditions)
        if result:
            self._stats["suggestions_generated"] += 1
        return result

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "ml_enabled": self._ml_enabled,
            "ml_model_available": self._ml_model is not None,
            "ml_model_trained": self._ml_model.is_trained()
            if self._ml_model
            else False,
            "heuristic_model": self._heuristic.name(),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
parameter_optimizer = ParameterOptimizer()
