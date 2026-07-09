"""
Machine Learning module — фундамент для будущих RL и ML моделей.

Содержит:
- parameter_optimizer: ML-модели для оптимизации параметров съёмки
- rl_pipeline: Reinforcement Learning interface (заглушка)
- image_features: Извлечение признаков из FITS-превью (заглушка)

Архитектура:
- Все модели наследуются от базовых ABC классов
- Feature flags для включения/выключения
- Graceful fallback на эвристики если ML недоступен
"""

from app.ml.parameter_optimizer import (
    BaseParameterModel,
    HeuristicParameterModel,
    parameter_optimizer,
)

__all__ = [
    "BaseParameterModel",
    "HeuristicParameterModel",
    "parameter_optimizer",
]
