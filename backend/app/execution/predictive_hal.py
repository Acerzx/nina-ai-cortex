"""
Predictive HAL — предсказательный слой безопасности.
Реализация идеи 3: анализ трендов для предсказания сбоев оборудования
и упреждающих действий.

Архитектура:
- Анализирует тренды метрик из ObservatoryState
- Использует линейную регрессию для экстраполяции
- Применяет динамические пороги confidence по типу действия:
  * Critical (парковка, отключение) > 0.95
  * Medium (автофокус, смена фильтра) > 0.85
  * Low (изменение экспозиции, интервала) > 0.70
- Публикует PREDICTIVE_ALERT для Watcher/Guardian
- Feature flag для включения/выключения

Использование:
    from app.execution.predictive_hal import predictive_hal
    predictions = await predictive_hal.check_all()

ИСПРАВЛЕНО (v4.1):
- Prediction конвертирован из @dataclass в Pydantic BaseModel
- field() заменён на Field() для поддержки ge/le валидации
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from enum import Enum
from pydantic import BaseModel, Field  # ← ДОБАВЛЕНО: BaseModel и Field вместо dataclass
from app.core.config import settings
from app.core.events import event_bus
from app.agents.observatory_state import observatory_state

logger = logging.getLogger("PredictiveHAL")


class PredictionSeverity(str, Enum):
    """Уровни серьёзности предсказаний."""

    INFO = "INFO"
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class ActionType(str, Enum):
    """Типы рекомендуемых действий (влияют на порог confidence)."""

    CRITICAL = "CRITICAL"  # Парковка, отключение — требует > 0.95
    MEDIUM = "MEDIUM"  # Автофокус, смена фильтра — требует > 0.85
    LOW = "LOW"  # Изменение экспозиции — требует > 0.70


class Prediction(BaseModel):
    """
    Результат предсказания.
    ИСПРАВЛЕНО (v4.1): Pydantic BaseModel вместо @dataclass
    для поддержки валидации confidence через Field(ge=0.0, le=1.0).
    """

    prediction_type: str
    severity: PredictionSeverity
    confidence: float = Field(ge=0.0, le=1.0)
    time_to_event_minutes: Optional[float] = None
    recommended_action: str = ""
    action_type: ActionType = ActionType.LOW
    evidence: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    def should_act(self, thresholds: Dict[str, float]) -> bool:
        """Проверяет, нужно ли действовать на основе порогов."""
        threshold = thresholds.get(self.action_type.value, 0.70)
        return self.confidence >= threshold

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация для обратной совместимости."""
        return {
            "prediction_type": self.prediction_type,
            "severity": self.severity.value,
            "confidence": round(self.confidence, 3),
            "time_to_event_minutes": self.time_to_event_minutes,
            "recommended_action": self.recommended_action,
            "action_type": self.action_type.value,
            "evidence": self.evidence,
            "timestamp": self.timestamp,
        }


class PredictiveHAL:
    """
    Предсказательный слой безопасности.
    """

    WINDOW_SHORT = 10
    WINDOW_MEDIUM = 20
    WINDOW_LONG = 40
    PREDICTION_HORIZON_MINUTES = 5.0
    MIN_POINTS_FOR_PREDICTION = 8

    def __init__(self):
        self._enabled = self._load_enabled_flag()
        self._thresholds = self._load_thresholds()
        self._recent_predictions: Dict[str, datetime] = {}
        self._prediction_cooldown_seconds = 300
        self._stats = {
            "total_predictions": 0,
            "accurate_predictions": 0,
            "false_positives": 0,
            "checks_performed": 0,
        }
        logger.info(
            f"🔮 Predictive HAL initialized "
            f"(enabled: {self._enabled}, thresholds: {self._thresholds})"
        )

    def _load_enabled_flag(self) -> bool:
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                hal_ff = getattr(ff, "hal", None)
                if hal_ff:
                    return getattr(hal_ff, "predictive_enabled", False)
        except Exception as e:
            logger.debug(f"Could not load HAL feature flag: {e}")
        return False

    def _load_thresholds(self) -> Dict[str, float]:
        defaults = {
            "CRITICAL": 0.95,
            "MEDIUM": 0.85,
            "LOW": 0.70,
        }
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                hal_ff = getattr(ff, "hal", None)
                if hal_ff:
                    return {
                        "CRITICAL": getattr(
                            hal_ff, "confidence_threshold_critical", 0.95
                        ),
                        "MEDIUM": getattr(hal_ff, "confidence_threshold_medium", 0.85),
                        "LOW": getattr(hal_ff, "confidence_threshold_low", 0.70),
                    }
        except Exception as e:
            logger.debug(f"Could not load HAL thresholds: {e}")
        return defaults

    async def check_all(self) -> List[Prediction]:
        """Выполняет все предсказания."""
        if not self._enabled:
            return []

        self._stats["checks_performed"] += 1
        predictions: List[Prediction] = []

        try:
            guider_pred = await self.predict_guider_failure()
            if guider_pred:
                predictions.append(guider_pred)

            focus_pred = await self.predict_focus_drift()
            if focus_pred:
                predictions.append(focus_pred)

            quality_pred = await self.predict_quality_degradation()
            if quality_pred:
                predictions.append(quality_pred)

            temp_pred = await self.predict_camera_overheat()
            if temp_pred:
                predictions.append(temp_pred)

            for pred in predictions:
                if self._should_publish(pred):
                    await self._publish_prediction(pred)
                    self._recent_predictions[pred.prediction_type] = datetime.now()
                    self._stats["total_predictions"] += 1

            return predictions
        except Exception as e:
            logger.error(f"Error in Predictive HAL check: {e}", exc_info=True)
            return []

    async def predict_guider_failure(
        self, window_minutes: float = 2.0
    ) -> Optional[Prediction]:
        """Предсказывает потерю гидирования на основе тренда RMS."""
        rms_ra_history = observatory_state.history.rms_ra
        rms_dec_history = observatory_state.history.rms_dec

        if len(rms_ra_history) < self.MIN_POINTS_FOR_PREDICTION:
            return None

        window_size = min(len(rms_ra_history), self.WINDOW_MEDIUM)
        recent_ra = rms_ra_history[-window_size:]
        recent_dec = (
            rms_dec_history[-window_size:]
            if len(rms_dec_history) >= window_size
            else []
        )

        trend_ra, intercept_ra = self._linear_regression(recent_ra)
        current_ra = recent_ra[-1]

        points_per_minute = 20.0
        future_points = int(self.PREDICTION_HORIZON_MINUTES * points_per_minute)
        predicted_ra = intercept_ra + trend_ra * (len(recent_ra) + future_points)

        from app.core.config import settings as cfg

        critical_rms = getattr(cfg.thresholds.guardian, "rms_recalibration", 3.0)

        if predicted_ra > critical_rms and current_ra < critical_rms:
            if trend_ra > 0:
                points_to_threshold = (critical_rms - intercept_ra) / trend_ra
                time_to_event = (
                    points_to_threshold - len(recent_ra)
                ) / points_per_minute
            else:
                time_to_event = None

            r_squared = self._calculate_r_squared(recent_ra, trend_ra, intercept_ra)
            proximity = current_ra / critical_rms
            confidence = min(0.99, (r_squared * 0.6 + proximity * 0.4))

            if confidence > 0.90:
                severity = PredictionSeverity.HIGH
            elif confidence > 0.75:
                severity = PredictionSeverity.MEDIUM
            else:
                severity = PredictionSeverity.LOW

            return Prediction(
                prediction_type="guider_failure",
                severity=severity,
                confidence=confidence,
                time_to_event_minutes=time_to_event,
                recommended_action=(
                    f"Рекомендуется перекалибровка гида через "
                    f"{time_to_event:.1f} мин (предсказанный RMS: "
                    f'{predicted_ra:.2f}")'
                    if time_to_event
                    else f'Рекомендуется перекалибровка гида (предсказанный RMS: {predicted_ra:.2f}")'
                ),
                action_type=ActionType.MEDIUM,
                evidence={
                    "current_rms_ra": current_ra,
                    "predicted_rms_ra": predicted_ra,
                    "trend_per_minute": trend_ra * points_per_minute,
                    "r_squared": r_squared,
                    "critical_threshold": critical_rms,
                    "window_points": window_size,
                },
            )
        return None

    async def predict_focus_drift(self) -> Optional[Prediction]:
        """Предсказывает дрейф фокуса на основе корреляции температуры и HFR."""
        temp_history = observatory_state.history.temperature
        hfr_history = observatory_state.history.hfr

        min_len = min(len(temp_history), len(hfr_history))
        if min_len < self.MIN_POINTS_FOR_PREDICTION:
            return None

        window_size = min(min_len, self.WINDOW_MEDIUM)
        recent_temp = temp_history[-window_size:]
        recent_hfr = hfr_history[-window_size:]

        correlation = self._pearson_correlation(recent_temp, recent_hfr)
        if abs(correlation) < 0.6:
            return None

        temp_trend, temp_intercept = self._linear_regression(recent_temp)
        if abs(temp_trend) < 0.01:
            return None

        hfr_trend, hfr_intercept = self._linear_regression(recent_hfr)
        current_hfr = recent_hfr[-1]
        current_temp = recent_temp[-1]

        points_per_minute = 20.0
        future_points = int(self.PREDICTION_HORIZON_MINUTES * points_per_minute)
        predicted_hfr = hfr_intercept + hfr_trend * (len(recent_hfr) + future_points)

        from app.core.config import settings as cfg

        hfr_target = getattr(cfg.thresholds.strategist, "hfr_target", 2.5)
        degradation_threshold = hfr_target * 1.3

        if (
            predicted_hfr > degradation_threshold
            and current_hfr < degradation_threshold
        ):
            if hfr_trend > 0:
                points_to_threshold = (
                    degradation_threshold - hfr_intercept
                ) / hfr_trend
                time_to_event = (
                    points_to_threshold - len(recent_hfr)
                ) / points_per_minute
            else:
                time_to_event = None

            trend_strength = min(1.0, abs(hfr_trend) * 10)
            confidence = min(0.95, abs(correlation) * 0.7 + trend_strength * 0.3)
            severity = (
                PredictionSeverity.MEDIUM
                if confidence > 0.80
                else PredictionSeverity.LOW
            )

            return Prediction(
                prediction_type="focus_drift",
                severity=severity,
                confidence=confidence,
                time_to_event_minutes=time_to_event,
                recommended_action=(
                    f"Температурный дрейф фокуса: запланируйте автофокус "
                    f"через {time_to_event:.1f} мин "
                    f"(корреляция T↔HFR: {correlation:.2f})"
                    if time_to_event
                    else "Температурный дрейф фокуса: рекомендуется автофокус"
                ),
                action_type=ActionType.MEDIUM,
                evidence={
                    "correlation": correlation,
                    "temp_trend_per_minute": temp_trend * points_per_minute,
                    "hfr_trend_per_minute": hfr_trend * points_per_minute,
                    "current_hfr": current_hfr,
                    "predicted_hfr": predicted_hfr,
                    "current_temp": current_temp,
                },
            )
        return None

    async def predict_quality_degradation(self) -> Optional[Prediction]:
        """Предсказывает общую деградацию качества изображения."""
        hfr_history = observatory_state.history.hfr
        if len(hfr_history) < self.MIN_POINTS_FOR_PREDICTION:
            return None

        window_size = min(len(hfr_history), self.WINDOW_MEDIUM)
        recent_hfr = hfr_history[-window_size:]

        trend, intercept = self._linear_regression(recent_hfr)
        current_hfr = recent_hfr[-1]

        points_per_minute = 20.0
        future_points = int(self.PREDICTION_HORIZON_MINUTES * points_per_minute)
        predicted_hfr = intercept + trend * (len(recent_hfr) + future_points)

        from app.core.config import settings as cfg

        hfr_target = getattr(cfg.thresholds.strategist, "hfr_target", 2.5)

        if (
            trend > 0
            and predicted_hfr > hfr_target * 1.5
            and current_hfr < hfr_target * 1.5
        ):
            r_squared = self._calculate_r_squared(recent_hfr, trend, intercept)
            confidence = min(0.90, r_squared * 0.8 + min(trend * 5, 0.2))

            return Prediction(
                prediction_type="quality_degradation",
                severity=PredictionSeverity.LOW,
                confidence=confidence,
                time_to_event_minutes=None,
                recommended_action=(
                    f"Прогнозируется деградация качества (HFR: "
                    f"{current_hfr:.2f} → {predicted_hfr:.2f}). "
                    f"Рассмотрите более частые автофокусы."
                ),
                action_type=ActionType.LOW,
                evidence={
                    "current_hfr": current_hfr,
                    "predicted_hfr": predicted_hfr,
                    "hfr_trend": trend,
                    "r_squared": r_squared,
                },
            )
        return None

    async def predict_camera_overheat(self) -> Optional[Prediction]:
        """Предсказывает перегрев камеры на основе тренда температуры."""
        temp_history = observatory_state.history.temperature
        if len(temp_history) < self.MIN_POINTS_FOR_PREDICTION:
            return None

        window_size = min(len(temp_history), self.WINDOW_MEDIUM)
        recent_temp = temp_history[-window_size:]

        trend, intercept = self._linear_regression(recent_temp)
        current_temp = recent_temp[-1]

        if trend <= 0:
            return None

        points_per_minute = 20.0
        future_points = int(self.PREDICTION_HORIZON_MINUTES * points_per_minute)
        predicted_temp = intercept + trend * (len(recent_temp) + future_points)

        warning_temp = -5.0
        if predicted_temp > warning_temp and current_temp < warning_temp:
            points_to_threshold = (warning_temp - intercept) / trend
            time_to_event = (points_to_threshold - len(recent_temp)) / points_per_minute
            confidence = min(0.85, abs(trend) * 20)

            return Prediction(
                prediction_type="camera_overheat",
                severity=PredictionSeverity.MEDIUM,
                confidence=confidence,
                time_to_event_minutes=time_to_event,
                recommended_action=(
                    f"Температура камеры растёт. Через {time_to_event:.1f} мин "
                    f"может превысить {warning_temp}°C. Проверьте охладитель."
                ),
                action_type=ActionType.MEDIUM,
                evidence={
                    "current_temp": current_temp,
                    "predicted_temp": predicted_temp,
                    "temp_trend": trend,
                },
            )
        return None

    def _should_publish(self, prediction: Prediction) -> bool:
        """Проверяет, нужно ли публиковать предсказание (cooldown)."""
        if not prediction.should_act(self._thresholds):
            return False

        last_time = self._recent_predictions.get(prediction.prediction_type)
        if last_time:
            elapsed = (datetime.now() - last_time).total_seconds()
            if elapsed < self._prediction_cooldown_seconds:
                return False
        return True

    async def _publish_prediction(self, prediction: Prediction):
        """Публикует предсказание как PREDICTIVE_ALERT."""
        alert_level = {
            PredictionSeverity.CRITICAL: "CRITICAL",
            PredictionSeverity.HIGH: "WARNING",
            PredictionSeverity.MEDIUM: "WARNING",
            PredictionSeverity.LOW: "INFO",
            PredictionSeverity.INFO: "INFO",
        }.get(prediction.severity, "INFO")

        await event_bus.publish(
            "PREDICTIVE_ALERT",
            {
                "level": alert_level,
                "message": prediction.recommended_action,
                "agent": "PredictiveHAL",
                "timestamp": prediction.timestamp,
                "prediction": prediction.to_dict(),
            },
        )
        logger.info(
            f"🔮 Predictive alert [{alert_level}]: "
            f"{prediction.prediction_type} "
            f"(confidence: {prediction.confidence:.2f})"
        )

    # ====================================================================
    # МАТЕМАТИЧЕСКИЕ МЕТОДЫ
    # ====================================================================
    def _linear_regression(self, values: List[float]) -> tuple:
        """Простая линейная регрессия. Returns: (slope, intercept)"""
        n = len(values)
        if n < 2:
            return 0.0, values[0] if values else 0.0

        x_mean = (n - 1) / 2.0
        y_mean = sum(values) / n

        numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return 0.0, y_mean

        slope = numerator / denominator
        intercept = y_mean - slope * x_mean
        return slope, intercept

    def _calculate_r_squared(
        self, values: List[float], slope: float, intercept: float
    ) -> float:
        """Вычисляет R² (коэффициент детерминации)."""
        n = len(values)
        if n < 2:
            return 0.0

        y_mean = sum(values) / n
        ss_res = sum((values[i] - (slope * i + intercept)) ** 2 for i in range(n))
        ss_tot = sum((values[i] - y_mean) ** 2 for i in range(n))

        if ss_tot == 0:
            return 0.0
        return max(0.0, 1.0 - ss_res / ss_tot)

    def _pearson_correlation(self, x: List[float], y: List[float]) -> float:
        """Вычисляет коэффициент корреляции Пирсона."""
        n = min(len(x), len(y))
        if n < 3:
            return 0.0

        x = x[:n]
        y = y[:n]

        x_mean = sum(x) / n
        y_mean = sum(y) / n

        numerator = sum((x[i] - x_mean) * (y[i] - y_mean) for i in range(n))
        x_std = (sum((xi - x_mean) ** 2 for xi in x)) ** 0.5
        y_std = (sum((yi - y_mean) ** 2 for yi in y)) ** 0.5

        if x_std == 0 or y_std == 0:
            return 0.0
        return numerator / (x_std * y_std)

    # ====================================================================
    # API
    # ====================================================================
    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику Predictive HAL."""
        return {
            **self._stats,
            "enabled": self._enabled,
            "thresholds": self._thresholds,
            "recent_predictions": {
                k: v.isoformat() for k, v in self._recent_predictions.items()
            },
            "cooldown_seconds": self._prediction_cooldown_seconds,
        }

    async def force_check(self) -> List[Dict[str, Any]]:
        """Принудительная проверка всех предсказаний (для API)."""
        predictions = await self.check_all()
        return [p.to_dict() for p in predictions]


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
predictive_hal = PredictiveHAL()
