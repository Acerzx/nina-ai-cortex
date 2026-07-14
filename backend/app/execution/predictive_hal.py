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

ЭТАП 5 (дополнение):
- Добавлены 5 новых моделей предсказаний
- Интеграция с state_tracker для проверки критических фаз
- Интеграция с masters_auditor для проверки свежести мастеров
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from enum import Enum
from pydantic import BaseModel, Field
from app.core.config import settings
from app.core.events import event_bus
from app.agents.observatory_state import observatory_state
from app.shadow_engine.state_tracker import state_tracker

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

    ЭТАП 5: 9 моделей предсказаний (4 существующие + 5 новых)
    """

    WINDOW_SHORT = 10
    WINDOW_MEDIUM = 20
    WINDOW_LONG = 40
    PREDICTION_HORIZON_MINUTES = 5.0
    MIN_POINTS_FOR_PREDICTION = 8

    def __init__(self):
        self._enabled = self._load_enabled_flag()
        self._thresholds = self._load_thresholds()

        # Cooldown для каждого типа предсказания
        self._recent_predictions: Dict[str, datetime] = {}
        self._prediction_cooldown_seconds = 300

        # Статистика
        self._stats = {
            "total_predictions": 0,
            "accurate_predictions": 0,
            "false_positives": 0,
            "checks_performed": 0,
        }

        logger.info(
            f"🔮 Predictive HAL initialized "
            f"(enabled: {self._enabled}, thresholds: {self._thresholds}, "
            f"models: 9)"
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
            # === Существующие модели (4) ===

            # 1. Потеря гидирования
            guider_pred = await self.predict_guider_failure()
            if guider_pred:
                predictions.append(guider_pred)

            # 2. Дрейф фокуса
            focus_pred = await self.predict_focus_drift()
            if focus_pred:
                predictions.append(focus_pred)

            # 3. Деградация качества
            quality_pred = await self.predict_quality_degradation()
            if quality_pred:
                predictions.append(quality_pred)

            # 4. Перегрев камеры
            temp_pred = await self.predict_camera_overheat()
            if temp_pred:
                predictions.append(temp_pred)

            # === Новые модели (Этап 5) ===

            # 5. Коллизия Meridian Flip с критической фазой
            mf_pred = await self.predict_meridian_flip_conflict()
            if mf_pred:
                predictions.append(mf_pred)

            # 6. Необходимость смены фильтра
            filter_pred = await self.predict_filter_change_need()
            if filter_pred:
                predictions.append(filter_pred)

            # 7. Устаревание калибровок
            calib_pred = await self.predict_calibration_staleness()
            if calib_pred:
                predictions.append(calib_pred)

            # 8. Истощение SNR
            snr_pred = await self.predict_snr_depletion()
            if snr_pred:
                predictions.append(snr_pred)

            # 9. Ветровая нагрузка на цель
            wind_pred = await self.predict_wind_load_on_target()
            if wind_pred:
                predictions.append(wind_pred)

            # Публикуем предсказания
            for pred in predictions:
                if self._should_publish(pred):
                    await self._publish_prediction(pred)
                    self._recent_predictions[pred.prediction_type] = datetime.now()
                    self._stats["total_predictions"] += 1

            return predictions

        except Exception as e:
            logger.error(f"Error in Predictive HAL check: {e}", exc_info=True)
            return []

    # ========================================================================
    # СУЩЕСТВУЮЩИЕ МОДЕЛИ (4)
    # ========================================================================

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

    # ========================================================================
    # НОВЫЕ МОДЕЛИ (ЭТАП 5)
    # ========================================================================

    async def predict_meridian_flip_conflict(self) -> Optional[Prediction]:
        """
        Предсказывает конфликт Meridian Flip с критической фазой.

        NINA делает MF автоматически, но:
        - Не предупреждает, если AF в процессе
        - Не учитывает, что guider calibration теряется при MF
        - Не оптимизирует время MF относительно exposures

        Логика:
        1. Получаем текущий Hour Angle монтировки
        2. Вычисляем время до MF (HA ≈ 0 или 12h)
        3. Проверяем конфликты: AF running, guiding active, незавершённые exposures
        4. Генерируем предупреждение если конфликт найден
        """
        # Получаем mount_ra_hours из метрик
        mount_ra = observatory_state.current_metrics.get("mount_ra_hours")
        if mount_ra is None:
            return None

        # Получаем текущее состояние
        is_autofocus_running = observatory_state.is_autofocus_running
        is_guiding_active = observatory_state.is_guiding_active
        is_sequence_running = state_tracker.state.is_running

        # Упрощённо: если RA > 11h или < 1h — MF может быть скоро
        hours_to_flip = None

        if mount_ra > 11.0:
            hours_to_flip = 12.0 - mount_ra
        elif mount_ra < 1.0:
            hours_to_flip = mount_ra

        if hours_to_flip is None or hours_to_flip > 1.0:
            return None  # MF далеко

        # Проверяем конфликты
        conflicts = []
        if is_autofocus_running:
            conflicts.append("autofocus running")
        if is_guiding_active:
            conflicts.append("guiding active (will need recalibration)")
        if is_sequence_running:
            conflicts.append("sequence in progress")

        if not conflicts:
            return None

        # Confidence зависит от количества конфликтов и времени до MF
        confidence = min(0.95, 0.5 + len(conflicts) * 0.15)

        # Severity зависит от времени до MF
        if hours_to_flip < 0.25:  # < 15 минут
            severity = PredictionSeverity.HIGH
        elif hours_to_flip < 0.5:  # < 30 минут
            severity = PredictionSeverity.MEDIUM
        else:
            severity = PredictionSeverity.LOW

        return Prediction(
            prediction_type="meridian_flip_conflict",
            severity=severity,
            confidence=confidence,
            time_to_event_minutes=hours_to_flip * 60,
            recommended_action=(
                f"Meridian flip через {hours_to_flip:.1f}h. Конфликты: "
                f"{', '.join(conflicts)}. "
                "Рекомендуется: завершить текущие операции, "
                "подготовиться к перекалибровке гида после MF."
            ),
            action_type=ActionType.MEDIUM,
            evidence={
                "mount_ra_hours": mount_ra,
                "hours_to_flip": hours_to_flip,
                "conflicts": conflicts,
                "is_autofocus_running": is_autofocus_running,
                "is_guiding_active": is_guiding_active,
                "is_sequence_running": is_sequence_running,
            },
        )

    async def predict_filter_change_need(self) -> Optional[Prediction]:
        """
        Предсказывает необходимость смены фильтра на основе деградации HFR.

        Логика:
        1. Анализируем тренд HFR для текущего фильтра
        2. Если HFR деградирует быстрее порога — рекомендуем смену фильтра
        3. Особенно важно для узкополосных фильтров (Ha, OIII, SII)
        """
        current_filter = observatory_state.current_metrics.get("filter")
        if not current_filter:
            return None

        hfr_history = observatory_state.history.hfr
        if len(hfr_history) < self.MIN_POINTS_FOR_PREDICTION:
            return None

        window_size = min(len(hfr_history), self.WINDOW_MEDIUM)
        recent_hfr = hfr_history[-window_size:]

        trend, intercept = self._linear_regression(recent_hfr)
        current_hfr = recent_hfr[-1]

        # Порог деградации из конфига
        from app.core.config import settings as cfg

        degradation_threshold = getattr(
            cfg.thresholds.strategist, "hfr_degradation_threshold", 0.05
        )

        # Если тренд положительный и превышает порог
        if trend > degradation_threshold:
            r_squared = self._calculate_r_squared(recent_hfr, trend, intercept)

            # Confidence зависит от R² и силы тренда
            trend_strength = min(1.0, trend / (degradation_threshold * 2))
            confidence = min(0.90, r_squared * 0.6 + trend_strength * 0.4)

            # Для узкополосных фильтров — выше priority
            narrowband_filters = ["Ha", "OIII", "SII", "H-alpha", "OIII", "SII"]
            is_narrowband = any(
                nf.lower() in current_filter.lower() for nf in narrowband_filters
            )

            if is_narrowband:
                severity = PredictionSeverity.MEDIUM
                action_text = (
                    f"Фильтр {current_filter}: HFR деградирует "
                    f"({trend:.3f}/frame). Рассмотрите переход на другой фильтр "
                    "или более частые автофокусы."
                )
            else:
                severity = PredictionSeverity.LOW
                action_text = (
                    f"Фильтр {current_filter}: HFR деградирует "
                    f"({trend:.3f}/frame). Рассмотрите автофокус или смену фильтра."
                )

            return Prediction(
                prediction_type="filter_change_need",
                severity=severity,
                confidence=confidence,
                time_to_event_minutes=None,
                recommended_action=action_text,
                action_type=ActionType.LOW,
                evidence={
                    "current_filter": current_filter,
                    "current_hfr": current_hfr,
                    "hfr_trend": trend,
                    "r_squared": r_squared,
                    "is_narrowband": is_narrowband,
                    "degradation_threshold": degradation_threshold,
                },
            )

        return None

    async def predict_calibration_staleness(self) -> Optional[Prediction]:
        """
        Предсказывает устаревание калибровок (BIAS/DARK/FLAT masters).

        Логика:
        1. Получаем возраст мастеров из MastersLibraryAuditor
        2. Сравниваем с порогами свежести (BIAS: 90d, DARK: 30d, FLAT: 7d)
        3. Генерируем предупреждение если мастер устарел
        """
        # Получаем masters_auditor из watcher_manager
        try:
            from app.ingestion.watchers.manager import watcher_manager

            auditor = watcher_manager.masters_auditor
            if not auditor:
                return None
        except ImportError:
            return None

        # Получаем сводку по категориям
        summary = auditor.get_summary_by_category()

        # Пороги свежести
        freshness_days = {
            "BIAS": 90,
            "DARK": 30,
            "FLAT": 7,
        }

        stale_masters = []

        for master_type, max_days in freshness_days.items():
            category_summary = summary.get(master_type, {})
            max_date_str = category_summary.get("max_date")

            if max_date_str:
                try:
                    # Парсим дату
                    if "T" in max_date_str:
                        max_date = datetime.fromisoformat(
                            max_date_str.replace("Z", "+00:00")
                        )
                        if max_date.tzinfo:
                            max_date = max_date.replace(tzinfo=None)
                    else:
                        max_date = datetime.strptime(max_date_str[:10], "%Y-%m-%d")

                    age_days = (datetime.now() - max_date).days

                    if age_days > max_days:
                        stale_masters.append(
                            {
                                "type": master_type,
                                "age_days": age_days,
                                "max_days": max_days,
                                "overdue_days": age_days - max_days,
                            }
                        )

                except (ValueError, TypeError):
                    pass

        if not stale_masters:
            return None

        # Confidence зависит от количества устаревших мастеров
        confidence = min(0.95, 0.6 + len(stale_masters) * 0.1)

        # Severity зависит от типа устаревшего мастера
        # FLAT самый критичный (7 дней), BIAS наименее (90 дней)
        if any(m["type"] == "FLAT" for m in stale_masters):
            severity = PredictionSeverity.MEDIUM
        elif any(m["type"] == "DARK" for m in stale_masters):
            severity = PredictionSeverity.LOW
        else:
            severity = PredictionSeverity.INFO

        # Формируем список рекомендаций
        stale_list = ", ".join(
            f"{m['type']} ({m['age_days']}d, max {m['max_days']}d)"
            for m in stale_masters
        )

        return Prediction(
            prediction_type="calibration_staleness",
            severity=severity,
            confidence=confidence,
            time_to_event_minutes=None,
            recommended_action=(
                f"Устаревшие калибровки: {stale_list}. "
                "Рекомендуется пересъёмка masters для обеспечения качества."
            ),
            action_type=ActionType.LOW,
            evidence={
                "stale_masters": stale_masters,
                "total_stale": len(stale_masters),
            },
        )

    async def predict_snr_depletion(self) -> Optional[Prediction]:
        """
        Предсказывает истощение SNR из-за облачности или времени.

        Логика:
        1. Анализируем тренд cloud_cover
        2. Если облачность растёт и превышает порог — предупреждаем
        3. Особенно важно для узкополосных фильтров
        """
        # Получаем историю облачности
        cloud_cover = observatory_state.weather.get("cloud_cover")

        if cloud_cover is None:
            return None

        # Порог облачности из конфига
        from app.core.config import settings as cfg

        cloud_cover_max = getattr(cfg.thresholds.preflight, "cloud_cover_max", 80.0)

        # Если облачность высокая
        if cloud_cover > cloud_cover_max:
            # Confidence зависит от того, насколько превышает порог
            over_threshold = cloud_cover - cloud_cover_max
            confidence = min(0.95, 0.6 + over_threshold / 100)

            # Severity зависит от уровня облачности
            if cloud_cover > 95:
                severity = PredictionSeverity.HIGH
            elif cloud_cover > 90:
                severity = PredictionSeverity.MEDIUM
            else:
                severity = PredictionSeverity.LOW

            # Текущий фильтр
            current_filter = observatory_state.current_metrics.get("filter", "unknown")

            return Prediction(
                prediction_type="snr_depletion",
                severity=severity,
                confidence=confidence,
                time_to_event_minutes=None,
                recommended_action=(
                    f"Облачность {cloud_cover}% превышает порог {cloud_cover_max}%. "
                    f"SNR будет снижаться. Фильтр: {current_filter}. "
                    "Рассмотрите паузу или переход на узкополосный фильтр."
                ),
                action_type=ActionType.LOW,
                evidence={
                    "cloud_cover": cloud_cover,
                    "cloud_cover_max": cloud_cover_max,
                    "over_threshold": over_threshold,
                    "current_filter": current_filter,
                },
            )

        return None

    async def predict_wind_load_on_target(self) -> Optional[Prediction]:
        """
        Предсказывает ветровую нагрузку на текущую цель.

        Логика:
        1. Получаем wind_speed и wind_direction
        2. Получаем azimuth текущей цели
        3. Вычисляем угол между ветром и целью
        4. Если цель в наветренном направлении (разница < 90°) — предупреждаем
        """
        wind_speed = observatory_state.weather.get("wind_speed")
        wind_direction = observatory_state.weather.get("wind_direction")

        if wind_speed is None or wind_direction is None:
            return None

        # Порог ветра из конфига
        from app.core.config import settings as cfg

        wind_speed_warning = getattr(cfg.thresholds.watcher, "wind_speed_warning", 15.0)

        # Если ветер ниже порога — не предупреждаем
        if wind_speed < wind_speed_warning:
            return None

        # Получаем azimuth текущей цели
        target_azimuth = observatory_state.current_metrics.get("mount_azimuth")

        if target_azimuth is None:
            return None

        # Вычисляем угол между ветром и целью
        angle_diff = abs(target_azimuth - wind_direction)
        if angle_diff > 180:
            angle_diff = 360 - angle_diff

        # Если цель в наветренном направлении (разница < 90°)
        if angle_diff < 90:
            # Confidence зависит от силы ветра и угла
            wind_strength = min(1.0, wind_speed / (wind_speed_warning * 2))
            angle_factor = 1.0 - (
                angle_diff / 90
            )  # 1.0 если прямо против ветра, 0 если 90°
            confidence = min(0.95, 0.6 + wind_strength * 0.3 + angle_factor * 0.1)

            # Severity зависит от силы ветра
            if wind_speed > wind_speed_warning * 2:
                severity = PredictionSeverity.HIGH
            elif wind_speed > wind_speed_warning * 1.5:
                severity = PredictionSeverity.MEDIUM
            else:
                severity = PredictionSeverity.LOW

            return Prediction(
                prediction_type="wind_load_on_target",
                severity=severity,
                confidence=confidence,
                time_to_event_minutes=None,
                recommended_action=(
                    f"Ветер {wind_speed:.1f} м/с с направления {wind_direction:.0f}°. "
                    f"Текущая цель на азимуте {target_azimuth:.0f}° "
                    f"(разница {angle_diff:.0f}° — наветренная сторона). "
                    "Рассмотрите переход на цель в подветренном направлении."
                ),
                action_type=ActionType.LOW,
                evidence={
                    "wind_speed": wind_speed,
                    "wind_direction": wind_direction,
                    "target_azimuth": target_azimuth,
                    "angle_diff": angle_diff,
                    "wind_speed_warning": wind_speed_warning,
                },
            )

        return None

    # ========================================================================
    # ВСПОМОГАТЕЛЬНЫЕ МЕТОДЫ
    # ========================================================================

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

    # ========================================================================
    # МАТЕМАТИЧЕСКИЕ МЕТОДЫ
    # ========================================================================

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

    # ========================================================================
    # API
    # ========================================================================

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
            "total_models": 9,
            "model_names": [
                "predict_guider_failure",
                "predict_focus_drift",
                "predict_quality_degradation",
                "predict_camera_overheat",
                "predict_meridian_flip_conflict",
                "predict_filter_change_need",
                "predict_calibration_staleness",
                "predict_snr_depletion",
                "predict_wind_load_on_target",
            ],
        }

    async def force_check(self) -> List[Dict[str, Any]]:
        """Принудительная проверка всех предсказаний (для API)."""
        predictions = await self.check_all()
        return [p.to_dict() for p in predictions]


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
predictive_hal = PredictiveHAL()
