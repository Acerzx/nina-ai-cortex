"""
Quality Score Calculator — единый модуль расчёта качества сессий.
Устраняет проблему С-10: дублирование формул quality_score в auditor_agent.py
и sessions_metadata.py.

Архитектура:
- Веса читаются из settings.quality_weights
- Формула основана на best practices астрофотографии (PixInsight, APP, N.I.N.A. community)
- Настраиваемые пороги через конфигурацию
- Используется AuditorAgent и SessionsMetadata

Факторы качества (согласованные веса):
┌─────────────────────────────────────────────────────────────┐
│  HFR (30%)        → Основной показатель резкости             │
│  Eccentricity (20%) → Качество гидирования                   │
│  Acceptance Rate (15%) → Стабильность системы                │
│  RMS (15%)        → Стабильность монтировки                  │
│  HFR Trend (10%)  → Стабильность во времени                  │
│  Problems (10%)   → Количество алертов/ошибок                │
└─────────────────────────────────────────────────────────────┘

Использование:
    from app.core.quality import calculate_quality_score

    score = calculate_quality_score(
        avg_hfr=2.3,
        avg_eccentricity=0.35,
        acceptance_rate=0.92,
        avg_rms_total=1.2,
        hfr_trend=0.01,
        problems_count=1,
    )
    # score = 8.5 (из 10.0)
"""

import logging
from typing import Optional, Dict, Any
from dataclasses import dataclass

logger = logging.getLogger("QualityCalculator")


@dataclass
class QualityWeights:
    """Веса факторов качества (должны суммироваться в 1.0)."""

    hfr: float = 0.30
    eccentricity: float = 0.20
    acceptance_rate: float = 0.15
    rms: float = 0.15
    hfr_trend: float = 0.10
    problems: float = 0.10

    def validate(self) -> bool:
        """Проверяет, что веса суммируются в ~1.0."""
        total = (
            self.hfr
            + self.eccentricity
            + self.acceptance_rate
            + self.rms
            + self.hfr_trend
            + self.problems
        )
        return abs(total - 1.0) < 0.01


@dataclass
class QualityThresholds:
    """Пороговые значения для расчёта штрафов/бонусов."""

    # HFR (pixels) — меньше = лучше
    hfr_excellent: float = 2.0
    hfr_good: float = 2.5
    hfr_acceptable: float = 3.0
    hfr_poor: float = 3.5

    # Eccentricity (0-1) — ближе к 0 = лучше
    eccentricity_excellent: float = 0.3
    eccentricity_good: float = 0.4
    eccentricity_acceptable: float = 0.5
    eccentricity_poor: float = 0.7

    # Acceptance Rate (0-1) — больше = лучше
    acceptance_excellent: float = 0.95
    acceptance_good: float = 0.90
    acceptance_acceptable: float = 0.80
    acceptance_poor: float = 0.70

    # RMS Total (arcsec) — меньше = лучше
    rms_excellent: float = 1.0
    rms_good: float = 1.5
    rms_acceptable: float = 2.0
    rms_poor: float = 3.0

    # HFR Trend (pixels/frame) — отрицательный = улучшение
    hfr_trend_degrading: float = 0.05
    hfr_trend_stable: float = 0.02
    hfr_trend_improving: float = -0.02

    # Problems count
    problems_few: int = 2
    problems_many: int = 5


def _load_weights() -> QualityWeights:
    """Загружает веса из settings.quality_weights."""
    try:
        from app.core.config import settings

        qw = getattr(settings, "quality_weights", None)
        if qw:
            weights = QualityWeights(
                hfr=getattr(qw, "hfr_weight", 0.30),
                eccentricity=getattr(qw, "eccentricity_weight", 0.20),
                acceptance_rate=getattr(qw, "acceptance_rate_weight", 0.15),
                rms=getattr(qw, "rms_weight", 0.15),
                hfr_trend=getattr(qw, "hfr_trend_weight", 0.10),
                problems=getattr(qw, "problems_weight", 0.10),
            )
            if weights.validate():
                return weights
            else:
                logger.warning("Quality weights do not sum to 1.0, using defaults")
    except Exception as e:
        logger.debug(f"Could not load quality_weights from settings: {e}")
    return QualityWeights()


def _load_thresholds() -> QualityThresholds:
    """Загружает пороги из settings.quality_thresholds."""
    try:
        from app.core.config import settings

        qt = getattr(settings, "quality_thresholds", None)
        if qt:
            return QualityThresholds(
                hfr_excellent=getattr(qt, "hfr_excellent", 2.0),
                hfr_good=getattr(qt, "hfr_good", 2.5),
                hfr_acceptable=getattr(qt, "hfr_acceptable", 3.0),
                hfr_poor=getattr(qt, "hfr_poor", 3.5),
                eccentricity_excellent=getattr(qt, "eccentricity_excellent", 0.3),
                eccentricity_good=getattr(qt, "eccentricity_good", 0.4),
                eccentricity_acceptable=getattr(qt, "eccentricity_acceptable", 0.5),
                eccentricity_poor=getattr(qt, "eccentricity_poor", 0.7),
                acceptance_excellent=getattr(qt, "acceptance_excellent", 0.95),
                acceptance_good=getattr(qt, "acceptance_good", 0.90),
                acceptance_acceptable=getattr(qt, "acceptance_acceptable", 0.80),
                acceptance_poor=getattr(qt, "acceptance_poor", 0.70),
                rms_excellent=getattr(qt, "rms_excellent", 1.0),
                rms_good=getattr(qt, "rms_good", 1.5),
                rms_acceptable=getattr(qt, "rms_acceptable", 2.0),
                rms_poor=getattr(qt, "rms_poor", 3.0),
                hfr_trend_degrading=getattr(qt, "hfr_trend_degrading", 0.05),
                hfr_trend_stable=getattr(qt, "hfr_trend_stable", 0.02),
                hfr_trend_improving=getattr(qt, "hfr_trend_improving", -0.02),
                problems_few=getattr(qt, "problems_few", 2),
                problems_many=getattr(qt, "problems_many", 5),
            )
    except Exception as e:
        logger.debug(f"Could not load quality_thresholds from settings: {e}")
    return QualityThresholds()


def calculate_quality_score(
    avg_hfr: Optional[float] = None,
    avg_eccentricity: Optional[float] = None,
    acceptance_rate: Optional[float] = None,
    avg_rms_total: Optional[float] = None,
    hfr_trend: Optional[float] = None,
    problems_count: int = 0,
    weights: Optional[QualityWeights] = None,
    thresholds: Optional[QualityThresholds] = None,
) -> float:
    """
    Рассчитывает overall quality score сессии (0.0 — 10.0).

    Формула основана на штрафах и бонусах от базового значения 10.0.
    Каждый фактор вносит вклад пропорционально своему весу.

    Args:
        avg_hfr: Средний Half Flux Radius (pixels)
        avg_eccentricity: Средняя эксцентричность звёзд (0-1)
        acceptance_rate: Процент принятых кадров (0-1)
        avg_rms_total: Средний RMS гидирования (arcsec)
        hfr_trend: Тренд HFR (pixels/frame, + = деградация)
        problems_count: Количество проблем/алертов за сессию
        weights: Веса факторов (None = из конфига)
        thresholds: Пороги (None = из конфига)

    Returns:
        Quality score от 0.0 до 10.0
    """
    if weights is None:
        weights = _load_weights()
    if thresholds is None:
        thresholds = _load_thresholds()

    score = 10.0

    # === 1. HFR penalty (вес: 30%) ===
    # Максимальный штраф: weights.hfr * 10 = 3.0 балла
    if avg_hfr is not None:
        max_penalty = weights.hfr * 10.0
        if avg_hfr > thresholds.hfr_poor:
            score -= max_penalty  # -3.0
        elif avg_hfr > thresholds.hfr_acceptable:
            score -= max_penalty * 0.6  # -1.8
        elif avg_hfr > thresholds.hfr_good:
            score -= max_penalty * 0.3  # -0.9
        elif avg_hfr <= thresholds.hfr_excellent:
            score += max_penalty * 0.15  # +0.45 бонус

    # === 2. Eccentricity penalty (вес: 20%) ===
    # Максимальный штраф: weights.eccentricity * 10 = 2.0 балла
    if avg_eccentricity is not None:
        max_penalty = weights.eccentricity * 10.0
        if avg_eccentricity > thresholds.eccentricity_poor:
            score -= max_penalty  # -2.0
        elif avg_eccentricity > thresholds.eccentricity_acceptable:
            score -= max_penalty * 0.6  # -1.2
        elif avg_eccentricity > thresholds.eccentricity_good:
            score -= max_penalty * 0.3  # -0.6
        elif avg_eccentricity <= thresholds.eccentricity_excellent:
            score += max_penalty * 0.15  # +0.3 бонус

    # === 3. Acceptance Rate (вес: 15%) ===
    # Максимальный штраф: weights.acceptance_rate * 10 = 1.5 балла
    if acceptance_rate is not None:
        max_penalty = weights.acceptance_rate * 10.0
        if acceptance_rate < thresholds.acceptance_poor:
            score -= max_penalty  # -1.5
        elif acceptance_rate < thresholds.acceptance_acceptable:
            score -= max_penalty * 0.6  # -0.9
        elif acceptance_rate >= thresholds.acceptance_excellent:
            score += max_penalty * 0.3  # +0.45 бонус

    # === 4. RMS Total penalty (вес: 15%) ===
    # Максимальный штраф: weights.rms * 10 = 1.5 балла
    if avg_rms_total is not None:
        max_penalty = weights.rms * 10.0
        if avg_rms_total > thresholds.rms_poor:
            score -= max_penalty  # -1.5
        elif avg_rms_total > thresholds.rms_acceptable:
            score -= max_penalty * 0.6  # -0.9
        elif avg_rms_total > thresholds.rms_good:
            score -= max_penalty * 0.3  # -0.45
        elif avg_rms_total <= thresholds.rms_excellent:
            score += max_penalty * 0.15  # +0.225 бонус

    # === 5. HFR Trend (вес: 10%) ===
    # Максимальный штраф: weights.hfr_trend * 10 = 1.0 балл
    if hfr_trend is not None:
        max_penalty = weights.hfr_trend * 10.0
        if hfr_trend > thresholds.hfr_trend_degrading:
            score -= max_penalty  # -1.0 (деградация)
        elif hfr_trend < thresholds.hfr_trend_improving:
            score += max_penalty * 0.3  # +0.3 бонус (улучшение)

    # === 6. Problems count (вес: 10%) ===
    # Максимальный штраф: weights.problems * 10 = 1.0 балл
    max_penalty = weights.problems * 10.0
    if problems_count > thresholds.problems_many:
        score -= max_penalty  # -1.0
    elif problems_count > thresholds.problems_few:
        score -= max_penalty * 0.5  # -0.5

    return round(max(0.0, min(10.0, score)), 2)


def grade_quality_score(score: float) -> str:
    """
    Определяет грейд по quality score.

    Args:
        score: Quality score (0-10)

    Returns:
        Грейд: EXCELLENT, GOOD, FAIR, POOR
    """
    if score >= 8.0:
        return "EXCELLENT"
    elif score >= 6.0:
        return "GOOD"
    elif score >= 4.0:
        return "FAIR"
    else:
        return "POOR"


def get_quality_breakdown(
    avg_hfr: Optional[float] = None,
    avg_eccentricity: Optional[float] = None,
    acceptance_rate: Optional[float] = None,
    avg_rms_total: Optional[float] = None,
    hfr_trend: Optional[float] = None,
    problems_count: int = 0,
) -> Dict[str, Any]:
    """
    Возвращает детальный breakdown расчёта quality score.
    Полезно для API и отладки.

    Returns:
        Dict с total_score, grade, и вкладами каждого фактора
    """
    weights = _load_weights()
    thresholds = _load_thresholds()

    total = calculate_quality_score(
        avg_hfr=avg_hfr,
        avg_eccentricity=avg_eccentricity,
        acceptance_rate=acceptance_rate,
        avg_rms_total=avg_rms_total,
        hfr_trend=hfr_trend,
        problems_count=problems_count,
        weights=weights,
        thresholds=thresholds,
    )

    # Рассчитываем вклад каждого фактора
    factors = {}

    if avg_hfr is not None:
        grade = (
            "excellent"
            if avg_hfr <= thresholds.hfr_excellent
            else "good"
            if avg_hfr <= thresholds.hfr_good
            else "acceptable"
            if avg_hfr <= thresholds.hfr_acceptable
            else "poor"
            if avg_hfr <= thresholds.hfr_poor
            else "critical"
        )
        factors["hfr"] = {
            "value": avg_hfr,
            "weight": weights.hfr,
            "grade": grade,
            "threshold": thresholds.hfr_good,
        }

    if avg_eccentricity is not None:
        grade = (
            "excellent"
            if avg_eccentricity <= thresholds.eccentricity_excellent
            else "good"
            if avg_eccentricity <= thresholds.eccentricity_good
            else "acceptable"
            if avg_eccentricity <= thresholds.eccentricity_acceptable
            else "poor"
        )
        factors["eccentricity"] = {
            "value": avg_eccentricity,
            "weight": weights.eccentricity,
            "grade": grade,
            "threshold": thresholds.eccentricity_good,
        }

    if acceptance_rate is not None:
        grade = (
            "excellent"
            if acceptance_rate >= thresholds.acceptance_excellent
            else "good"
            if acceptance_rate >= thresholds.acceptance_good
            else "acceptable"
            if acceptance_rate >= thresholds.acceptance_acceptable
            else "poor"
        )
        factors["acceptance_rate"] = {
            "value": acceptance_rate,
            "weight": weights.acceptance_rate,
            "grade": grade,
            "threshold": thresholds.acceptance_good,
        }

    if avg_rms_total is not None:
        grade = (
            "excellent"
            if avg_rms_total <= thresholds.rms_excellent
            else "good"
            if avg_rms_total <= thresholds.rms_good
            else "acceptable"
            if avg_rms_total <= thresholds.rms_acceptable
            else "poor"
        )
        factors["rms"] = {
            "value": avg_rms_total,
            "weight": weights.rms,
            "grade": grade,
            "threshold": thresholds.rms_good,
        }

    if hfr_trend is not None:
        grade = (
            "improving"
            if hfr_trend < thresholds.hfr_trend_improving
            else "stable"
            if hfr_trend <= thresholds.hfr_trend_stable
            else "degrading"
        )
        factors["hfr_trend"] = {
            "value": hfr_trend,
            "weight": weights.hfr_trend,
            "grade": grade,
            "threshold": thresholds.hfr_trend_stable,
        }

    factors["problems"] = {
        "value": problems_count,
        "weight": weights.problems,
        "grade": "few"
        if problems_count <= thresholds.problems_few
        else "moderate"
        if problems_count <= thresholds.problems_many
        else "many",
        "threshold": thresholds.problems_few,
    }

    return {
        "total_score": total,
        "grade": grade_quality_score(total),
        "factors": factors,
        "weights": {
            "hfr": weights.hfr,
            "eccentricity": weights.eccentricity,
            "acceptance_rate": weights.acceptance_rate,
            "rms": weights.rms,
            "hfr_trend": weights.hfr_trend,
            "problems": weights.problems,
        },
    }
