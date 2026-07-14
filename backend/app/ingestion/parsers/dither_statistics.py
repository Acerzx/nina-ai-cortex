"""
Dither Statistics Parser — парсинг метрик качества дизеринга.
Обрабатывает CSV/JSON отчёты от плагина Dither Statistics.

Извлекаемые метрики:
- CD² (Centered L₂ Discrepancy) — глобальная равномерность
- GFM (Gap-Fill Metric) — эффективность покрытия для drizzle (1×, 2×, 3×)
- Voronoi CV — локальная пространственная равномерность
- NNI (Nearest Neighbor Index) — индекс ближайшего соседа
- Combined Score — интегральная оценка качества

Использование:
    from app.ingestion.parsers.dither_statistics import (
        parse_dither_statistics_csv,
        parse_dither_statistics_json,
        calculate_combined_score,
    )

    # Парсинг CSV
    report = parse_dither_statistics_csv(file_path)

    # Парсинг JSON
    report = parse_dither_statistics_json(file_path)

    # Расчёт Combined Score
    score = calculate_combined_score(cd=0.15, gfm_2x=0.96, voronoi_cv=0.35)
"""

import csv
import json
import logging
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger("DitherStatisticsParser")


# ============================================================================
# МОДЕЛИ ДАННЫХ
# ============================================================================


@dataclass
class DitherPosition:
    """Позиция одного дизеринга."""

    index: int
    x_offset: float  # Смещение по X в пикселях
    y_offset: float  # Смещение по Y в пикселях
    cumulative_x: float  # Кумулятивное смещение по X
    cumulative_y: float  # Кумулятивное смещение по Y
    timestamp: Optional[str] = None
    settle_time_seconds: Optional[float] = None


@dataclass
class CD2Metric:
    """
    Centered L₂ Discrepancy (CD²) — мера глобальной равномерности распределения.

    Интерпретация (STRICT grading scale):
    - < 0.05: Excellent (50-100+ well-distributed dithers)
    - 0.05-0.10: Very Good (40-50 dithers)
    - 0.10-0.20: Good (25-35 dithers)
    - 0.20-0.35: Acceptable (15-25 dithers)
    - 0.35-0.50: Fair (some clustering present)
    - > 0.50: Poor (heavily clustered or biased patterns)
    """

    value: float
    grade: str
    description: str

    @classmethod
    def from_value(cls, value: float) -> "CD2Metric":
        """Создаёт метрику из значения с автоматическим грейдингом."""
        if value < 0.05:
            grade = "Excellent"
            description = "Excellent uniformity (50-100+ well-distributed dithers)"
        elif value < 0.10:
            grade = "Very Good"
            description = "Very Good uniformity (40-50 dithers)"
        elif value < 0.20:
            grade = "Good"
            description = "Good uniformity (25-35 dithers)"
        elif value < 0.35:
            grade = "Acceptable"
            description = "Acceptable uniformity (15-25 dithers)"
        elif value < 0.50:
            grade = "Fair"
            description = "Fair uniformity (some clustering present)"
        else:
            grade = "Poor"
            description = "Poor uniformity (heavily clustered or biased patterns)"

        return cls(value=value, grade=grade, description=description)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": round(self.value, 4),
            "grade": self.grade,
            "description": self.description,
        }


@dataclass
class VoronoiCVMetric:
    """
    Voronoi Cell Coefficient of Variation (CV) — локальная пространственная равномерность.

    Интерпретация:
    - < 0.25: Excellent (near-regular distribution)
    - 0.25-0.40: Good (low clustering)
    - 0.40-0.60: Acceptable (random-like, OK for drizzle!)
    - 0.60-0.80: Fair (moderate clustering)
    - > 0.80: Poor (significant clustering or gaps)

    Note: Even random distribution (CV ≈ 0.5) is acceptable for drizzle processing.
    """

    value: float
    grade: str
    description: str

    @classmethod
    def from_value(cls, value: float) -> "VoronoiCVMetric":
        """Создаёт метрику из значения с автоматическим грейдингом."""
        if value < 0.25:
            grade = "Excellent"
            description = "Excellent local uniformity (near-regular distribution)"
        elif value < 0.40:
            grade = "Good"
            description = "Good local distribution (low clustering)"
        elif value < 0.60:
            grade = "Acceptable"
            description = "Acceptable uniformity (random-like, OK for drizzle!)"
        elif value < 0.80:
            grade = "Fair"
            description = "Fair uniformity (moderate clustering)"
        else:
            grade = "Poor"
            description = "Poor uniformity - significant clustering or gaps"

        return cls(value=value, grade=grade, description=description)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": round(self.value, 4),
            "grade": self.grade,
            "description": self.description,
        }


@dataclass
class GFMMetric:
    """
    Gap-Fill Metric (GFM) — эффективность субпиксельного покрытия для drizzle.

    Интерпретация (CORRECTED targets):
    - 1× Drizzle: Target ≥98% (easy: achievable with 10-15 dithers)
    - 2× Drizzle: Target ≥95% (moderate: requires 25-30 dithers)
    - 3× Drizzle: Target ≥90% (very demanding: needs 80+ dithers)

    Critical Understanding: Higher drizzle scales are MORE challenging because:
    - Scale 2× creates 4× more output pixels than 1×
    - Scale 3× creates 9× more output pixels than 1×
    - More pixels require exponentially more dithers for equivalent coverage
    """

    scale_1x: Optional[float] = None  # Coverage at 1× drizzle (%)
    scale_2x: Optional[float] = None  # Coverage at 2× drizzle (%)
    scale_3x: Optional[float] = None  # Coverage at 3× drizzle (%)
    grade: str = "Unknown"
    description: str = ""

    def __post_init__(self):
        """Автоматический грейдинг на основе 2× drizzle coverage."""
        if self.scale_2x is not None:
            if self.scale_2x >= 0.95:
                self.grade = "Excellent"
                self.description = "Excellent 2× drizzle coverage (≥95%)"
            elif self.scale_2x >= 0.90:
                self.grade = "Good"
                self.description = "Good 2× drizzle coverage (90-95%)"
            elif self.scale_2x >= 0.85:
                self.grade = "Acceptable"
                self.description = "Acceptable 2× drizzle coverage (85-90%)"
            elif self.scale_2x >= 0.75:
                self.grade = "Fair"
                self.description = "Fair 2× drizzle coverage (75-85%)"
            else:
                self.grade = "Poor"
                self.description = "Poor 2× drizzle coverage (<75%)"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "scale_1x": round(self.scale_1x * 100, 2)
            if self.scale_1x is not None
            else None,
            "scale_2x": round(self.scale_2x * 100, 2)
            if self.scale_2x is not None
            else None,
            "scale_3x": round(self.scale_3x * 100, 2)
            if self.scale_3x is not None
            else None,
            "grade": self.grade,
            "description": self.description,
        }


@dataclass
class NNIMetric:
    """
    Nearest Neighbor Index (NNI) — индекс ближайшего соседа.

    Сравнивает среднее расстояние до ближайшего соседа с ожидаемым
    для случайного распределения.

    Интерпретация:
    - NNI > 1.5: Excellent (almost regular grid)
    - NNI > 1.2: Good (quasi-random distribution)
    - NNI ≈ 1.0: Acceptable (random distribution, fine for drizzle!)
    - NNI < 0.8: Fair (some clustering)
    - NNI < 0.6: Poor (significant clustering)
    """

    value: float
    grade: str
    description: str

    @classmethod
    def from_value(cls, value: float) -> "NNIMetric":
        """Создаёт метрику из значения с автоматическим грейдингом."""
        if value > 1.5:
            grade = "Excellent"
            description = "Excellent (almost regular grid)"
        elif value > 1.2:
            grade = "Good"
            description = "Good (quasi-random distribution)"
        elif value > 0.8:
            grade = "Acceptable"
            description = "Acceptable (random distribution, fine for drizzle!)"
        elif value > 0.6:
            grade = "Fair"
            description = "Fair (some clustering)"
        else:
            grade = "Poor"
            description = "Poor (significant clustering)"

        return cls(value=value, grade=grade, description=description)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": round(self.value, 4),
            "grade": self.grade,
            "description": self.description,
        }


@dataclass
class DitherAnomaly:
    """Обнаруженная аномалия в паттерне дизеринга."""

    anomaly_type: str  # CLUSTERING, GAPS, BIAS, INSUFFICIENT_COVERAGE
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    metric_value: Optional[float] = None
    threshold_value: Optional[float] = None
    recommendation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "anomaly_type": self.anomaly_type,
            "severity": self.severity,
            "description": self.description,
            "metric_value": self.metric_value,
            "threshold_value": self.threshold_value,
            "recommendation": self.recommendation,
        }


@dataclass
class DitherStatisticsReport:
    """Полный отчёт по качеству дизеринга."""

    file_name: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    # Базовая статистика
    total_dithers: int = 0
    avg_settle_time_seconds: Optional[float] = None
    total_pixel_drift_x: Optional[float] = None
    total_pixel_drift_y: Optional[float] = None

    # Расширенные метрики
    cd2: Optional[CD2Metric] = None
    voronoi_cv: Optional[VoronoiCVMetric] = None
    gfm: Optional[GFMMetric] = None
    nni: Optional[NNIMetric] = None

    # Интегральная оценка
    combined_score: Optional[float] = None
    overall_grade: str = "Unknown"

    # Позиции дизеринга
    positions: List[DitherPosition] = field(default_factory=list)

    # Обнаруженные аномалии
    anomalies: List[DitherAnomaly] = field(default_factory=list)

    # Рекомендации
    recommendations: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_name": self.file_name,
            "timestamp": self.timestamp,
            "total_dithers": self.total_dithers,
            "avg_settle_time_seconds": self.avg_settle_time_seconds,
            "total_pixel_drift_x": self.total_pixel_drift_x,
            "total_pixel_drift_y": self.total_pixel_drift_y,
            "cd2": self.cd2.to_dict() if self.cd2 else None,
            "voronoi_cv": self.voronoi_cv.to_dict() if self.voronoi_cv else None,
            "gfm": self.gfm.to_dict() if self.gfm else None,
            "nni": self.nni.to_dict() if self.nni else None,
            "combined_score": round(self.combined_score, 4)
            if self.combined_score is not None
            else None,
            "overall_grade": self.overall_grade,
            "positions_count": len(self.positions),
            "anomalies": [a.to_dict() for a in self.anomalies],
            "anomalies_count": len(self.anomalies),
            "recommendations": self.recommendations,
        }


# ============================================================================
# ФУНКЦИИ РАСЧЁТА МЕТРИК
# ============================================================================


def calculate_combined_score(
    cd: float,
    gfm_2x: float,
    voronoi_cv: float,
    weights: Optional[Dict[str, float]] = None,
) -> float:
    """
    Рассчитывает Combined Score по формуле плагина.

    Formula:
        Combined Score = w1×(1 - normalized_CD) + w2×GFM_2x + w3×(1 - Voronoi_CV)

    Weights (calibrated for strict grading):
        - w1 = 0.35 (CD - global uniformity)
        - w2 = 0.45 (GFM - drizzle coverage, most directly useful)
        - w3 = 0.20 (Voronoi CV - local uniformity, supplementary)

    Args:
        cd: Centered L₂ Discrepancy value (0-1)
        gfm_2x: Gap-Fill Metric at 2× drizzle (0-1)
        voronoi_cv: Voronoi Cell CV value (0-1)
        weights: Опциональные веса (по умолчанию стандартные)

    Returns:
        Combined Score (0-1)
    """
    if weights is None:
        weights = {
            "cd": 0.35,
            "gfm": 0.45,
            "voronoi": 0.20,
        }

    # Нормализация CD (предполагаем, что CD уже в диапазоне 0-1)
    normalized_cd = min(1.0, cd)

    score = (
        weights["cd"] * (1.0 - normalized_cd)
        + weights["gfm"] * gfm_2x
        + weights["voronoi"] * (1.0 - voronoi_cv)
    )

    return max(0.0, min(1.0, score))


def grade_combined_score(score: float) -> Tuple[str, str]:
    """
    Определяет грейд и описание для Combined Score.

    Quality Ratings:
        - Excellent (≥0.85): Professional-grade pattern, suitable for 3× drizzle
        - Good (≥0.75): High-quality, recommended for 2× drizzle
        - Acceptable (≥0.60): Standard quality, adequate for 1× drizzle
        - Fair (0.40-0.60): Suboptimal, consider more dithers
        - Poor (<0.40): Insufficient quality, expect artifacts

    Args:
        score: Combined Score (0-1)

    Returns:
        Tuple (grade, description)
    """
    if score >= 0.85:
        grade = "Excellent"
        description = "Professional-grade pattern, suitable for 3× drizzle"
    elif score >= 0.75:
        grade = "Good"
        description = "High-quality, recommended for 2× drizzle"
    elif score >= 0.60:
        grade = "Acceptable"
        description = "Standard quality, adequate for 1× drizzle"
    elif score >= 0.40:
        grade = "Fair"
        description = "Suboptimal, consider more dithers"
    else:
        grade = "Poor"
        description = "Insufficient quality, expect artifacts"

    return grade, description


def detect_dither_anomalies(
    positions: List[DitherPosition],
    cd2: Optional[CD2Metric] = None,
    voronoi_cv: Optional[VoronoiCVMetric] = None,
    gfm: Optional[GFMMetric] = None,
) -> List[DitherAnomaly]:
    """
    Детектирует аномалии в паттерне дизеринга.

    Проверяемые аномалии:
    - CLUSTERING: Кластеризация точек (высокий Voronoi CV)
    - GAPS: Пропуски в покрытии (низкий GFM)
    - BIAS: Смещение паттерна (несимметричное распределение)
    - INSUFFICIENT_COVERAGE: Недостаточное количество дизерингов

    Args:
        positions: Список позиций дизеринга
        cd2: Метрика CD²
        voronoi_cv: Метрика Voronoi CV
        gfm: Метрика GFM

    Returns:
        Список обнаруженных аномалий
    """
    anomalies = []

    # Проверка 1: Недостаточное количество дизерингов
    if len(positions) < 10:
        anomalies.append(
            DitherAnomaly(
                anomaly_type="INSUFFICIENT_COVERAGE",
                severity="HIGH",
                description=f"Only {len(positions)} dithers detected. Minimum 10-15 recommended for good coverage.",
                metric_value=len(positions),
                threshold_value=10,
                recommendation="Increase dither frequency or total number of frames",
            )
        )

    # Проверка 2: Кластеризация (высокий Voronoi CV)
    if voronoi_cv and voronoi_cv.value > 0.60:
        severity = "CRITICAL" if voronoi_cv.value > 0.80 else "HIGH"
        anomalies.append(
            DitherAnomaly(
                anomaly_type="CLUSTERING",
                severity=severity,
                description=f"Significant clustering detected (Voronoi CV = {voronoi_cv.value:.3f})",
                metric_value=voronoi_cv.value,
                threshold_value=0.60,
                recommendation="Use random dither pattern instead of systematic (spiral/grid)",
            )
        )

    # Проверка 3: Пропуски в покрытии (низкий GFM)
    if gfm and gfm.scale_2x is not None and gfm.scale_2x < 0.85:
        severity = "HIGH" if gfm.scale_2x < 0.75 else "MEDIUM"
        anomalies.append(
            DitherAnomaly(
                anomaly_type="GAPS",
                severity=severity,
                description=f"Poor sub-pixel coverage at 2× drizzle ({gfm.scale_2x * 100:.1f}%)",
                metric_value=gfm.scale_2x,
                threshold_value=0.85,
                recommendation="Increase number of dithers to 30+ for better 2× drizzle coverage",
            )
        )

    # Проверка 4: Смещение паттерна (Bias)
    if len(positions) >= 5:
        x_offsets = [p.x_offset for p in positions]
        y_offsets = [p.y_offset for p in positions]

        mean_x = np.mean(x_offsets)
        mean_y = np.mean(y_offsets)

        # Если среднее смещение значительно отличается от нуля
        bias_threshold = 2.0  # пикселей
        if abs(mean_x) > bias_threshold or abs(mean_y) > bias_threshold:
            anomalies.append(
                DitherAnomaly(
                    anomaly_type="BIAS",
                    severity="MEDIUM",
                    description=f"Dither pattern biased (mean offset: X={mean_x:.2f}, Y={mean_y:.2f} px)",
                    metric_value=max(abs(mean_x), abs(mean_y)),
                    threshold_value=bias_threshold,
                    recommendation="Check dither settings - pattern should be centered around origin",
                )
            )

    # Проверка 5: Высокий CD² (глобальная неравномерность)
    if cd2 and cd2.value > 0.35:
        severity = "HIGH" if cd2.value > 0.50 else "MEDIUM"
        anomalies.append(
            DitherAnomaly(
                anomaly_type="POOR_UNIFORMITY",
                severity=severity,
                description=f"Poor global uniformity (CD² = {cd2.value:.3f})",
                metric_value=cd2.value,
                threshold_value=0.35,
                recommendation="Review dither strategy - consider quasi-random patterns",
            )
        )

    return anomalies


def generate_recommendations(
    report: DitherStatisticsReport,
) -> List[str]:
    """
    Генерирует рекомендации по оптимизации стратегии дизеринга.

    Args:
        report: Отчёт по качеству дизеринга

    Returns:
        Список рекомендаций
    """
    recommendations = []

    # Рекомендация 1: На основе Combined Score
    if report.combined_score is not None:
        if report.combined_score < 0.60:
            recommendations.append(
                "Overall dither quality is suboptimal. "
                "Consider increasing dither frequency to 30+ frames for better coverage."
            )
        elif report.combined_score < 0.75:
            recommendations.append(
                "Dither quality is acceptable but could be improved. "
                "Target 25-30 dithers for Good quality."
            )

    # Рекомендация 2: На основе CD²
    if report.cd2:
        if report.cd2.grade == "Poor":
            recommendations.append(
                "Global uniformity is poor. "
                "Switch from systematic patterns (spiral/grid) to random dithering."
            )
        elif report.cd2.grade == "Fair":
            recommendations.append(
                "Global uniformity could be improved. "
                "Consider quasi-random dither patterns for better coverage."
            )

    # Рекомендация 3: На основе GFM
    if report.gfm:
        if report.gfm.scale_2x is not None and report.gfm.scale_2x < 0.90:
            recommendations.append(
                f"2× drizzle coverage is only {report.gfm.scale_2x * 100:.1f}%. "
                "For high-quality 2× drizzle, aim for ≥95% coverage with 30+ dithers."
            )
        if report.gfm.scale_3x is not None and report.gfm.scale_3x < 0.85:
            recommendations.append(
                f"3× drizzle coverage is only {report.gfm.scale_3x * 100:.1f}%. "
                "For 3× drizzle, you need 80+ well-distributed dithers."
            )

    # Рекомендация 4: На основе Voronoi CV
    if report.voronoi_cv and report.voronoi_cv.value > 0.60:
        recommendations.append(
            "Significant clustering detected in dither pattern. "
            "Use random dither with amplitude 2-5 pixels for better local uniformity."
        )

    # Рекомендация 5: На основе NNI
    if report.nni and report.nni.value < 0.8:
        recommendations.append(
            "Dither points are clustered (low NNI). "
            "Increase dither amplitude or use random pattern instead of systematic."
        )

    # Рекомендация 6: На основе аномалий
    for anomaly in report.anomalies:
        if anomaly.recommendation and anomaly.recommendation not in recommendations:
            recommendations.append(anomaly.recommendation)

    # Рекомендация 7: Если всё хорошо
    if (
        not recommendations
        and report.combined_score is not None
        and report.combined_score >= 0.75
    ):
        recommendations.append(
            "Excellent dither quality! Current strategy is working well. "
            "Continue with current settings."
        )

    return recommendations


# ============================================================================
# ФУНКЦИИ ПАРСИНГА
# ============================================================================


async def parse_dither_statistics_csv(
    file_path: Path,
) -> Optional[DitherStatisticsReport]:
    """
    Парсит CSV файл от Dither Statistics плагина.

    Ожидаемый формат CSV:
        Index,X_Offset,Y_Offset,Cumulative_X,Cumulative_Y,Settle_Time,CD2,Voronoi_CV,GFM_1x,GFM_2x,GFM_3x,NNI

    Args:
        file_path: Путь к CSV файлу

    Returns:
        DitherStatisticsReport или None при ошибке
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        lines = content.strip().split("\n")
        if len(lines) < 2:
            logger.warning(f"CSV file too short: {file_path}")
            return None

        # Парсим заголовок
        reader = csv.DictReader(lines)
        rows = list(reader)

        if not rows:
            logger.warning(f"No data in CSV: {file_path}")
            return None

        report = DitherStatisticsReport(file_name=file_path.name)
        report.total_dithers = len(rows)

        # Извлекаем позиции
        positions = []
        settle_times = []
        cd2_values = []
        voronoi_cv_values = []
        gfm_1x_values = []
        gfm_2x_values = []
        gfm_3x_values = []
        nni_values = []

        for row in rows:
            try:
                pos = DitherPosition(
                    index=int(row.get("Index", 0)),
                    x_offset=float(row.get("X_Offset", 0)),
                    y_offset=float(row.get("Y_Offset", 0)),
                    cumulative_x=float(row.get("Cumulative_X", 0)),
                    cumulative_y=float(row.get("Cumulative_Y", 0)),
                    timestamp=row.get("Timestamp"),
                    settle_time_seconds=float(row["Settle_Time"])
                    if row.get("Settle_Time")
                    else None,
                )
                positions.append(pos)

                if pos.settle_time_seconds is not None:
                    settle_times.append(pos.settle_time_seconds)

                # Извлекаем метрики (могут быть в последней строке)
                if "CD2" in row and row["CD2"]:
                    cd2_values.append(float(row["CD2"]))
                if "Voronoi_CV" in row and row["Voronoi_CV"]:
                    voronoi_cv_values.append(float(row["Voronoi_CV"]))
                if "GFM_1x" in row and row["GFM_1x"]:
                    gfm_1x_values.append(float(row["GFM_1x"]))
                if "GFM_2x" in row and row["GFM_2x"]:
                    gfm_2x_values.append(float(row["GFM_2x"]))
                if "GFM_3x" in row and row["GFM_3x"]:
                    gfm_3x_values.append(float(row["GFM_3x"]))
                if "NNI" in row and row["NNI"]:
                    nni_values.append(float(row["NNI"]))

            except (ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid row: {e}")
                continue

        report.positions = positions

        # Среднее время оседания
        if settle_times:
            report.avg_settle_time_seconds = float(np.mean(settle_times))

        # Общее смещение
        if positions:
            report.total_pixel_drift_x = positions[-1].cumulative_x
            report.total_pixel_drift_y = positions[-1].cumulative_y

        # Метрики (берём последние значения или средние)
        if cd2_values:
            cd2_value = cd2_values[-1]  # Последнее значение
            report.cd2 = CD2Metric.from_value(cd2_value)

        if voronoi_cv_values:
            voronoi_cv_value = voronoi_cv_values[-1]
            report.voronoi_cv = VoronoiCVMetric.from_value(voronoi_cv_value)

        if gfm_1x_values or gfm_2x_values or gfm_3x_values:
            report.gfm = GFMMetric(
                scale_1x=gfm_1x_values[-1] if gfm_1x_values else None,
                scale_2x=gfm_2x_values[-1] if gfm_2x_values else None,
                scale_3x=gfm_3x_values[-1] if gfm_3x_values else None,
            )

        if nni_values:
            nni_value = nni_values[-1]
            report.nni = NNIMetric.from_value(nni_value)

        # Расчёт Combined Score
        if (
            report.cd2
            and report.gfm
            and report.gfm.scale_2x is not None
            and report.voronoi_cv
        ):
            report.combined_score = calculate_combined_score(
                cd=report.cd2.value,
                gfm_2x=report.gfm.scale_2x,
                voronoi_cv=report.voronoi_cv.value,
            )
            report.overall_grade, _ = grade_combined_score(report.combined_score)

        # Детекция аномалий
        report.anomalies = detect_dither_anomalies(
            positions=positions,
            cd2=report.cd2,
            voronoi_cv=report.voronoi_cv,
            gfm=report.gfm,
        )

        # Генерация рекомендаций
        report.recommendations = generate_recommendations(report)

        logger.info(
            f"✅ Parsed Dither Statistics CSV: {file_path.name}, "
            f"{report.total_dithers} dithers, "
            f"combined score: {report.combined_score:.3f if report.combined_score else 'N/A'}, "
            f"grade: {report.overall_grade}"
        )

        return report

    except Exception as e:
        logger.error(f"❌ Failed to parse Dither Statistics CSV {file_path}: {e}")
        return None


async def parse_dither_statistics_json(
    file_path: Path,
) -> Optional[DitherStatisticsReport]:
    """
    Парсит JSON файл от Dither Statistics плагина.

    Ожидаемый формат JSON:
        {
            "total_dithers": 30,
            "avg_settle_time": 2.5,
            "positions": [
                {"index": 1, "x": 2.5, "y": -1.2, "cumulative_x": 2.5, "cumulative_y": -1.2, ...},
                ...
            ],
            "metrics": {
                "cd2": 0.15,
                "voronoi_cv": 0.35,
                "gfm_1x": 0.98,
                "gfm_2x": 0.96,
                "gfm_3x": 0.91,
                "nni": 1.25
            }
        }

    Args:
        file_path: Путь к JSON файлу

    Returns:
        DitherStatisticsReport или None при ошибке
    """
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            content = f.read()

        data = json.loads(content)

        report = DitherStatisticsReport(file_name=file_path.name)
        report.total_dithers = data.get("total_dithers", 0)
        report.avg_settle_time_seconds = data.get("avg_settle_time")

        # Извлекаем позиции
        positions = []
        for pos_data in data.get("positions", []):
            try:
                pos = DitherPosition(
                    index=pos_data.get("index", 0),
                    x_offset=pos_data.get("x", pos_data.get("x_offset", 0)),
                    y_offset=pos_data.get("y", pos_data.get("y_offset", 0)),
                    cumulative_x=pos_data.get("cumulative_x", 0),
                    cumulative_y=pos_data.get("cumulative_y", 0),
                    timestamp=pos_data.get("timestamp"),
                    settle_time_seconds=pos_data.get("settle_time"),
                )
                positions.append(pos)
            except (ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid position: {e}")
                continue

        report.positions = positions

        # Общее смещение
        if positions:
            report.total_pixel_drift_x = positions[-1].cumulative_x
            report.total_pixel_drift_y = positions[-1].cumulative_y

        # Извлекаем метрики
        metrics = data.get("metrics", {})

        if "cd2" in metrics:
            report.cd2 = CD2Metric.from_value(metrics["cd2"])

        if "voronoi_cv" in metrics:
            report.voronoi_cv = VoronoiCVMetric.from_value(metrics["voronoi_cv"])

        if any(k in metrics for k in ["gfm_1x", "gfm_2x", "gfm_3x"]):
            report.gfm = GFMMetric(
                scale_1x=metrics.get("gfm_1x"),
                scale_2x=metrics.get("gfm_2x"),
                scale_3x=metrics.get("gfm_3x"),
            )

        if "nni" in metrics:
            report.nni = NNIMetric.from_value(metrics["nni"])

        # Расчёт Combined Score
        if (
            report.cd2
            and report.gfm
            and report.gfm.scale_2x is not None
            and report.voronoi_cv
        ):
            report.combined_score = calculate_combined_score(
                cd=report.cd2.value,
                gfm_2x=report.gfm.scale_2x,
                voronoi_cv=report.voronoi_cv.value,
            )
            report.overall_grade, _ = grade_combined_score(report.combined_score)

        # Детекция аномалий
        report.anomalies = detect_dither_anomalies(
            positions=positions,
            cd2=report.cd2,
            voronoi_cv=report.voronoi_cv,
            gfm=report.gfm,
        )

        # Генерация рекомендаций
        report.recommendations = generate_recommendations(report)

        logger.info(
            f"✅ Parsed Dither Statistics JSON: {file_path.name}, "
            f"{report.total_dithers} dithers, "
            f"combined score: {report.combined_score:.3f if report.combined_score else 'N/A'}, "
            f"grade: {report.overall_grade}"
        )

        return report

    except Exception as e:
        logger.error(f"❌ Failed to parse Dither Statistics JSON {file_path}: {e}")
        return None


# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================


def analyze_dither_trend(
    positions: List[DitherPosition],
    window_size: int = 10,
) -> Optional[Dict[str, Any]]:
    """
    Анализирует тренд эффективности дизеринга.

    Проверяет, улучшается или ухудшается покрытие со временем.

    Args:
        positions: Список позиций дизеринга
        window_size: Размер окна для анализа тренда

    Returns:
        Словарь с информацией о тренде или None если недостаточно данных
    """
    if len(positions) < window_size * 2:
        return None

    # Разделяем на ранние и поздние дизеринги
    early = positions[:window_size]
    late = positions[-window_size:]

    # Рассчитываем среднее расстояние между точками для каждого окна
    def avg_distance(positions: List[DitherPosition]) -> float:
        if len(positions) < 2:
            return 0.0

        distances = []
        for i in range(len(positions) - 1):
            dx = positions[i + 1].x_offset - positions[i].x_offset
            dy = positions[i + 1].y_offset - positions[i].y_offset
            distances.append(np.sqrt(dx**2 + dy**2))

        return float(np.mean(distances))

    early_distance = avg_distance(early)
    late_distance = avg_distance(late)

    # Определяем тренд
    if late_distance > early_distance * 1.2:
        trend = "IMPROVING"
        description = "Dither coverage is improving over time"
    elif late_distance < early_distance * 0.8:
        trend = "DEGRADING"
        description = "Dither coverage is degrading over time"
    else:
        trend = "STABLE"
        description = "Dither coverage is stable"

    return {
        "trend": trend,
        "description": description,
        "early_avg_distance": round(early_distance, 3),
        "late_avg_distance": round(late_distance, 3),
        "change_percent": round(
            (late_distance - early_distance) / early_distance * 100, 2
        )
        if early_distance > 0
        else 0,
    }
