"""
Hocus Focus Parser — расширенный анализ качества звезд.
Парсит CSV-отчеты Hocus Focus плагина с детальной аналитикой.

ИСПРАВЛЕНО (Этап 9):
- Добавлены расширенные метрики: кома, астигматизм, эллиптичность, flux, background
- Расширен Z-Score фильтр для комы и астигматизма
- Добавлен расчет интегрального качества звезды (star_quality_score)
- Добавлена детекция проблем коллимации
- Добавлены процентили для расширенной аналитики
"""

import csv
import logging
import numpy as np
from pathlib import Path
from typing import List, Optional
from pydantic import BaseModel, Field

logger = logging.getLogger("HocusFocusParser")


class StarData(BaseModel):
    """Данные по одной звезде из Hocus Focus CSV"""

    x: float = Field(alias="X")
    y: float = Field(alias="Y")
    fwhm: Optional[float] = Field(alias="FWHM", default=None)
    hfr: Optional[float] = Field(alias="HFR", default=None)
    eccentricity: Optional[float] = Field(alias="Eccentricity", default=None)
    angle: Optional[float] = Field(alias="Angle", default=None)
    # Расширенные метрики (Этап 9)
    coma: Optional[float] = Field(alias="Coma", default=None)
    astigmatism: Optional[float] = Field(alias="Astigmatism", default=None)
    flux: Optional[float] = Field(alias="Flux", default=None)
    background: Optional[float] = Field(alias="Background", default=None)

    # Внутренние метки
    is_anomaly: bool = False
    anomaly_reason: Optional[str] = None

    # Интегральное качество (0-10)
    star_quality_score: float = 0.0

    class Config:
        populate_by_name = True

    def calculate_quality_score(self) -> float:
        """
        Рассчитывает интегральный качество звезды (0-10).

        Факторы:
        - FWHM (меньше = лучше) - 30%
        - HFR (меньше = лучше) - 25%
        - Eccentricity (ближе к 0 = лучше) - 20%
        - Coma (ближе к 0 = лучше) - 15%
        - Astigmatism (ближе к 0 = лучше) - 10%
        """
        score = 10.0

        # FWHM penalty (идеально < 2.0, плохо > 4.0)
        if self.fwhm:
            if self.fwhm > 4.0:
                score -= 3.0
            elif self.fwhm > 3.0:
                score -= 2.0
            elif self.fwhm > 2.5:
                score -= 1.0

        # HFR penalty (идеально < 1.5, плохо > 3.0)
        if self.hfr:
            if self.hfr > 3.0:
                score -= 2.5
            elif self.hfr > 2.5:
                score -= 1.5
            elif self.hfr > 2.0:
                score -= 0.5

        # Eccentricity penalty (идеально < 0.3, плохо > 0.7)
        if self.eccentricity:
            if self.eccentricity > 0.7:
                score -= 2.0
            elif self.eccentricity > 0.5:
                score -= 1.0
            elif self.eccentricity > 0.3:
                score -= 0.5

        # Coma penalty (если доступно)
        if self.coma:
            if abs(self.coma) > 0.5:
                score -= 1.5
            elif abs(self.coma) > 0.3:
                score -= 0.5

        # Astigmatism penalty (если доступно)
        if self.astigmatism:
            if abs(self.astigmatism) > 0.5:
                score -= 1.0
            elif abs(self.astigmatism) > 0.3:
                score -= 0.5

        self.star_quality_score = max(0.0, min(10.0, score))
        return self.star_quality_score


class CollimationIssue(BaseModel):
    """Выявленная проблема коллимации."""

    issue_type: str  # "coma", "astigmatism", "radial_coma"
    severity: str  # "LOW", "MEDIUM", "HIGH"
    description: str
    value: float
    threshold: float


class HocusFocusReport(BaseModel):
    """Итоговый отчет по кадру после фильтрации"""

    file_name: str
    total_stars_detected: int
    valid_stars_count: int
    anomalies_count: int

    # Агрегированные метрики (Median более устойчив к выбросам, чем Mean)
    median_fwhm: Optional[float] = None
    median_hfr: Optional[float] = None
    median_eccentricity: Optional[float] = None

    # Расширенные метрики (Этап 9)
    median_coma: Optional[float] = None
    median_astigmatism: Optional[float] = None
    median_star_quality: Optional[float] = None

    # Процентили (для понимания распределения)
    fwhm_25th: Optional[float] = None
    fwhm_75th: Optional[float] = None
    hfr_25th: Optional[float] = None
    hfr_75th: Optional[float] = None

    # Детекция проблем коллимации (Этап 9)
    collimation_issues: List[CollimationIssue] = Field(default_factory=list)

    stars: List[StarData] = []


def parse_hocus_focus_csv(file_path: Path) -> List[StarData]:
    """Парсит CSV файл Hocus Focus с расширенными метриками."""
    stars = []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            # Hocus Focus использует запятые или точки с запятой в зависимости от локали
            # Определяем разделитель по первой строке
            sample = f.readline()
            f.seek(0)
            delimiter = ";" if ";" in sample else ","

            reader = csv.DictReader(f, delimiter=delimiter)
            for row in reader:
                try:
                    # Очистка данных: замена запятых на точки для float (европейская локаль)
                    cleaned_row = {
                        k: float(str(v).replace(",", "."))
                        if v and k not in ["X", "Y"]
                        else float(v)
                        for k, v in row.items()
                        if v and v.strip()
                    }

                    star = StarData(**cleaned_row)
                    # Рассчитываем качество звезды
                    star.calculate_quality_score()
                    stars.append(star)
                except ValueError as e:
                    logger.debug(f"Skipping invalid star row: {e}")
    except Exception as e:
        logger.error(f"Failed to parse Hocus Focus CSV {file_path}: {e}")

    return stars


def filter_anomalies(
    stars: List[StarData],
    z_threshold: float = 3.0,
    detect_collimation: bool = True,
) -> HocusFocusReport:
    """
    Применяет Z-Score фильтр для отсеивания аномальных звезд.

    ИСПРАВЛЕНО (Этап 9):
    - Расширен Z-Score фильтр для комы и астигматизма
    - Добавлена детекция проблем коллимации

    Аномалии:
    - Горячие пиксели
    - Пересвеченные звезды
    - Звезды на краях поля с сильной комой
    - Звезды с экстремальным эксцентриситетом
    """
    if not stars:
        return HocusFocusReport(
            file_name="",
            total_stars_detected=0,
            valid_stars_count=0,
            anomalies_count=0,
        )

    fwhm_values = np.array([s.fwhm for s in stars if s.fwhm is not None])
    coma_values = np.array([s.coma for s in stars if s.coma is not None])
    astigmatism_values = np.array(
        [s.astigmatism for s in stars if s.astigmatism is not None]
    )

    valid_stars = []
    anomalies_count = 0

    # Z-Score для FWHM
    if len(fwhm_values) >= 3:
        mean_fwhm = np.mean(fwhm_values)
        std_fwhm = np.std(fwhm_values)
    else:
        mean_fwhm = 0
        std_fwhm = 0

    # Z-Score для комы (если доступно)
    if len(coma_values) >= 3:
        mean_coma = np.mean(coma_values)
        std_coma = np.std(coma_values)
    else:
        mean_coma = 0
        std_coma = 0

    # Z-Score для астигматизма (если доступно)
    if len(astigmatism_values) >= 3:
        mean_astigmatism = np.mean(astigmatism_values)
        std_astigmatism = np.std(astigmatism_values)
    else:
        mean_astigmatism = 0
        std_astigmatism = 0

    for star in stars:
        is_anomaly = False
        anomaly_reasons = []

        # Проверка FWHM
        if star.fwhm is not None and std_fwhm > 0:
            z_score = abs(star.fwhm - mean_fwhm) / std_fwhm
            if z_score > z_threshold:
                is_anomaly = True
                anomaly_reasons.append(f"FWHM Z-Score {z_score:.2f} > {z_threshold}")

        # Проверка комы (Этап 9)
        if star.coma is not None and std_coma > 0:
            z_score_coma = abs(star.coma - mean_coma) / std_coma
            if z_score_coma > z_threshold:
                is_anomaly = True
                anomaly_reasons.append(
                    f"Coma Z-Score {z_score_coma:.2f} > {z_threshold}"
                )

        # Проверка астигматизма (Этап 9)
        if star.astigmatism is not None and std_astigmatism > 0:
            z_score_astigmatism = (
                abs(star.astigmatism - mean_astigmatism) / std_astigmatism
            )
            if z_score_astigmatism > z_threshold:
                is_anomaly = True
                anomaly_reasons.append(
                    f"Astigmatism Z-Score {z_score_astigmatism:.2f} > {z_threshold}"
                )

        # Дополнительная проверка: экстремальный эксцентриситет
        if star.eccentricity is not None and star.eccentricity > 0.85:
            is_anomaly = True
            anomaly_reasons.append(f"Extreme Eccentricity {star.eccentricity:.2f}")

        if is_anomaly:
            star.is_anomaly = True
            star.anomaly_reason = "; ".join(anomaly_reasons)
            anomalies_count += 1
        else:
            valid_stars.append(star)

    # Расчет агрегатов по очищенным данным
    valid_fwhm = [s.fwhm for s in valid_stars if s.fwhm is not None]
    valid_hfr = [s.hfr for s in valid_stars if s.hfr is not None]
    valid_ecc = [s.eccentricity for s in valid_stars if s.eccentricity is not None]
    valid_coma = [s.coma for s in valid_stars if s.coma is not None]
    valid_astigmatism = [
        s.astigmatism for s in valid_stars if s.astigmatism is not None
    ]
    valid_quality = [s.star_quality_score for s in valid_stars]

    # Детекция проблем коллимации (Этап 9)
    collimation_issues = []
    if detect_collimation:
        collimation_issues = _detect_collimation_issues(
            valid_stars, valid_coma, valid_astigmatism
        )

    return HocusFocusReport(
        file_name="",  # Заполняется в Watcher
        total_stars_detected=len(stars),
        valid_stars_count=len(valid_stars),
        anomalies_count=anomalies_count,
        median_fwhm=float(np.median(valid_fwhm)) if valid_fwhm else None,
        median_hfr=float(np.median(valid_hfr)) if valid_hfr else None,
        median_eccentricity=float(np.median(valid_ecc)) if valid_ecc else None,
        median_coma=float(np.median(valid_coma)) if valid_coma else None,
        median_astigmatism=float(np.median(valid_astigmatism))
        if valid_astigmatism
        else None,
        median_star_quality=float(np.median(valid_quality)) if valid_quality else None,
        fwhm_25th=float(np.percentile(valid_fwhm, 25)) if valid_fwhm else None,
        fwhm_75th=float(np.percentile(valid_fwhm, 75)) if valid_fwhm else None,
        hfr_25th=float(np.percentile(valid_hfr, 25)) if valid_hfr else None,
        hfr_75th=float(np.percentile(valid_hfr, 75)) if valid_hfr else None,
        collimation_issues=collimation_issues,
        stars=valid_stars,
    )


def _detect_collimation_issues(
    stars: List[StarData],
    coma_values: List[float],
    astigmatism_values: List[float],
) -> List[CollimationIssue]:
    """
    Детектирует проблемы коллимации на основе комы и астигматизма.

    Проблемы:
    - Высокая средняя кома → децентрирование зеркала
    - Высокий средний астигматизм → перекос корректора
    - Радиальная зависимость комы (coma растёт к краю поля)
    """
    issues = []

    # Проверка средней комы
    if coma_values:
        median_coma = float(np.median(coma_values))
        if abs(median_coma) > 0.3:
            severity = "HIGH" if abs(median_coma) > 0.5 else "MEDIUM"
            issues.append(
                CollimationIssue(
                    issue_type="coma",
                    severity=severity,
                    description=f"High median coma: {median_coma:.3f}. Check primary mirror alignment.",
                    value=median_coma,
                    threshold=0.3,
                )
            )

    # Проверка среднего астигматизма
    if astigmatism_values:
        median_astigmatism = float(np.median(astigmatism_values))
        if abs(median_astigmatism) > 0.3:
            severity = "HIGH" if abs(median_astigmatism) > 0.5 else "MEDIUM"
            issues.append(
                CollimationIssue(
                    issue_type="astigmatism",
                    severity=severity,
                    description=f"High median astigmatism: {median_astigmatism:.3f}. Check corrector tilt.",
                    value=median_astigmatism,
                    threshold=0.3,
                )
            )

    # Проверка радиальной зависимости комы (coma растёт к краю поля)
    if len(stars) > 10 and coma_values:
        # Разделяем звезды на центральные и периферийные
        center_stars = [
            s for s in stars if abs(s.x - 500) < 200 and abs(s.y - 500) < 200
        ]
        edge_stars = [s for s in stars if abs(s.x - 500) > 300 or abs(s.y - 500) > 300]

        if center_stars and edge_stars:
            center_coma = [s.coma for s in center_stars if s.coma is not None]
            edge_coma = [s.coma for s in edge_stars if s.coma is not None]

            if center_coma and edge_coma:
                center_median = float(np.median(center_coma))
                edge_median = float(np.median(edge_coma))

                # Если кома на краю значительно выше, чем в центре
                if abs(edge_median - center_median) > 0.3:
                    issues.append(
                        CollimationIssue(
                            issue_type="radial_coma",
                            severity="MEDIUM",
                            description=f"Radial coma detected. Center: {center_median:.3f}, Edge: {edge_median:.3f}",
                            value=edge_median - center_median,
                            threshold=0.3,
                        )
                    )

    return issues
