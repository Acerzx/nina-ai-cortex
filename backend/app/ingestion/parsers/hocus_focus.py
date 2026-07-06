import csv
import logging
import numpy as np
from pathlib import Path
from typing import List, Optional, Dict, Any
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
    coma: Optional[float] = Field(alias="Coma", default=None)
    astigmatism: Optional[float] = Field(alias="Astigmatism", default=None)
    flux: Optional[float] = Field(alias="Flux", default=None)
    background: Optional[float] = Field(alias="Background", default=None)

    # Внутренние метки
    is_anomaly: bool = False
    anomaly_reason: Optional[str] = None

    class Config:
        populate_by_name = True


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

    # Процентили (для понимания распределения)
    fwhm_25th: Optional[float] = None
    fwhm_75th: Optional[float] = None

    stars: List[StarData] = []


def parse_hocus_focus_csv(file_path: Path) -> List[StarData]:
    """Парсит CSV файл Hocus Focus"""
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
                    stars.append(StarData(**cleaned_row))
                except ValueError as e:
                    logger.debug(f"Skipping invalid star row: {e}")
    except Exception as e:
        logger.error(f"Failed to parse Hocus Focus CSV {file_path}: {e}")

    return stars


def filter_anomalies(
    stars: List[StarData], z_threshold: float = 3.0
) -> HocusFocusReport:
    """
    Применяет Z-Score фильтр для отсеивания аномальных звезд.
    Аномалии: горячие пиксели, пересвеченные звезды, звезды на краях поля с сильной комой.
    """
    if not stars:
        return HocusFocusReport(
            file_name="", total_stars_detected=0, valid_stars_count=0, anomalies_count=0
        )

    fwhm_values = np.array([s.fwhm for s in stars if s.fwhm is not None])

    valid_stars = []
    anomalies_count = 0

    if len(fwhm_values) >= 3:
        mean_fwhm = np.mean(fwhm_values)
        std_fwhm = np.std(fwhm_values)

        for star in stars:
            if star.fwhm is not None and std_fwhm > 0:
                z_score = abs(star.fwhm - mean_fwhm) / std_fwhm
                if z_score > z_threshold:
                    star.is_anomaly = True
                    star.anomaly_reason = f"FWHM Z-Score {z_score:.2f} > {z_threshold}"
                    anomalies_count += 1
                    continue

            # Дополнительная проверка: экстремальный эксцентриситет (например, > 0.8)
            if star.eccentricity is not None and star.eccentricity > 0.85:
                star.is_anomaly = True
                star.anomaly_reason = f"Extreme Eccentricity {star.eccentricity:.2f}"
                anomalies_count += 1
                continue

            valid_stars.append(star)
    else:
        valid_stars = stars

    # Расчет агрегатов по очищенным данным
    valid_fwhm = [s.fwhm for s in valid_stars if s.fwhm is not None]
    valid_hfr = [s.hfr for s in valid_stars if s.hfr is not None]
    valid_ecc = [s.eccentricity for s in valid_stars if s.eccentricity is not None]

    return HocusFocusReport(
        file_name="",  # Заполняется в Watcher
        total_stars_detected=len(stars),
        valid_stars_count=len(valid_stars),
        anomalies_count=anomalies_count,
        median_fwhm=float(np.median(valid_fwhm)) if valid_fwhm else None,
        median_hfr=float(np.median(valid_hfr)) if valid_hfr else None,
        median_eccentricity=float(np.median(valid_ecc)) if valid_ecc else None,
        fwhm_25th=float(np.percentile(valid_fwhm, 25)) if valid_fwhm else None,
        fwhm_75th=float(np.percentile(valid_fwhm, 75)) if valid_fwhm else None,
        stars=valid_stars,
    )
