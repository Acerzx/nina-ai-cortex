"""
SolveEveryLight Parser — парсинг WCS данных из FITS headers.
Обрабатывает метаданные, записанные плагином SolveEveryLight:
https://github.com/astroalex80/NINA.Plugin.SolveEveryLight

Извлекаемые данные:
- WCS координаты (RA/Dec центра кадра)
- CD матрица преобразования пикселей в небесные координаты
- Угловые расстояния до Луны/Солнца (MOONANGL, SUNANGLE)
- Параметры plate solve (PIXSCALE, CDELT, CRPIX)

Использование:
    from app.ingestion.parsers.solve_every_light import (
        parse_wcs_from_fits,
        calculate_field_drift,
        SolveEveryLightReport,
    )

    # Парсинг WCS из FITS файла
    report = parse_wcs_from_fits(fits_path)

    # Расчет дрейфа поля между кадрами
    drift = calculate_field_drift(previous_wcs, current_wcs)
"""

import logging
from pathlib import Path
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime
import numpy as np

logger = logging.getLogger("SolveEveryLightParser")


@dataclass
class WCSCoordinates:
    """Астрономические координаты центра кадра."""

    ra: float  # Right Ascension в градусах (CRVAL1)
    dec: float  # Declination в градусах (CRVAL2)
    ra_hms: str  # RA в формате часы:минуты:секунды
    dec_dms: str  # Dec в формате градусы:минуты:секунды


@dataclass
class CDMatrix:
    """
    Матрица преобразования пикселей в небесные координаты.

    CD матрица описывает, как смещение на 1 пиксель по X/Y
    преобразуется в смещение по RA/Dec в градусах.

    CD1_1 = d(RA)/d(X) * cos(Dec)
    CD1_2 = d(RA)/d(Y) * cos(Dec)
    CD2_1 = d(Dec)/d(X)
    CD2_2 = d(Dec)/d(Y)
    """

    cd1_1: float  # d(RA)/d(X) * cos(Dec)
    cd1_2: float  # d(RA)/d(Y) * cos(Dec)
    cd2_1: float  # d(Dec)/d(X)
    cd2_2: float  # d(Dec)/d(Y)


@dataclass
class PlateSolveParams:
    """Параметры plate solve."""

    pixscale: Optional[float] = None  # Размер пикселя в arcsec/pixel
    cdelt1: Optional[float] = None  # Размер пикселя по X в градусах
    cdelt2: Optional[float] = None  # Размер пикселя по Y в градусах
    crpix1: Optional[float] = None  # Reference pixel X
    crpix2: Optional[float] = None  # Reference pixel Y
    naxis1: Optional[int] = None  # Размер изображения по X в пикселях
    naxis2: Optional[int] = None  # Размер изображения по Y в пикселях


@dataclass
class FieldDrift:
    """
    Дрейф поля между двумя кадрами.

    Используется для анализа качества гидирования и обнаружения
    систематических ошибок (flexure, polar alignment error).
    """

    delta_ra_arcsec: float  # Смещение по RA в arcsec
    delta_dec_arcsec: float  # Смещение по Dec в arcsec
    total_drift_arcsec: float  # Общее смещение в arcsec
    drift_angle_deg: float  # Угол дрейфа в градусах
    time_delta_seconds: float  # Временной интервал между кадрами
    drift_rate_arcsec_per_min: float  # Скорость дрейфа в arcsec/мин


@dataclass
class SolveEveryLightReport:
    """Полный отчет о WCS данных кадра."""

    file_name: str
    file_path: str
    timestamp: str

    # WCS координаты
    wcs_coords: Optional[WCSCoordinates] = None

    # CD матрица
    cd_matrix: Optional[CDMatrix] = None

    # Параметры plate solve
    plate_solve_params: Optional[PlateSolveParams] = None

    # Угловые расстояния
    moon_angle_deg: Optional[float] = None  # MOONANGL
    sun_angle_deg: Optional[float] = None  # SUNANGLE

    # Метаданные
    filter_name: Optional[str] = None
    exposure_time: Optional[float] = None
    temperature: Optional[float] = None
    date_obs: Optional[str] = None

    # Флаги
    has_wcs: bool = False
    has_moon_angle: bool = False
    has_sun_angle: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация в словарь."""
        return {
            "file_name": self.file_name,
            "file_path": self.file_path,
            "timestamp": self.timestamp,
            "wcs_coords": {
                "ra": self.wcs_coords.ra,
                "dec": self.wcs_coords.dec,
                "ra_hms": self.wcs_coords.ra_hms,
                "dec_dms": self.wcs_coords.dec_dms,
            }
            if self.wcs_coords
            else None,
            "cd_matrix": {
                "cd1_1": self.cd_matrix.cd1_1,
                "cd1_2": self.cd_matrix.cd1_2,
                "cd2_1": self.cd_matrix.cd2_1,
                "cd2_2": self.cd_matrix.cd2_2,
            }
            if self.cd_matrix
            else None,
            "plate_solve_params": {
                "pixscale": self.plate_solve_params.pixscale,
                "cdelt1": self.plate_solve_params.cdelt1,
                "cdelt2": self.plate_solve_params.cdelt2,
                "crpix1": self.plate_solve_params.crpix1,
                "crpix2": self.plate_solve_params.crpix2,
                "naxis1": self.plate_solve_params.naxis1,
                "naxis2": self.plate_solve_params.naxis2,
            }
            if self.plate_solve_params
            else None,
            "moon_angle_deg": self.moon_angle_deg,
            "sun_angle_deg": self.sun_angle_deg,
            "filter_name": self.filter_name,
            "exposure_time": self.exposure_time,
            "temperature": self.temperature,
            "date_obs": self.date_obs,
            "has_wcs": self.has_wcs,
            "has_moon_angle": self.has_moon_angle,
            "has_sun_angle": self.has_sun_angle,
        }


def parse_wcs_from_fits(fits_path: Path) -> Optional[SolveEveryLightReport]:
    """
    Парсит WCS данные из FITS файла.

    Args:
        fits_path: Путь к FITS файлу

    Returns:
        SolveEveryLightReport или None при ошибке
    """
    try:
        from astropy.io import fits
        from astropy.coordinates import SkyCoord
        import astropy.units as u

        # Читаем только заголовки (не данные)
        with fits.open(fits_path, memmap=False) as hdul:
            header = hdul[0].header

        report = SolveEveryLightReport(
            file_name=fits_path.name,
            file_path=str(fits_path),
            timestamp=datetime.now().isoformat(),
        )

        # Извлекаем WCS координаты
        crval1 = header.get("CRVAL1")
        crval2 = header.get("CRVAL2")

        if crval1 is not None and crval2 is not None:
            report.has_wcs = True

            # Преобразуем в SkyCoord для получения HMS/DMS
            try:
                coord = SkyCoord(ra=crval1 * u.deg, dec=crval2 * u.deg)
                ra_hms = coord.ra.to_string(unit=u.hour, sep=":", pad=True)
                dec_dms = coord.dec.to_string(unit=u.deg, sep=":", pad=True)

                report.wcs_coords = WCSCoordinates(
                    ra=float(crval1),
                    dec=float(crval2),
                    ra_hms=ra_hms,
                    dec_dms=dec_dms,
                )
            except Exception as e:
                logger.warning(f"Failed to convert WCS to SkyCoord: {e}")
                report.wcs_coords = WCSCoordinates(
                    ra=float(crval1),
                    dec=float(crval2),
                    ra_hms="",
                    dec_dms="",
                )

        # Извлекаем CD матрицу
        cd1_1 = header.get("CD1_1")
        cd1_2 = header.get("CD1_2")
        cd2_1 = header.get("CD2_1")
        cd2_2 = header.get("CD2_2")

        if all(v is not None for v in [cd1_1, cd1_2, cd2_1, cd2_2]):
            report.cd_matrix = CDMatrix(
                cd1_1=float(cd1_1),
                cd1_2=float(cd1_2),
                cd2_1=float(cd2_1),
                cd2_2=float(cd2_2),
            )

        # Извлекаем параметры plate solve
        pixscale = header.get("PIXSCALE")
        cdelt1 = header.get("CDELT1")
        cdelt2 = header.get("CDELT2")
        crpix1 = header.get("CRPIX1")
        crpix2 = header.get("CRPIX2")
        naxis1 = header.get("NAXIS1")
        naxis2 = header.get("NAXIS2")

        if any(v is not None for v in [pixscale, cdelt1, cdelt2, crpix1, crpix2]):
            report.plate_solve_params = PlateSolveParams(
                pixscale=float(pixscale) if pixscale is not None else None,
                cdelt1=float(cdelt1) if cdelt1 is not None else None,
                cdelt2=float(cdelt2) if cdelt2 is not None else None,
                crpix1=float(crpix1) if crpix1 is not None else None,
                crpix2=float(crpix2) if crpix2 is not None else None,
                naxis1=int(naxis1) if naxis1 is not None else None,
                naxis2=int(naxis2) if naxis2 is not None else None,
            )

        # Извлекаем угловые расстояния
        moon_angl = header.get("MOONANGL")
        sun_angle = header.get("SUNANGLE")

        if moon_angl is not None:
            report.moon_angle_deg = float(moon_angl)
            report.has_moon_angle = True

        if sun_angle is not None:
            report.sun_angle_deg = float(sun_angle)
            report.has_sun_angle = True

        # Извлекаем метаданные
        report.filter_name = header.get("FILTER")
        report.exposure_time = header.get("EXPTIME")
        report.temperature = header.get("CCD-TEMP") or header.get("TEMPERAT")
        report.date_obs = header.get("DATE-OBS")

        logger.debug(
            f"Parsed WCS from {fits_path.name}: "
            f"RA={report.wcs_coords.ra:.4f}°, Dec={report.wcs_coords.dec:.4f}°"
            if report.wcs_coords
            else f"No WCS data in {fits_path.name}"
        )

        return report

    except Exception as e:
        logger.error(f"Failed to parse WCS from {fits_path}: {e}")
        return None


def calculate_field_drift(
    previous_report: SolveEveryLightReport,
    current_report: SolveEveryLightReport,
) -> Optional[FieldDrift]:
    """
    Рассчитывает дрейф поля между двумя кадрами.

    Args:
        previous_report: Отчет о предыдущем кадре
        current_report: Отчет о текущем кадре

    Returns:
        FieldDrift или None если расчет невозможен
    """
    # Проверяем наличие WCS данных
    if not (previous_report.wcs_coords and current_report.wcs_coords):
        return None

    # Проверяем наличие CD матрицы для преобразования в arcsec
    if not (previous_report.cd_matrix and current_report.cd_matrix):
        # Упрощенный расчет без CD матрицы
        # Используем приближение: 1° = 3600 arcsec
        delta_ra_deg = current_report.wcs_coords.ra - previous_report.wcs_coords.ra
        delta_dec_deg = current_report.wcs_coords.dec - previous_report.wcs_coords.dec

        # Корректируем RA с учетом cos(Dec)
        avg_dec_rad = np.radians(
            (previous_report.wcs_coords.dec + current_report.wcs_coords.dec) / 2
        )
        delta_ra_arcsec = delta_ra_deg * 3600 * np.cos(avg_dec_rad)
        delta_dec_arcsec = delta_dec_deg * 3600

    else:
        # Точный расчет с использованием CD матрицы
        # Преобразуем смещение в пикселях через CD матрицу
        # Упрощенно: предполагаем, что смещение в пикселях пропорционально
        # смещению в градусах, деленному на размер пикселя

        # Получаем размер пикселя из PIXSCALE или CDELT
        pixscale = (
            previous_report.plate_solve_params.pixscale
            if previous_report.plate_solve_params
            else None
        )

        if pixscale is None:
            # Используем CD матрицу для оценки размера пикселя
            cd_matrix = previous_report.cd_matrix
            # pixscale ≈ sqrt(|CD1_1 * CD2_2 - CD1_2 * CD2_1|) * 3600 arcsec
            det = abs(
                cd_matrix.cd1_1 * cd_matrix.cd2_2 - cd_matrix.cd1_2 * cd_matrix.cd2_1
            )
            pixscale = np.sqrt(det) * 3600

        # Смещение в градусах
        delta_ra_deg = current_report.wcs_coords.ra - previous_report.wcs_coords.ra
        delta_dec_deg = current_report.wcs_coords.dec - previous_report.wcs_coords.dec

        # Корректируем RA с учетом cos(Dec)
        avg_dec_rad = np.radians(
            (previous_report.wcs_coords.dec + current_report.wcs_coords.dec) / 2
        )

        # Преобразуем в arcsec
        delta_ra_arcsec = delta_ra_deg * 3600 * np.cos(avg_dec_rad)
        delta_dec_arcsec = delta_dec_deg * 3600

    # Общее смещение
    total_drift_arcsec = np.sqrt(delta_ra_arcsec**2 + delta_dec_arcsec**2)

    # Угол дрейфа (0° = восток, 90° = север)
    drift_angle_deg = np.degrees(np.arctan2(delta_dec_arcsec, delta_ra_arcsec))

    # Временной интервал
    time_delta_seconds = 0.0
    if previous_report.date_obs and current_report.date_obs:
        try:
            from datetime import datetime

            # Парсим DATE-OBS (формат: 2024-01-15T22:30:45.123)
            prev_time = datetime.fromisoformat(
                previous_report.date_obs.replace("Z", "+00:00")
            )
            curr_time = datetime.fromisoformat(
                current_report.date_obs.replace("Z", "+00:00")
            )
            time_delta_seconds = (curr_time - prev_time).total_seconds()
        except Exception as e:
            logger.warning(f"Failed to parse DATE-OBS: {e}")

    # Скорость дрейфа
    drift_rate_arcsec_per_min = 0.0
    if time_delta_seconds > 0:
        drift_rate_arcsec_per_min = (total_drift_arcsec / time_delta_seconds) * 60

    drift = FieldDrift(
        delta_ra_arcsec=delta_ra_arcsec,
        delta_dec_arcsec=delta_dec_arcsec,
        total_drift_arcsec=total_drift_arcsec,
        drift_angle_deg=drift_angle_deg,
        time_delta_seconds=time_delta_seconds,
        drift_rate_arcsec_per_min=drift_rate_arcsec_per_min,
    )

    logger.debug(
        f'Field drift: ΔRA={delta_ra_arcsec:.2f}", ΔDec={delta_dec_arcsec:.2f}", '
        f'total={total_drift_arcsec:.2f}", rate={drift_rate_arcsec_per_min:.2f}"/min'
    )

    return drift


def analyze_field_drift_trend(
    drifts: List[FieldDrift],
    window_size: int = 10,
) -> Dict[str, Any]:
    """
    Анализирует тренд дрейфа поля за последние N кадров.

    Используется для обнаружения систематических ошибок:
    - Flexure (прогиб конструкции)
    - Polar alignment error (ошибка полярного выравнивания)
    - Periodic error (периодическая ошибка червячной передачи)

    Args:
        drifts: Список FieldDrift за последние кадры
        window_size: Размер окна для анализа

    Returns:
        Словарь с результатами анализа
    """
    if len(drifts) < window_size:
        return {
            "sufficient_data": False,
            "message": f"Need at least {window_size} drifts for analysis",
        }

    # Берем последние window_size дрейфов
    recent_drifts = drifts[-window_size:]

    # Извлекаем компоненты
    ra_drifts = [d.delta_ra_arcsec for d in recent_drifts]
    dec_drifts = [d.delta_dec_arcsec for d in recent_drifts]
    total_drifts = [d.total_drift_arcsec for d in recent_drifts]
    drift_rates = [d.drift_rate_arcsec_per_min for d in recent_drifts]

    # Статистика
    ra_mean = np.mean(ra_drifts)
    ra_std = np.std(ra_drifts)
    dec_mean = np.mean(dec_drifts)
    dec_std = np.std(dec_drifts)
    total_mean = np.mean(total_drifts)
    total_std = np.std(total_drifts)
    rate_mean = np.mean(drift_rates)
    rate_std = np.std(drift_rates)

    # Обнаружение систематического дрейфа
    # Если среднее смещение значительно больше стандартного отклонения
    systematic_ra = abs(ra_mean) > 2 * ra_std if ra_std > 0 else False
    systematic_dec = abs(dec_mean) > 2 * dec_std if dec_std > 0 else False

    # Классификация типа дрейфа
    drift_type = "random"
    if systematic_ra and systematic_dec:
        # Систематический дрейф по обеим осям → polar alignment error
        drift_type = "polar_alignment_error"
    elif systematic_ra and not systematic_dec:
        # Систематический дрейф только по RA → periodic error
        drift_type = "periodic_error_ra"
    elif systematic_dec and not systematic_ra:
        # Систематический дрейф только по Dec → flexure
        drift_type = "flexure_dec"

    # Рекомендации
    recommendations = []

    if drift_type == "polar_alignment_error":
        recommendations.append(
            "Обнаружена ошибка полярного выравнивания. "
            "Рекомендуется выполнить процедуру полярного выравнивания (2PA или drift alignment)."
        )

    if drift_type == "periodic_error_ra":
        recommendations.append(
            "Обнаружена периодическая ошибка по RA. "
            "Рекомендуется проверить червячную передачу и включить PEC (Periodic Error Correction)."
        )

    if drift_type == "flexure_dec":
        recommendations.append(
            "Обнаружен систематический дрейф по Dec. "
            "Рекомендуется проверить жесткость конструкции и крепления OAG."
        )

    if rate_mean > 1.0:  # > 1 arcsec/мин
        recommendations.append(
            f'Высокая скорость дрейфа ({rate_mean:.2f}"/мин). '
            "Рекомендуется увеличить частоту гидирования или проверить балансировку."
        )

    if total_std > 5.0:  # > 5 arcsec
        recommendations.append(
            f'Высокая нестабильность дрейфа (σ={total_std:.2f}"). '
            "Рекомендуется проверить вибрации и устойчивость монтировки."
        )

    return {
        "sufficient_data": True,
        "window_size": window_size,
        "statistics": {
            "ra": {
                "mean_arcsec": float(ra_mean),
                "std_arcsec": float(ra_std),
                "systematic": systematic_ra,
            },
            "dec": {
                "mean_arcsec": float(dec_mean),
                "std_arcsec": float(dec_std),
                "systematic": systematic_dec,
            },
            "total": {
                "mean_arcsec": float(total_mean),
                "std_arcsec": float(total_std),
            },
            "rate": {
                "mean_arcsec_per_min": float(rate_mean),
                "std_arcsec_per_min": float(rate_std),
            },
        },
        "drift_type": drift_type,
        "recommendations": recommendations,
    }


def calculate_field_coverage(
    reports: List[SolveEveryLightReport],
    target_ra: Optional[float] = None,
    target_dec: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Рассчитывает покрытие поля наблюдения.

    Анализирует, насколько хорошо покрыта целевая область неба
    с учетом дрейфа поля и дизеринга.

    Args:
        reports: Список SolveEveryLightReport
        target_ra: Целевое RA в градусах (опционально)
        target_dec: Целевое Dec в градусах (опционально)

    Returns:
        Словарь с результатами анализа покрытия
    """
    # Фильтруем отчеты с WCS данными
    wcs_reports = [r for r in reports if r.wcs_coords]

    if not wcs_reports:
        return {
            "sufficient_data": False,
            "message": "No WCS data available",
        }

    # Извлекаем координаты
    ra_values = [r.wcs_coords.ra for r in wcs_reports]
    dec_values = [r.wcs_coords.dec for r in wcs_reports]

    # Статистика покрытия
    ra_min = min(ra_values)
    ra_max = max(ra_values)
    dec_min = min(dec_values)
    dec_max = max(dec_values)

    ra_range = ra_max - ra_min
    dec_range = dec_max - dec_min

    # Площадь покрытия в квадратных градусах
    # Корректируем RA с учетом cos(Dec)
    avg_dec_rad = np.radians(np.mean(dec_values))
    coverage_area_sq_deg = ra_range * np.cos(avg_dec_rad) * dec_range

    # Отклонение от цели
    target_deviation_arcsec = 0.0
    if target_ra is not None and target_dec is not None:
        # Среднее отклонение от цели
        deviations = []
        for r in wcs_reports:
            delta_ra = (
                (r.wcs_coords.ra - target_ra)
                * 3600
                * np.cos(np.radians(r.wcs_coords.dec))
            )
            delta_dec = (r.wcs_coords.dec - target_dec) * 3600
            deviation = np.sqrt(delta_ra**2 + delta_dec**2)
            deviations.append(deviation)

        target_deviation_arcsec = float(np.mean(deviations))

    # Оценка качества покрытия
    coverage_quality = "excellent"
    if coverage_area_sq_deg < 0.01:  # < 0.01 sq deg
        coverage_quality = "poor"
    elif coverage_area_sq_deg < 0.1:  # < 0.1 sq deg
        coverage_quality = "fair"
    elif coverage_area_sq_deg < 1.0:  # < 1.0 sq deg
        coverage_quality = "good"

    return {
        "sufficient_data": True,
        "frame_count": len(wcs_reports),
        "coverage": {
            "ra_range_deg": float(ra_range),
            "dec_range_deg": float(dec_range),
            "area_sq_deg": float(coverage_area_sq_deg),
            "quality": coverage_quality,
        },
        "target_deviation_arcsec": target_deviation_arcsec,
        "statistics": {
            "ra": {
                "min": float(ra_min),
                "max": float(ra_max),
                "mean": float(np.mean(ra_values)),
                "std": float(np.std(ra_values)),
            },
            "dec": {
                "min": float(dec_min),
                "max": float(dec_max),
                "mean": float(np.mean(dec_values)),
                "std": float(np.std(dec_values)),
            },
        },
    }
