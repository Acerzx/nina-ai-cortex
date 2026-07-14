"""
Guiding Analyzer Parser — расширенный парсинг метрик гидирования.
Обрабатывает CSV/JSON отчеты от плагина Guiding Analyzer.

Извлекаемые метрики:
- FFT спектр (периодические ошибки червячной передачи)
- Backlash (люфт в механизме)
- Polar alignment error (ошибка полярного выравнивания)
- Stability ratio (стабильность гидирования)
- RMS тренды по осям RA/Dec

Использование:
    from app.ingestion.parsers.guiding_analyzer import (
        parse_guiding_analyzer_csv,
        parse_guiding_analyzer_json,
    )

    # Парсинг CSV
    report = parse_guiding_analyzer_csv(file_path)

    # Парсинг JSON
    report = parse_guiding_analyzer_json(file_path)
"""

import csv
import json
import logging
import numpy as np
from pathlib import Path
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
import aiofiles

logger = logging.getLogger("GuidingAnalyzerParser")


@dataclass
class FFTPeak:
    """Пик в FFT спектре (периодическая ошибка)."""

    frequency: float  # Hz
    period_seconds: float  # секунды
    amplitude: float  # амплитуда в arcsec
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "frequency": self.frequency,
            "period_seconds": self.period_seconds,
            "amplitude": self.amplitude,
            "description": self.description,
        }


@dataclass
class BacklashData:
    """Данные о backlash (люфте)."""

    ra_backlash_arcsec: Optional[float] = None
    dec_backlash_arcsec: Optional[float] = None
    ra_backlash_ms: Optional[float] = None
    dec_backlash_ms: Optional[float] = None
    severity: str = "LOW"  # LOW, MEDIUM, HIGH

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ra_backlash_arcsec": self.ra_backlash_arcsec,
            "dec_backlash_arcsec": self.dec_backlash_arcsec,
            "ra_backlash_ms": self.ra_backlash_ms,
            "dec_backlash_ms": self.dec_backlash_ms,
            "severity": self.severity,
        }


@dataclass
class PolarAlignmentError:
    """Ошибка полярного выравнивания."""

    az_error_arcmin: Optional[float] = None  # ошибка по азимуту
    alt_error_arcmin: Optional[float] = None  # ошибка по высоте
    total_error_arcmin: Optional[float] = None  # общая ошибка
    direction_degrees: Optional[float] = None  # направление ошибки
    correction_needed: str = ""  # описание необходимой коррекции

    def to_dict(self) -> Dict[str, Any]:
        return {
            "az_error_arcmin": self.az_error_arcmin,
            "alt_error_arcmin": self.alt_error_arcmin,
            "total_error_arcmin": self.total_error_arcmin,
            "direction_degrees": self.direction_degrees,
            "correction_needed": self.correction_needed,
        }


@dataclass
class GuidingStability:
    """Метрики стабильности гидирования."""

    stability_ratio: Optional[float] = None  # 0-1, чем выше тем лучше
    ra_stability: Optional[float] = None
    dec_stability: Optional[float] = None
    trend_ra: Optional[float] = None  # тренд RMS RA
    trend_dec: Optional[float] = None  # тренд RMS Dec
    is_degrading: bool = False  # деградирует ли гидирование

    def to_dict(self) -> Dict[str, Any]:
        return {
            "stability_ratio": self.stability_ratio,
            "ra_stability": self.ra_stability,
            "dec_stability": self.dec_stability,
            "trend_ra": self.trend_ra,
            "trend_dec": self.trend_dec,
            "is_degrading": self.is_degrading,
        }


@dataclass
class GuidingAnomaly:
    """Обнаруженная аномалия гидирования."""

    anomaly_type: str  # "PE", "BACKLASH", "VIBRATION", "POLAR_ERROR", "DEGRADATION"
    severity: str  # "LOW", "MEDIUM", "HIGH", "CRITICAL"
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
class GuidingAnalyzerReport:
    """Полный отчет от Guiding Analyzer."""

    file_name: str
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    # Базовые метрики
    total_samples: int = 0
    avg_rms_ra: Optional[float] = None
    avg_rms_dec: Optional[float] = None
    avg_rms_total: Optional[float] = None
    peak_rms_ra: Optional[float] = None
    peak_rms_dec: Optional[float] = None

    # Расширенные метрики
    fft_peaks: List[FFTPeak] = field(default_factory=list)
    backlash: Optional[BacklashData] = None
    polar_error: Optional[PolarAlignmentError] = None
    stability: Optional[GuidingStability] = None
    anomalies: List[GuidingAnomaly] = field(default_factory=list)

    # Статистика
    skewness_ra: Optional[float] = None
    skewness_dec: Optional[float] = None
    kurtosis_ra: Optional[float] = None
    kurtosis_dec: Optional[float] = None
    correlation_ra_dec: Optional[float] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "file_name": self.file_name,
            "timestamp": self.timestamp,
            "total_samples": self.total_samples,
            "avg_rms_ra": self.avg_rms_ra,
            "avg_rms_dec": self.avg_rms_dec,
            "avg_rms_total": self.avg_rms_total,
            "peak_rms_ra": self.peak_rms_ra,
            "peak_rms_dec": self.peak_rms_dec,
            "fft_peaks": [p.to_dict() for p in self.fft_peaks],
            "backlash": self.backlash.to_dict() if self.backlash else None,
            "polar_error": self.polar_error.to_dict() if self.polar_error else None,
            "stability": self.stability.to_dict() if self.stability else None,
            "anomalies": [a.to_dict() for a in self.anomalies],
            "skewness_ra": self.skewness_ra,
            "skewness_dec": self.skewness_dec,
            "kurtosis_ra": self.kurtosis_ra,
            "kurtosis_dec": self.kurtosis_dec,
            "correlation_ra_dec": self.correlation_ra_dec,
        }


async def parse_guiding_analyzer_csv(
    file_path: Path,
) -> Optional[GuidingAnalyzerReport]:
    """
    Парсит CSV файл от Guiding Analyzer.

    Args:
        file_path: Путь к CSV файлу

    Returns:
        GuidingAnalyzerReport или None при ошибке
    """
    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            content = await f.read()

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

        report = GuidingAnalyzerReport(file_name=file_path.name)
        report.total_samples = len(rows)

        # Извлекаем RMS данные
        rms_ra_values = []
        rms_dec_values = []
        rms_total_values = []

        for row in rows:
            try:
                if "rms_ra" in row and row["rms_ra"]:
                    rms_ra_values.append(float(row["rms_ra"]))
                if "rms_dec" in row and row["rms_dec"]:
                    rms_dec_values.append(float(row["rms_dec"]))
                if "rms_total" in row and row["rms_total"]:
                    rms_total_values.append(float(row["rms_total"]))
            except (ValueError, TypeError):
                continue

        # Базовая статистика
        if rms_ra_values:
            report.avg_rms_ra = float(np.mean(rms_ra_values))
            report.peak_rms_ra = float(np.max(rms_ra_values))
            report.skewness_ra = float(skewness(rms_ra_values))
            report.kurtosis_ra = float(kurtosis(rms_ra_values))

        if rms_dec_values:
            report.avg_rms_dec = float(np.mean(rms_dec_values))
            report.peak_rms_dec = float(np.max(rms_dec_values))
            report.skewness_dec = float(skewness(rms_dec_values))
            report.kurtosis_dec = float(kurtosis(rms_dec_values))

        if rms_total_values:
            report.avg_rms_total = float(np.mean(rms_total_values))

        # Корреляция RA/Dec
        if (
            rms_ra_values
            and rms_dec_values
            and len(rms_ra_values) == len(rms_dec_values)
        ):
            report.correlation_ra_dec = float(
                pearson_correlation(rms_ra_values, rms_dec_values)
            )

        # Анализ FFT (если есть данные)
        if len(rms_ra_values) > 10:
            report.fft_peaks = detect_fft_peaks(rms_ra_values, sample_rate=1.0)

        # Детекция аномалий
        report.anomalies = detect_guiding_anomalies(report)

        # Расчет стабильности
        report.stability = calculate_stability(rms_ra_values, rms_dec_values)

        logger.info(
            f"✅ Parsed Guiding Analyzer CSV: {file_path.name}, "
            f"{report.total_samples} samples, "
            f"avg RMS: {report.avg_rms_total:.2f} arcsec"
        )

        return report

    except Exception as e:
        logger.error(f"❌ Failed to parse Guiding Analyzer CSV {file_path}: {e}")
        return None


async def parse_guiding_analyzer_json(
    file_path: Path,
) -> Optional[GuidingAnalyzerReport]:
    """
    Парсит JSON файл от Guiding Analyzer.

    Args:
        file_path: Путь к JSON файлу

    Returns:
        GuidingAnalyzerReport или None при ошибке
    """
    try:
        async with aiofiles.open(file_path, "r", encoding="utf-8") as f:
            content = await f.read()

        data = json.loads(content)

        report = GuidingAnalyzerReport(file_name=file_path.name)

        # Извлекаем базовые метрики
        if "statistics" in data:
            stats = data["statistics"]
            report.total_samples = stats.get("total_samples", 0)
            report.avg_rms_ra = stats.get("avg_rms_ra")
            report.avg_rms_dec = stats.get("avg_rms_dec")
            report.avg_rms_total = stats.get("avg_rms_total")
            report.peak_rms_ra = stats.get("peak_rms_ra")
            report.peak_rms_dec = stats.get("peak_rms_dec")
            report.skewness_ra = stats.get("skewness_ra")
            report.skewness_dec = stats.get("skewness_dec")
            report.kurtosis_ra = stats.get("kurtosis_ra")
            report.kurtosis_dec = stats.get("kurtosis_dec")
            report.correlation_ra_dec = stats.get("correlation_ra_dec")

        # Извлекаем FFT пики
        if "fft_peaks" in data:
            for peak_data in data["fft_peaks"]:
                report.fft_peaks.append(
                    FFTPeak(
                        frequency=peak_data.get("frequency", 0.0),
                        period_seconds=peak_data.get("period_seconds", 0.0),
                        amplitude=peak_data.get("amplitude", 0.0),
                        description=peak_data.get("description", ""),
                    )
                )

        # Извлекаем backlash
        if "backlash" in data:
            bl = data["backlash"]
            report.backlash = BacklashData(
                ra_backlash_arcsec=bl.get("ra_backlash_arcsec"),
                dec_backlash_arcsec=bl.get("dec_backlash_arcsec"),
                ra_backlash_ms=bl.get("ra_backlash_ms"),
                dec_backlash_ms=bl.get("dec_backlash_ms"),
                severity=bl.get("severity", "LOW"),
            )

        # Извлекаем polar error
        if "polar_error" in data:
            pe = data["polar_error"]
            report.polar_error = PolarAlignmentError(
                az_error_arcmin=pe.get("az_error_arcmin"),
                alt_error_arcmin=pe.get("alt_error_arcmin"),
                total_error_arcmin=pe.get("total_error_arcmin"),
                direction_degrees=pe.get("direction_degrees"),
                correction_needed=pe.get("correction_needed", ""),
            )

        # Извлекаем stability
        if "stability" in data:
            stab = data["stability"]
            report.stability = GuidingStability(
                stability_ratio=stab.get("stability_ratio"),
                ra_stability=stab.get("ra_stability"),
                dec_stability=stab.get("dec_stability"),
                trend_ra=stab.get("trend_ra"),
                trend_dec=stab.get("trend_dec"),
                is_degrading=stab.get("is_degrading", False),
            )

        # Извлекаем аномалии
        if "anomalies" in data:
            for anomaly_data in data["anomalies"]:
                report.anomalies.append(
                    GuidingAnomaly(
                        anomaly_type=anomaly_data.get("anomaly_type", "UNKNOWN"),
                        severity=anomaly_data.get("severity", "LOW"),
                        description=anomaly_data.get("description", ""),
                        metric_value=anomaly_data.get("metric_value"),
                        threshold_value=anomaly_data.get("threshold_value"),
                        recommendation=anomaly_data.get("recommendation", ""),
                    )
                )

        logger.info(
            f"✅ Parsed Guiding Analyzer JSON: {file_path.name}, "
            f"{report.total_samples} samples, "
            f"{len(report.anomalies)} anomalies detected"
        )

        return report

    except Exception as e:
        logger.error(f"❌ Failed to parse Guiding Analyzer JSON {file_path}: {e}")
        return None


def detect_fft_peaks(
    signal: List[float],
    sample_rate: float = 1.0,
    threshold: float = 0.1,
    min_period: float = 10.0,
    max_period: float = 600.0,
) -> List[FFTPeak]:
    """
    Детектирует пики в FFT спектре (периодические ошибки).

    Args:
        signal: Временной ряд RMS значений
        sample_rate: Частота дискретизации (Hz)
        threshold: Минимальная амплитуда пика (относительно максимума)
        min_period: Минимальный период (секунды)
        max_period: Максимальный период (секунды)

    Returns:
        Список FFTPeak объектов
    """
    try:
        if len(signal) < 20:
            return []

        # FFT анализ
        signal_array = np.array(signal)
        fft_result = np.fft.fft(signal_array)
        fft_freqs = np.fft.fftfreq(len(signal_array), d=1.0 / sample_rate)

        # Амплитудный спектр
        amplitudes = np.abs(fft_result) / len(signal_array)

        # Только положительные частоты
        positive_mask = fft_freqs > 0
        freqs = fft_freqs[positive_mask]
        amps = amplitudes[positive_mask]

        # Находим пики
        peaks = []
        max_amp = np.max(amps)

        for i in range(1, len(amps) - 1):
            # Локальный максимум
            if amps[i] > amps[i - 1] and amps[i] > amps[i + 1]:
                # Проверяем порог
                if amps[i] > threshold * max_amp:
                    freq = freqs[i]
                    period = 1.0 / freq if freq > 0 else 0

                    # Фильтруем по периоду
                    if min_period <= period <= max_period:
                        # Определяем тип периодической ошибки
                        description = classify_pe_peak(period)

                        peaks.append(
                            FFTPeak(
                                frequency=float(freq),
                                period_seconds=float(period),
                                amplitude=float(amps[i]),
                                description=description,
                            )
                        )

        # Сортируем по амплитуде (убывание)
        peaks.sort(key=lambda p: p.amplitude, reverse=True)

        # Возвращаем топ-5 пиков
        return peaks[:5]

    except Exception as e:
        logger.error(f"FFT analysis failed: {e}")
        return []


def classify_pe_peak(period: float) -> str:
    """
    Классифицирует тип периодической ошибки по периоду.

    Args:
        period: Период в секундах

    Returns:
        Описание типа ошибки
    """
    if 10 <= period <= 30:
        return "High-frequency vibration (mount resonance)"
    elif 30 < period <= 120:
        return "Worm gear periodic error (typical)"
    elif 120 < period <= 300:
        return "Long-period error (gear train)"
    elif 300 < period <= 600:
        return "Very long-period error (possible polar misalignment)"
    else:
        return "Unknown periodic error"


def detect_guiding_anomalies(report: GuidingAnalyzerReport) -> List[GuidingAnomaly]:
    """
    Детектирует аномалии гидирования на основе отчета.

    Args:
        report: GuidingAnalyzerReport

    Returns:
        Список GuidingAnomaly объектов
    """
    anomalies = []

    # 1. Высокий RMS
    if report.avg_rms_total and report.avg_rms_total > 2.0:
        severity = "CRITICAL" if report.avg_rms_total > 3.0 else "HIGH"
        anomalies.append(
            GuidingAnomaly(
                anomaly_type="HIGH_RMS",
                severity=severity,
                description=f"High guiding RMS: {report.avg_rms_total:.2f} arcsec",
                metric_value=report.avg_rms_total,
                threshold_value=2.0,
                recommendation="Check mount balance, polar alignment, and guiding parameters",
            )
        )

    # 2. Периодические ошибки (PE)
    if report.fft_peaks:
        for peak in report.fft_peaks[:3]:  # Топ-3 пика
            if peak.amplitude > 0.5:  # Значительная амплитуда
                anomalies.append(
                    GuidingAnomaly(
                        anomaly_type="PERIODIC_ERROR",
                        severity="MEDIUM",
                        description=f"Periodic error detected: {peak.description} (period: {peak.period_seconds:.1f}s)",
                        metric_value=peak.period_seconds,
                        threshold_value=None,
                        recommendation=f"Consider PEC training or mount maintenance",
                    )
                )

    # 3. Backlash
    if report.backlash:
        if (
            report.backlash.ra_backlash_arcsec
            and report.backlash.ra_backlash_arcsec > 1.0
        ):
            anomalies.append(
                GuidingAnomaly(
                    anomaly_type="BACKLASH",
                    severity=report.backlash.severity,
                    description=f"RA backlash detected: {report.backlash.ra_backlash_arcsec:.2f} arcsec",
                    metric_value=report.backlash.ra_backlash_arcsec,
                    threshold_value=1.0,
                    recommendation="Enable backlash compensation in PHD2 or adjust mount",
                )
            )

        if (
            report.backlash.dec_backlash_arcsec
            and report.backlash.dec_backlash_arcsec > 1.0
        ):
            anomalies.append(
                GuidingAnomaly(
                    anomaly_type="BACKLASH",
                    severity=report.backlash.severity,
                    description=f"Dec backlash detected: {report.backlash.dec_backlash_arcsec:.2f} arcsec",
                    metric_value=report.backlash.dec_backlash_arcsec,
                    threshold_value=1.0,
                    recommendation="Enable Dec backlash compensation or use unidirectional guiding",
                )
            )

    # 4. Polar alignment error
    if report.polar_error and report.polar_error.total_error_arcmin:
        if report.polar_error.total_error_arcmin > 5.0:
            severity = (
                "HIGH" if report.polar_error.total_error_arcmin > 10.0 else "MEDIUM"
            )
            anomalies.append(
                GuidingAnomaly(
                    anomaly_type="POLAR_ERROR",
                    severity=severity,
                    description=f"Polar alignment error: {report.polar_error.total_error_arcmin:.1f} arcmin",
                    metric_value=report.polar_error.total_error_arcmin,
                    threshold_value=5.0,
                    recommendation=report.polar_error.correction_needed
                    or "Improve polar alignment",
                )
            )

    # 5. Degradation trend
    if report.stability and report.stability.is_degrading:
        anomalies.append(
            GuidingAnomaly(
                anomaly_type="DEGRADATION",
                severity="HIGH",
                description="Guiding quality is degrading over time",
                metric_value=report.stability.stability_ratio,
                threshold_value=None,
                recommendation="Check for mechanical issues, wind, or temperature changes",
            )
        )

    # 6. High skewness (асимметричное распределение)
    if report.skewness_ra and abs(report.skewness_ra) > 1.5:
        anomalies.append(
            GuidingAnomaly(
                anomaly_type="ASYMMETRIC_RA",
                severity="MEDIUM",
                description=f"Asymmetric RA guiding distribution (skewness: {report.skewness_ra:.2f})",
                metric_value=report.skewness_ra,
                threshold_value=1.5,
                recommendation="Check for systematic drift or mount imbalance",
            )
        )

    if report.skewness_dec and abs(report.skewness_dec) > 1.5:
        anomalies.append(
            GuidingAnomaly(
                anomaly_type="ASYMMETRIC_DEC",
                severity="MEDIUM",
                description=f"Asymmetric Dec guiding distribution (skewness: {report.skewness_dec:.2f})",
                metric_value=report.skewness_dec,
                threshold_value=1.5,
                recommendation="Check Dec axis for mechanical issues or polar alignment",
            )
        )

    return anomalies


def calculate_stability(
    rms_ra_values: List[float], rms_dec_values: List[float], window_size: int = 10
) -> GuidingStability:
    """
    Рассчитывает метрики стабильности гидирования.

    Args:
        rms_ra_values: Список RMS RA значений
        rms_dec_values: Список RMS Dec значений
        window_size: Размер окна для трендового анализа

    Returns:
        GuidingStability объект
    """
    stability = GuidingStability()

    if not rms_ra_values or not rms_dec_values:
        return stability

    # Стабильность по осям (inverse of coefficient of variation)
    if len(rms_ra_values) > 1:
        ra_mean = np.mean(rms_ra_values)
        ra_std = np.std(rms_ra_values)
        if ra_mean > 0:
            stability.ra_stability = float(1.0 / (1.0 + ra_std / ra_mean))

    if len(rms_dec_values) > 1:
        dec_mean = np.mean(rms_dec_values)
        dec_std = np.std(rms_dec_values)
        if dec_mean > 0:
            stability.dec_stability = float(1.0 / (1.0 + dec_std / dec_mean))

    # Общая стабильность
    if stability.ra_stability and stability.dec_stability:
        stability.stability_ratio = (
            stability.ra_stability + stability.dec_stability
        ) / 2.0

    # Трендовый анализ (последние N значений vs первые N)
    if len(rms_ra_values) >= window_size * 2:
        early_ra = np.mean(rms_ra_values[:window_size])
        late_ra = np.mean(rms_ra_values[-window_size:])
        stability.trend_ra = float(late_ra - early_ra)

    if len(rms_dec_values) >= window_size * 2:
        early_dec = np.mean(rms_dec_values[:window_size])
        late_dec = np.mean(rms_dec_values[-window_size:])
        stability.trend_dec = float(late_dec - early_dec)

    # Деградация (тренд > 0.5 arcsec)
    if stability.trend_ra and stability.trend_dec:
        stability.is_degrading = stability.trend_ra > 0.5 or stability.trend_dec > 0.5

    return stability


def skewness(data: List[float]) -> float:
    """Вычисляет коэффициент асимметрии."""
    if len(data) < 3:
        return 0.0

    arr = np.array(data)
    mean = np.mean(arr)
    std = np.std(arr)

    if std == 0:
        return 0.0

    return float(np.mean(((arr - mean) / std) ** 3))


def kurtosis(data: List[float]) -> float:
    """Вычисляет коэффициент эксцесса."""
    if len(data) < 4:
        return 0.0

    arr = np.array(data)
    mean = np.mean(arr)
    std = np.std(arr)

    if std == 0:
        return 0.0

    return float(np.mean(((arr - mean) / std) ** 4) - 3.0)


def pearson_correlation(x: List[float], y: List[float]) -> float:
    """Вычисляет коэффициент корреляции Пирсона."""
    if len(x) != len(y) or len(x) < 2:
        return 0.0

    arr_x = np.array(x)
    arr_y = np.array(y)

    corr_matrix = np.corrcoef(arr_x, arr_y)
    return float(corr_matrix[0, 1])
