"""
Dither Statistics & Guiding Analyzer Watchers
Мониторят экспорты качества дизеринга и FFT-анализа гидирования.
Используют расширенные parsers для детального анализа.

ЭТАП 9 (расширение):
- Использует dither_statistics.py parser для CD², GFM, Voronoi CV
- Использует guiding_analyzer.py parser для FFT анализа, PE, backlash
- Публикует расширенные события с полными метриками
"""

import logging
import json
import csv
import aiofiles
from pathlib import Path
from typing import Dict, Any, List, Optional
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.dither_statistics import (
    parse_dither_statistics_csv,
    parse_dither_statistics_json,
)
from app.ingestion.parsers.guiding_analyzer import (
    parse_guiding_analyzer_csv,
    parse_guiding_analyzer_json,
)
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings

logger = logging.getLogger("DitherGuidingWatcher")


class DitherStatisticsWatcher(BaseFileWatcher):
    """
    Расширенный мониторинг качества дизеринга.

    Использует dither_statistics.py parser для полного анализа:
    - CD² (Centered L₂ Discrepancy)
    - GFM (Gap-Fill Metric) для 1×, 2×, 3× drizzle
    - Voronoi CV (Cell Coefficient of Variation)
    - NNI (Nearest Neighbor Index)
    - Combined Quality Score

    Публикует события:
    - DITHER_STATS: базовые метрики (для совместимости)
    - DITHER_STATS_EXTENDED: расширенные метрики с полным анализом
    """

    def __init__(self, registry: CapabilityRegistry):
        path_str = getattr(settings.watchers, "dither_statistics_path", None)
        path = (
            Path(path_str)
            if path_str
            else Path.home() / "Documents" / "NINA" / "DitherStatistics"
        )
        super().__init__(
            watch_path=path, target_files=[".csv", ".json"], registry=registry
        )
        logger.info(f"🎯 DitherStatisticsWatcher initialized (path: {path})")

    async def process_file(self, path: Path) -> None:
        """Обработка файла с расширенным анализом."""
        if path.suffix.lower() not in [".csv", ".json"]:
            return

        logger.info(f"📊 Processing Dither Statistics: {path.name}")

        try:
            # Используем расширенный parser
            if path.suffix.lower() == ".json":
                report = await parse_dither_statistics_json(path)
            else:
                report = await parse_dither_statistics_csv(path)

            if not report:
                logger.warning(f"No data extracted from {path.name}")
                return

            # Публикуем базовое событие (для совместимости)
            basic_payload = {
                "file_name": report.file_name,
                "total_dithers": report.total_dithers,
                "avg_settle_time": report.avg_settle_time_seconds,
                "timestamp": report.timestamp,
            }
            await event_bus.publish("DITHER_STATS", basic_payload)

            # Публикуем расширенное событие
            extended_payload = {
                "file_name": report.file_name,
                "timestamp": report.timestamp,
                "basic": {
                    "total_dithers": report.total_dithers,
                    "avg_settle_time": report.avg_settle_time_seconds,
                    "total_pixel_drift_x": report.total_pixel_drift_x,
                    "total_pixel_drift_y": report.total_pixel_drift_y,
                },
                "quality_metrics": {
                    "cd2": report.cd2.to_dict() if report.cd2 else None,
                    "voronoi_cv": report.voronoi_cv.to_dict()
                    if report.voronoi_cv
                    else None,
                    "gfm": report.gfm.to_dict() if report.gfm else None,
                    "nni": report.nni.to_dict() if report.nni else None,
                },
                "combined_score": report.combined_score,
                "overall_grade": report.overall_grade,
                "anomalies": {
                    "count": len(report.anomalies),
                    "details": [a.to_dict() for a in report.anomalies],
                },
                "recommendations": report.recommendations,
                "positions_count": len(report.positions),
            }
            await event_bus.publish("DITHER_STATS_EXTENDED", extended_payload)

            # Логируем ключевые метрики
            grade_info = (
                f"grade={report.overall_grade}" if report.overall_grade else "grade=N/A"
            )
            score_info = (
                f"score={report.combined_score:.3f}"
                if report.combined_score
                else "score=N/A"
            )

            logger.info(
                f"✅ Dither Statistics [{path.name}]: "
                f"dithers={report.total_dithers}, "
                f"{grade_info}, "
                f"{score_info}, "
                f"anomalies={len(report.anomalies)}"
            )

            # Логируем рекомендации если есть
            if report.recommendations:
                for rec in report.recommendations[:3]:  # Первые 3
                    logger.info(f"   💡 {rec}")

        except Exception as e:
            logger.error(
                f"Error processing Dither Statistics {path.name}: {e}", exc_info=True
            )


class GuidingAnalyzerWatcher(BaseFileWatcher):
    """
    Расширенный мониторинг FFT-анализа гидирования.

    Использует guiding_analyzer.py parser для полного анализа:
    - FFT спектр с детекцией периодических ошибок (PE)
    - Backlash анализ (люфт в RA/Dec)
    - Polar alignment error estimation
    - Stability ratio
    - Anomaly detection

    Публикует события:
    - GUIDING_ANALYSIS: базовые метрики (для совместимости)
    - GUIDING_ANALYSIS_EXTENDED: расширенные метрики с FFT анализом
    """

    def __init__(self, registry: CapabilityRegistry):
        path_str = getattr(settings.watchers, "guiding_analyzer_path", None)
        path = (
            Path(path_str)
            if path_str
            else Path.home() / "Documents" / "NINA" / "GuidingAnalyzer"
        )
        super().__init__(
            watch_path=path, target_files=[".csv", ".json"], registry=registry
        )
        logger.info(f"🎯 GuidingAnalyzerWatcher initialized (path: {path})")

    async def process_file(self, path: Path) -> None:
        """Обработка файла с расширенным FFT анализом."""
        if path.suffix.lower() not in [".csv", ".json"]:
            return

        logger.info(f"📊 Processing Guiding Analyzer: {path.name}")

        try:
            # Используем расширенный parser
            if path.suffix.lower() == ".json":
                report = await parse_guiding_analyzer_json(path)
            else:
                report = await parse_guiding_analyzer_csv(path)

            if not report:
                logger.warning(f"No data extracted from {path.name}")
                return

            # Публикуем базовое событие (для совместимости)
            basic_payload = {
                "file_name": report.file_name,
                "total_samples": report.total_samples,
                "avg_rms_ra": report.avg_rms_ra,
                "avg_rms_dec": report.avg_rms_dec,
                "avg_rms_total": report.avg_rms_total,
                "timestamp": report.timestamp,
            }
            await event_bus.publish("GUIDING_ANALYSIS", basic_payload)

            # Публикуем расширенное событие
            extended_payload = {
                "file_name": report.file_name,
                "timestamp": report.timestamp,
                "basic": {
                    "total_samples": report.total_samples,
                    "avg_rms_ra": report.avg_rms_ra,
                    "avg_rms_dec": report.avg_rms_dec,
                    "avg_rms_total": report.avg_rms_total,
                    "peak_rms_ra": report.peak_rms_ra,
                    "peak_rms_dec": report.peak_rms_dec,
                },
                "fft_analysis": {
                    "peaks_count": len(report.fft_peaks),
                    "peaks": [p.to_dict() for p in report.fft_peaks[:5]],  # Топ-5 пиков
                },
                "backlash": report.backlash.to_dict() if report.backlash else None,
                "polar_error": report.polar_error.to_dict()
                if report.polar_error
                else None,
                "stability": report.stability.to_dict() if report.stability else None,
                "statistics": {
                    "skewness_ra": report.skewness_ra,
                    "skewness_dec": report.skewness_dec,
                    "kurtosis_ra": report.kurtosis_ra,
                    "kurtosis_dec": report.kurtosis_dec,
                    "correlation_ra_dec": report.correlation_ra_dec,
                },
                "anomalies": {
                    "count": len(report.anomalies),
                    "details": [a.to_dict() for a in report.anomalies],
                },
            }
            await event_bus.publish("GUIDING_ANALYSIS_EXTENDED", extended_payload)

            # Логируем ключевые метрики
            rms_info = (
                f'RMS={report.avg_rms_total:.2f}"'
                if report.avg_rms_total
                else "RMS=N/A"
            )
            fft_info = (
                f"FFT_peaks={len(report.fft_peaks)}"
                if report.fft_peaks
                else "FFT_peaks=0"
            )
            backlash_info = ""
            if report.backlash:
                if report.backlash.ra_backlash_arcsec:
                    backlash_info += (
                        f', backlash_RA={report.backlash.ra_backlash_arcsec:.2f}"'
                    )
                if report.backlash.dec_backlash_arcsec:
                    backlash_info += (
                        f', backlash_Dec={report.backlash.dec_backlash_arcsec:.2f}"'
                    )

            polar_info = ""
            if report.polar_error and report.polar_error.total_error_arcmin:
                polar_info = (
                    f", polar_error={report.polar_error.total_error_arcmin:.1f}'"
                )

            logger.info(
                f"✅ Guiding Analysis [{path.name}]: "
                f"samples={report.total_samples}, "
                f"{rms_info}, "
                f"{fft_info}"
                f"{backlash_info}"
                f"{polar_info}, "
                f"anomalies={len(report.anomalies)}"
            )

            # Логируем FFT пики если есть
            if report.fft_peaks:
                for peak in report.fft_peaks[:3]:  # Первые 3
                    logger.info(
                        f"   📈 FFT peak: period={peak.period_seconds:.1f}s, "
                        f'amplitude={peak.amplitude:.3f}" ({peak.description})'
                    )

            # Логируем аномалии если есть
            if report.anomalies:
                for anomaly in report.anomalies[:3]:  # Первые 3
                    logger.warning(
                        f"   ⚠️ {anomaly.severity}: {anomaly.anomaly_type} - {anomaly.description}"
                    )

        except Exception as e:
            logger.error(
                f"Error processing Guiding Analyzer {path.name}: {e}", exc_info=True
            )
