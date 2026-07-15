"""
Hocus Focus Watcher — расширенный анализ качества звёзд.
Использует BaseFileWatcher (watchdog) для event-driven мониторинга.

ЭТАП 9 (улучшения):
- Парсинг расширенных метрик: кома, астигматизм (если доступны в CSV)
- Расчёт интегрального качества звезды (star_quality_score)
- Детекция проблем коллимации, фокусировки, трекинга
- Публикация HOCUS_FOCUS_ANALYSIS + HOCUS_FOCUS_EXTENDED_ANALYSIS
- Трендовая аналитика качества
- Интеграция с RAG для долгосрочного хранения

Hocus Focus: https://github.com/ghilios/hocus-focus
GUID: 0f1d10b6-d306-4168-b751-d454cbac9670
"""

import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from app.core.executors import async_read_csv
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.hocus_focus import (
    StarData,
    HocusFocusReport,
    filter_anomalies,
)
from app.core.capability_registry import CapabilityRegistry

from backend.app.core.math_utils import calculate_trend

logger = logging.getLogger("HocusFocusWatcher")


class HocusFocusWatcher(BaseFileWatcher):
    """
    Мониторит CSV-отчёты Hocus Focus.
    Анализирует КАЖДУЮ звезду и применяет Z-Score фильтрацию.

    ЭТАП 9: Расширенная аналитика:
    - Парсинг комы, астигматизма (graceful degradation если поля отсутствуют)
    - Расчёт star_quality_score для каждой звезды
    - Детекция проблем коллимации (radial coma, astigmatism)
    - Детекция проблем фокусировки (FWHM, HFR, IQR)
    - Детекция проблем трекинга (eccentricity, directional)
    - Трендовая аналитика качества
    - Интеграция с RAG

    АРХИТЕКТУРА:
    - BaseFileWatcher (watchdog) → event-driven, НЕ polling
    - process_file() вызывается автоматически при изменении CSV
    - Debouncing 1.5s из BaseFileWatcher
    """

    HOCUS_FOCUS_GUID = "0f1d10b6-d306-4168-b751-d454cbac9670"

    def __init__(self, registry: CapabilityRegistry):
        # Динамическое получение пути из XML-профиля N.I.N.A. через DI
        hf_path = registry.get_plugin_path(self.HOCUS_FOCUS_GUID, "SavePath")
        if not hf_path:
            logger.warning(
                "Hocus Focus SavePath not found in profile registry. Using fallback."
            )
            hf_path = settings.nina_environment.appdata_root / "HocusFocusIntermediate"

        # Вызов базового __init__ (watchdog, debouncing, event-driven)
        super().__init__(watch_path=hf_path, target_files=[".csv"], registry=registry)

        # Трендовая история
        self._fwhm_history: List[float] = []
        self._hfr_history: List[float] = []
        self._quality_history: List[Dict[str, Any]] = []
        self._max_history_size: int = 100

        logger.info(f"🔭 HocusFocusWatcher initialized (watching: {hf_path})")

    # ====================================================================
    # ОСНОВНОЙ МЕТОД (вызывается BaseFileWatcher через watchdog)
    # ====================================================================

    async def process_file(self, path: Path) -> None:
        """
        Обработка изменённого CSV-файла Hocus Focus.
        Вызывается автоматически BaseFileWatcher при изменении файла.
        """
        if path.suffix.lower() != ".csv":
            return

        logger.info(f"Parsing Hocus Focus report: {path.name}")

        # Асинхронное чтение CSV через executor
        raw_rows = await async_read_csv(path, delimiter=None)
        if not raw_rows:
            logger.warning(f"No data found in {path.name}")
            return

        # Конвертируем в StarData
        stars: List[StarData] = []
        for row in raw_rows:
            try:
                cleaned_row = {}
                for k, v in row.items():
                    if not v or not str(v).strip():
                        continue
                    try:
                        cleaned_row[k] = float(str(v).replace(",", "."))
                    except (ValueError, TypeError):
                        continue
                star = StarData(**cleaned_row)
                # ЭТАП 9: расчёт качества звезды
                star.star_quality_score = self._calculate_star_quality_score(star)
                star.is_problematic, star.anomaly_reason = self._detect_star_problems(
                    star
                )
                stars.append(star)
            except (ValueError, TypeError) as e:
                logger.debug(f"Skipping invalid star row: {e}")

        if not stars:
            logger.warning(f"No valid stars found in {path.name}")
            return

        # Z-Score фильтрация аномалий
        report = filter_anomalies(stars)
        report.file_name = path.stem

        # ЭТАП 9: Расширенная аналитика
        extended = self._perform_extended_analysis(stars, report)

        # Публикация базового события (обратная совместимость)
        await event_bus.publish(
            "HOCUS_FOCUS_ANALYSIS",
            {
                "file_name": report.file_name,
                "report": report.model_dump(exclude={"stars"}),
            },
        )

        # ЭТАП 9: Публикация расширенного события
        await event_bus.publish("HOCUS_FOCUS_EXTENDED_ANALYSIS", extended)

        # Обновление трендовой истории
        self._update_quality_history(report, extended)

        # RAG индексация (non-blocking)
        try:
            await self._index_in_rag(report, extended)
        except Exception as e:
            logger.debug(f"RAG indexing skipped: {e}")

        logger.info(
            f"HF Analysis [{path.stem}]: "
            f"Total={report.total_stars_detected}, "
            f"Valid={report.valid_stars_count}, "
            f"Anomalies={report.anomalies_count}, "
            f"Median FWHM="
            f"{report.median_fwhm:.2f if report.median_fwhm else 'N/A'}, "
            f"Quality={extended.get('quality_grade', 'N/A')}, "
            f"Issues={extended.get('total_issues', 0)}"
        )

    # ====================================================================
    # ЭТАП 9: РАСШИРЕННАЯ АНАЛИТИКА
    # ====================================================================

    def _perform_extended_analysis(
        self, stars: List[StarData], report: HocusFocusReport
    ) -> Dict[str, Any]:
        """Выполняет расширенную аналитику звёзд."""
        # Расширенные метрики (graceful: поля могут отсутствовать)
        coma_values = [s.coma for s in stars if getattr(s, "coma", None) is not None]
        astig_values = [
            s.astigmatism for s in stars if getattr(s, "astigmatism", None) is not None
        ]
        quality_scores = [s.star_quality_score for s in stars]
        problematic = [s for s in stars if s.is_problematic]

        avg_coma = sum(coma_values) / len(coma_values) if coma_values else None
        avg_astig = sum(astig_values) / len(astig_values) if astig_values else None
        avg_quality = (
            sum(quality_scores) / len(quality_scores) if quality_scores else None
        )

        # Детекция проблем
        collimation = self._detect_collimation_issues(stars, avg_coma, avg_astig)
        focus = self._detect_focus_issues(report)
        tracking = self._detect_tracking_issues(report, stars)

        all_issues = collimation + focus + tracking

        # Общая оценка качества (0-10)
        quality_score = self._calculate_overall_quality(
            report, avg_coma, avg_astig, all_issues, len(problematic)
        )
        quality_grade = self._determine_quality_grade(quality_score)

        return {
            "file_name": report.file_name,
            "timestamp": datetime.now().isoformat(),
            "basic_metrics": {
                "total_stars": report.total_stars_detected,
                "valid_stars": report.valid_stars_count,
                "anomalies": report.anomalies_count,
                "median_fwhm": report.median_fwhm,
                "median_hfr": report.median_hfr,
                "median_eccentricity": report.median_eccentricity,
                "fwhm_25th": report.fwhm_25th,
                "fwhm_75th": report.fwhm_75th,
            },
            "extended_metrics": {
                "avg_coma": avg_coma,
                "avg_astigmatism": avg_astig,
                "avg_quality_score": avg_quality,
                "problematic_stars_count": len(problematic),
            },
            "collimation_issues": collimation,
            "focus_issues": focus,
            "tracking_issues": tracking,
            "total_issues": len(all_issues),
            "overall_quality_score": quality_score,
            "quality_grade": quality_grade,
        }

    # ====================================================================
    # РАСЧЁТ КАЧЕСТВА ЗВЕЗДЫ
    # ====================================================================

    def _calculate_star_quality_score(self, star: StarData) -> float:
        """Интегральное качество звезды (0-10)."""
        score = 10.0

        if star.fwhm is not None:
            if star.fwhm > 4.0:
                score -= 3.0
            elif star.fwhm > 3.0:
                score -= 2.0
            elif star.fwhm > 2.5:
                score -= 1.0

        if star.hfr is not None:
            if star.hfr > 3.0:
                score -= 2.5
            elif star.hfr > 2.5:
                score -= 1.5
            elif star.hfr > 2.0:
                score -= 0.5

        if star.eccentricity is not None:
            if star.eccentricity > 0.7:
                score -= 2.0
            elif star.eccentricity > 0.5:
                score -= 1.0
            elif star.eccentricity > 0.3:
                score -= 0.5

        coma = getattr(star, "coma", None)
        if coma is not None:
            if abs(coma) > 0.5:
                score -= 1.5
            elif abs(coma) > 0.3:
                score -= 0.5

        astig = getattr(star, "astigmatism", None)
        if astig is not None:
            if abs(astig) > 0.5:
                score -= 1.0
            elif abs(astig) > 0.3:
                score -= 0.5

        return max(0.0, min(10.0, score))

    def _detect_star_problems(self, star: StarData) -> tuple:
        """Детектирует проблемы с отдельной звездой."""
        problems = []
        if star.fwhm is not None and star.fwhm > 4.0:
            problems.append("FWHM too high")
        if star.eccentricity is not None and star.eccentricity > 0.7:
            problems.append("High eccentricity")
        coma = getattr(star, "coma", None)
        if coma is not None and abs(coma) > 0.5:
            problems.append("High coma")
        astig = getattr(star, "astigmatism", None)
        if astig is not None and abs(astig) > 0.5:
            problems.append("High astigmatism")
        if problems:
            return True, "; ".join(problems)
        return False, None

    # ====================================================================
    # ДЕТЕКЦИЯ ПРОБЛЕМ
    # ====================================================================

    def _detect_collimation_issues(
        self,
        stars: List[StarData],
        avg_coma: Optional[float],
        avg_astig: Optional[float],
    ) -> List[Dict[str, Any]]:
        """Детектирует проблемы коллимации."""
        issues: List[Dict[str, Any]] = []

        if avg_coma is not None and abs(avg_coma) > 0.3:
            issues.append(
                {
                    "type": "coma",
                    "severity": "HIGH" if abs(avg_coma) > 0.5 else "MEDIUM",
                    "value": round(avg_coma, 4),
                    "description": (
                        f"High average coma: {avg_coma:.3f}. Check collimation."
                    ),
                }
            )

        if avg_astig is not None and abs(avg_astig) > 0.3:
            issues.append(
                {
                    "type": "astigmatism",
                    "severity": "HIGH" if abs(avg_astig) > 0.5 else "MEDIUM",
                    "value": round(avg_astig, 4),
                    "description": (
                        f"High average astigmatism: {avg_astig:.3f}. "
                        f"Check optics alignment."
                    ),
                }
            )

        # Радиальная кома (растёт к краю поля)
        if avg_coma is not None and len(stars) > 10:
            cx, cy = 500.0, 500.0  # Примерный центр поля
            center = [s for s in stars if abs(s.x - cx) < 200 and abs(s.y - cy) < 200]
            edge = [s for s in stars if abs(s.x - cx) > 300 or abs(s.y - cy) > 300]
            if center and edge:
                c_coma = [getattr(s, "coma", None) for s in center]
                e_coma = [getattr(s, "coma", None) for s in edge]
                c_coma = [v for v in c_coma if v is not None]
                e_coma = [v for v in e_coma if v is not None]
                if c_coma and e_coma:
                    c_med = sum(c_coma) / len(c_coma)
                    e_med = sum(e_coma) / len(e_coma)
                    if abs(e_med - c_med) > 0.3:
                        issues.append(
                            {
                                "type": "radial_coma",
                                "severity": "MEDIUM",
                                "value": round(e_med - c_med, 4),
                                "description": (
                                    f"Radial coma: center={c_med:.3f}, edge={e_med:.3f}"
                                ),
                            }
                        )
        return issues

    def _detect_focus_issues(self, report: HocusFocusReport) -> List[Dict[str, Any]]:
        """Детектирует проблемы фокусировки."""
        issues: List[Dict[str, Any]] = []

        if report.median_fwhm is not None and report.median_fwhm > 3.5:
            issues.append(
                {
                    "type": "high_fwhm",
                    "severity": "HIGH" if report.median_fwhm > 4.5 else "MEDIUM",
                    "value": round(report.median_fwhm, 3),
                    "description": (
                        f"High median FWHM: {report.median_fwhm:.2f}. Check focus."
                    ),
                }
            )

        if report.median_hfr is not None and report.median_hfr > 2.5:
            issues.append(
                {
                    "type": "high_hfr",
                    "severity": "HIGH" if report.median_hfr > 3.0 else "MEDIUM",
                    "value": round(report.median_hfr, 3),
                    "description": (
                        f"High median HFR: {report.median_hfr:.2f}. Check focus."
                    ),
                }
            )

        if report.fwhm_75th is not None and report.fwhm_25th is not None:
            iqr = report.fwhm_75th - report.fwhm_25th
            if iqr > 1.0:
                issues.append(
                    {
                        "type": "high_fwhm_variation",
                        "severity": "MEDIUM",
                        "value": round(iqr, 3),
                        "description": (
                            f"High FWHM spread (IQR={iqr:.2f}). "
                            f"Focus drift or turbulence."
                        ),
                    }
                )
        return issues

    def _detect_tracking_issues(
        self, report: HocusFocusReport, stars: List[StarData]
    ) -> List[Dict[str, Any]]:
        """Детектирует проблемы трекинга."""
        issues: List[Dict[str, Any]] = []

        if report.median_eccentricity is not None and report.median_eccentricity > 0.5:
            issues.append(
                {
                    "type": "high_eccentricity",
                    "severity": (
                        "HIGH" if report.median_eccentricity > 0.7 else "MEDIUM"
                    ),
                    "value": round(report.median_eccentricity, 4),
                    "description": (
                        f"High eccentricity: {report.median_eccentricity:.3f}. "
                        f"Check guiding."
                    ),
                }
            )

        # Направленная эксцентриситет
        if len(stars) > 10:
            hi_ecc = [
                s for s in stars if s.eccentricity is not None and s.eccentricity > 0.5
            ]
            if len(hi_ecc) > len(stars) * 0.3:
                angles = [
                    s.angle for s in hi_ecc if getattr(s, "angle", None) is not None
                ]
                if angles:
                    avg_a = sum(angles) / len(angles)
                    issues.append(
                        {
                            "type": "directional_tracking",
                            "severity": "MEDIUM",
                            "value": round(avg_a, 1),
                            "description": (
                                f"Directional tracking issue. Avg angle: {avg_a:.1f}°"
                            ),
                        }
                    )
        return issues

    # ====================================================================
    # ОБЩАЯ ОЦЕНКА КАЧЕСТВА
    # ====================================================================

    def _calculate_overall_quality(
        self,
        report: HocusFocusReport,
        avg_coma: Optional[float],
        avg_astig: Optional[float],
        all_issues: List[Dict[str, Any]],
        problematic_count: int,
    ) -> float:
        """Общая оценка качества кадра (0-10)."""
        score = 10.0

        if report.median_fwhm is not None:
            if report.median_fwhm > 4.0:
                score -= 2.5
            elif report.median_fwhm > 3.0:
                score -= 1.5
            elif report.median_fwhm > 2.5:
                score -= 0.5

        if report.median_hfr is not None:
            if report.median_hfr > 3.0:
                score -= 2.0
            elif report.median_hfr > 2.5:
                score -= 1.0
            elif report.median_hfr > 2.0:
                score -= 0.5

        if report.median_eccentricity is not None:
            if report.median_eccentricity > 0.7:
                score -= 2.0
            elif report.median_eccentricity > 0.5:
                score -= 1.0
            elif report.median_eccentricity > 0.3:
                score -= 0.5

        if avg_coma is not None and abs(avg_coma) > 0.3:
            score -= 1.0
        if avg_astig is not None and abs(avg_astig) > 0.3:
            score -= 1.0

        hi = sum(1 for i in all_issues if i.get("severity") == "HIGH")
        md = sum(1 for i in all_issues if i.get("severity") == "MEDIUM")
        score -= hi * 1.0
        score -= md * 0.5

        if report.valid_stars_count > 0:
            ratio = problematic_count / report.valid_stars_count
            if ratio > 0.3:
                score -= 1.5
            elif ratio > 0.1:
                score -= 0.5

        return round(max(0.0, min(10.0, score)), 2)

    @staticmethod
    def _determine_quality_grade(score: float) -> str:
        if score >= 8.0:
            return "EXCELLENT"
        if score >= 6.0:
            return "GOOD"
        if score >= 4.0:
            return "FAIR"
        return "POOR"

    # ====================================================================
    # ТРЕНДОВАЯ ИСТОРИЯ
    # ====================================================================

    def _update_quality_history(
        self, report: HocusFocusReport, extended: Dict[str, Any]
    ) -> None:
        if report.median_fwhm is not None:
            self._fwhm_history.append(report.median_fwhm)
            if len(self._fwhm_history) > self._max_history_size:
                self._fwhm_history = self._fwhm_history[-self._max_history_size :]

        if report.median_hfr is not None:
            self._hfr_history.append(report.median_hfr)
            if len(self._hfr_history) > self._max_history_size:
                self._hfr_history = self._hfr_history[-self._max_history_size :]

        self._quality_history.append(
            {
                "timestamp": extended["timestamp"],
                "file_name": report.file_name,
                "quality_score": extended["overall_quality_score"],
                "quality_grade": extended["quality_grade"],
                "median_fwhm": report.median_fwhm,
                "median_hfr": report.median_hfr,
                "issues_count": extended["total_issues"],
            }
        )
        if len(self._quality_history) > self._max_history_size:
            self._quality_history = self._quality_history[-self._max_history_size :]

    def get_quality_trend(self, window: int = 10) -> Optional[Dict[str, Any]]:
        """Тренд качества за последние N кадров.
        ИСПРАВЛЕНО (С-4): использует calculate_trend из core.math_utils.
        """
        if len(self._quality_history) < window:
            return None
        recent = self._quality_history[-window:]
        scores = [h["quality_score"] for h in recent]
        fwhm_vals = [h["median_fwhm"] for h in recent if h["median_fwhm"]]
        hfr_vals = [h["median_hfr"] for h in recent if h["median_hfr"]]
        # ИСПРАВЛЕНО (С-4): единая функция из math_utils вместо inline helper
        return {
            "window": window,
            "quality_trend": calculate_trend(scores),
            "fwhm_trend": calculate_trend(fwhm_vals) if fwhm_vals else None,
            "hfr_trend": calculate_trend(hfr_vals) if hfr_vals else None,
            "avg_quality": sum(scores) / len(scores),
        }

    # ====================================================================
    # RAG ИНТЕГРАЦИЯ
    # ====================================================================

    async def _index_in_rag(
        self, report: HocusFocusReport, extended: Dict[str, Any]
    ) -> None:
        try:
            from app.core.rag_engine import rag_engine

            if not rag_engine._initialized:
                return

            doc = (
                f"Hocus Focus Analysis - {extended['timestamp']}\n"
                f"File: {report.file_name}\n"
                f"Stars: {report.total_stars_detected} total, "
                f"{report.valid_stars_count} valid, "
                f"{report.anomalies_count} anomalies\n"
                f"Median FWHM: "
                f"{report.median_fwhm:.2f if report.median_fwhm else 'N/A'}\n"
                f"Median HFR: "
                f"{report.median_hfr:.2f if report.median_hfr else 'N/A'}\n"
                f"Quality: {extended['quality_grade']} "
                f"({extended['overall_quality_score']}/10)\n"
                f"Issues: {extended['total_issues']}\n"
            )
            for cat in ("collimation_issues", "focus_issues", "tracking_issues"):
                for iss in extended.get(cat, []):
                    doc += f"- [{iss['severity']}] {iss['description']}\n"

            await rag_engine.add_document(
                text=doc,
                metadata={
                    "type": "hocus_focus_analysis",
                    "file_name": report.file_name,
                    "quality_grade": extended["quality_grade"],
                    "quality_score": extended["overall_quality_score"],
                    "timestamp": extended["timestamp"],
                },
                chunk_type="error_log",
            )
        except Exception as e:
            logger.debug(f"RAG indexing error: {e}")

    # ====================================================================
    # СТАТИСТИКА
    # ====================================================================

    def get_stats(self) -> Dict[str, Any]:
        return {
            "watching": str(self.watch_path),
            "fwhm_history_size": len(self._fwhm_history),
            "hfr_history_size": len(self._hfr_history),
            "quality_history_size": len(self._quality_history),
            "latest_quality": (
                self._quality_history[-1] if self._quality_history else None
            ),
            "quality_trend": self.get_quality_trend(10),
        }
