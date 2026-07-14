"""
Hocus Focus Watcher - расширенный анализ качества звезд.
ЭТАП 9 (улучшения):
- Добавлен парсинг расширенных метрик: кома, астигматизм, эллиптичность
- Добавлен расчет интегрального качества звезды (star_quality_score)
- Добавлена детекция проблем коллимации
- Публикация события HOCUS_FOCUS_EXTENDED_ANALYSIS
- Интеграция с RAG для хранения истории качества
Hocus Focus предоставляет детальный анализ каждой звезды:
- FWHM (Full Width at Half Maximum)
- HFR (Half Flux Radius)
- Eccentricity (эллиптичность)
- Coma (кома - асимметрия звезды)
- Astigmatism (астигматизм)
- Peak intensity
- Background level
"""

import asyncio
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass, field

from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.config import settings
from app.core.rag_engine import rag_engine
from app.core.capability_registry import CapabilityRegistry

logger = logging.getLogger("HocusFocusWatcher")


@dataclass
class StarMetrics:
    """Расширенные метрики отдельной звезды."""

    x: float
    y: float
    fwhm: float
    hfr: float
    eccentricity: float
    peak_intensity: float
    background: float
    # Расширенные метрики
    coma: Optional[float] = None
    astigmatism: Optional[float] = None
    ellipticity: Optional[float] = None
    # Рассчитанные метрики
    star_quality_score: float = 0.0
    is_problematic: bool = False
    problem_reason: Optional[str] = None


@dataclass
class HocusFocusAnalysis:
    """Полный анализ кадра от Hocus Focus."""

    timestamp: str
    session_id: str
    frame_number: int
    filter_name: str
    exposure_time: float
    temperature: float

    # Агрегированные метрики
    star_count: int
    avg_fwhm: float
    avg_hfr: float
    avg_eccentricity: float
    median_fwhm: float
    median_hfr: float
    std_fwhm: float
    std_hfr: float

    # Расширенные метрики
    avg_coma: Optional[float] = None
    avg_astigmatism: Optional[float] = None
    avg_ellipticity: Optional[float] = None

    # Детекция проблем
    collimation_issues: List[Dict[str, Any]] = field(default_factory=list)
    focus_issues: List[Dict[str, Any]] = field(default_factory=list)
    tracking_issues: List[Dict[str, Any]] = field(default_factory=list)

    # Качество
    overall_quality_score: float = 0.0
    quality_grade: str = "UNKNOWN"  # EXCELLENT, GOOD, FAIR, POOR

    # Детали
    stars: List[StarMetrics] = field(default_factory=list)


class HocusFocusWatcher(BaseFileWatcher):
    """
    Мониторит CSV-отчеты Hocus Focus.
    Анализирует КАЖДУЮ звезду и применяет Z-Score фильтрацию.
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

        # ИСПРАВЛЕНО: Вызов базового __init__ с правильными параметрами
        super().__init__(watch_path=hf_path, target_files=[".csv"], registry=registry)

        # ИСПРАВЛЕНО: Инициализация атрибутов для асинхронного цикла
        self._running = False
        self._task = None
        self.processed_files = set()

        # Кэш последних значений для трендового анализа
        self._fwhm_history: List[float] = []
        self._hfr_history: List[float] = []

        # Расширенная аналитика
        self._collimation_issues: List[Dict[str, Any]] = []

        logger.info(f"🔭 HocusFocusWatcher initialized (watching: {hf_path})")

    async def start(self):
        """Запускает watcher."""
        if self._running:
            return

        self._running = True
        self._task = asyncio.create_task(self._watch_loop())
        logger.info("HocusFocusWatcher started")

    async def stop(self):
        """Останавливает watcher."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("HocusFocusWatcher stopped")

    async def _watch_loop(self):
        """Основной цикл наблюдения за файлами."""
        while self._running:
            try:
                await self._scan_for_new_files()
                await asyncio.sleep(settings.watchers.hocus_focus_scan_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in HocusFocusWatcher loop: {e}")
                await asyncio.sleep(5)

    async def _scan_for_new_files(self):
        """Сканирует директорию на новые CSV файлы."""
        try:
            watch_dir = Path(self.watch_path)
            if not watch_dir.exists():
                return

            csv_files = list(watch_dir.glob("*.csv"))

            for csv_file in csv_files:
                if csv_file.name not in self.processed_files:
                    await self._process_csv_file(csv_file)
                    self.processed_files.add(csv_file.name)

        except Exception as e:
            logger.error(f"Error scanning for Hocus Focus files: {e}")

    async def _process_csv_file(self, csv_file: Path):
        """Обрабатывает один CSV файл Hocus Focus."""
        try:
            logger.info(f"Processing Hocus Focus file: {csv_file.name}")

            # Читаем CSV файл
            stars = await self._parse_csv(csv_file)

            if not stars:
                logger.warning(f"No stars found in {csv_file.name}")
                return

            # Анализируем звезды
            analysis = await self._analyze_stars(stars, csv_file)

            # Публикуем базовое событие (для совместимости)
            await event_bus.publish(
                "HOCUS_FOCUS_ANALYSIS",
                {
                    "session_id": analysis.session_id,
                    "frame_number": analysis.frame_number,
                    "star_count": analysis.star_count,
                    "avg_fwhm": analysis.avg_fwhm,
                    "avg_hfr": analysis.avg_hfr,
                    "timestamp": analysis.timestamp,
                },
            )

            # Публикуем расширенное событие (ЭТАП 9)
            await event_bus.publish(
                "HOCUS_FOCUS_EXTENDED_ANALYSIS",
                {
                    "analysis": self._serialize_analysis(analysis),
                    "timestamp": analysis.timestamp,
                },
            )

            # Индексируем в RAG для истории
            await self._index_in_rag(analysis)

            # Обновляем историю
            self._update_quality_history(analysis)

            logger.info(
                f"Hocus Focus analysis complete: "
                f"{analysis.star_count} stars, "
                f"avg FWHM={analysis.avg_fwhm:.2f}, "
                f"quality={analysis.quality_grade}"
            )

        except Exception as e:
            logger.error(f"Error processing {csv_file.name}: {e}")

    async def _parse_csv(self, csv_file: Path) -> List[StarMetrics]:
        """Парсит CSV файл Hocus Focus."""
        stars = []
        try:
            with open(csv_file, "r", encoding="utf-8") as f:
                lines = f.readlines()

            if len(lines) < 2:
                return stars

            # Парсим заголовок
            header = lines[0].strip().split(",")
            header = [h.strip().lower() for h in header]

            # Определяем индексы колонок
            col_indices = {}
            for i, col_name in enumerate(header):
                if "x" in col_name:
                    col_indices["x"] = i
                elif "y" in col_name:
                    col_indices["y"] = i
                elif "fwhm" in col_name:
                    col_indices["fwhm"] = i
                elif "hfr" in col_name:
                    col_indices["hfr"] = i
                elif "ecc" in col_name:
                    col_indices["eccentricity"] = i
                elif "peak" in col_name:
                    col_indices["peak"] = i
                elif "background" in col_name or "bg" in col_name:
                    col_indices["background"] = i
                elif "coma" in col_name:
                    col_indices["coma"] = i
                elif "astig" in col_name:
                    col_indices["astigmatism"] = i
                elif "ellip" in col_name:
                    col_indices["ellipticity"] = i

            # Парсим данные
            for line in lines[1:]:
                values = line.strip().split(",")
                if len(values) < 4:
                    continue

                try:
                    star = StarMetrics(
                        x=float(values[col_indices.get("x", 0)]),
                        y=float(values[col_indices.get("y", 1)]),
                        fwhm=float(values[col_indices.get("fwhm", 2)]),
                        hfr=float(values[col_indices.get("hfr", 3)]),
                        eccentricity=float(values[col_indices.get("eccentricity", 4)])
                        if "eccentricity" in col_indices
                        else 0.0,
                        peak_intensity=float(values[col_indices.get("peak", 5)])
                        if "peak" in col_indices
                        else 0.0,
                        background=float(values[col_indices.get("background", 6)])
                        if "background" in col_indices
                        else 0.0,
                        coma=float(values[col_indices.get("coma", 7)])
                        if "coma" in col_indices
                        else None,
                        astigmatism=float(values[col_indices.get("astigmatism", 8)])
                        if "astigmatism" in col_indices
                        else None,
                        ellipticity=float(values[col_indices.get("ellipticity", 9)])
                        if "ellipticity" in col_indices
                        else None,
                    )
                    stars.append(star)
                except (ValueError, IndexError) as e:
                    logger.debug(f"Skipping invalid star data: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error parsing CSV {csv_file}: {e}")

        return stars

    async def _analyze_stars(
        self, stars: List[StarMetrics], csv_file: Path
    ) -> HocusFocusAnalysis:
        """Анализирует список звезд и создает полный отчет."""
        # Извлекаем метаданные из имени файла
        metadata = self._extract_metadata_from_filename(csv_file.name)

        # Рассчитываем star_quality_score для каждой звезды
        for star in stars:
            star.star_quality_score = self._calculate_star_quality_score(star)
            star.is_problematic, star.problem_reason = self._detect_star_problems(star)

        # Агрегированные метрики
        fwhm_values = [s.fwhm for s in stars]
        hfr_values = [s.hfr for s in stars]
        eccentricity_values = [s.eccentricity for s in stars]

        avg_fwhm = sum(fwhm_values) / len(fwhm_values)
        avg_hfr = sum(hfr_values) / len(hfr_values)
        avg_eccentricity = sum(eccentricity_values) / len(eccentricity_values)

        median_fwhm = sorted(fwhm_values)[len(fwhm_values) // 2]
        median_hfr = sorted(hfr_values)[len(hfr_values) // 2]

        std_fwhm = (
            sum((x - avg_fwhm) ** 2 for x in fwhm_values) / len(fwhm_values)
        ) ** 0.5
        std_hfr = (sum((x - avg_hfr) ** 2 for x in hfr_values) / len(hfr_values)) ** 0.5

        # Расширенные метрики
        coma_values = [s.coma for s in stars if s.coma is not None]
        astigmatism_values = [s.astigmatism for s in stars if s.astigmatism is not None]
        ellipticity_values = [s.ellipticity for s in stars if s.ellipticity is not None]

        avg_coma = sum(coma_values) / len(coma_values) if coma_values else None
        avg_astigmatism = (
            sum(astigmatism_values) / len(astigmatism_values)
            if astigmatism_values
            else None
        )
        avg_ellipticity = (
            sum(ellipticity_values) / len(ellipticity_values)
            if ellipticity_values
            else None
        )

        # Детекция проблем
        collimation_issues = self._detect_collimation_issues(
            stars, avg_coma, avg_astigmatism
        )
        focus_issues = self._detect_focus_issues(avg_fwhm, avg_hfr, std_fwhm)
        tracking_issues = self._detect_tracking_issues(avg_eccentricity, stars)

        # Общее качество
        overall_quality_score = self._calculate_overall_quality(
            avg_fwhm,
            avg_hfr,
            avg_eccentricity,
            std_fwhm,
            collimation_issues,
            focus_issues,
            tracking_issues,
        )
        quality_grade = self._determine_quality_grade(overall_quality_score)

        return HocusFocusAnalysis(
            timestamp=datetime.now().isoformat(),
            session_id=metadata.get("session_id", "unknown"),
            frame_number=metadata.get("frame_number", 0),
            filter_name=metadata.get("filter_name", "unknown"),
            exposure_time=metadata.get("exposure_time", 0.0),
            temperature=metadata.get("temperature", 0.0),
            star_count=len(stars),
            avg_fwhm=avg_fwhm,
            avg_hfr=avg_hfr,
            avg_eccentricity=avg_eccentricity,
            median_fwhm=median_fwhm,
            median_hfr=median_hfr,
            std_fwhm=std_fwhm,
            std_hfr=std_hfr,
            avg_coma=avg_coma,
            avg_astigmatism=avg_astigmatism,
            avg_ellipticity=avg_ellipticity,
            collimation_issues=collimation_issues,
            focus_issues=focus_issues,
            tracking_issues=tracking_issues,
            overall_quality_score=overall_quality_score,
            quality_grade=quality_grade,
            stars=stars,
        )

    def _extract_metadata_from_filename(self, filename: str) -> Dict[str, Any]:
        """Извлекает метаданные из имени файла."""
        # Пример: M31_20260115_frame042_Ha_300s_-10C.csv
        metadata = {
            "session_id": "unknown",
            "frame_number": 0,
            "filter_name": "unknown",
            "exposure_time": 0.0,
            "temperature": 0.0,
        }

        try:
            # Парсим имя файла
            parts = filename.replace(".csv", "").split("_")

            if len(parts) >= 2:
                metadata["session_id"] = f"{parts[0]}_{parts[1]}"

            for part in parts:
                if part.startswith("frame"):
                    try:
                        metadata["frame_number"] = int(part[5:])
                    except ValueError:
                        pass
                elif part.endswith("s") and part[:-1].isdigit():
                    try:
                        metadata["exposure_time"] = float(part[:-1])
                    except ValueError:
                        pass
                elif part.endswith("C") and (part[:-1].replace("-", "").isdigit()):
                    try:
                        metadata["temperature"] = float(part[:-1])
                    except ValueError:
                        pass
                elif part in ["L", "R", "G", "B", "Ha", "OIII", "SII"]:
                    metadata["filter_name"] = part

        except Exception as e:
            logger.debug(f"Error extracting metadata from {filename}: {e}")

        return metadata

    def _calculate_star_quality_score(self, star: StarMetrics) -> float:
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
        if star.fwhm > 4.0:
            score -= 3.0
        elif star.fwhm > 3.0:
            score -= 2.0
        elif star.fwhm > 2.5:
            score -= 1.0

        # HFR penalty (идеально < 1.5, плохо > 3.0)
        if star.hfr > 3.0:
            score -= 2.5
        elif star.hfr > 2.5:
            score -= 1.5
        elif star.hfr > 2.0:
            score -= 0.5

        # Eccentricity penalty (идеально < 0.3, плохо > 0.7)
        if star.eccentricity > 0.7:
            score -= 2.0
        elif star.eccentricity > 0.5:
            score -= 1.0
        elif star.eccentricity > 0.3:
            score -= 0.5

        # Coma penalty (если доступно)
        if star.coma is not None:
            if abs(star.coma) > 0.5:
                score -= 1.5
            elif abs(star.coma) > 0.3:
                score -= 0.5

        # Astigmatism penalty (если доступно)
        if star.astigmatism is not None:
            if abs(star.astigmatism) > 0.5:
                score -= 1.0
            elif abs(star.astigmatism) > 0.3:
                score -= 0.5

        return max(0.0, min(10.0, score))

    def _detect_star_problems(self, star: StarMetrics) -> tuple[bool, Optional[str]]:
        """Детектирует проблемы с отдельной звездой."""
        problems = []

        if star.fwhm > 4.0:
            problems.append("FWHM too high")

        if star.eccentricity > 0.7:
            problems.append("High eccentricity")

        if star.coma is not None and abs(star.coma) > 0.5:
            problems.append("High coma")

        if star.astigmatism is not None and abs(star.astigmatism) > 0.5:
            problems.append("High astigmatism")

        if problems:
            return True, "; ".join(problems)

        return False, None

    def _detect_collimation_issues(
        self,
        stars: List[StarMetrics],
        avg_coma: Optional[float],
        avg_astigmatism: Optional[float],
    ) -> List[Dict[str, Any]]:
        """Детектирует проблемы коллимации."""
        issues = []

        # Проверяем среднюю кому
        if avg_coma is not None and abs(avg_coma) > 0.3:
            issues.append(
                {
                    "type": "coma",
                    "severity": "HIGH" if abs(avg_coma) > 0.5 else "MEDIUM",
                    "value": avg_coma,
                    "description": f"High average coma: {avg_coma:.3f}. Check collimation.",
                }
            )

        # Проверяем средний астигматизм
        if avg_astigmatism is not None and abs(avg_astigmatism) > 0.3:
            issues.append(
                {
                    "type": "astigmatism",
                    "severity": "HIGH" if abs(avg_astigmatism) > 0.5 else "MEDIUM",
                    "value": avg_astigmatism,
                    "description": f"High average astigmatism: {avg_astigmatism:.3f}. Check optics alignment.",
                }
            )

        # Проверяем распределение комы по полю (радиальная зависимость)
        if avg_coma is not None and len(stars) > 10:
            center_stars = [
                s for s in stars if abs(s.x - 500) < 200 and abs(s.y - 500) < 200
            ]
            edge_stars = [
                s for s in stars if abs(s.x - 500) > 300 or abs(s.y - 500) > 300
            ]

            if center_stars and edge_stars:
                center_coma = sum(
                    s.coma for s in center_stars if s.coma is not None
                ) / len(center_stars)
                edge_coma = sum(s.coma for s in edge_stars if s.coma is not None) / len(
                    edge_stars
                )

                if abs(edge_coma - center_coma) > 0.3:
                    issues.append(
                        {
                            "type": "radial_coma",
                            "severity": "MEDIUM",
                            "value": edge_coma - center_coma,
                            "description": f"Radial coma variation detected. Center: {center_coma:.3f}, Edge: {edge_coma:.3f}",
                        }
                    )

        return issues

    def _detect_focus_issues(
        self, avg_fwhm: float, avg_hfr: float, std_fwhm: float
    ) -> List[Dict[str, Any]]:
        """Детектирует проблемы фокусировки."""
        issues = []

        # Высокий FWHM
        if avg_fwhm > 3.5:
            issues.append(
                {
                    "type": "high_fwhm",
                    "severity": "HIGH" if avg_fwhm > 4.5 else "MEDIUM",
                    "value": avg_fwhm,
                    "description": f"High average FWHM: {avg_fwhm:.2f}. Check focus.",
                }
            )

        # Высокий HFR
        if avg_hfr > 2.5:
            issues.append(
                {
                    "type": "high_hfr",
                    "severity": "HIGH" if avg_hfr > 3.0 else "MEDIUM",
                    "value": avg_hfr,
                    "description": f"High average HFR: {avg_hfr:.2f}. Check focus.",
                }
            )

        # Высокая вариация FWHM (проблемы с фокусом или атмосферой)
        if std_fwhm > 1.0:
            issues.append(
                {
                    "type": "high_fwhm_variation",
                    "severity": "MEDIUM",
                    "value": std_fwhm,
                    "description": f"High FWHM variation: {std_fwhm:.2f}. Possible focus drift or atmospheric turbulence.",
                }
            )

        return issues

    def _detect_tracking_issues(
        self, avg_eccentricity: float, stars: List[StarMetrics]
    ) -> List[Dict[str, Any]]:
        """Детектирует проблемы трекинга."""
        issues = []

        # Высокая эллиптичность (проблемы с гидированием)
        if avg_eccentricity > 0.5:
            issues.append(
                {
                    "type": "high_eccentricity",
                    "severity": "HIGH" if avg_eccentricity > 0.7 else "MEDIUM",
                    "value": avg_eccentricity,
                    "description": f"High average eccentricity: {avg_eccentricity:.3f}. Check guiding.",
                }
            )

        # Направленная эллиптичность (проблемы с гидированием в одной оси)
        if len(stars) > 10:
            high_ecc_stars = [s for s in stars if s.eccentricity > 0.5]
            if len(high_ecc_stars) > len(stars) * 0.3:
                # Проверяем направление
                angles = []
                for star in high_ecc_stars:
                    if hasattr(star, "angle"):
                        angles.append(star.angle)

                if angles:
                    avg_angle = sum(angles) / len(angles)
                    issues.append(
                        {
                            "type": "directional_tracking",
                            "severity": "MEDIUM",
                            "value": avg_angle,
                            "description": f"Directional tracking issue detected. Average angle: {avg_angle:.1f}°",
                        }
                    )

        return issues

    def _calculate_overall_quality(
        self,
        avg_fwhm: float,
        avg_hfr: float,
        avg_eccentricity: float,
        std_fwhm: float,
        collimation_issues: List[Dict[str, Any]],
        focus_issues: List[Dict[str, Any]],
        tracking_issues: List[Dict[str, Any]],
    ) -> float:
        """Рассчитывает общее качество кадра (0-10)."""
        score = 10.0

        # FWHM penalty
        if avg_fwhm > 4.0:
            score -= 2.5
        elif avg_fwhm > 3.0:
            score -= 1.5
        elif avg_fwhm > 2.5:
            score -= 0.5

        # HFR penalty
        if avg_hfr > 3.0:
            score -= 2.0
        elif avg_hfr > 2.5:
            score -= 1.0
        elif avg_hfr > 2.0:
            score -= 0.5

        # Eccentricity penalty
        if avg_eccentricity > 0.7:
            score -= 2.0
        elif avg_eccentricity > 0.5:
            score -= 1.0
        elif avg_eccentricity > 0.3:
            score -= 0.5

        # FWHM variation penalty
        if std_fwhm > 1.0:
            score -= 1.0

        # Issues penalty
        high_severity_issues = sum(
            1
            for issue in collimation_issues + focus_issues + tracking_issues
            if issue.get("severity") == "HIGH"
        )
        medium_severity_issues = sum(
            1
            for issue in collimation_issues + focus_issues + tracking_issues
            if issue.get("severity") == "MEDIUM"
        )

        score -= high_severity_issues * 1.0
        score -= medium_severity_issues * 0.5

        return max(0.0, min(10.0, score))

    def _determine_quality_grade(self, score: float) -> str:
        """Определяет градацию качества."""
        if score >= 8.0:
            return "EXCELLENT"
        elif score >= 6.0:
            return "GOOD"
        elif score >= 4.0:
            return "FAIR"
        else:
            return "POOR"

    def _serialize_analysis(self, analysis: HocusFocusAnalysis) -> Dict[str, Any]:
        """Сериализует анализ для публикации."""
        return {
            "timestamp": analysis.timestamp,
            "session_id": analysis.session_id,
            "frame_number": analysis.frame_number,
            "filter_name": analysis.filter_name,
            "exposure_time": analysis.exposure_time,
            "temperature": analysis.temperature,
            "star_count": analysis.star_count,
            "avg_fwhm": analysis.avg_fwhm,
            "avg_hfr": analysis.avg_hfr,
            "avg_eccentricity": analysis.avg_eccentricity,
            "median_fwhm": analysis.median_fwhm,
            "median_hfr": analysis.median_hfr,
            "std_fwhm": analysis.std_fwhm,
            "std_hfr": analysis.std_hfr,
            "avg_coma": analysis.avg_coma,
            "avg_astigmatism": analysis.avg_astigmatism,
            "avg_ellipticity": analysis.avg_ellipticity,
            "collimation_issues": analysis.collimation_issues,
            "focus_issues": analysis.focus_issues,
            "tracking_issues": analysis.tracking_issues,
            "overall_quality_score": analysis.overall_quality_score,
            "quality_grade": analysis.quality_grade,
            "problematic_star_count": sum(
                1 for s in analysis.stars if s.is_problematic
            ),
        }

    async def _index_in_rag(self, analysis: HocusFocusAnalysis):
        """Индексирует анализ в RAG для истории."""
        try:
            document = f"""
Hocus Focus Analysis - {analysis.timestamp}
Session: {analysis.session_id}
Frame: {analysis.frame_number}
Filter: {analysis.filter_name}
Exposure: {analysis.exposure_time}s
Temperature: {analysis.temperature}°C

Quality Metrics:
- Stars detected: {analysis.star_count}
- Average FWHM: {analysis.avg_fwhm:.2f} pixels
- Average HFR: {analysis.avg_hfr:.2f} pixels
- Average Eccentricity: {analysis.avg_eccentricity:.3f}
- Overall Quality: {analysis.quality_grade} ({analysis.overall_quality_score:.1f}/10)

Issues Detected:
- Collimation issues: {len(analysis.collimation_issues)}
- Focus issues: {len(analysis.focus_issues)}
- Tracking issues: {len(analysis.tracking_issues)}
"""

            if analysis.collimation_issues:
                document += "\nCollimation Issues:\n"
                for issue in analysis.collimation_issues:
                    document += f"- {issue['description']}\n"

            if analysis.focus_issues:
                document += "\nFocus Issues:\n"
                for issue in analysis.focus_issues:
                    document += f"- {issue['description']}\n"

            if analysis.tracking_issues:
                document += "\nTracking Issues:\n"
                for issue in analysis.tracking_issues:
                    document += f"- {issue['description']}\n"

            metadata = {
                "type": "hocus_focus_analysis",
                "session_id": analysis.session_id,
                "frame_number": analysis.frame_number,
                "filter_name": analysis.filter_name,
                "quality_grade": analysis.quality_grade,
                "overall_quality_score": analysis.overall_quality_score,
                "timestamp": analysis.timestamp,
            }

            await rag_engine.add_document(document, metadata)

        except Exception as e:
            logger.error(f"Error indexing Hocus Focus analysis in RAG: {e}")

    def _update_quality_history(self, analysis: HocusFocusAnalysis):
        """Обновляет историю качества для трендового анализа."""
        self.quality_history.append(
            {
                "timestamp": analysis.timestamp,
                "session_id": analysis.session_id,
                "frame_number": analysis.frame_number,
                "quality_score": analysis.overall_quality_score,
                "quality_grade": analysis.quality_grade,
                "avg_fwhm": analysis.avg_fwhm,
                "avg_hfr": analysis.avg_hfr,
                "issue_count": len(analysis.collimation_issues)
                + len(analysis.focus_issues)
                + len(analysis.tracking_issues),
            }
        )

        # Ограничиваем размер истории
        if len(self.quality_history) > self.max_history_size:
            self.quality_history = self.quality_history[-self.max_history_size :]

    def get_quality_trend(self, window: int = 10) -> Optional[Dict[str, Any]]:
        """Возвращает тренд качества за последние N кадров."""
        if len(self.quality_history) < window:
            return None

        recent = self.quality_history[-window:]

        scores = [h["quality_score"] for h in recent]
        fwhm_values = [h["avg_fwhm"] for h in recent]
        hfr_values = [h["avg_hfr"] for h in recent]

        # Рассчитываем тренды (простая линейная регрессия)
        def calculate_trend(values):
            n = len(values)
            x_mean = (n - 1) / 2
            y_mean = sum(values) / n

            numerator = sum((i - x_mean) * (values[i] - y_mean) for i in range(n))
            denominator = sum((i - x_mean) ** 2 for i in range(n))

            if denominator == 0:
                return 0.0

            return numerator / denominator

        return {
            "window": window,
            "quality_trend": calculate_trend(scores),
            "fwhm_trend": calculate_trend(fwhm_values),
            "hfr_trend": calculate_trend(hfr_values),
            "avg_quality": sum(scores) / len(scores),
            "avg_fwhm": sum(fwhm_values) / len(fwhm_values),
            "avg_hfr": sum(hfr_values) / len(hfr_values),
        }


# Singleton instance
hocus_focus_watcher = HocusFocusWatcher(
    registry=CapabilityRegistry(settings.nina_environment.profiles_dir)
)
