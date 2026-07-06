"""
Hocus Focus Watcher
Мониторит CSV-отчеты Hocus Focus с детальной аналитикой звезд.
Устраняет Упрощение #2.
"""

import logging
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.hocus_focus import parse_hocus_focus_csv, filter_anomalies
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings

logger = logging.getLogger("HocusFocusWatcher")


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

        super().__init__(watch_path=hf_path, target_files=[".csv"], registry=registry)

    async def process_file(self, path: Path) -> None:
        if path.suffix.lower() != ".csv":
            return

        logger.info(f"Parsing Hocus Focus report: {path.name}")
        stars = parse_hocus_focus_csv(path)

        if not stars:
            logger.warning(f"No stars found in {path.name}")
            return

        report = filter_anomalies(stars)
        report.file_name = path.stem

        logger.info(
            f"HF Analysis [{path.stem}]: Total={report.total_stars_detected}, "
            f"Valid={report.valid_stars_count}, Anomalies={report.anomalies_count}, "
            f"Median FWHM={report.median_fwhm:.2f}"
            if report.median_fwhm
            else "N/A"
        )

        payload = {
            "file_name": report.file_name,
            "report": report.model_dump(exclude={"stars"}),
        }
        await event_bus.publish("HOCUS_FOCUS_ANALYSIS", payload)
