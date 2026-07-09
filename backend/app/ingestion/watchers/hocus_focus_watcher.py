"""
Hocus Focus Watcher
Мониторит CSV-отчеты Hocus Focus с детальной аналитикой звезд.
Устраняет Упрощение #2.
"""

import logging
from app.core.executors import async_read_csv
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
        """
        ИСПРАВЛЕНО (v4.0 — проблема #13): async_read_csv для CSV парсинга.
        """
        if path.suffix.lower() != ".csv":
            return

        logger.info(f"Parsing Hocus Focus report: {path.name}")

        # ИСПРАВЛЕНО: Асинхронное чтение CSV
        raw_rows = await async_read_csv(path, delimiter=None)  # auto-detect
        if not raw_rows:
            logger.warning(f"No data found in {path.name}")
            return

        # Конвертируем в StarData
        from app.ingestion.parsers.hocus_focus import StarData, filter_anomalies

        stars = []
        for row in raw_rows:
            try:
                # Очистка данных: замена запятых на точки для float
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

        if not stars:
            logger.warning(f"No valid stars found in {path.name}")
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
