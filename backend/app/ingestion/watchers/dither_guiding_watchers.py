import logging
import json
import csv
import aiofiles
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.config import settings

logger = logging.getLogger("DitherGuidingWatcher")


class DitherStatisticsWatcher(BaseFileWatcher):
    """Мониторит экспорты Dither Statistics (CD, GFM, Voronoi CV)."""

    def __init__(self):
        # Путь берется из settings.yaml или дефолтный
        path_str = getattr(settings.watchers, "dither_statistics_path", None)
        path = (
            Path(path_str)
            if path_str
            else Path.home() / "Documents" / "NINA" / "DitherStatistics"
        )
        super().__init__(watch_path=path, target_files=[".csv", ".json"])

    async def process_file(self, path: Path) -> None:
        if path.suffix.lower() not in [".csv", ".json"]:
            return

        try:
            if path.suffix.lower() == ".json":
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    data = json.loads(await f.read())
                await event_bus.publish(
                    "DITHER_STATS", {"file": path.name, "data": data}
                )
            else:
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    content = await f.read()
                reader = csv.DictReader(content.splitlines())
                rows = list(reader)
                await event_bus.publish(
                    "DITHER_STATS",
                    {
                        "file": path.name,
                        "rows_count": len(rows),
                        "last_row": rows[-1] if rows else None,
                    },
                )
        except Exception as e:
            logger.error(f"Error parsing Dither Statistics {path.name}: {e}")


class GuidingAnalyzerWatcher(BaseFileWatcher):
    """Мониторит экспорты Guiding Analyzer (FFT, PE, Backlash)."""

    def __init__(self):
        path_str = getattr(settings.watchers, "guiding_analyzer_path", None)
        path = (
            Path(path_str)
            if path_str
            else Path.home() / "Documents" / "NINA" / "GuidingAnalyzer"
        )
        super().__init__(watch_path=path, target_files=[".csv", ".json"])

    async def process_file(self, path: Path) -> None:
        if path.suffix.lower() not in [".csv", ".json"]:
            return

        try:
            if path.suffix.lower() == ".json":
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    data = json.loads(await f.read())
                await event_bus.publish(
                    "GUIDING_ANALYSIS", {"file": path.name, "data": data}
                )
            else:
                async with aiofiles.open(path, "r", encoding="utf-8") as f:
                    content = await f.read()
                reader = csv.DictReader(content.splitlines())
                rows = list(reader)
                await event_bus.publish(
                    "GUIDING_ANALYSIS", {"file": path.name, "rows_count": len(rows)}
                )
        except Exception as e:
            logger.error(f"Error parsing Guiding Analyzer {path.name}: {e}")
