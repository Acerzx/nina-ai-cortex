import json
import logging
import aiofiles
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.capability_registry import capability_registry
from app.core.config import settings

logger = logging.getLogger("LiveStackWatcher")


class LiveStackWatcher(BaseFileWatcher):
    LIVESTACK_GUID = "10bc1716-54af-425e-b307-c0ca1ce10600"

    def __init__(self):
        working_dir = capability_registry.get_plugin_path(
            self.LIVESTACK_GUID, "WorkingDirectory"
        )
        if not working_dir:
            logger.warning(
                "LiveStack WorkingDirectory not found in profile. Using fallback."
            )
            working_dir = Path(settings.nina_environment.sessions_root) / "Live"

        super().__init__(
            watch_path=working_dir,
            target_files=["stack_status.json", "history.csv", ".json", ".csv"],
        )

    async def process_file(self, path: Path) -> None:
        if path.suffix.lower() not in [".json", ".csv"]:
            return

        # Игнорируем сами FITS-файлы (калиброванные и стек)
        if "calibrated" in path.name.lower() or "stacked" in path.name.lower():
            return

        try:
            if path.suffix.lower() == ".json" and "status" in path.name.lower():
                await self._process_status(path)
            elif path.suffix.lower() == ".csv" and "history" in path.name.lower():
                await self._process_history(path)
        except Exception as e:
            logger.error(f"Error processing LiveStack file {path.name}: {e}")

    async def _process_status(self, path: Path):
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            content = await f.read()
        data = json.loads(content)
        await event_bus.publish("LIVESTACK_STATUS", data)
        logger.debug(f"LiveStack status updated: {data.get('state', 'unknown')}")

    async def _process_history(self, path: Path):
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            content = await f.read()
        lines = content.strip().split("\n")
        await event_bus.publish(
            "LIVESTACK_HISTORY",
            {"lines_count": len(lines), "last_line": lines[-1] if lines else None},
        )
