"""
AutoFocus Analysis Watcher
Мониторит отчеты автофокуса (JSON/CSV).
Устраняет Упрощение #11 (частично).
"""

import json, logging
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings

logger = logging.getLogger("AutoFocusWatcher")


class AutoFocusAnalysisWatcher(BaseFileWatcher):
    """Мониторит папку AutoFocus Analysis."""

    AUTOFOCUS_GUID = "97021132-0c25-4443-b947-fe5efbe0a3d6"

    def __init__(self, registry: CapabilityRegistry):
        af_path = registry.get_plugin_path(self.AUTOFOCUS_GUID, "DefaultFolder")
        if not af_path:
            af_path = settings.nina_environment.appdata_root / "AutoFocus"
        super().__init__(af_path, [".json", ".csv"], registry)

    async def process_file(self, path: Path):
        if path.suffix.lower() == ".json":
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                await event_bus.publish(
                    "AUTOFOCUS_REPORT", {"file": path.name, "data": data}
                )
            except Exception as e:
                logger.error(f"AF JSON error: {e}")
