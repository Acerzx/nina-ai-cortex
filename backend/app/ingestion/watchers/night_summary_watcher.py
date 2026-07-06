"""
Night Summary Watcher
Мониторит итоговые отчеты за ночь (NightSummary.json).
Устраняет Упрощение #7.
"""

import json, logging
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings

logger = logging.getLogger("NightSummaryWatcher")


class NightSummaryWatcher(BaseFileWatcher):
    """Мониторит NightSummary.json в папках сессий."""

    def __init__(self, registry: CapabilityRegistry):
        super().__init__(
            settings.nina_environment.sessions_root, ["NightSummary.json"], registry
        )

    async def process_file(self, path: Path):
        if path.name == "NightSummary.json":
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                await event_bus.publish(
                    "NIGHT_SUMMARY", {"session_id": path.parent.name, "data": data}
                )
            except Exception as e:
                logger.error(f"NightSummary error: {e}")
