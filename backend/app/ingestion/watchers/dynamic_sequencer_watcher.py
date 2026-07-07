"""
Dynamic Sequencer Watcher
Мониторит JSON-проекты Dynamic Sequencer на предмет внешних изменений.
Устраняет Упрощение #24.
"""

import json
import logging
from pathlib import Path
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings

logger = logging.getLogger("DynamicSequencerWatcher")


class DynamicSequencerWatcher(BaseFileWatcher):
    """
    Мониторит папку проектов Dynamic Sequencer.
    При изменении JSON-проекта публикует событие DYNAMIC_SEQUENCER_UPDATE.
    """

    def __init__(self, registry: CapabilityRegistry):
        path_str = getattr(settings.watchers, "dynamic_sequencer_path", None)
        path = (
            Path(path_str)
            if path_str
            else Path.home() / "Documents" / "DynamicSequencer" / "Projects"
        )
        super().__init__(watch_path=path, target_files=[".json"], registry=registry)

    async def process_file(self, path: Path) -> None:
        """Обработка измененного JSON-проекта Dynamic Sequencer."""
        if path.suffix.lower() != ".json":
            return

        logger.info(f"Dynamic Sequencer project updated: {path.name}")

        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)

            # Извлекаем ключевую информацию
            project_name = data.get("Name", path.stem)
            targets = data.get("Targets", [])

            # Публикуем событие для AI-агентов
            await event_bus.publish(
                "DYNAMIC_SEQUENCER_UPDATE",
                {
                    "file": path.name,
                    "project_name": project_name,
                    "targets_count": len(targets),
                    "data": data,
                },
            )

            logger.debug(
                f"Dynamic Sequencer project '{project_name}' updated: {len(targets)} targets"
            )

        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in Dynamic Sequencer project {path.name}: {e}")
        except Exception as e:
            logger.error(f"Error parsing Dynamic Sequencer project {path.name}: {e}")
