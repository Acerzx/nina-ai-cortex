"""
Shadow Sequence State Tracker
Сопоставляет WebSocket события ninaAPI v2 с теневым графом секвенсора.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class SequenceStateTracker:
    def __init__(self, shadow_graph: Dict[str, Any]):
        self.shadow_graph = shadow_graph
        self.current_item_id: Optional[str] = None
        self.current_item_name: Optional[str] = None
        self.sequence_running: bool = False
        self.last_event_time: Optional[datetime] = None
        self.latest_image_stats: Optional[Dict[str, Any]] = None
        self.safety_status: bool = True  # По умолчанию безопасно

    async def process_event(self, event: Dict[str, Any]):
        """Обрабатывает событие от ninaAPI v2."""
        event_type = event.get("Event")
        self.last_event_time = datetime.now()

        if event_type == "SEQUENCE-STARTING":
            self.sequence_running = True
            logger.info("▶️ Sequence started")

        elif event_type == "SEQUENCE-FINISHED":
            self.sequence_running = False
            logger.info("⏹️ Sequence finished")

        elif event_type == "IMAGE-SAVE":
            # Сохраняем статистику последнего кадра
            self.latest_image_stats = event.get("ImageStatistics", {})
            target = self.latest_image_stats.get("TargetName", "Unknown")
            hfr = self.latest_image_stats.get("HFR", "N/A")
            logger.debug(f"📊 Frame stats updated for {target} (HFR: {hfr})")

        elif event_type == "SAFETY-CHANGED":
            self.safety_status = event.get("IsSafe", True)
            status = "SAFE" if self.safety_status else "UNSAFE"
            logger.warning(f"🛡️ Safety status: {status}")

        elif event_type == "SEQUENCE-ENTITY-FAILED":
            entity = event.get("Entity", "Unknown")
            error = event.get("Error", "Unknown error")
            logger.error(f"🚨 Sequence entity failed: {entity} - {error}")

        elif event_type == "MOUNT-BEFORE-FLIP":
            logger.info("🔄 Meridian Flip starting...")

        elif event_type == "MOUNT-AFTER-FLIP":
            logger.info("✅ Meridian flip completed")

        elif event_type == "TS-NEWTARGETSTART":
            # Target Scheduler выбрал новую цель
            target_name = event.get("TargetName", "Unknown")
            project_name = event.get("ProjectName", "Unknown")
            logger.info(
                f"🎯 New target selected by scheduler: {target_name} (Project: {project_name})"
            )

    def get_current_state(self) -> Dict[str, Any]:
        """Возвращает текущее состояние системы."""
        return {
            "sequence_running": self.sequence_running,
            "safety_status": self.safety_status,
            "latest_image": self.latest_image_stats,
            "last_event_time": self.last_event_time.isoformat()
            if self.last_event_time
            else None,
            "global_variables": self.shadow_graph.get("global_variables", {}),
        }
