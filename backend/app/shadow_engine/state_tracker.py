"""
Shadow Sequence State Tracker
Сопоставляет WebSocket события с теневым графом секвенсора.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

logger = logging.getLogger(__name__)


class SequenceStateTracker:
    """
    Отслеживает текущее состояние выполнения секвенсора,
    сопоставляя WebSocket события с теневым графом.
    """

    def __init__(self, shadow_graph: Dict[str, Any]):
        self.shadow_graph = shadow_graph
        self.current_item_id: Optional[str] = None
        self.current_item_name: Optional[str] = None
        self.current_container_path: List[str] = []
        self.sequence_started: bool = False
        self.last_event_time: Optional[datetime] = None

        # Индекс для быстрого поиска элементов по ID
        self.item_index: Dict[str, Dict[str, Any]] = {}
        self._build_index()

    def _build_index(self):
        """Строит индекс всех элементов графа для быстрого поиска."""
        self._index_node(self.shadow_graph.get("graph", {}))
        logger.info(f"📊 Built index with {len(self.item_index)} items")

    def _index_node(self, node: Any, path: List[str] = None):
        """Рекурсивно индексирует узел графа."""
        if path is None:
            path = []

        if isinstance(node, dict):
            item_id = node.get("id")
            if item_id:
                self.item_index[item_id] = {"node": node, "path": path.copy()}

            # Рекурсивно обходим children и instructions
            for child in node.get("children", []):
                self._index_node(child, path + [node.get("name", "Unknown")])

            for instruction in node.get("instructions", []):
                self._index_node(instruction, path + [node.get("name", "Unknown")])

        elif isinstance(node, list):
            for item in node:
                self._index_node(item, path)

    async def process_event(self, event: Dict[str, Any]):
        """Обрабатывает WebSocket событие и обновляет состояние."""
        event_type = event.get("type")
        self.last_event_time = datetime.now()

        if event_type == "SequenceStarted":
            self.sequence_started = True
            logger.info("▶️ Sequence started")

        elif event_type == "SequenceStopped":
            self.sequence_started = False
            self.current_item_id = None
            self.current_item_name = None
            self.current_container_path = []
            logger.info("⏹️ Sequence stopped")

        elif event_type == "SequenceItemStarted":
            item_id = event.get("itemId")
            if item_id and item_id in self.item_index:
                item_data = self.item_index[item_id]
                self.current_item_id = item_id
                self.current_item_name = item_data["node"].get(
                    "name", item_data["node"].get("type", "Unknown")
                )
                self.current_container_path = item_data["path"]

                logger.info(f"🎯 Executing: {self.current_item_name}")
                logger.debug(f"   Path: {' > '.join(self.current_container_path)}")

        elif event_type == "SequenceItemCompleted":
            item_id = event.get("itemId")
            if item_id == self.current_item_id:
                logger.info(f"✅ Completed: {self.current_item_name}")
                self.current_item_id = None
                self.current_item_name = None

        elif event_type == "MeridianFlipStarted":
            logger.info("🔄 Meridian Flip started")

        elif event_type == "MeridianFlipCompleted":
            logger.info("✅ Meridian Flip completed")

        elif event_type == "Error":
            error_msg = event.get("message", "Unknown error")
            logger.error(f"🚨 Sequence Error: {error_msg}")

    def get_current_state(self) -> Dict[str, Any]:
        """Возвращает текущее состояние секвенсора."""
        return {
            "sequence_running": self.sequence_started,
            "current_item_id": self.current_item_id,
            "current_item_name": self.current_item_name,
            "current_container_path": self.current_container_path,
            "last_event_time": self.last_event_time.isoformat()
            if self.last_event_time
            else None,
            "global_variables": self.shadow_graph.get("global_variables", {}),
        }
