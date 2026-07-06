import logging
from typing import Dict, Any, List, Optional
from pydantic import BaseModel

logger = logging.getLogger("StateTracker")


class SequenceState(BaseModel):
    is_running: bool = False
    current_item_id: Optional[str] = None
    current_item_name: Optional[str] = None
    current_item_type: Optional[str] = None
    container_path: List[str] = []  # Путь контейнеров для Safety Interceptor
    global_variables: Dict[str, Any] = {}
    is_message_box_active: bool = False
    is_approaching_shutdown: bool = False
    is_flat_mode: bool = False  # Раннее обнаружение


class StateTracker:
    def __init__(self):
        self.state = SequenceState()
        self._shadow_graph = None

    def set_shadow_graph(self, graph: Dict):
        self._shadow_graph = graph
        if graph and "global_variables" in graph:
            self.state.global_variables = graph["global_variables"]

    async def handle_sequence_item_started(self, data: Dict):
        item_name = data.get("Name", "")
        item_type = data.get("Type", "")

        self.state.current_item_name = item_name
        self.state.current_item_type = item_type
        self.state.is_running = True

        # Обновление пути контейнеров (упрощенно, на основе имени)
        if "Container" in item_type:
            self.state.container_path.append(item_name)

        # Раннее обнаружение FLAT_MODE (до появления FITS/JSON)
        if "Перемещение для съемки FLAT" in item_name or "FLAT" in item_name.upper():
            if not self.state.is_flat_mode:
                self.state.is_flat_mode = True
                logger.info("🟦 FLAT_MODE pre-activated via Shadow Engine")

        if "Shutdown" in item_type:
            self.state.is_approaching_shutdown = True

    async def handle_sequence_item_completed(self, data: Dict):
        item_name = data.get("Name", "")
        if (
            "Container" in data.get("Type", "")
            and self.state.container_path
            and self.state.container_path[-1] == item_name
        ):
            self.state.container_path.pop()

        if "Перемещение для съемки FLAT" in item_name:
            pass  # Сбросится при появлении LIGHT кадров или явном окончании

    def get_state(self) -> Dict:
        return self.state.model_dump()


state_tracker = StateTracker()
