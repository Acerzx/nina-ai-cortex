"""
N.I.N.A. Shadow Engine State Tracker
Отслеживает текущее состояние секвенсора на основе WebSocket событий и теневого графа.
Устраняет Упрощение #15.
"""

import logging
from typing import Dict, Any, Optional, List, Set
from pydantic import BaseModel, Field
from datetime import datetime

logger = logging.getLogger("StateTracker")


class SequenceState(BaseModel):
    """Полное состояние секвенсора"""

    is_running: bool = False
    current_item_id: Optional[str] = None
    current_item_name: Optional[str] = None
    current_item_type: Optional[str] = None
    container_path: List[str] = Field(default_factory=list)  # Путь контейнеров (имена)
    container_path_ids: List[str] = Field(default_factory=list)  # Путь контейнеров (ID)
    global_variables: Dict[str, Any] = Field(default_factory=dict)
    active_triggers: List[str] = Field(default_factory=list)
    is_message_box_active: bool = False
    message_box_text: Optional[str] = None
    is_approaching_shutdown: bool = False
    is_flat_mode: bool = False  # Раннее обнаружение FLAT_MODE
    sequence_start_time: Optional[str] = None
    last_update: Optional[str] = None


class StateTracker:
    """
    Отслеживает состояние секвенсора на основе WebSocket событий и Shadow Graph.
    Устраняет Упрощение #15: полное состояние секвенсора.

    Ключевое улучшение: использует теневой граф для корректного управления container_path,
    предотвращая утечку памяти и обеспечивая точное отслеживание вложенности.
    """

    def __init__(self):
        self.state = SequenceState()
        self._shadow_graph: Optional[Dict] = None
        self._node_map: Dict[str, Dict] = {}  # id -> node
        self._parent_map: Dict[str, str] = {}  # child_id -> parent_id
        self._container_children: Dict[
            str, List[str]
        ] = {}  # container_id -> [child_ids]

    def set_shadow_graph(self, graph: Dict):
        """
        Устанавливает теневой граф секвенсора и строит индексы для быстрого поиска.
        """
        self._shadow_graph = graph
        if not graph:
            logger.warning("Empty shadow graph provided")
            return

        # Извлекаем глобальные переменные
        if "global_variables" in graph:
            self.state.global_variables = graph["global_variables"]

        # Строим карту узлов и parent-child отношений
        self._node_map.clear()
        self._parent_map.clear()
        self._container_children.clear()

        if "graph" in graph:
            self._build_node_map(graph["graph"], parent_id=None)

        logger.info(
            f"Shadow graph loaded: {len(self._node_map)} nodes, "
            f"{len(self._parent_map)} parent-child relations"
        )

    def _build_node_map(self, node: Any, parent_id: Optional[str]):
        """Рекурсивно строит карту узлов и parent-child отношений."""
        if isinstance(node, list):
            for item in node:
                self._build_node_map(item, parent_id)
            return

        if not isinstance(node, dict):
            return

        node_id = node.get("id")
        node_type = node.get("type", "")

        if node_id:
            self._node_map[node_id] = node
            if parent_id:
                self._parent_map[node_id] = parent_id

        # Для контейнеров рекурсивно обрабатываем children, instructions, message_boxes
        if "Container" in node_type or node_type == "SmartExposure":
            children_ids = []

            # Children (вложенные контейнеры)
            for child in node.get("children", []):
                if isinstance(child, dict) and "id" in child:
                    children_ids.append(child["id"])
                    self._build_node_map(child, node_id)

            # Instructions
            for instr in node.get("instructions", []):
                if isinstance(instr, dict) and "id" in instr:
                    children_ids.append(instr["id"])
                    self._build_node_map(instr, node_id)

            # MessageBoxes
            for mb in node.get("message_boxes", []):
                if isinstance(mb, dict) and "id" in mb:
                    children_ids.append(mb["id"])
                    self._build_node_map(mb, node_id)

            self._container_children[node_id] = children_ids

    def _find_container_path_for_item(
        self, item_id: str
    ) -> tuple[List[str], List[str]]:
        """
        Находит путь контейнеров (имена и ID) для данного элемента.
        Возвращает (container_names, container_ids) от корня к листу.
        """
        container_ids = []
        container_names = []

        current_id = item_id
        visited = set()  # Защита от циклов

        while (
            current_id and current_id in self._parent_map and current_id not in visited
        ):
            visited.add(current_id)
            parent_id = self._parent_map[current_id]
            parent_node = self._node_map.get(parent_id)

            if parent_node:
                parent_type = parent_node.get("type", "")
                # Добавляем только контейнеры (не TriggerRunner и т.п.)
                if "Container" in parent_type or parent_type == "SmartExposure":
                    container_ids.insert(0, parent_id)
                    container_names.insert(0, parent_node.get("name", "Unnamed"))

            current_id = parent_id

        return container_names, container_ids

    def _is_sibling(self, item1_id: str, item2_id: str) -> bool:
        """Проверяет, являются ли два элемента siblings (имеют общего родителя)."""
        if item1_id not in self._parent_map or item2_id not in self._parent_map:
            return False
        return self._parent_map.get(item1_id) == self._parent_map.get(item2_id)

    async def handle_sequence_item_started(self, data: Dict):
        """
        Обработка события SequenceItemStarted.
        Ключевой метод: обновляет container_path на основе теневого графа.
        """
        item_id = data.get("Id") or data.get("id")
        item_name = data.get("Name", "")
        item_type = data.get("Type", "")

        self.state.current_item_id = item_id
        self.state.current_item_name = item_name
        self.state.current_item_type = item_type
        self.state.is_running = True
        self.state.last_update = datetime.now().isoformat()

        # Обновляем container_path на основе теневого графа
        if item_id and self._node_map:
            new_container_names, new_container_ids = self._find_container_path_for_item(
                item_id
            )

            # Находим общий префикс между старым и новым путем
            old_ids = self.state.container_path_ids
            common_prefix_len = 0
            for i, (old_id, new_id) in enumerate(zip(old_ids, new_container_ids)):
                if old_id == new_id:
                    common_prefix_len = i + 1
                else:
                    break

            # Логируем переход между контейнерами
            if common_prefix_len < len(old_ids):
                exited_containers = old_ids[common_prefix_len:]
                for cid in exited_containers:
                    cnode = self._node_map.get(cid, {})
                    logger.debug(f"⬅️ Exited container: {cnode.get('name', cid)}")

            if common_prefix_len < len(new_container_ids):
                entered_containers = new_container_ids[common_prefix_len:]
                for cid in entered_containers:
                    cnode = self._node_map.get(cid, {})
                    logger.debug(f"➡️ Entered container: {cnode.get('name', cid)}")

            self.state.container_path = new_container_names
            self.state.container_path_ids = new_container_ids

        # === Раннее обнаружение FLAT_MODE ===
        # Детектируем вход в "Перемещение для съемки FLAT" или аналогичные контейнеры
        flat_keywords = ["перемещение для съемки flat", "flat", "take trained flats"]
        item_name_lower = item_name.lower()

        if any(kw in item_name_lower for kw in flat_keywords):
            if not self.state.is_flat_mode:
                self.state.is_flat_mode = True
                logger.info(
                    f"🟦 FLAT_MODE pre-activated via Shadow Engine (item: {item_name})"
                )

        # Если это LIGHT-кадр и мы были в FLAT_MODE - сбрасываем
        if item_type and "TakeExposure" in item_type:
            # Проверяем ImageType в узле графа
            node = self._node_map.get(item_id, {})
            image_type = node.get("image_type", "")
            if image_type == "LIGHT" and self.state.is_flat_mode:
                self.state.is_flat_mode = False
                logger.info("🟩 FLAT_MODE deactivated (LIGHT exposure started)")

        # === Детекция MessageBox ===
        if "MessageBox" in item_type:
            self.state.is_message_box_active = True
            # Пытаемся найти текст из теневого графа
            node = self._node_map.get(item_id, {})
            self.state.message_box_text = node.get("text", data.get("Text", ""))
            logger.info(
                f"📢 MessageBox activated: {self.state.message_box_text[:50]}..."
            )

        # === Детекция приближения к Shutdown ===
        if any(kw in item_type for kw in ["ShutdownPcInstruction", "ShutdownNina"]):
            self.state.is_approaching_shutdown = True
            logger.warning("⚠️ Approaching Shutdown instruction!")

        logger.info(f"▶️ Sequence Item Started: {item_name} ({item_type})")

    async def handle_sequence_item_completed(self, data: Dict):
        """Обработка события SequenceItemCompleted."""
        item_id = data.get("Id") or data.get("id")
        item_name = data.get("Name", "")
        item_type = data.get("Type", "")

        # Сброс MessageBox
        if self.state.is_message_box_active and "MessageBox" in item_type:
            self.state.is_message_box_active = False
            self.state.message_box_text = None
            logger.debug("MessageBox deactivated")

        self.state.last_update = datetime.now().isoformat()
        logger.info(f"✅ Sequence Item Completed: {item_name}")

    async def handle_sequence_started(self, data: Dict):
        """Обработка события SequenceStarted."""
        self.state.is_running = True
        self.state.sequence_start_time = datetime.now().isoformat()
        self.state.last_update = datetime.now().isoformat()
        self.state.container_path = []
        self.state.container_path_ids = []
        self.state.is_flat_mode = False
        self.state.is_approaching_shutdown = False
        logger.info("🚀 Sequence Started")

    async def handle_sequence_stopped(self, data: Dict):
        """Обработка события SequenceStopped."""
        self.state.is_running = False
        self.state.current_item_id = None
        self.state.current_item_name = None
        self.state.current_item_type = None
        self.state.is_approaching_shutdown = False
        self.state.is_message_box_active = False
        self.state.last_update = datetime.now().isoformat()
        logger.info("🛑 Sequence Stopped")

    def get_current_container(self) -> Optional[str]:
        """Возвращает имя текущего (верхнего) контейнера."""
        return self.state.container_path[-1] if self.state.container_path else None

    def is_in_final_stage(self) -> bool:
        """
        Проверяет, находимся ли мы в финальной стадии секвенсора
        (EndAreaContainer или контейнеры с ключевыми словами "Отключение", "Деактивация").
        Используется Safety Interceptor.
        """
        final_keywords = ["endarea", "конец", "отключение", "деактивация", "shutdown"]
        for container_name in self.state.container_path:
            if any(kw in container_name.lower() for kw in final_keywords):
                return True
        return False

    def get_state(self) -> Dict:
        """Возвращает текущее состояние в виде словаря."""
        return self.state.model_dump()

    def get_node_info(self, node_id: str) -> Optional[Dict]:
        """Возвращает информацию об узле по ID из теневого графа."""
        return self._node_map.get(node_id)


# Singleton instance (для совместимости с существующим кодом)
# В будущем можно перевести на DI через WatcherManager
state_tracker = StateTracker()
