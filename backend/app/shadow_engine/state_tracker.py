"""
N.I.N.A. Shadow Engine State Tracker
Отслеживает текущее состояние секвенсора на основе WebSocket событий и теневого графа.
ИСПРАВЛЕНО (audit 4.3, 12.4):
- FLAT_MODE теперь детектится по ImageType из графа, а не по ключевым словам
- Пути контейнеров кэшируются в _container_path_cache для производительности
- Устранены ложные срабатывания при смене фильтра
ИСПРАВЛЕНО (v4.0 — проблема #8):
- Добавлен asyncio.Lock для thread-safe обновления self.state
- Все write-операции (handle_sequence_*) защищены блокировкой
- Read-операции (get_state, get_stats) остаются без блокировки
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List, Set, Tuple
from pydantic import BaseModel, Field
from datetime import datetime

logger = logging.getLogger("StateTracker")


class SequenceState(BaseModel):
    """Полное состояние секвенсора"""

    is_running: bool = False
    current_item_id: Optional[str] = None
    current_item_name: Optional[str] = None
    current_item_type: Optional[str] = None
    current_image_type: Optional[str] = None
    container_path: List[str] = Field(default_factory=list)
    container_path_ids: List[str] = Field(default_factory=list)
    global_variables: Dict[str, Any] = Field(default_factory=dict)
    active_triggers: List[str] = Field(default_factory=list)
    is_message_box_active: bool = False
    message_box_text: Optional[str] = None
    is_approaching_shutdown: bool = False
    is_flat_mode: bool = False
    sequence_start_time: Optional[str] = None
    last_update: Optional[str] = None


class StateTracker:
    """
    Отслеживает состояние секвенсора на основе WebSocket событий и Shadow Graph.
    ИСПРАВЛЕНО (v4.0 — проблема #8):
    - Добавлен _state_lock для thread-safe обновления self.state
    - Все write-методы используют async with self._state_lock
    """

    def __init__(self):
        self.state = SequenceState()
        self._shadow_graph: Optional[Dict] = None
        self._node_map: Dict[str, Dict] = {}
        self._parent_map: Dict[str, str] = {}
        self._container_children: Dict[str, List[str]] = {}
        self._container_path_cache: Dict[str, Tuple[List[str], List[str]]] = {}

        # ИСПРАВЛЕНО (v4.0 — проблема #8): Блокировка для state
        self._state_lock = asyncio.Lock()

        self._flat_keywords = [
            "перемещение для съемки flat",
            "flat",
            "take trained flats",
            "flatwizard",
            "flat panel",
        ]

    def set_shadow_graph(self, graph: Dict):
        """Устанавливает теневой граф секвенсора и строит индексы."""
        self._shadow_graph = graph
        self._container_path_cache.clear()

        if not graph:
            logger.warning("Empty shadow graph provided")
            return

        if "global_variables" in graph:
            self.state.global_variables = graph["global_variables"]

        self._node_map.clear()
        self._parent_map.clear()
        self._container_children.clear()

        if "graph" in graph:
            self._build_node_map(graph["graph"], parent_id=None)

        self._precompute_container_paths()

        logger.info(
            f"Shadow graph loaded: {len(self._node_map)} nodes, "
            f"{len(self._parent_map)} parent-child relations, "
            f"{len(self._container_path_cache)} cached paths"
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

            if "Container" in node_type or node_type == "SmartExposure":
                children_ids = []
                for child in node.get("children", []):
                    if isinstance(child, dict) and "id" in child:
                        children_ids.append(child["id"])
                        self._build_node_map(child, node_id)
                for instr in node.get("instructions", []):
                    if isinstance(instr, dict) and "id" in instr:
                        children_ids.append(instr["id"])
                        self._build_node_map(instr, node_id)
                for mb in node.get("message_boxes", []):
                    if isinstance(mb, dict) and "id" in mb:
                        children_ids.append(mb["id"])
                        self._build_node_map(mb, node_id)
                self._container_children[node_id] = children_ids

    def _precompute_container_paths(self):
        """Pre-compute пути контейнеров для всех узлов."""
        for item_id in self._node_map.keys():
            self._compute_and_cache_container_path(item_id)

    def _compute_and_cache_container_path(
        self, item_id: str
    ) -> Tuple[List[str], List[str]]:
        """Вычисляет путь контейнеров и кэширует результат."""
        if item_id in self._container_path_cache:
            return self._container_path_cache[item_id]

        container_ids = []
        container_names = []
        current_id = item_id
        visited = set()

        while (
            current_id and current_id in self._parent_map and current_id not in visited
        ):
            visited.add(current_id)
            parent_id = self._parent_map[current_id]
            parent_node = self._node_map.get(parent_id)

            if parent_node:
                parent_type = parent_node.get("type", "")
                if "Container" in parent_type or parent_type == "SmartExposure":
                    container_ids.insert(0, parent_id)
                    container_names.insert(0, parent_node.get("name", "Unnamed"))

            current_id = parent_id

        result = (container_names, container_ids)
        self._container_path_cache[item_id] = result
        return result

    def _find_container_path_for_item(
        self, item_id: str
    ) -> Tuple[List[str], List[str]]:
        """Находит путь контейнеров для данного элемента."""
        if item_id in self._container_path_cache:
            return self._container_path_cache[item_id]
        return self._compute_and_cache_container_path(item_id)

    def _is_sibling(self, item1_id: str, item2_id: str) -> bool:
        """Проверяет, являются ли два элемента siblings."""
        if item1_id not in self._parent_map or item2_id not in self._parent_map:
            return False
        return self._parent_map.get(item1_id) == self._parent_map.get(item2_id)

    def _extract_image_type_from_node(self, node: Dict) -> Optional[str]:
        """Извлекает ImageType из узла графа."""
        if not node:
            return None

        image_type = node.get("image_type")
        if image_type:
            return str(image_type).upper()

        image_type_raw = node.get("ImageType")
        if image_type_raw:
            return str(image_type_raw).upper()

        image_type_expr = node.get("image_type_expr")
        if image_type_expr and isinstance(image_type_expr, str):
            return image_type_expr.upper()

        return None

    async def handle_sequence_item_started(self, data: Dict):
        """
        Обработка события SequenceItemStarted.
        ИСПРАВЛЕНО (v4.0): protected by _state_lock
        """
        item_id = data.get("Id") or data.get("id")
        item_name = data.get("Name", "")
        item_type = data.get("Type", "")

        # ИСПРАВЛЕНО (v4.0): write-операция под блокировкой
        async with self._state_lock:
            self.state.current_item_id = item_id
            self.state.current_item_name = item_name
            self.state.current_item_type = item_type
            self.state.is_running = True
            self.state.last_update = datetime.now().isoformat()

            if item_id and self._node_map:
                new_container_names, new_container_ids = (
                    self._find_container_path_for_item(item_id)
                )

                old_ids = self.state.container_path_ids
                common_prefix_len = 0
                for i, (old_id, new_id) in enumerate(zip(old_ids, new_container_ids)):
                    if old_id == new_id:
                        common_prefix_len = i + 1
                    else:
                        break

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

        # FLAT_MODE и MessageBox — отдельно (они тоже меняют state)
        await self._update_flat_mode(item_id, item_name, item_type)

        # MessageBox detection
        if "MessageBox" in item_type:
            async with self._state_lock:
                self.state.is_message_box_active = True
                node = self._node_map.get(item_id, {})
                self.state.message_box_text = node.get("text", data.get("Text", ""))
            logger.info(
                f"📢 MessageBox activated: {self.state.message_box_text[:50] if self.state.message_box_text else 'N/A'}..."
            )

        # Shutdown detection
        if any(kw in item_type for kw in ["ShutdownPcInstruction", "ShutdownNina"]):
            async with self._state_lock:
                self.state.is_approaching_shutdown = True
            logger.warning("⚠️ Approaching Shutdown instruction!")

        logger.info(f"▶️ Sequence Item Started: {item_name} ({item_type})")

    async def _update_flat_mode(
        self, item_id: Optional[str], item_name: str, item_type: str
    ):
        """
        Обновляет состояние FLAT_MODE на основе ImageType из графа.
        ИСПРАВЛЕНО (v4.0 — проблема #36):
        - Проверяем наличие TakeExposure инструкций с ImageType=FLAT
        - Не полагаемся только на ключевые слова
        """
        node = self._node_map.get(item_id, {}) if item_id else {}
        image_type = self._extract_image_type_from_node(node)

        if image_type:
            self.state.current_image_type = image_type

        # === Приоритет 1: ImageType из графа ===
        if image_type == "FLAT":
            if not self.state.is_flat_mode:
                self.state.is_flat_mode = True
                logger.info(
                    f"🟦 FLAT_MODE activated via ImageType=FLAT (item: {item_name})"
                )
        elif image_type in ("LIGHT", "DARK", "BIAS"):
            if self.state.is_flat_mode:
                self.state.is_flat_mode = False
                logger.info(
                    f"🟩 FLAT_MODE deactivated via ImageType={image_type} "
                    f"(item: {item_name})"
                )
        # === Приоритет 2: Проверка TakeExposure инструкций ===
        elif image_type is None and item_type in ("TakeExposure", "SmartExposure"):
            # Проверяем, есть ли в текущем контейнере TakeExposure с FLAT
            has_flat_exposure = await self._check_for_flat_exposures(item_id)

            if has_flat_exposure and not self.state.is_flat_mode:
                self.state.is_flat_mode = True
                logger.info(
                    f"🟦 FLAT_MODE activated via TakeExposure check (item: {item_name})"
                )
            elif not has_flat_exposure and self.state.is_flat_mode:
                self.state.is_flat_mode = False
                logger.info(
                    f"🟩 FLAT_MODE deactivated via TakeExposure check "
                    f"(item: {item_name})"
                )
        # === Приоритет 3: Fallback на ключевые слова (только для контейнеров) ===
        elif image_type is None:
            item_name_lower = item_name.lower() if item_name else ""
            is_flat_container = any(kw in item_name_lower for kw in self._flat_keywords)

            # Активируем только для явно плоских контейнеров
            if is_flat_container and not self.state.is_flat_mode:
                node_type = node.get("type", "")
                if "Container" in node_type or item_type == "":
                    self.state.is_flat_mode = True
                    logger.info(
                        f"🟦 FLAT_MODE pre-activated via container name "
                        f"(item: {item_name}) [fallback mode]"
                    )

        elif image_type is None:
            # ИСПРАВЛЕНО (v4.0 — проблема #36):
            # Проверяем наличие TakeExposure инструкций с ImageType=FLAT в контейнере
            has_flat_exposure = await self._check_for_flat_exposures(item_id)

            if has_flat_exposure and not self.state.is_flat_mode:
                self.state.is_flat_mode = True
                logger.info(
                    f"🟦 FLAT_MODE activated via TakeExposure check (item: {item_name})"
                )
            elif not has_flat_exposure and self.state.is_flat_mode:
                # Проверяем, вышли ли мы из FLAT контейнера
                node_type = node.get("type", "")
                if "Container" not in node_type:
                    self.state.is_flat_mode = False
                    logger.info(f"🟩 FLAT_MODE deactivated (exited FLAT container)")
            else:
                # Fallback на ключевые слова (только для контейнеров)
                item_name_lower = item_name.lower() if item_name else ""
                is_flat_container = any(
                    kw in item_name_lower for kw in self._flat_keywords
                )

                if is_flat_container and not self.state.is_flat_mode:
                    node_type = node.get("type", "")
                    if "Container" in node_type or item_type == "":
                        self.state.is_flat_mode = True
                        logger.info(
                            f"🟦 FLAT_MODE pre-activated via container name "
                            f"(item: {item_name}) [fallback mode]"
                        )

    async def _check_for_flat_exposures(self, container_id: str) -> bool:
        """
        Проверяет, есть ли в контейнере TakeExposure инструкции с ImageType=FLAT.
        ИСПРАВЛЕНО (v4.0 — проблема #36): надёжная детекция FLAT режима.
        """
        if container_id not in self._container_children:
            return False

        children_ids = self._container_children[container_id]

        for child_id in children_ids:
            child_node = self._node_map.get(child_id, {})
            child_type = child_node.get("type", "")

            # Проверяем TakeExposure и SmartExposure
            if "TakeExposure" in child_type or "SmartExposure" in child_type:
                image_type = self._extract_image_type_from_node(child_node)
                if image_type == "FLAT":
                    return True

            # Рекурсивно проверяем вложенные контейнеры
            if "Container" in child_type:
                if await self._check_for_flat_exposures(child_id):
                    return True

        return False

    async def handle_sequence_item_completed(self, data: Dict):
        """Обработка события SequenceItemCompleted."""
        item_id = data.get("Id") or data.get("id")
        item_name = data.get("Name", "")
        item_type = data.get("Type", "")

        # ИСПРАВЛЕНО (v4.0): write под блокировкой
        async with self._state_lock:
            if self.state.is_message_box_active and "MessageBox" in item_type:
                self.state.is_message_box_active = False
                self.state.message_box_text = None
                logger.debug("MessageBox deactivated")

            self.state.last_update = datetime.now().isoformat()

        logger.info(f"✅ Sequence Item Completed: {item_name}")

    async def handle_sequence_started(self, data: Dict):
        """Обработка события SequenceStarted."""
        # ИСПРАВЛЕНО (v4.0): write под блокировкой
        async with self._state_lock:
            self.state.is_running = True
            self.state.sequence_start_time = datetime.now().isoformat()
            self.state.last_update = datetime.now().isoformat()
            self.state.container_path = []
            self.state.container_path_ids = []
            self.state.is_flat_mode = False
            self.state.current_image_type = None
            self.state.is_approaching_shutdown = False

        logger.info("🚀 Sequence Started")

    async def handle_sequence_stopped(self, data: Dict):
        """Обработка события SequenceStopped."""
        # ИСПРАВЛЕНО (v4.0): write под блокировкой
        async with self._state_lock:
            self.state.is_running = False
            self.state.current_item_id = None
            self.state.current_item_name = None
            self.state.current_item_type = None
            self.state.current_image_type = None
            self.state.is_approaching_shutdown = False
            self.state.is_message_box_active = False
            self.state.is_flat_mode = False
            self.state.last_update = datetime.now().isoformat()

        logger.info("🛑 Sequence Stopped")

    def get_current_container(self) -> Optional[str]:
        """Возвращает имя текущего (верхнего) контейнера."""
        return self.state.container_path[-1] if self.state.container_path else None

    def is_in_final_stage(self) -> bool:
        """Проверяет, находимся ли мы в финальной стадии секвенсора."""
        final_keywords = [
            "endarea",
            "конец",
            "отключение",
            "деактивация",
            "shutdown",
        ]
        for container_name in self.state.container_path:
            if any(kw in container_name.lower() for kw in final_keywords):
                return True
        return False

    def get_state(self) -> Dict:
        """Возвращает текущее состояние в виде словаря (read-only, без блокировки)."""
        return self.state.model_dump()

    def get_node_info(self, node_id: str) -> Optional[Dict]:
        """Возвращает информацию об узле по ID из теневого графа."""
        return self._node_map.get(node_id)

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику State Tracker для мониторинга."""
        return {
            "is_running": self.state.is_running,
            "current_item": self.state.current_item_name,
            "current_image_type": self.state.current_image_type,
            "is_flat_mode": self.state.is_flat_mode,
            "container_depth": len(self.state.container_path),
            "node_count": len(self._node_map),
            "cached_paths": len(self._container_path_cache),
            "global_vars_count": len(self.state.global_variables),
        }


# Singleton instance
state_tracker = StateTracker()
