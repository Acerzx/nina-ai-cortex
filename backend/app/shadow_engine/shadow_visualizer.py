"""
Shadow Engine Visualizer — генерация визуализаций теневого графа.
Реализация идеи 5: Mermaid экспорт для документации и отчётов.

Архитектура:
- Mermaid генерируется из shadow graph (StateTracker._node_map)
- Цветовая кодировка по типам узлов
- Опциональная детализация (топ-уровень vs полная)
- API endpoint: GET /api/v1/sequence/shadow/mermaid

Использование:
    from app.shadow_engine.shadow_visualizer import shadow_visualizer

    # Генерация Mermaid
    mermaid_code = shadow_visualizer.generate_mermaid(include_details=True)
"""

import logging
from typing import Dict, Any, Optional, Set
from datetime import datetime

from app.shadow_engine.state_tracker import state_tracker

logger = logging.getLogger("ShadowVisualizer")


class ShadowVisualizer:
    """
    Генератор визуализаций теневого графа.

    Features:
    - Mermaid синтаксис для markdown/github
    - Цветовая кодировка по типам узлов
    - Настраиваемая детализация
    - Статистика графа
    """

    # Цветовая схема Mermaid
    STYLES = {
        "Container": "fill:#e1f5ff,stroke:#01579b,stroke-width:2px",
        "SmartExposure": "fill:#fff9c4,stroke:#f57f17,stroke-width:2px",
        "Instruction": "fill:#e8f5e9,stroke:#2e7d32,stroke-width:1px",
        "Trigger": "fill:#ffcdd2,stroke:#c62828,stroke-width:1px",
        "Condition": "fill:#f3e5f5,stroke:#6a1b9a,stroke-width:1px",
        "MessageBox": "fill:#ffe0b2,stroke:#e65100,stroke-width:2px",
        "Annotation": "fill:#f5f5f5,stroke:#757575,stroke-width:1px",
        "GlobalVariable": "fill:#e0f2f1,stroke:#00695c,stroke-width:1px",
        "Shutdown": "fill:#d32f2f,stroke:#b71c1c,stroke-width:3px,color:#fff",
        "Default": "fill:#fafafa,stroke:#9e9e9e,stroke-width:1px",
    }

    # Ключевые слова для определения критических узлов
    CRITICAL_KEYWORDS = [
        "shutdown",
        "park",
        "meridian",
        "flip",
    ]

    def __init__(self):
        self._stats = {
            "total_diagrams_generated": 0,
        }

    def generate_mermaid(
        self,
        include_details: bool = True,
        max_depth: int = 10,
        show_triggers: bool = True,
        show_conditions: bool = True,
        highlight_active: bool = True,
    ) -> str:
        """
        Генерирует Mermaid диаграмму теневого графа.

        Args:
            include_details: Включать инструкции и триггеры
            max_depth: Максимальная глубина рекурсии
            show_triggers: Показывать триггеры
            show_conditions: Показывать условия
            highlight_active: Подсвечивать активный узел

        Returns:
            Mermaid код (без ``` обёртки)
        """
        if not state_tracker._node_map:
            return "graph TD\n    A[Shadow graph not loaded]"

        lines = ["graph TD"]
        visited: Set[str] = set()

        # Получаем текущий активный узел
        active_node_id = state_tracker.state.current_item_id

        # Рекурсивная генерация
        self._generate_node_mermaid(
            lines=lines,
            visited=visited,
            depth=0,
            max_depth=max_depth,
            include_details=include_details,
            show_triggers=show_triggers,
            show_conditions=show_conditions,
            active_node_id=active_node_id if highlight_active else None,
        )

        # Добавляем стили
        lines.append("")
        lines.append("    %% Styles")
        for node_type, style in self.STYLES.items():
            lines.append(f"    classDef {node_type.lower()} {style}")

        self._stats["total_diagrams_generated"] += 1

        return "\n".join(lines)

    def _generate_node_mermaid(
        self,
        lines: list,
        visited: Set[str],
        depth: int,
        max_depth: int,
        include_details: bool,
        show_triggers: bool,
        show_conditions: bool,
        active_node_id: Optional[str],
        parent_id: Optional[str] = None,
    ):
        """Рекурсивно генерирует Mermaid для всех узлов."""
        if depth > max_depth:
            return

        for node_id, node in state_tracker._node_map.items():
            if node_id in visited:
                continue

            # Проверяем, что это корневой узел или на нужной глубине
            if depth == 0 and node_id in state_tracker._parent_map:
                continue  # Не корневой

            if depth > 0:
                # Обрабатываем только детей текущего parent
                if state_tracker._parent_map.get(node_id) != parent_id:
                    continue

            visited.add(node_id)

            # Определяем тип узла
            node_type = self._classify_node(node)

            # Формируем label
            name = node.get("name", node_id)
            label = self._escape_mermaid(name)

            if include_details:
                node_type_short = node.get("type", "")
                label = f"{label}<br/><small>{node_type_short}</small>"

            # Добавляем узел
            safe_id = self._safe_id(node_id)

            # Подсветка активного узла
            if node_id == active_node_id:
                lines.append(f'    {safe_id}["🔵 {label}"]:::active')
            else:
                lines.append(f'    {safe_id}["{label}"]:::{node_type.lower()}')

            # Связь с родителем
            if parent_id:
                parent_safe_id = self._safe_id(parent_id)
                lines.append(f"    {parent_safe_id} --> {safe_id}")

            # Триггеры
            if show_triggers and "triggers" in node:
                for trigger in node["triggers"]:
                    trigger_id = trigger.get(
                        "id", f"trig_{node_id}_{trigger.get('name', 'x')}"
                    )
                    trigger_safe_id = self._safe_id(trigger_id)
                    trigger_name = self._escape_mermaid(trigger.get("name", "Trigger"))
                    lines.append(
                        f'    {trigger_safe_id}("⚡ {trigger_name}"):::trigger'
                    )
                    lines.append(f"    {safe_id} -. {trigger_safe_id}")

            # Условия
            if show_conditions and "conditions" in node:
                for cond in node["conditions"]:
                    cond_type = cond.get("type", "Condition")
                    cond_id = f"cond_{node_id}_{cond_type}"
                    cond_safe_id = self._safe_id(cond_id)
                    lines.append(f'    {cond_safe_id}{{"🔶 {cond_type}"}}:::condition')
                    lines.append(f"    {cond_safe_id} --> {safe_id}")

            # Рекурсия для детей
            if "Container" in node.get("type", "") or node_type == "SmartExposure":
                self._generate_node_mermaid(
                    lines=lines,
                    visited=visited,
                    depth=depth + 1,
                    max_depth=max_depth,
                    include_details=include_details,
                    show_triggers=show_triggers,
                    show_conditions=show_conditions,
                    active_node_id=active_node_id,
                    parent_id=node_id,
                )

    def _classify_node(self, node: Dict[str, Any]) -> str:
        """Определяет тип узла для стилизации."""
        node_type = node.get("type", "")
        name = node.get("name", "").lower()

        # Критические узлы
        if any(kw in name for kw in self.CRITICAL_KEYWORDS):
            return "Shutdown"

        # Типы
        if "Container" in node_type:
            return "Container"
        if "SmartExposure" in node_type:
            return "SmartExposure"
        if "MessageBox" in node_type:
            return "MessageBox"
        if "GlobalVariable" in node_type:
            return "GlobalVariable"
        if "Annotation" in node_type:
            return "Annotation"

        return "Instruction"

    def _safe_id(self, node_id: str) -> str:
        """Преобразует ID в безопасный для Mermaid формат."""
        # Mermaid не любит спецсимволы в ID
        return "n_" + "".join(c if c.isalnum() else "_" for c in str(node_id))

    def _escape_mermaid(self, text: str) -> str:
        """Экранирует специальные символы для Mermaid."""
        return (
            str(text)
            .replace('"', "'")
            .replace("\n", " ")
            .replace("\r", "")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )

    def generate_full_html_report(self) -> str:
        """
        Генерирует полный HTML-отчёт с интерактивной Mermaid диаграммой.
        Используется для экспорта через API.
        """
        mermaid_code = self.generate_mermaid()

        # Статистика графа
        stats = state_tracker.get_stats()

        html = f"""<!DOCTYPE html>
<html lang="ru">
<head>
    <meta charset="UTF-8">
    <title>N.I.N.A. Shadow Graph Visualization</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js"></script>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            margin: 20px;
            background: #fafafa;
            color: #333;
        }}
        h1 {{ color: #01579b; }}
        .stats {{
            background: #fff;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        .stats-item {{
            display: inline-block;
            margin-right: 30px;
            padding: 8px 15px;
            background: #e1f5ff;
            border-radius: 4px;
        }}
        .mermaid {{
            background: #fff;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .legend {{
            margin-top: 20px;
            padding: 15px;
            background: #fff;
            border-radius: 8px;
        }}
        .legend-item {{
            display: inline-block;
            margin-right: 15px;
            padding: 5px 10px;
            border-radius: 4px;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <h1>🌌 N.I.N.A. Shadow Graph Visualization</h1>
    <p>Generated: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
    
    <div class="stats">
        <div class="stats-item"><strong>Nodes:</strong> {stats.get("node_count", 0)}</div>
        <div class="stats-item"><strong>Container Depth:</strong> {stats.get("container_depth", 0)}</div>
        <div class="stats-item"><strong>Global Variables:</strong> {stats.get("global_vars_count", 0)}</div>
        <div class="stats-item"><strong>Current Item:</strong> {stats.get("current_item", "None")}</div>
        <div class="stats-item"><strong>Running:</strong> {"✅ Yes" if stats.get("is_running") else "❌ No"}</div>
        <div class="stats-item"><strong>FLAT Mode:</strong> {"🟦 Yes" if stats.get("is_flat_mode") else "⬜ No"}</div>
    </div>
    
    <div class="mermaid">
{mermaid_code}
    </div>
    
    <div class="legend">
        <h3>Legend</h3>
        <span class="legend-item" style="background: #e1f5ff;">📦 Container</span>
        <span class="legend-item" style="background: #fff9c4;">⏱ SmartExposure</span>
        <span class="legend-item" style="background: #e8f5e9;">⚙ Instruction</span>
        <span class="legend-item" style="background: #ffcdd2;">⚡ Trigger</span>
        <span class="legend-item" style="background: #f3e5f5;">🔶 Condition</span>
        <span class="legend-item" style="background: #ffe0b2;">📢 MessageBox</span>
        <span class="legend-item" style="background: #d32f2f; color: white;">🚨 Critical</span>
    </div>
    
    <script>
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'default',
            flowchart: {{
                useMaxWidth: true,
                htmlLabels: true,
                curve: 'basis'
            }}
        }});
    </script>
</body>
</html>"""
        return html

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику визуализатора."""
        return {
            **self._stats,
            "available_styles": list(self.STYLES.keys()),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
shadow_visualizer = ShadowVisualizer()
