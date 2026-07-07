"""
Copilot Agent — интерактивная помощь при ручных шагах (MessageBox, 2PA, OAG Focus).
Предоставляет пошаговые инструкции и визуализацию.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.rag_engine import rag_engine

logger = logging.getLogger("CopilotAgent")


class InteractiveGuide(BaseModel):
    """Пошаговая инструкция."""

    step_id: str
    title: str
    description: str
    instructions: List[str]
    visual_aids: List[str] = Field(default_factory=list)
    action_buttons: List[Dict[str, str]] = Field(default_factory=list)
    timeout_seconds: Optional[int] = None


class CopilotAgent(BaseAgent):
    """
    Агент интерактивной помощи.

    Responsibilities:
    - Генерация пошаговых инструкций для ручных шагов
    - Помощь при MessageBox (выбор фильтра, подтверждение)
    - Инструкции для Two Point Polar Alignment
    - Помощь при OAG Focus Assist
    - Интеграция с RAG для контекста из документации

    Trigger Events:
    - MessageBox shown
    - TwoPointPolarAlignment instruction
    - OagManualFocusInstruction
    - FilterSelectorInstruction
    """

    def __init__(self):
        super().__init__(name="Copilot", role="Interactive Assistant")

        # Активные инструкции
        self._active_guides: Dict[str, InteractiveGuide] = {}

    async def initialize(self):
        """Инициализация агента помощи."""
        await super().initialize()

        # Подписываемся на события
        event_bus.subscribe("SEQUENCE_ITEM_STARTED", self._on_sequence_item_started)
        event_bus.subscribe("LOG_EVENT", self._on_log_event)

        logger.info("✅ Copilot Agent initialized")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("SEQUENCE_ITEM_STARTED", self._on_sequence_item_started)
        event_bus.unsubscribe("LOG_EVENT", self._on_log_event)

        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует текущий шаг и генерирует инструкцию.
        """
        sequence_state = observatory_state.current_metrics.get("sequence", {})

        # Проверяем, есть ли активный MessageBox
        if observatory_state.current_metrics.get("is_message_box_active"):
            guide = await self._generate_messagebox_guide()
            if guide:
                decision = AgentDecision(
                    agent=self.name,
                    decision_type="INTERACTIVE_GUIDE_GENERATED",
                    inputs={"step": "MessageBox"},
                    outputs={"guide": guide.model_dump()},
                    rationale="Сгенерирована инструкция для MessageBox",
                    confidence=0.95,
                )
                self.log_decision(decision)
                return decision

        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет принятое решение (публикует инструкцию для UI)."""
        if decision.decision_type == "INTERACTIVE_GUIDE_GENERATED":
            guide_data = decision.outputs.get("guide", {})

            # Публикуем инструкцию для Frontend
            await event_bus.publish(
                "COPILOT_GUIDE_READY",
                {"guide": guide_data, "timestamp": datetime.now().isoformat()},
            )

            return True

        return False

    async def _on_sequence_item_started(self, data: Dict[str, Any]) -> None:
        """Обработка начала нового шага секвенсора."""
        item_type = data.get("Type", "")
        item_name = data.get("Name", "")

        # Two Point Polar Alignment
        if "TwoPointPolarAlignment" in item_type:
            await self._generate_2pa_guide()

        # OAG Focus Assist
        elif "OagManualFocus" in item_type:
            await self._generate_oag_focus_guide()

        # Filter Selector
        elif "FilterSelector" in item_type:
            await self._generate_filter_selector_guide()

    async def _on_log_event(self, data: Dict[str, Any]) -> None:
        """Обработка событий из логов."""
        event_type = data.get("event_type", "")

        if event_type == "messagebox_shown":
            await self._generate_messagebox_guide()

    async def _generate_messagebox_guide(self) -> Optional[InteractiveGuide]:
        """Генерирует инструкцию для MessageBox."""
        # Получаем текст MessageBox из Shadow Engine
        from app.shadow_engine.state_tracker import state_tracker

        message_text = state_tracker.state.message_box_text

        if not message_text:
            return None

        # Ищем контекст в RAG
        rag_context = await self.get_rag_context(
            query=f"MessageBox: {message_text}", max_tokens=1000
        )

        # Определяем тип MessageBox и генерируем кнопки
        action_buttons = []

        if "filter" in message_text.lower():
            action_buttons = [
                {"label": "Ha", "action": "select_filter", "value": "Ha"},
                {"label": "OIII", "action": "select_filter", "value": "OIII"},
                {"label": "SII", "action": "select_filter", "value": "SII"},
                {"label": "Пропустить", "action": "skip", "value": ""},
            ]
        elif "confirm" in message_text.lower() or "yes/no" in message_text.lower():
            action_buttons = [
                {"label": "Да", "action": "confirm", "value": "yes"},
                {"label": "Нет", "action": "confirm", "value": "no"},
            ]
        else:
            action_buttons = [{"label": "OK", "action": "acknowledge", "value": "ok"}]

        guide = InteractiveGuide(
            step_id=f"msgbox_{datetime.now().timestamp()}",
            title="Требуется действие",
            description=message_text,
            instructions=[
                "Прочитайте сообщение выше",
                "Выберите appropriate действие ниже",
                "Нажмите кнопку для продолжения",
            ],
            visual_aids=[],
            action_buttons=action_buttons,
            timeout_seconds=300,  # 5 минут
        )

        self._active_guides[guide.step_id] = guide

        # Публикуем инструкцию
        await event_bus.publish(
            "COPILOT_GUIDE_READY",
            {"guide": guide.model_dump(), "timestamp": datetime.now().isoformat()},
        )

        return guide

    async def _generate_2pa_guide(self) -> InteractiveGuide:
        """Генерирует инструкцию для Two Point Polar Alignment."""
        guide = InteractiveGuide(
            step_id="2pa_alignment",
            title="Полярное выравнивание по 2 точкам",
            description="Эта процедура требует ручного поворота монтировки на 90° по RA",
            instructions=[
                "Шаг 1: Убедитесь, что монтировка находится в домашней позиции",
                "Шаг 2: Когда будет предложено, поверните монтировку на 90° по оси RA",
                "Шаг 3: Дождитесь завершения экспозиции (10-20 секунд)",
                "Шаг 4: Система автоматически рассчитает ошибку выравнивания",
                "Шаг 5: Следуйте инструкциям для корректировки",
            ],
            visual_aids=["diagram_2pa_step1.png", "diagram_2pa_step2.png"],
            action_buttons=[
                {"label": "Готов к шагу 1", "action": "ready", "value": "step1"},
                {"label": "Пропустить", "action": "skip", "value": ""},
            ],
            timeout_seconds=600,  # 10 минут
        )

        self._active_guides[guide.step_id] = guide

        await event_bus.publish(
            "COPILOT_GUIDE_READY",
            {"guide": guide.model_dump(), "timestamp": datetime.now().isoformat()},
        )

        return guide

    async def _generate_oag_focus_guide(self) -> InteractiveGuide:
        """Генерирует инструкцию для OAG Focus Assist."""
        guide = InteractiveGuide(
            step_id="oag_focus",
            title="Фокусировка OAG",
            description="Ручная фокусировка внеосевого гида",
            instructions=[
                "Шаг 1: Откройте крышку телескопа",
                "Шаг 2: Запустите серию коротких экспозиций (1-2 секунды)",
                "Шаг 3: Наблюдайте за FWHM на графике",
                "Шаг 4: Вращайте фокусер до минимума FWHM",
                "Шаг 5: Зафиксируйте позицию фокусера",
            ],
            visual_aids=["fwhm_vs_position_graph.png"],
            action_buttons=[
                {"label": "Начать фокусировку", "action": "start", "value": ""},
                {"label": "Завершить", "action": "finish", "value": ""},
            ],
            timeout_seconds=900,  # 15 минут
        )

        self._active_guides[guide.step_id] = guide

        await event_bus.publish(
            "COPILOT_GUIDE_READY",
            {"guide": guide.model_dump(), "timestamp": datetime.now().isoformat()},
        )

        return guide

    async def _generate_filter_selector_guide(self) -> InteractiveGuide:
        """Генерирует инструкцию для выбора фильтра."""
        guide = InteractiveGuide(
            step_id="filter_selector",
            title="Выбор фильтра",
            description="Выберите фильтр для следующей серии экспозиций",
            instructions=[
                "Рекомендация: Ha для эмиссионных туманностей",
                "Рекомендация: OIII для планетарных туманностей",
                "Рекомендация: L для широкой полосы (галактики)",
            ],
            visual_aids=[],
            action_buttons=[
                {"label": "Ha", "action": "select", "value": "Ha"},
                {"label": "OIII", "action": "select", "value": "OIII"},
                {"label": "SII", "action": "select", "value": "SII"},
                {"label": "L", "action": "select", "value": "L"},
                {"label": "Пропустить", "action": "skip", "value": ""},
            ],
            timeout_seconds=120,  # 2 минуты
        )

        self._active_guides[guide.step_id] = guide

        await event_bus.publish(
            "COPILOT_GUIDE_READY",
            {"guide": guide.model_dump(), "timestamp": datetime.now().isoformat()},
        )

        return guide

    async def generate_guide(
        self, step: str, params: Dict[str, Any]
    ) -> Optional[InteractiveGuide]:
        """Генерирует инструкцию (вызывается Orchestrator'ом)."""
        if step == "messagebox":
            return await self._generate_messagebox_guide()
        elif step == "2pa":
            return await self._generate_2pa_guide()
        elif step == "oag_focus":
            return await self._generate_oag_focus_guide()
        elif step == "filter_selector":
            return await self._generate_filter_selector_guide()

        return None
