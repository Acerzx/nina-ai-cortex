"""
Scheduler Agent — планирует сессии на основе погоды, видимости целей, приоритетов.
Оптимизирует порядок целей для максимального использования ясного времени.
"""

import logging
import math
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.execution.dynamic_editor import dynamic_editor

logger = logging.getLogger("SchedulerAgent")


class Target(BaseModel):
    """Цель для съемки."""

    name: str
    ra_hours: float
    ra_minutes: float
    ra_seconds: float
    dec_degrees: float
    dec_minutes: float
    dec_seconds: float
    priority: int = Field(ge=1, le=10)
    filter: str
    exposure_time: float
    frames_needed: int
    frames_completed: int = 0


class NightPlan(BaseModel):
    """План на ночь."""

    date: str
    targets: List[Target]
    weather_forecast: Dict[str, Any]
    moon_phase: float
    estimated_duration_hours: float
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    status: str = "PLANNED"  # PLANNED, EXECUTING, COMPLETED, CANCELLED


class SchedulerAgent(BaseAgent):
    """
    Агент планирования сессий.

    Responsibilities:
    - Построение плана на ночь на основе погоды и видимости целей
    - Оптимизация порядка целей (приоритет + видимость + погода)
    - Оценка времени завершения
    - Адаптация плана при изменении условий

    Логика приоритизации:
    1. Видимость цели (altitude > 30°)
    2. Приоритет цели (1-10)
    3. Погодные условия (облачность, ветер)
    4. Расстояние до Луны (избегаем при яркой Луне)
    """

    def __init__(self):
        super().__init__(name="Scheduler", role="Session Planning")

        # Текущий план
        self._current_plan: Optional[NightPlan] = None

        # Ограничения
        self.min_altitude = 30.0  # Минимальная высота для съемки
        self.moon_avoidance_angle = 30.0  # Минимальное расстояние до Луны

    async def initialize(self):
        """Инициализация агента планирования."""
        await super().initialize()

        # Подписываемся на события
        event_bus.subscribe("SEQUENCE_STARTED", self._on_sequence_started)
        event_bus.subscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)
        event_bus.subscribe("WEATHER_UPDATE", self._on_weather_update)
        event_bus.subscribe(
            "DYNAMIC_SEQUENCER_UPDATE", self._on_dynamic_sequencer_update
        )

        logger.info("✅ Scheduler Agent initialized")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("SEQUENCE_STARTED", self._on_sequence_started)
        event_bus.unsubscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)
        event_bus.unsubscribe("WEATHER_UPDATE", self._on_weather_update)
        event_bus.unsubscribe(
            "DYNAMIC_SEQUENCER_UPDATE", self._on_dynamic_sequencer_update
        )

        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Строит или обновляет план на ночь.
        """
        # Получаем доступные цели
        available_targets = await self._get_available_targets()

        if not available_targets:
            logger.warning("No targets available for planning")
            return None

        # Строим план
        plan = await self._build_night_plan(available_targets)

        if plan:
            self._current_plan = plan

            decision = AgentDecision(
                agent=self.name,
                decision_type="NIGHT_PLAN_CREATED",
                inputs={"targets_count": len(available_targets)},
                outputs={"plan": plan.model_dump()},
                rationale=f"План на ночь создан: {len(plan.targets)} целей",
                confidence=0.95,
            )
            self.log_decision(decision)
            return decision

        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет план (обновляет Dynamic Sequencer)."""
        if decision.decision_type == "NIGHT_PLAN_CREATED":
            plan_data = decision.outputs.get("plan", {})

            # Обновляем Dynamic Sequencer с планом
            success = await self._apply_plan_to_dynamic_sequencer(plan_data)

            if success:
                logger.info("✅ Night plan applied to Dynamic Sequencer")

            return success

        return False

    async def _on_sequence_started(self, data: Dict[str, Any]) -> None:
        """Обработка начала секвенсора."""
        logger.info("📅 Sequence started, monitoring plan execution...")

        if self._current_plan:
            self._current_plan.status = "EXECUTING"

    async def _on_sequence_stopped(self, data: Dict[str, Any]) -> None:
        """Обработка остановки секвенсора."""
        if self._current_plan:
            self._current_plan.status = "COMPLETED"
            logger.info("📅 Night plan completed")

    async def _on_weather_update(self, data: Dict[str, Any]) -> None:
        """Обработка обновления погоды."""
        weather = data.get("weather", {})

        # Проверяем, нужно ли адаптировать план
        cloud_cover = weather.get("cloud_cover")
        wind_speed = weather.get("wind_speed")

        if self._current_plan and cloud_cover and cloud_cover > 70.0:
            logger.warning(
                f"☁️ High cloud cover ({cloud_cover}%), considering plan adaptation"
            )
            # Можно добавить логику адаптации плана

    async def _on_dynamic_sequencer_update(self, data: Dict[str, Any]) -> None:
        """Обработка обновления Dynamic Sequencer."""
        # Обновляем информацию о целях
        logger.debug("Dynamic Sequencer updated")

    async def _get_available_targets(self) -> List[Target]:
        """Получает список доступных целей из Dynamic Sequencer."""
        try:
            projects = await dynamic_editor.list_projects()

            if not projects:
                return []

            # Берем первый активный проект
            project_name = projects[0]["name"]
            project = await dynamic_editor.get_project(project_name)

            if not project:
                return []

            # Извлекаем цели
            targets_data = project.get("Targets", [])
            targets = []

            for target_data in targets_data:
                if not target_data.get("active", True):
                    continue

                # Извлекаем координаты
                coords = target_data.get("InputCoordinates", {})

                target = Target(
                    name=target_data.get(
                        "TargetName", target_data.get("Name", "Unknown")
                    ),
                    ra_hours=coords.get("RAHours", 0),
                    ra_minutes=coords.get("RAMinutes", 0),
                    ra_seconds=coords.get("RASeconds", 0),
                    dec_degrees=coords.get("DecDegrees", 0),
                    dec_minutes=coords.get("DecMinutes", 0),
                    dec_seconds=coords.get("DecSeconds", 0),
                    priority=target_data.get("priority", 5),
                    filter=target_data.get("filter", "L"),
                    exposure_time=target_data.get("exposureTime", 60.0),
                    frames_needed=target_data.get("acceptedAmount", 100),
                    frames_completed=target_data.get("completedAmount", 0),
                )

                targets.append(target)

            return targets

        except Exception as e:
            logger.error(f"Failed to get available targets: {e}")
            return []

    async def _build_night_plan(self, targets: List[Target]) -> Optional[NightPlan]:
        """Строит план на ночь."""
        if not targets:
            return None

        # Получаем текущие условия
        weather = observatory_state.weather
        moon_angle = observatory_state.astronomy.get("moon_angle")

        # Рассчитываем видимость каждой цели
        scored_targets = []

        for target in targets:
            score = await self._calculate_target_score(target, weather, moon_angle)
            scored_targets.append((target, score))

        # Сортируем по score (высший первый)
        scored_targets.sort(key=lambda x: x[1], reverse=True)

        # Формируем упорядоченный список целей
        ordered_targets = [t for t, _ in scored_targets]

        # Оцениваем общую продолжительность
        total_duration = sum(
            (t.frames_needed - t.frames_completed) * t.exposure_time / 3600.0
            for t in ordered_targets
        )

        return NightPlan(
            date=datetime.now().strftime("%Y-%m-%d"),
            targets=ordered_targets,
            weather_forecast=weather,
            moon_phase=moon_angle or 0.0,
            estimated_duration_hours=total_duration,
        )

    async def _calculate_target_score(
        self, target: Target, weather: Dict[str, Any], moon_angle: Optional[float]
    ) -> float:
        """Рассчитывает score цели на основе различных факторов."""
        score = 0.0

        # 1. Приоритет цели (0-40 баллов)
        score += target.priority * 4.0

        # 2. Видимость (высота над горизонтом) - 0-30 баллов
        altitude = await self._calculate_altitude(target)
        if altitude > self.min_altitude:
            # Нормализуем: 30° = 0 баллов, 90° = 30 баллов
            altitude_score = min(30.0, (altitude - self.min_altitude) / 2.0)
            score += altitude_score
        else:
            # Цель ниже горизонта - большой штраф
            score -= 50.0

        # 3. Расстояние до Луны - 0-20 баллов
        if moon_angle is not None:
            moon_distance = await self._calculate_moon_distance(target)
            if moon_distance > self.moon_avoidance_angle:
                # Далеко от Луны - бонус
                score += min(20.0, (moon_distance - self.moon_avoidance_angle))
            else:
                # Близко к Луне - штраф (особенно для узкополосных фильтров)
                if target.filter not in ["Ha", "OIII", "SII"]:
                    score -= 10.0

        # 4. Прогресс съемки - 0-10 баллов
        if target.frames_needed > 0:
            progress = target.frames_completed / target.frames_needed
            # Бонус за частично отснятые цели
            score += progress * 10.0

        return score

    async def _calculate_altitude(self, target: Target) -> float:
        """Рассчитывает текущую высоту цели над горизонтом."""
        # Упрощенный расчет (в реальности нужен полноценный astronomical calculation)
        # Используем координаты монтировки из ObservatoryState

        mount_altitude = observatory_state.current_metrics.get("mount_altitude")
        mount_azimuth = observatory_state.current_metrics.get("mount_azimuth")

        if mount_altitude is None:
            # Если координаты недоступны, возвращаем среднее значение
            return 45.0

        # Это очень упрощенный расчет
        # В реальности нужно использовать astronomical libraries (astropy.coordinates)
        return mount_altitude

    async def _calculate_moon_distance(self, target: Target) -> float:
        """Рассчитывает угловое расстояние до Луны."""
        # Упрощенный расчет
        # В реальности нужно использовать astropy.coordinates

        moon_angle = observatory_state.astronomy.get("moon_angle")
        if moon_angle is None:
            return 90.0  # Предполагаем, что Луна далеко

        return abs(moon_angle)

    async def _apply_plan_to_dynamic_sequencer(self, plan_data: Dict[str, Any]) -> bool:
        """Применяет план к Dynamic Sequencer."""
        try:
            projects = await dynamic_editor.list_projects()
            if not projects:
                return False

            project_name = projects[0]["name"]
            targets = plan_data.get("targets", [])

            # Обновляем приоритеты и порядок целей
            for i, target_data in enumerate(targets):
                target_name = target_data.get("name")
                new_priority = (
                    len(targets) - i
                )  # Обратный порядок (первый = высший приоритет)

                await dynamic_editor.update_target(
                    project_name=project_name,
                    target_name=target_name,
                    updates={"priority": new_priority},
                    reason=f"Scheduler optimization (rank {i + 1})",
                )

            return True

        except Exception as e:
            logger.error(f"Failed to apply plan: {e}")
            return False

    async def build_nightly_plan(
        self, targets: List[Target], weather: Dict[str, Any]
    ) -> Optional[NightPlan]:
        """Строит план на ночь (вызывается Orchestrator'ом)."""
        return await self._build_night_plan(targets)

    async def on_sequence_started(self, data: Dict[str, Any]) -> None:
        """Обработка начала секвенсора (вызывается Orchestrator'ом)."""
        await self._on_sequence_started(data)

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Строит или обновляет план на ночь.
        """
        # Делегируем в _make_decision через Template Method
        return await self._make_decision(context)

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        HOOK: Принимает решение на основе контекста.
        Реализация абстрактного метода из BaseAgent.
        """
        # Получаем доступные цели
        available_targets = await self._get_available_targets()

        if not available_targets:
            logger.warning("No targets available for planning")
            return None

        # Строим план
        plan = await self._build_night_plan(available_targets)

        if plan:
            self._current_plan = plan

            return AgentDecision(
                agent=self.name,
                decision_type="NIGHT_PLAN_CREATED",
                inputs={"targets_count": len(available_targets)},
                outputs={"plan": plan.model_dump()},
                rationale=f"План на ночь создан: {len(plan.targets)} целей",
                confidence=0.95,
            )

        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет план (обновляет Dynamic Sequencer)."""
        # Делегируем в _perform_action через Template Method
        return await self._perform_action(decision)

    async def _perform_action(self, decision: AgentDecision) -> bool:
        """
        HOOK: Выполняет действие решения.
        Реализация абстрактного метода из BaseAgent.
        """
        if decision.decision_type == "NIGHT_PLAN_CREATED":
            plan_data = decision.outputs.get("plan", {})

            # Обновляем Dynamic Sequencer с планом
            success = await self._apply_plan_to_dynamic_sequencer(plan_data)

            if success:
                logger.info("✅ Night plan applied to Dynamic Sequencer")

            return success

        return False
