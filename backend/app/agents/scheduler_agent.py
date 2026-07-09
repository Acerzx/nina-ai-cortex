"""
Scheduler Agent — планирует сессии на основе погоды, видимости целей, приоритетов.
ИСПРАВЛЕНО (v4.0):
- НЕ дублируем астрономические расчёты (используем данные N.I.N.A.)
- Используем mount_altitude для текущей цели
- Используем moon_angle из FITS headers (угловое расстояние)
- Валидация данных перед использованием
- Обновление всех параметров целей при изменении приоритета
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.execution.dynamic_editor import dynamic_editor

logger = logging.getLogger("SchedulerAgent")


class Target(BaseModel):
    """Цель для съемки."""

    name: str
    ra_hours: float = 0.0
    ra_minutes: float = 0.0
    ra_seconds: float = 0.0
    dec_degrees: float = 0.0
    dec_minutes: float = 0.0
    dec_seconds: float = 0.0
    priority: int = Field(ge=1, le=10)
    filter: str
    exposure_time: float
    frames_needed: int
    frames_completed: int = 0
    # Runtime данные от N.I.N.A.
    current_altitude: Optional[float] = None
    current_azimuth: Optional[float] = None


class NightPlan(BaseModel):
    """План на ночь."""

    date: str
    targets: List[Target]
    weather_forecast: Dict[str, Any]
    moon_phase: float
    estimated_duration_hours: float
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    status: str = "PLANNED"


class SchedulerAgent(BaseAgent):
    """
    Агент планирования сессий.
    ИСПРАВЛЕНО (v4.0):
    - Используем данные N.I.N.A. вместо собственных расчётов
    - Валидация доступности данных
    - Обновление всех параметров целей
    """

    def __init__(self):
        super().__init__(name="Scheduler", role="Session Planning")
        self._current_plan: Optional[NightPlan] = None
        self.min_altitude = 30.0
        self.moon_avoidance_angle = 30.0

    async def initialize(self):
        """Инициализация агента планирования."""
        await super().initialize()
        event_bus.subscribe("SEQUENCE_STARTED", self._on_sequence_started)
        event_bus.subscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)
        event_bus.subscribe("WEATHER_UPDATE", self._on_weather_update)
        event_bus.subscribe(
            "DYNAMIC_SEQUENCER_UPDATE", self._on_dynamic_sequencer_update
        )
        logger.info("✅ Scheduler Agent initialized (using N.I.N.A. data)")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("SEQUENCE_STARTED", self._on_sequence_started)
        event_bus.unsubscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)
        event_bus.unsubscribe("WEATHER_UPDATE", self._on_weather_update)
        event_bus.unsubscribe(
            "DYNAMIC_SEQUENCER_UPDATE", self._on_dynamic_sequencer_update
        )
        await super().shutdown()

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """Принимает решение на основе контекста."""
        available_targets = await self._get_available_targets()
        if not available_targets:
            logger.warning("No targets available for planning")
            return None

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

    async def _perform_action(self, decision: AgentDecision) -> bool:
        """Выполняет действие решения."""
        if decision.decision_type == "NIGHT_PLAN_CREATED":
            plan_data = decision.outputs.get("plan", {})
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
        cloud_cover = weather.get("cloud_cover")
        if self._current_plan and cloud_cover and cloud_cover > 70.0:
            logger.warning(
                f"☁️ High cloud cover ({cloud_cover}%), considering plan adaptation"
            )

    async def _on_dynamic_sequencer_update(self, data: Dict[str, Any]) -> None:
        """Обработка обновления Dynamic Sequencer."""
        logger.debug("Dynamic Sequencer updated")

    async def _get_available_targets(self) -> List[Target]:
        """Получает список доступных целей из Dynamic Sequencer."""
        try:
            projects = await dynamic_editor.list_projects()
            if not projects:
                return []

            project_name = projects[0]["name"]
            project = await dynamic_editor.get_project(project_name)
            if not project:
                return []

            targets_data = project.get("Targets", [])
            targets = []

            # Получаем текущие метрики монтировки
            mount_altitude = observatory_state.current_metrics.get("mount_altitude")
            mount_azimuth = observatory_state.current_metrics.get("mount_azimuth")

            for target_data in targets_data:
                if not target_data.get("active", True):
                    continue

                coords = target_data.get("InputCoordinates", {})

                # Создаём Target с runtime данными
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
                    # Runtime данные от N.I.N.A.
                    current_altitude=mount_altitude,
                    current_azimuth=mount_azimuth,
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

        weather = observatory_state.weather
        moon_angle = observatory_state.astronomy.get("moon_angle")

        # Рассчитываем score для каждой цели
        scored_targets = []
        for target in targets:
            score = await self._calculate_target_score(target, weather, moon_angle)
            scored_targets.append((target, score))

        # Сортируем по score (высший первый)
        scored_targets.sort(key=lambda x: x[1], reverse=True)
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
        """
        Рассчитывает score цели на основе различных факторов.
        ИСПРАВЛЕНО (v4.0 — проблемы #29, #30):
        - Используем mount_altitude из ObservatoryState (не пересчитываем!)
        - Используем moon_angle из FITS (угловое расстояние, не фаза!)
        """
        score = 0.0

        # 1. Приоритет цели (0-40 баллов)
        score += target.priority * 4.0

        # 2. Видимость (высота над горизонтом) - 0-30 баллов
        # ИСПРАВЛЕНО: используем текущую высоту монтировки из ObservatoryState
        mount_altitude = observatory_state.current_metrics.get("mount_altitude")

        if mount_altitude is not None and mount_altitude > self.min_altitude:
            # Нормализуем: 30° = 0 баллов, 90° = 30 баллов
            altitude_score = min(30.0, (mount_altitude - self.min_altitude) / 2.0)
            score += altitude_score
        elif mount_altitude is None:
            # Данные недоступны — небольшой штраф
            logger.debug(f"Altitude not available for target {target.name}")
            score -= 5.0
        else:
            # Цель ниже горизонта - большой штраф
            score -= 50.0

        # 3. Расстояние до Луны - 0-20 баллов
        # ИСПРАВЛЕНО: moon_angle из FITS — это уже угловое расстояние!
        if moon_angle is not None:
            moon_distance = moon_angle  # Уже угловое расстояние от N.I.N.A.
            if moon_distance > self.moon_avoidance_angle:
                # Далеко от Луны - бонус
                score += min(20.0, (moon_distance - self.moon_avoidance_angle))
            else:
                # Близко к Луне - штраф (особенно для узкополосных фильтров)
                if target.filter not in ["Ha", "OIII", "SII"]:
                    score -= 10.0
        else:
            logger.debug(f"Moon angle not available for target {target.name}")

        # 4. Прогресс съемки - 0-10 баллов
        if target.frames_needed > 0:
            progress = target.frames_completed / target.frames_needed
            # Бонус за частично отснятые цели
            score += progress * 10.0

        return score

    async def _apply_plan_to_dynamic_sequencer(self, plan_data: Dict[str, Any]) -> bool:
        """
        Применяет план к Dynamic Sequencer.
        ИСПРАВЛЕНО (v4.0 — проблема #31): обновляем все параметры целей,
        не только priority.
        """
        try:
            projects = await dynamic_editor.list_projects()
            if not projects:
                return False

            project_name = projects[0]["name"]
            targets = plan_data.get("targets", [])

            # Обновляем приоритеты и другие параметры
            for i, target_data in enumerate(targets):
                target_name = target_data.get("name")
                new_priority = (
                    len(targets) - i
                )  # Обратный порядок (первый = высший приоритет)

                # ИСПРАВЛЕНО: обновляем не только priority, но и другие поля
                updates = {
                    "priority": new_priority,
                    "active": True,
                }

                # Если есть дополнительные параметры в target_data — добавляем их
                if "exposure_time" in target_data:
                    updates["exposureTime"] = target_data["exposure_time"]
                if "filter" in target_data:
                    updates["filter"] = target_data["filter"]
                if "frames_needed" in target_data:
                    updates["acceptedAmount"] = target_data["frames_needed"]

                await dynamic_editor.update_target(
                    project_name=project_name,
                    target_name=target_name,
                    updates=updates,
                    reason=f"Scheduler optimization (rank {i + 1})",
                )

            return True
        except Exception as e:
            logger.error(f"Failed to apply plan: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику агента."""
        base_stats = super().get_stats()
        return {
            **base_stats,
            "current_plan_status": self._current_plan.status
            if self._current_plan
            else None,
            "targets_in_plan": len(self._current_plan.targets)
            if self._current_plan
            else 0,
        }
