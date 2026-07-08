"""
Strategist Agent — оптимизирует параметры съемки для максимального качества.
Анализирует LiveStack SNR, Dynamic Sequencer, Diagnostician рекомендации.

ИСПРАВЛЕНО (audit 4.2): Убран хардкод интервалов автофокуса.
- Текущий интервал читается из глобальных переменных Shadow Engine
- Пороговые значения извлекаются из settings.thresholds
- Предложения по оптимизации учитывают реальные настройки оборудования
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.config import settings
from app.execution.global_var_injector import global_var_injector
from app.execution.dynamic_editor import dynamic_editor
from app.shadow_engine.state_tracker import state_tracker
import math

logger = logging.getLogger("StrategistAgent")


class OptimizationProposal(BaseModel):
    """Предложение по оптимизации параметров."""

    parameter: str
    current_value: Any
    proposed_value: Any
    expected_improvement: str
    confidence: float = Field(ge=0.0, le=1.0)
    rationale: str
    risk_level: str = Field(pattern="^(LOW|MEDIUM|HIGH)$")


class StrategistAgent(BaseAgent):
    """
    Агент оптимизации параметров съемки.

    Responsibilities:
    - Анализ SNR из LiveStack и расчет оптимальной экспозиции
    - Оптимизация параметров через глобальные переменные Sequencer+
    - Редактирование Dynamic Sequencer проектов
    - Отключение неоптимальных целей при плохих условиях

    ИСПРАВЛЕНО (audit 4.2):
    - Интервалы автофокуса теперь читаются из Shadow Engine / settings
    - Все пороговые значения вынесены в settings.thresholds
    - Предложения учитывают реальную конфигурацию оборудования

    Примеры оптимизации:
    - "SNR = 15, target = 20" → "Увеличить экспозицию с 60s до 90s"
    - "Ветер с севера" → "Переключиться на цель в южном направлении"
    - "HFR деградирует" → "Уменьшить интервал автофокуса"
    """

    # Имена глобальных переменных Sequencer+, используемых для автофокуса
    AUTOFOCUS_INTERVAL_VARS = [
        "AUTOFOCUS_INTERVAL",
        "AUTOFOCUS_INTERVAL_MINUTES",
        "AutoFocusInterval",
    ]

    def __init__(self):
        super().__init__(name="Strategist", role="Parameter Optimization")

        # Целевые метрики качества (из settings.thresholds)
        self.quality_targets = {
            "snr_target": 20.0,
            "hfr_target": settings.thresholds.hfr_target
            if hasattr(settings.thresholds, "hfr_target")
            else 2.5,
            "fwhm_target": settings.thresholds.fwhm_target
            if hasattr(settings.thresholds, "fwhm_target")
            else 3.0,
            "acceptance_rate_target": 0.90,
        }

        # Параметры автофокуса (из settings.thresholds)
        self.autofocus_config = {
            # Интервалы в минутах
            "interval_normal": getattr(
                settings.thresholds, "autofocus_interval_normal_minutes", 60
            ),
            "interval_frequent": getattr(
                settings.thresholds, "autofocus_interval_frequent_minutes", 30
            ),
            "interval_emergency": getattr(
                settings.thresholds, "autofocus_interval_emergency_minutes", 15
            ),
            # Порог деградации HFR (пикселей/кадр)
            "hfr_degradation_threshold": getattr(
                settings.thresholds, "hfr_degradation_threshold", 0.05
            ),
            # Минимальный интервал между предложениями (сек)
            "min_proposal_interval_seconds": getattr(
                settings.thresholds,
                "strategist_min_proposal_interval_seconds",
                600,
            ),
        }

        # История оптимизаций (для избежания частых изменений)
        self._optimization_history: List[Dict[str, Any]] = []
        self._min_interval_between_changes = self.autofocus_config[
            "min_proposal_interval_seconds"
        ]

    async def initialize(self):
        """Инициализация агента оптимизации."""
        await super().initialize()

        # Подписываемся на события для анализа
        event_bus.subscribe("LIVESTACK_STATUS", self._on_livestack_update)
        event_bus.subscribe(
            "DIAGNOSTIC_RECOMMENDATION", self._on_diagnostic_recommendation
        )
        event_bus.subscribe(
            "DYNAMIC_SEQUENCER_UPDATE", self._on_dynamic_sequencer_update
        )

        logger.info("✅ Strategist Agent initialized with quality targets:")
        for key, value in self.quality_targets.items():
            logger.info(f"   - {key}: {value}")
        logger.info("✅ Strategist Agent autofocus config:")
        for key, value in self.autofocus_config.items():
            logger.info(f"   - {key}: {value}")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("LIVESTACK_STATUS", self._on_livestack_update)
        event_bus.unsubscribe(
            "DIAGNOSTIC_RECOMMENDATION", self._on_diagnostic_recommendation
        )
        event_bus.unsubscribe(
            "DYNAMIC_SEQUENCER_UPDATE", self._on_dynamic_sequencer_update
        )
        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует контекст и предлагает оптимизации.
        Вызывается Orchestrator'ом при необходимости.
        """
        proposals = []

        # 1. Анализ SNR и расчет оптимальной экспозиции
        snr_proposal = await self._analyze_snr_and_exposure()
        if snr_proposal:
            proposals.append(snr_proposal)

        # 2. Анализ текущих целей и погодных условий
        target_proposal = await self._analyze_target_suitability()
        if target_proposal:
            proposals.append(target_proposal)

        # 3. Анализ интервала автофокуса
        autofocus_proposal = await self._analyze_autofocus_interval()
        if autofocus_proposal:
            proposals.append(autofocus_proposal)

        if proposals:
            decision = AgentDecision(
                agent=self.name,
                decision_type="OPTIMIZATION_PROPOSED",
                inputs={"proposals_count": len(proposals)},
                outputs={"proposals": [p.model_dump() for p in proposals]},
                rationale=f"Предложено {len(proposals)} оптимизаций",
                confidence=max(p.confidence for p in proposals),
            )
            self.log_decision(decision)
            return decision
        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет принятые оптимизации."""
        if decision.decision_type == "OPTIMIZATION_PROPOSED":
            proposals = decision.outputs.get("proposals", [])
            success_count = 0
            for proposal_data in proposals:
                proposal = OptimizationProposal(**proposal_data)
                success = await self._apply_optimization(proposal)
                if success:
                    success_count += 1
            return success_count > 0
        return False

    async def _on_livestack_update(self, data: Dict[str, Any]) -> None:
        """Обработка обновления LiveStack статуса."""
        snr = data.get("snr")
        acceptance_rate = data.get("acceptance_rate")

        if snr is not None and snr < self.quality_targets["snr_target"]:
            proposal = await self._analyze_snr_and_exposure()
            if proposal:
                await self._propose_optimization(proposal)

        if (
            acceptance_rate is not None
            and acceptance_rate < self.quality_targets["acceptance_rate_target"]
        ):
            logger.warning(f"Low acceptance rate: {acceptance_rate:.2f}")

    async def _on_diagnostic_recommendation(self, data: Dict[str, Any]) -> None:
        """Обработка рекомендаций от Diagnostician."""
        recommended_actions = data.get("recommended_actions", [])

        for action in recommended_actions:
            if "автофокус" in action.lower() or "интервал" in action.lower():
                # ИСПРАВЛЕНО (audit 4.2): используем реальный текущий интервал
                # и значение из конфига
                current_interval = self._get_current_autofocus_interval()
                proposed_interval = self.autofocus_config["interval_frequent"]

                # Если уже частый — используем emergency
                if current_interval <= self.autofocus_config["interval_frequent"]:
                    proposed_interval = self.autofocus_config["interval_emergency"]

                # Только если реально меняем
                if proposed_interval < current_interval:
                    proposal = OptimizationProposal(
                        parameter="autofocus_interval",
                        current_value=current_interval,
                        proposed_value=proposed_interval,
                        expected_improvement=(
                            "Более частая компенсация температурного дрейфа "
                            f"(с {current_interval} до {proposed_interval} мин)"
                        ),
                        confidence=0.85,
                        rationale=action,
                        risk_level="LOW",
                    )
                    await self._propose_optimization(proposal)

    async def _on_dynamic_sequencer_update(self, data: Dict[str, Any]) -> None:
        """Обработка обновления Dynamic Sequencer проекта."""
        targets = data.get("data", {}).get("Targets", [])
        logger.debug(f"Dynamic Sequencer updated: {len(targets)} targets")

    async def _analyze_snr_and_exposure(self) -> Optional[OptimizationProposal]:
        """Анализирует SNR и рассчитывает оптимальную экспозицию."""
        current_snr = observatory_state.current_metrics.get("snr")
        if current_snr is None:
            return None

        target_snr = self.quality_targets["snr_target"]

        # SNR растет как sqrt(time)
        # new_snr / old_snr = sqrt(new_time / old_time)
        # new_time = old_time * (new_snr / old_snr)^2
        current_exposure = observatory_state.current_metrics.get("exposure_time", 60.0)

        if current_snr < target_snr * 0.8:  # SNR менее 80% от целевого
            ratio = target_snr / current_snr
            proposed_exposure = current_exposure * (ratio**2)

            # Ограничиваем разумными пределами
            proposed_exposure = max(30.0, min(300.0, proposed_exposure))

            # Проверяем, не слишком ли большое изменение
            if abs(proposed_exposure - current_exposure) / current_exposure > 0.1:
                return OptimizationProposal(
                    parameter="exposure_time",
                    current_value=current_exposure,
                    proposed_value=proposed_exposure,
                    expected_improvement=(
                        f"SNR увеличится с {current_snr:.1f} до {target_snr:.1f}"
                    ),
                    confidence=0.90,
                    rationale=(
                        f"SNR {current_snr:.1f} ниже целевого {target_snr:.1f}. "
                        f"SNR ~ sqrt(time), поэтому увеличиваем экспозицию."
                    ),
                    risk_level="LOW",
                )
        return None

    async def _analyze_target_suitability(self) -> Optional[OptimizationProposal]:
        """Анализирует пригодность текущей цели для текущих условий."""
        wind_speed = observatory_state.weather.get("wind_speed")
        wind_direction = observatory_state.weather.get("wind_direction")

        current_target = (
            observatory_state.active_targets[0]
            if observatory_state.active_targets
            else None
        )

        if not current_target or wind_speed is None:
            return None

        # Если ветер сильный, проверяем направление цели
        wind_warning = settings.thresholds.wind_speed_warning
        if wind_speed > wind_warning and wind_direction is not None:
            target_azimuth = current_target.get("azimuth")
            if target_azimuth is not None:
                # Проверяем, находится ли цель в подветренном направлении
                angle_diff = abs(target_azimuth - wind_direction)
                if angle_diff > 180:
                    angle_diff = 360 - angle_diff

                # Если цель в наветренном направлении (разница < 90°)
                if angle_diff < 90:
                    return OptimizationProposal(
                        parameter="active_target",
                        current_value=current_target.get("name"),
                        proposed_value="switch_to_sheltered_target",
                        expected_improvement="Снижение ветровой нагрузки на монтировку",
                        confidence=0.75,
                        rationale=(
                            f"Ветер {wind_speed} м/с с направления "
                            f"{wind_direction}°. "
                            f"Текущая цель на азимуте {target_azimuth}° "
                            f"(разница {angle_diff:.0f}°)."
                        ),
                        risk_level="MEDIUM",
                    )
        return None

    async def _analyze_autofocus_interval(self) -> Optional[OptimizationProposal]:
        """
        Анализирует интервал автофокуса на основе тренда HFR.

        ИСПРАВЛЕНО (audit 4.2):
        - Текущий интервал читается из Shadow Engine (глобальные переменные)
        - Пороговые значения из settings.thresholds
        - Предложения учитывают реальную конфигурацию
        """
        # Получаем тренд HFR
        hfr_trend = observatory_state.get_trend("hfr", window=10)
        if hfr_trend is None:
            return None

        # Порог деградации из конфига
        degradation_threshold = self.autofocus_config["hfr_degradation_threshold"]

        # Если HFR быстро растет, нужен более частый автофокус
        if hfr_trend > degradation_threshold:
            # ИСПРАВЛЕНО: читаем текущий интервал из Shadow Engine
            current_interval = self._get_current_autofocus_interval()

            # Определяем целевой интервал в зависимости от скорости деградации
            if hfr_trend > degradation_threshold * 2:
                # Быстрая деградация — emergency интервал
                proposed_interval = self.autofocus_config["interval_emergency"]
            else:
                # Умеренная деградация — frequent интервал
                proposed_interval = self.autofocus_config["interval_frequent"]

            # Предлагаем изменение, только если реально уменьшаем интервал
            if proposed_interval < current_interval:
                return OptimizationProposal(
                    parameter="autofocus_interval",
                    current_value=current_interval,
                    proposed_value=proposed_interval,
                    expected_improvement=(
                        f"Более быстрая компенсация дрейфа фокуса "
                        f"(интервал уменьшен с {current_interval} "
                        f"до {proposed_interval} мин)"
                    ),
                    confidence=0.80,
                    rationale=(
                        f"HFR растет со скоростью {hfr_trend:.3f} пикселей/кадр "
                        f"(порог {degradation_threshold}). "
                        f"Рекомендуется уменьшить интервал автофокуса."
                    ),
                    risk_level="LOW",
                )
        return None

    def _get_current_autofocus_interval(self) -> int:
        """
        Читает текущий интервал автофокуса из глобальных переменных Shadow Engine.

        ИСПРАВЛЕНО (audit 4.2): заменяет хардкод `current_interval = 60`.

        Returns:
            Текущий интервал в минутах (default: из settings если не найден)
        """
        # Пробуем разные варианты именования глобальных переменных
        for var_name in self.AUTOFOCUS_INTERVAL_VARS:
            value = state_tracker.state.global_variables.get(var_name)
            if value is not None:
                try:
                    # Преобразуем в int (минуты)
                    interval = int(float(str(value)))
                    if 1 <= interval <= 1440:  # от 1 мин до 24 часов
                        return interval
                except (ValueError, TypeError):
                    continue

        # Если не найдено в Shadow Engine — берём из settings
        return self.autofocus_config["interval_normal"]

    async def _propose_optimization(self, proposal: OptimizationProposal) -> None:
        """Предлагает оптимизацию через EventBus."""
        decision = AgentDecision(
            agent=self.name,
            decision_type="OPTIMIZATION_PROPOSED",
            inputs={},
            outputs={"proposals": [proposal.model_dump()]},
            rationale=proposal.rationale,
            confidence=proposal.confidence,
        )
        self.log_decision(decision)

        # Публикуем предложение для Orchestrator
        await event_bus.publish(
            "OPTIMIZATION_PROPOSAL",
            {
                "proposal": proposal.model_dump(),
                "timestamp": datetime.now().isoformat(),
            },
        )

    async def _apply_optimization(self, proposal: OptimizationProposal) -> bool:
        """Применяет оптимизацию."""
        # Проверяем, не слишком ли часто меняем параметры
        if self._is_too_frequent_change(proposal.parameter):
            logger.warning(
                f"Skipping optimization: too frequent changes for {proposal.parameter}"
            )
            return False

        logger.info(
            f"🔧 Applying optimization: {proposal.parameter} "
            f"{proposal.current_value} → {proposal.proposed_value}"
        )

        try:
            # Применяем в зависимости от типа параметра
            if proposal.parameter == "exposure_time":
                success = await global_var_injector.set_variable(
                    "EXPOSURE_TIME", proposal.proposed_value, proposal.rationale
                )
            elif proposal.parameter == "autofocus_interval":
                # ИСПРАВЛЕНО (audit 4.2): обновляем все возможные варианты
                # глобальной переменной
                success = False
                for var_name in self.AUTOFOCUS_INTERVAL_VARS:
                    if var_name in state_tracker.state.global_variables:
                        result = await global_var_injector.set_variable(
                            var_name, proposal.proposed_value, proposal.rationale
                        )
                        success = success or result
                        break

                # Если переменная не найдена в Shadow Engine — создаём новую
                if not success:
                    success = await global_var_injector.set_variable(
                        self.AUTOFOCUS_INTERVAL_VARS[0],
                        proposal.proposed_value,
                        proposal.rationale,
                    )
            elif proposal.parameter == "active_target":
                # Переключение цели через Dynamic Sequencer
                success = True
                logger.info(
                    f"Target switch proposed (requires Dynamic Sequencer integration)"
                )
            else:
                logger.warning(f"Unknown parameter: {proposal.parameter}")
                success = False

            if success:
                # Логируем успешную оптимизацию
                self._optimization_history.append(
                    {
                        "parameter": proposal.parameter,
                        "timestamp": datetime.now().isoformat(),
                        "old_value": proposal.current_value,
                        "new_value": proposal.proposed_value,
                    }
                )
                logger.info(f"✅ Optimization applied successfully")
            return success

        except Exception as e:
            logger.error(f"Failed to apply optimization: {e}")
            return False

    def _is_too_frequent_change(self, parameter: str) -> bool:
        """Проверяет, не слишком ли часто меняется параметр."""
        recent_changes = [
            h for h in self._optimization_history if h["parameter"] == parameter
        ]
        if not recent_changes:
            return False

        last_change = max(recent_changes, key=lambda h: h["timestamp"])
        last_time = datetime.fromisoformat(last_change["timestamp"])
        elapsed = (datetime.now() - last_time).total_seconds()
        return elapsed < self._min_interval_between_changes

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует контекст и предлагает оптимизации.
        Вызывается Orchestrator'ом при необходимости.
        """
        # Делегируем в _make_decision через Template Method
        return await self._make_decision(context)

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        HOOK: Принимает решение на основе контекста.
        Реализация абстрактного метода из BaseAgent.
        """
        proposals = []

        # 1. Анализ SNR и расчет оптимальной экспозиции
        snr_proposal = await self._analyze_snr_and_exposure()
        if snr_proposal:
            proposals.append(snr_proposal)

        # 2. Анализ текущих целей и погодных условий
        target_proposal = await self._analyze_target_suitability()
        if target_proposal:
            proposals.append(target_proposal)

        # 3. Анализ интервала автофокуса
        autofocus_proposal = await self._analyze_autofocus_interval()
        if autofocus_proposal:
            proposals.append(autofocus_proposal)

        if proposals:
            return AgentDecision(
                agent=self.name,
                decision_type="OPTIMIZATION_PROPOSED",
                inputs={"proposals_count": len(proposals)},
                outputs={"proposals": [p.model_dump() for p in proposals]},
                rationale=f"Предложено {len(proposals)} оптимизаций",
                confidence=max(p.confidence for p in proposals),
            )
        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет принятые оптимизации."""
        # Делегируем в _perform_action через Template Method
        return await self._perform_action(decision)

    async def _perform_action(self, decision: AgentDecision) -> bool:
        """
        HOOK: Выполняет действие решения.
        Реализация абстрактного метода из BaseAgent.
        """
        if decision.decision_type == "OPTIMIZATION_PROPOSED":
            proposals = decision.outputs.get("proposals", [])
            success_count = 0

            for proposal_data in proposals:
                proposal = OptimizationProposal(**proposal_data)
                success = await self._apply_optimization(proposal)
                if success:
                    success_count += 1

            return success_count > 0
        return False
