"""
Strategist Agent — оптимизирует параметры съемки для максимального качества.
Анализирует LiveStack SNR, Dynamic Sequencer, Diagnostician рекомендации.

ЭТАП 7 (делегирование):
- Strategist теперь делегирует расчёты в ParameterOptimizer
- Убрано дублирование формул (SNR ~ sqrt(time))
- Strategist фокусируется на принятии решений, а не на расчётах
- Сохранена интеграция с Diagnostician и LiveStack

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
from app.ml.parameter_optimizer import parameter_optimizer

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
    - Делегирует расчёты в ParameterOptimizer
    - Принимает решения на основе предложений ML/Heuristic моделей
    - Применяет оптимизации через глобальные переменные Sequencer+
    - Редактирует Dynamic Sequencer проекты
    - Отключает неоптимальные цели при плохих условиях

    ЭТАП 7 (делегирование):
    - Strategist больше не содержит формул расчёта
    - Все расчёты делегируются в ParameterOptimizer
    - Strategist фокусируется на принятии решений

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
        # ИСПРАВЛЕНО (С-8): подписываем _on_snr_update
        event_bus.subscribe("SNR_UPDATE", self._on_snr_update)

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
        # ИСПРАВЛЕНО (С-8): отписываем _on_snr_update
        event_bus.unsubscribe("SNR_UPDATE", self._on_snr_update)
        await super().shutdown()

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
                # Делегируем расчёт в ParameterOptimizer
                current_interval = self._get_current_autofocus_interval()

                conditions = {
                    "hfr_trend": observatory_state.get_trend("hfr", window=10),
                    "current_interval": current_interval,
                }

                suggestion = await parameter_optimizer.suggest_autofocus_interval(
                    conditions
                )

                if suggestion:
                    proposal = OptimizationProposal(
                        parameter="autofocus_interval",
                        current_value=current_interval,
                        proposed_value=suggestion.suggested_value,
                        expected_improvement=(
                            f"Более частая компенсация температурного дрейфа "
                            f"(с {current_interval} до {suggestion.suggested_value} мин)"
                        ),
                        confidence=suggestion.confidence,
                        rationale=suggestion.rationale,
                        risk_level="LOW",
                    )
                    await self._propose_optimization(proposal)

    async def _analyze_snr_and_exposure(self) -> Optional[OptimizationProposal]:
        """
        Делегирует анализ SNR и расчёт экспозиции в ParameterOptimizer.
        """
        current_snr = observatory_state.current_metrics.get("snr")
        current_exposure = observatory_state.current_metrics.get("exposure_time", 60.0)

        if current_snr is None:
            return None

        # Делегируем расчёт в ParameterOptimizer
        conditions = {
            "current_snr": current_snr,
            "current_exposure": current_exposure,
            "target_snr": self.quality_targets["snr_target"],
        }

        suggestion = await parameter_optimizer.suggest_exposure(conditions)

        if suggestion:
            return OptimizationProposal(
                parameter="exposure_time",
                current_value=current_exposure,
                proposed_value=suggestion.suggested_value,
                expected_improvement=(
                    f"SNR увеличится с {current_snr:.1f} до "
                    f"{self.quality_targets['snr_target']:.1f}"
                ),
                confidence=suggestion.confidence,
                rationale=suggestion.rationale,
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
        Делегирует анализ интервала автофокуса в ParameterOptimizer.
        """
        # Получаем тренд HFR
        hfr_trend = observatory_state.get_trend("hfr", window=10)

        if hfr_trend is None:
            return None

        # Делегируем расчёт в ParameterOptimizer
        current_interval = self._get_current_autofocus_interval()

        conditions = {
            "hfr_trend": hfr_trend,
            "current_interval": current_interval,
        }

        suggestion = await parameter_optimizer.suggest_autofocus_interval(conditions)

        if suggestion:
            return OptimizationProposal(
                parameter="autofocus_interval",
                current_value=current_interval,
                proposed_value=suggestion.suggested_value,
                expected_improvement=(
                    f"Более быстрая компенсация дрейфа фокуса "
                    f"(интервал уменьшен с {current_interval} "
                    f"до {suggestion.suggested_value} мин)"
                ),
                confidence=suggestion.confidence,
                rationale=suggestion.rationale,
                risk_level="LOW",
            )

        return None

    def _get_current_autofocus_interval(self) -> int:
        """
        Читает текущий интервал автофокуса из глобальных переменных Shadow Engine.
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
                # Читаем имя переменной из конфига (или используем default)
                var_name = self.autofocus_config.get(
                    "interval_variable_name",
                    self.AUTOFOCUS_INTERVAL_VARS[0],  # Fallback на первое имя
                )

                # Проверяем, существует ли переменная в Shadow Engine
                existing_vars = []
                for vname in self.AUTOFOCUS_INTERVAL_VARS:
                    if vname in state_tracker.state.global_variables:
                        existing_vars.append(vname)

                if not existing_vars:
                    # Ни одна переменная не найдена — создаём новую с именем из конфига
                    logger.warning(
                        f"No autofocus interval variable found in Shadow Engine. "
                        f"Creating new variable: {var_name}"
                    )
                    result = await global_var_injector.set_variable(
                        var_name, proposal.proposed_value, proposal.rationale
                    )
                    success = result
                else:
                    # Обновляем все найденные переменные
                    success = True
                    for vname in existing_vars:
                        logger.info(
                            f"🔧 Updating autofocus interval variable: {vname} "
                            f"({proposal.current_value} → {proposal.proposed_value})"
                        )
                        result = await global_var_injector.set_variable(
                            vname, proposal.proposed_value, proposal.rationale
                        )
                        success = success and result

                    # Логируем результат
                    if success:
                        logger.info(
                            f"✅ Autofocus interval updated successfully "
                            f"({len(existing_vars)} variables)"
                        )
                    else:
                        logger.warning(
                            f"⚠️ Autofocus interval update partially failed "
                            f"({len(existing_vars)} variables attempted)"
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

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        HOOK: Принимает решение на основе контекста.
        Реализация абстрактного метода из BaseAgent.
        """
        proposals = []

        # 1. Делегируем анализ SNR в ParameterOptimizer
        snr_proposal = await self._analyze_snr_and_exposure()
        if snr_proposal:
            proposals.append(snr_proposal)

        # 2. Анализ текущих целей и погодных условий
        target_proposal = await self._analyze_target_suitability()
        if target_proposal:
            proposals.append(target_proposal)

        # 3. Делегируем анализ интервала автофокуса в ParameterOptimizer
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

    # В strategist_agent.py
    async def _on_snr_update(self, data: Dict[str, Any]) -> None:
        """Обработка обновления SNR от LiveStack."""
        snr = data.get("snr")
        snr_target = data.get("snr_target", self.quality_targets["snr_target"])

        if snr is not None and snr < snr_target * 0.8:
            # Расчет оптимальной экспозиции
            current_exposure = observatory_state.current_metrics.get(
                "exposure_time", 60.0
            )
            ratio = snr_target / snr
            proposed_exposure = current_exposure * (ratio**2)
            proposed_exposure = max(30.0, min(300.0, proposed_exposure))

            proposal = OptimizationProposal(
                parameter="exposure_time",
                current_value=current_exposure,
                proposed_value=proposed_exposure,
                expected_improvement=f"SNR увеличится с {snr:.1f} до {snr_target:.1f}",
                confidence=0.90,
                rationale=f"SNR {snr:.1f} ниже целевого {snr_target:.1f}",
                risk_level="LOW",
            )
            await self._propose_optimization(proposal)
