"""
Calibrator Agent — управляет библиотекой мастер-кадров, проверяет свежесть калибровок.
Обеспечивает правильное применение калибровочных кадров.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.ingestion.watchers.masters_auditor import MastersLibraryAuditor

logger = logging.getLogger("CalibratorAgent")


class CalibrationCheck(BaseModel):
    """Результат проверки калибровки."""

    master_type: str
    params: Dict[str, Any]
    matching_master: Optional[Dict[str, Any]] = None
    is_fresh: bool = True
    age_days: Optional[int] = None
    recommendation: str


class CalibratorAgent(BaseAgent):
    """
    Агент управления калибровочными кадрами.

    Responsibilities:
    - Проверка свежести мастер-кадров (Bias/Dark/Flat)
    - Подбор оптимальных мастеров по параметрам
    - Планирование съемки новых мастеров при необходимости
    - Валидация качества мастеров

    Критерии свежести:
    - Bias: возраст < 90 дней
    - Dark: возраст < 30 дней, температура ± 2°C
    - Flat: возраст < 7 дней, тот же фильтр
    """

    def __init__(self, masters_auditor: MastersLibraryAuditor):
        super().__init__(name="Calibrator", role="Calibration Management")

        self.masters_auditor = masters_auditor

        # Пороговые значения свежести
        self.freshness_thresholds = {
            "BIAS": 90,  # days
            "DARK": 30,  # days
            "FLAT": 7,  # days
        }

        # Температурный допуск для Dark
        self.temperature_tolerance = 2.0  # °C

    async def initialize(self):
        """Инициализация агента калибровки."""
        await super().initialize()

        # Подписываемся на события
        event_bus.subscribe("NEW_FRAME", self._on_new_frame)
        event_bus.subscribe("MASTERS_INDEXED", self._on_masters_indexed)

        logger.info("✅ Calibrator Agent initialized with freshness thresholds:")
        for key, value in self.freshness_thresholds.items():
            logger.info(f"   - {key}: {value} days")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("NEW_FRAME", self._on_new_frame)
        event_bus.unsubscribe("MASTERS_INDEXED", self._on_masters_indexed)

        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Проверяет калибровки для текущих параметров съемки.
        """
        checks = await self._check_all_calibrations()

        stale_calibrations = [c for c in checks if not c.is_fresh]

        if stale_calibrations:
            decision = AgentDecision(
                agent=self.name,
                decision_type="CALIBRATION_STALE",
                inputs={"stale_count": len(stale_calibrations)},
                outputs={"checks": [c.model_dump() for c in checks]},
                rationale=f"Обнаружено {len(stale_calibrations)} устаревших калибровок",
                confidence=1.0,
            )
            self.log_decision(decision)
            return decision

        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет действия по обновлению калибровок."""
        if decision.decision_type == "CALIBRATION_STALE":
            checks = decision.outputs.get("checks", [])

            # Публикуем рекомендации
            for check_data in checks:
                check = CalibrationCheck(**check_data)
                if not check.is_fresh:
                    await event_bus.publish(
                        "ALERT",
                        {
                            "level": "WARNING",
                            "message": f"Калибровка {check.master_type} устарела: {check.recommendation}",
                            "agent": self.name,
                            "timestamp": datetime.now().isoformat(),
                        },
                    )

            return True

        return False

    async def _on_new_frame(self, data: Dict[str, Any]) -> None:
        """Проверяет калибровки для каждого нового кадра."""
        frame = data.get("frame", {})
        if not frame:
            return

        # Проверяем, есть ли подходящий мастер
        check = await self._check_calibration_for_frame(frame)

        if check and not check.is_fresh:
            logger.warning(f"Stale calibration for frame: {check.recommendation}")

    async def _on_masters_indexed(self, data: Dict[str, Any]) -> None:
        """Обработка события индексации мастеров."""
        logger.info("📚 Masters library indexed, checking freshness...")

        # Проверяем все калибровки
        await self.analyze(
            AgentContext(
                current_metrics=observatory_state.current_metrics,
                weather=observatory_state.weather,
                astronomy=observatory_state.astronomy,
                sequence_state={},
                safety_status=observatory_state.safety_status,
                active_alerts=[],
            )
        )

    async def _check_all_calibrations(self) -> List[CalibrationCheck]:
        """Проверяет все типы калибровок."""
        checks = []

        # Получаем текущие параметры съемки
        current_temp = observatory_state.current_metrics.get("camera_temp", -15.0)
        current_exposure = observatory_state.current_metrics.get("exposure_time", 60.0)
        current_gain = observatory_state.current_metrics.get("gain", 85)
        current_filter = observatory_state.current_metrics.get("filter")

        # Проверяем Bias
        bias_check = await self._check_master(
            "BIAS", temperature=current_temp, gain=current_gain
        )
        if bias_check:
            checks.append(bias_check)

        # Проверяем Dark
        dark_check = await self._check_master(
            "DARK",
            temperature=current_temp,
            exposure=current_exposure,
            gain=current_gain,
        )
        if dark_check:
            checks.append(dark_check)

        # Проверяем Flat
        if current_filter:
            flat_check = await self._check_master(
                "FLAT",
                temperature=current_temp,
                filter_name=current_filter,
                gain=current_gain,
            )
            if flat_check:
                checks.append(flat_check)

        return checks

    async def _check_calibration_for_frame(
        self, frame: Dict[str, Any]
    ) -> Optional[CalibrationCheck]:
        """Проверяет калибровку для конкретного кадра."""
        image_type = frame.get("image_type", "LIGHT")

        if image_type != "LIGHT":
            return None

        temperature = frame.get("temperature", -15.0)
        exposure = frame.get("exposure_time", 60.0)
        gain = frame.get("gain", 85)
        filter_name = frame.get("filter")

        # Проверяем Dark
        dark_check = await self._check_master(
            "DARK", temperature=temperature, exposure=exposure, gain=gain
        )

        return dark_check

    async def _check_master(
        self,
        master_type: str,
        temperature: float,
        exposure: Optional[float] = None,
        gain: Optional[int] = None,
        filter_name: Optional[str] = None,
    ) -> Optional[CalibrationCheck]:
        """Проверяет наличие и свежесть мастера."""
        params = {
            "temperature": temperature,
            "exposure": exposure,
            "gain": gain,
            "filter": filter_name,
        }

        # Ищем подходящий мастер
        matching_master = self.masters_auditor.find_matching_master(
            image_type=master_type,
            temperature=temperature,
            exposure=exposure,
            gain=gain,
            filter_name=filter_name,
            temp_tolerance=self.temperature_tolerance,
        )

        if not matching_master:
            return CalibrationCheck(
                master_type=master_type,
                params=params,
                matching_master=None,
                is_fresh=False,
                recommendation=f"Не найден подходящий {master_type} мастер. Требуется съемка.",
            )

        # Проверяем свежесть
        date_obs = matching_master.get("date_obs", "")
        if date_obs:
            try:
                master_date = datetime.fromisoformat(date_obs.replace("Z", "+00:00"))
                age_days = (datetime.now() - master_date).days

                threshold = self.freshness_thresholds.get(master_type, 30)
                is_fresh = age_days <= threshold

                recommendation = ""
                if not is_fresh:
                    recommendation = (
                        f"{master_type} мастер устарел ({age_days} дней). "
                        f"Рекомендуется обновить (порог: {threshold} дней)."
                    )

                return CalibrationCheck(
                    master_type=master_type,
                    params=params,
                    matching_master=matching_master,
                    is_fresh=is_fresh,
                    age_days=age_days,
                    recommendation=recommendation,
                )

            except (ValueError, TypeError) as e:
                logger.warning(f"Cannot parse master date: {date_obs}")

        # Если дата не указана, считаем мастер свежим
        return CalibrationCheck(
            master_type=master_type,
            params=params,
            matching_master=matching_master,
            is_fresh=True,
            recommendation="Мастер найден",
        )
