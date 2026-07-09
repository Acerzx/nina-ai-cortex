"""
Calibrator Agent — управляет библиотекой мастер-кадров.

ИСПРАВЛЕНО (audit 7.2):
- Все магические числа вынесены в settings.thresholds.calibrator
- Пороги свежести мастеров читаются из конфигурации
- Cooldown для повторяющихся алертов из конфига
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.config import settings
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

    ИСПРАВЛЕНО (audit 7.2):
    - freshness_thresholds извлекаются из settings.thresholds.calibrator
    - Магические числа 90/30/7 заменены на именованные константы из конфига
    - temperature_tolerance и alert_cooldown_seconds из конфига
    """

    def __init__(self, masters_auditor: MastersLibraryAuditor):
        super().__init__(name="Calibrator", role="Calibration Management")
        self.masters_auditor = masters_auditor

        # ИСПРАВЛЕНО (audit 7.2): Пороги извлекаются из конфига
        cal_cfg = settings.thresholds.calibrator
        self.freshness_thresholds = {
            "BIAS": cal_cfg.bias_freshness_days,
            "DARK": cal_cfg.dark_freshness_days,
            "FLAT": cal_cfg.flat_freshness_days,
        }
        self.temperature_tolerance = cal_cfg.temperature_tolerance

        # Throttling для повторяющихся алертов
        self._recent_alerts: Dict[str, datetime] = {}
        self._alert_cooldown_seconds = cal_cfg.alert_cooldown_seconds

    async def initialize(self):
        """Инициализация агента калибровки."""
        await super().initialize()
        event_bus.subscribe("NEW_FRAME", self._on_new_frame)
        event_bus.subscribe("MASTERS_INDEXED", self._on_masters_indexed)

        logger.info("✅ Calibrator Agent initialized with freshness thresholds:")
        for key, value in self.freshness_thresholds.items():
            logger.info(f"   - {key}: {value} days")
        logger.info(f"   - temperature_tolerance: {self.temperature_tolerance}°C")
        logger.info(f"   - alert_cooldown: {self._alert_cooldown_seconds}s")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("NEW_FRAME", self._on_new_frame)
        event_bus.unsubscribe("MASTERS_INDEXED", self._on_masters_indexed)
        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """Проверяет калибровки для текущих параметров съемки."""
        checks = await self._check_all_calibrations()
        stale_calibrations = [c for c in checks if not c.is_fresh]

        if stale_calibrations:
            decision = AgentDecision(
                agent=self.name,
                decision_type="CALIBRATION_STALE",
                inputs={"stale_count": len(stale_calibrations)},
                outputs={"checks": [c.model_dump() for c in checks]},
                rationale=(
                    f"Обнаружено {len(stale_calibrations)} устаревших калибровок"
                ),
                confidence=1.0,
            )
            self.log_decision(decision)
            return decision
        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет действия по обновлению калибровок."""
        if decision.decision_type == "CALIBRATION_STALE":
            checks = decision.outputs.get("checks", [])
            for check_data in checks:
                check = CalibrationCheck(**check_data)
                if not check.is_fresh:
                    # Проверяем throttling перед публикацией
                    alert_key = f"{check.master_type}_{check.recommendation}"
                    if not self._is_alert_in_cooldown(alert_key):
                        await event_bus.publish(
                            "ALERT",
                            {
                                "level": "WARNING",
                                "message": (
                                    f"Калибровка {check.master_type} "
                                    f"устарела: {check.recommendation}"
                                ),
                                "agent": self.name,
                                "timestamp": datetime.now().isoformat(),
                            },
                        )
                        self._recent_alerts[alert_key] = datetime.now()
            return True
        return False

    async def _on_new_frame(self, data: Dict[str, Any]) -> None:
        """Проверяет калибровки для каждого нового кадра."""
        frame = data.get("frame", {})
        if not frame:
            return

        check = await self._check_calibration_for_frame(frame)
        if check and not check.is_fresh:
            # Throttling
            alert_key = f"{check.master_type}_{check.recommendation}"
            if self._is_alert_in_cooldown(alert_key):
                logger.debug(f"Calibration alert throttled: {alert_key}")
                return

            self._recent_alerts[alert_key] = datetime.now()
            logger.warning(f"Stale calibration for frame: {check.recommendation}")

    def _is_alert_in_cooldown(self, alert_key: str) -> bool:
        """Проверяет, находится ли алерт в cooldown периоде."""
        last_time = self._recent_alerts.get(alert_key)
        if not last_time:
            return False
        elapsed = (datetime.now() - last_time).total_seconds()
        return elapsed < self._alert_cooldown_seconds

    async def _on_masters_indexed(self, data: Dict[str, Any]) -> None:
        """Обработка события индексации мастеров."""
        logger.info("📚 Masters library indexed, checking freshness...")
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

        current_temp = observatory_state.current_metrics.get("camera_temp")
        if current_temp is None:
            current_temp = -15.0
        current_exposure = (
            observatory_state.current_metrics.get("exposure_time") or 60.0
        )
        current_gain = observatory_state.current_metrics.get("gain") or 85
        current_filter = observatory_state.current_metrics.get("filter")

        bias_check = await self._check_master(
            "BIAS", temperature=current_temp, gain=current_gain
        )
        if bias_check:
            checks.append(bias_check)

        dark_check = await self._check_master(
            "DARK",
            temperature=current_temp,
            exposure=current_exposure,
            gain=current_gain,
        )
        if dark_check:
            checks.append(dark_check)

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

        dark_check = await self._check_master(
            "DARK",
            temperature=temperature,
            exposure=exposure,
            gain=gain,
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
        """
        Проверяет наличие и свежесть мастера.
        Использует thresholds из конфига для проверки свежести.
        """
        if temperature is None:
            logger.debug(f"Skipping {master_type} check: temperature is None")
            return None

        params = {
            "temperature": temperature,
            "exposure": exposure,
            "gain": gain,
            "filter": filter_name,
        }

        if not self.masters_auditor:
            logger.debug("Masters auditor not available")
            return None

        try:
            matching_master = self.masters_auditor.find_matching_master(
                image_type=master_type,
                temperature=temperature,
                exposure=exposure,
                gain=gain,
                filter_name=filter_name,
                temp_tolerance=self.temperature_tolerance,
            )
        except Exception as e:
            logger.error(f"Error finding matching {master_type} master: {e}")
            return None

        if not matching_master:
            return CalibrationCheck(
                master_type=master_type,
                params=params,
                matching_master=None,
                is_fresh=False,
                recommendation=(
                    f"Не найден подходящий {master_type} мастер. Требуется съемка."
                ),
            )

        # Проверяем свежесть мастера
        date_obs = matching_master.get("date_obs", "")
        if date_obs:
            try:
                date_str = date_obs.replace("Z", "+00:00")
                try:
                    master_date = datetime.fromisoformat(date_str)
                except ValueError:
                    master_date = datetime.strptime(date_obs[:10], "%Y-%m-%d")
                if master_date.tzinfo is not None:
                    master_date = master_date.replace(tzinfo=None)

                age_days = (datetime.now() - master_date).days

                # ИСПРАВЛЕНО (audit 7.2): Порог свежести из конфига
                threshold = self.freshness_thresholds.get(master_type, 30)
                is_fresh = age_days <= threshold

                recommendation = ""
                if not is_fresh:
                    recommendation = (
                        f"{master_type} мастер устарел "
                        f"({age_days} дней). "
                        f"Рекомендуется обновить "
                        f"(порог: {threshold} дней)."
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
                logger.debug(f"Cannot parse master date '{date_obs}': {e}")
                return CalibrationCheck(
                    master_type=master_type,
                    params=params,
                    matching_master=matching_master,
                    is_fresh=True,
                    recommendation="Мастер найден (дата не указана)",
                )
        return None

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        HOOK: Принимает решение на основе контекста.
        Реализация абстрактного метода из BaseAgent.
        """
        checks = await self._check_all_calibrations()
        stale_calibrations = [c for c in checks if not c.is_fresh]

        if stale_calibrations:
            return AgentDecision(
                agent=self.name,
                decision_type="CALIBRATION_STALE",
                inputs={"stale_count": len(stale_calibrations)},
                outputs={"checks": [c.model_dump() for c in checks]},
                rationale=(
                    f"Обнаружено {len(stale_calibrations)} устаревших калибровок"
                ),
                confidence=1.0,
            )
        return None

    async def _perform_action(self, decision: AgentDecision) -> bool:
        """
        HOOK: Выполняет действие решения.
        Реализация абстрактного метода из BaseAgent.
        """
        if decision.decision_type == "CALIBRATION_STALE":
            checks = decision.outputs.get("checks", [])

            for check_data in checks:
                check = CalibrationCheck(**check_data)
                if not check.is_fresh:
                    # Проверяем throttling перед публикацией
                    alert_key = f"{check.master_type}_{check.recommendation}"
                    if not self._is_alert_in_cooldown(alert_key):
                        await event_bus.publish(
                            "ALERT",
                            {
                                "level": "WARNING",
                                "message": (
                                    f"Калибровка {check.master_type} "
                                    f"устарела: {check.recommendation}"
                                ),
                                "agent": self.name,
                                "timestamp": datetime.now().isoformat(),
                            },
                        )
                        self._recent_alerts[alert_key] = datetime.now()

            return True

        return False
