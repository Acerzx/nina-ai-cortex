"""
Guardian Agent — обеспечивает безопасность оборудования и данных.
Имеет наивысший приоритет в системе.

ИСПРАВЛЕНО: Использует trigger_emulator вместо прямого httpx.
"""

import logging
from typing import Dict, Any, Optional
from datetime import datetime
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.execution.trigger_emulator import trigger_emulator
from app.execution.hal import hal
from app.execution.device_commander import device_commander

logger = logging.getLogger("GuardianAgent")


class GuardianAgent(BaseAgent):
    """
    Агент безопасности.

    Responsibilities:
    - Мониторинг погодных условий (ветер, облачность, влажность)
    - Контроль безопасности оборудования (лимиты высоты, коллизии)
    - Автоматическая парковка при критических условиях
    - Перехват Shutdown PC
    - Управление автофокусом и гидированием
    """

    def __init__(self):
        super().__init__(name="Guardian", role="Safety & Security")

        # Пороговые значения безопасности
        self.safety_thresholds = {
            "wind_speed_park": 20.0,
            "cloud_cover_pause": 80.0,
            "humidity_warning": 90.0,
            "rms_recalibration": 3.0,
            "temperature_alarm": 5.0,
        }

        self._parked = False
        self._paused = False

    async def initialize(self):
        """Инициализация агента безопасности."""
        await super().initialize()

        event_bus.subscribe("ALERT", self._handle_alert)
        event_bus.subscribe("LOG_EVENT", self._handle_log_event)

        logger.info("✅ Guardian Agent initialized with safety thresholds:")
        for key, value in self.safety_thresholds.items():
            logger.info(f"   - {key}: {value}")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("ALERT", self._handle_alert)
        event_bus.unsubscribe("LOG_EVENT", self._handle_log_event)

        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """Анализирует контекст безопасности."""
        if await self._check_critical_conditions():
            decision = AgentDecision(
                agent=self.name,
                decision_type="EMERGENCY_PARK",
                inputs={"reason": "Critical safety conditions detected"},
                outputs={"action": "park_mount"},
                rationale="Критические условия безопасности - необходима немедленная парковка",
                confidence=1.0,
            )
            self.log_decision(decision)
            return decision

        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет решение безопасности."""
        if decision.decision_type == "EMERGENCY_PARK":
            return await self._emergency_park()
        elif decision.decision_type == "TRIGGER_AUTOFOCUS":
            return await self._trigger_autofocus(
                decision.inputs.get("reason", "AI decision")
            )
        elif decision.decision_type == "TRIGGER_DITHER":
            return await self._trigger_dither(
                decision.inputs.get("reason", "AI decision")
            )

        return False

    async def _handle_alert(self, data: Dict[str, Any]) -> None:
        """Обработка алертов от Watcher."""
        level = data.get("level", "INFO")

        if level == "CRITICAL":
            await self._handle_critical_alert(data)
        elif level == "WARNING":
            await self._handle_warning_alert(data)

    async def _handle_log_event(self, data: Dict[str, Any]) -> None:
        """Обработка событий из логов."""
        event_type = data.get("event_type", "")

        if event_type == "safety_unsafe":
            await self._emergency_park()
        elif event_type == "guiding_lost":
            await self._trigger_guider_calibration("Guiding lost")

    async def _check_critical_conditions(self) -> bool:
        """Проверяет критические условия безопасности."""
        if observatory_state.safety_status == "UNSAFE":
            logger.critical("🚨 Safety Monitor UNSAFE - emergency park required")
            return True

        wind_speed = observatory_state.weather.get("wind_speed")
        if wind_speed and wind_speed > self.safety_thresholds["wind_speed_park"]:
            logger.critical(f"🚨 Wind speed {wind_speed} m/s - park required")
            return True

        cloud_cover = observatory_state.weather.get("cloud_cover")
        if cloud_cover and cloud_cover > 95.0:
            logger.warning(f"☁️ Cloud cover {cloud_cover}% - consider parking")
            return True

        return False

    async def _handle_critical_alert(self, data: Dict[str, Any]) -> None:
        """Обработка критического алерта."""
        message = data.get("message", "")
        context = data.get("context", {})

        logger.critical(f"🚨 CRITICAL ALERT: {message}")

        metric = context.get("metric", "")

        if metric in ["WIND_GUST", "WIND_SPEED"]:
            await self._emergency_park()
        elif metric in ["RMS_RA", "RMS_DEC"]:
            rms_value = context.get("current_value", 0)
            if rms_value > self.safety_thresholds["rms_recalibration"]:
                await self._trigger_guider_calibration("High RMS detected")
        elif metric == "CAMERA_TEMP":
            await self._generate_temperature_alarm(context)

    async def _handle_warning_alert(self, data: Dict[str, Any]) -> None:
        """Обработка предупреждения."""
        message = data.get("message", "")
        context = data.get("context", {})

        logger.warning(f"⚠️ WARNING: {message}")

        metric = context.get("metric", "")

        if metric == "HFR":
            # ИСПРАВЛЕНО: Используем trigger_emulator вместо прямого httpx
            await self._trigger_autofocus("HFR degradation detected")
        elif metric == "RMS_RA" or metric == "RMS_DEC":
            await self._trigger_dither("High RMS detected")

    async def _emergency_park(self) -> bool:
        """Аварийная парковка монтировки."""
        if self._parked:
            logger.warning("Mount already parked")
            return True

        logger.critical("🅿️ EMERGENCY PARK initiated")

        try:
            is_safe, reason = hal.validate_slew()
            if not is_safe:
                logger.error(f"Park blocked by HAL: {reason}")
                return False

            result = await device_commander.send_action("Mount", "Park")

            if result.get("status") == "success":
                self._parked = True
                logger.info("✅ Mount parked successfully")

                await event_bus.publish(
                    "ALERT",
                    {
                        "level": "CRITICAL",
                        "message": "Mount parked due to safety conditions",
                        "agent": self.name,
                        "timestamp": datetime.now().isoformat(),
                    },
                )

                return True
            else:
                logger.error(f"Failed to park mount: {result}")
                return False

        except Exception as e:
            logger.error(f"Error during emergency park: {e}")
            return False

    async def _trigger_autofocus(self, reason: str) -> bool:
        """
        Запускает автофокус через TriggerEmulator.
        """
        if observatory_state.is_autofocus_running:
            logger.warning("Autofocus already running")
            return True

        logger.info(f"🔍 Triggering autofocus: {reason}")

        try:
            # ИСПРАВЛЕНО: используем trigger_emulator с правильным путём
            success = await trigger_emulator.fire_trigger(
                "autofocus",
                reason=reason,
            )

            if success:
                decision = AgentDecision(
                    agent=self.name,
                    decision_type="TRIGGER_AUTOFOCUS",
                    inputs={"reason": reason},
                    outputs={"success": True},
                    rationale=f"Автофокус запущен: {reason}",
                    confidence=0.95,
                )
                self.log_decision(decision)
            else:
                logger.warning(f"⚠️ Autofocus trigger failed or blocked")

            return success

        except Exception as e:
            logger.error(f"Error triggering autofocus: {e}")
            return False

    async def _trigger_dither(self, reason: str) -> bool:
        """Запускает дизеринг."""
        logger.info(f"🎯 Triggering dither: {reason}")

        try:
            success = await trigger_emulator.fire_trigger("dither", reason)

            if success:
                decision = AgentDecision(
                    agent=self.name,
                    decision_type="TRIGGER_DITHER",
                    inputs={"reason": reason},
                    outputs={"success": True},
                    rationale=f"Дизеринг запущен: {reason}",
                    confidence=0.9,
                )
                self.log_decision(decision)

            return success

        except Exception as e:
            logger.error(f"Error triggering dither: {e}")
            return False

    async def _trigger_guider_calibration(self, reason: str) -> bool:
        """Запускает калибровку гида."""
        if observatory_state.is_guiding_active:
            logger.warning("Cannot calibrate while guiding is active")
            return False

        logger.info(f"🎯 Triggering guider calibration: {reason}")

        try:
            success = await trigger_emulator.fire_trigger("guider_calibration", reason)

            if success:
                decision = AgentDecision(
                    agent=self.name,
                    decision_type="TRIGGER_GUIDER_CALIBRATION",
                    inputs={"reason": reason},
                    outputs={"success": True},
                    rationale=f"Калибровка гида запущена: {reason}",
                    confidence=0.9,
                )
                self.log_decision(decision)

            return success

        except Exception as e:
            logger.error(f"Error triggering guider calibration: {e}")
            return False

    async def _generate_temperature_alarm(self, context: Dict[str, Any]) -> None:
        """Генерирует аларм по температуре."""
        current_temp = context.get("current_value")
        setpoint = context.get("setpoint")

        message = (
            f"Temperature deviation: current {current_temp}°C, setpoint {setpoint}°C"
        )

        await event_bus.publish(
            "ALERT",
            {
                "level": "WARNING",
                "message": message,
                "agent": self.name,
                "timestamp": datetime.now().isoformat(),
                "context": context,
            },
        )

    async def handle_critical_alert(self, data: Dict[str, Any]) -> None:
        """Обработка критического алерта (вызывается Orchestrator'ом)."""
        await self._handle_critical_alert(data)
