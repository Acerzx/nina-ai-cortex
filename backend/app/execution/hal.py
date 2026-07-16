"""
HAL (Hardware Abstraction Layer) — финальная валидация всех команд перед
отправкой в N.I.N.A.

ИСПРАВЛЕНО (audit 3.3): флаг _critical_phase теперь корректно устанавливается
через подписку на события Shadow Engine (SEQUENCE_ITEM_STARTED/COMPLETED).
При входе в критические инструкции (ShutdownPcInstruction, ShutdownNina,
MeridianFlipInstruction, ParkScopeInstruction, TwoPointPolarAlignmentSequenceItem)
флаг устанавливается в True, блокируя небезопасные триггеры.
"""

import logging
from typing import Set
from app.shadow_engine.state_tracker import state_tracker
from app.core.events import event_bus
from app.core.config import settings

logger = logging.getLogger("HAL")


# Типы инструкций, во время выполнения которых нельзя вмешиваться
# в работу оборудования (автофокус, гидирование, slew и т.д.)
CRITICAL_INSTRUCTION_TYPES: Set[str] = {
    "ShutdownPcInstruction",
    "ShutdownNina",
    "MeridianFlipInstruction",
    "ParkScopeInstruction",
    "TwoPointPolarAlignmentSequenceItem",
    "CenterAfterDriftInstruction",
    "CenterInstruction",
    "SlewScopeInstruction",
    "SlewScopeToAltAzInstruction",
}


class HAL:
    """
    Hardware Abstraction Layer — финальный барьер безопасности.
    Валидирует все команды перед отправкой в N.I.N.A. Advanced API.

    Проверки:
    1. Safety Monitor (UNSAFE → блокировка всех команд)
    2. Критическая фаза (Shutdown/MeridianFlip/Park → блокировка триггеров)
    3. Занятость камеры (exposure → блокировка InterruptWhenRMSAbove и аналогов)
    4. Лимит высоты (target_altitude < min_altitude_limit → блокировка slew)
    5. Приближение к Shutdown (is_approaching_shutdown → блокировка переменных)
    """

    def __init__(self):
        self._safety_status = "UNKNOWN"
        self._camera_busy = False
        self._critical_phase = False
        self._current_altitude = 90.0
        # Название текущей критической инструкции (для логов)
        self._current_critical_item: str = ""

    async def start(self):
        """Запуск HAL — подписка на события EventBus."""
        event_bus.subscribe("PROMETHEUS_UPDATE", self._on_metrics)
        event_bus.subscribe("LOG_EVENT", self._on_log)
        # ИСПРАВЛЕНО (audit 3.3): подписка на события Shadow Engine
        # для корректного управления флагом _critical_phase
        event_bus.subscribe("SEQUENCE_ITEM_STARTED", self._on_sequence_item_started)
        event_bus.subscribe("SEQUENCE_ITEM_COMPLETED", self._on_sequence_item_completed)
        event_bus.subscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)
        logger.info("✅ HAL started (with critical phase tracking)")

    async def stop(self):
        """Корректная остановка HAL."""
        try:
            event_bus.unsubscribe("PROMETHEUS_UPDATE", self._on_metrics)
            event_bus.unsubscribe("LOG_EVENT", self._on_log)
            event_bus.unsubscribe(
                "SEQUENCE_ITEM_STARTED", self._on_sequence_item_started
            )
            event_bus.unsubscribe(
                "SEQUENCE_ITEM_COMPLETED", self._on_sequence_item_completed
            )
            event_bus.unsubscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)
            logger.info("🛑 HAL stopped")
        except Exception as e:
            logger.debug(f"Error stopping HAL: {e}")

    async def _on_metrics(self, data: dict):
        """Обновление метрик из Prometheus/InfluxDB."""
        if "mount_altitude" in data and data["mount_altitude"] is not None:
            self._current_altitude = data["mount_altitude"]

    async def _on_log(self, data: dict):
        """
        Обработка событий из логов N.I.N.A.
        ИСПРАВЛЕНО (В-3): Сброс _critical_phase при детекции ошибки.
        Раньше, если критическая инструкция падала без SEQUENCE_ITEM_COMPLETED,
        флаг оставался True навсегда, блокируя все последующие триггеры.
        """
        msg = data.get("message", "")

        if "Safety Monitor" in msg and "UNSAFE" in msg:
            self._safety_status = "UNSAFE"
            logger.warning("🚨 HAL: Safety Monitor -> UNSAFE")
        elif "Safety Monitor" in msg and "SAFE" in msg:
            self._safety_status = "SAFE"
            logger.info("✅ HAL: Safety Monitor -> SAFE")

        if "Starting exposure" in msg:
            self._camera_busy = True
        elif "Exposure completed" in msg or "Exposure failed" in msg:
            self._camera_busy = False

        # ИСПРАВЛЕНО (В-3): Сброс critical phase при ошибке
        # Если во время критической фазы произошла ошибка,
        # инструкция не завершится нормально и SEQUENCE_ITEM_COMPLETED
        # может не прийти. Сбрасываем флаг для предотвращения блокировки.
        msg_lower = msg.lower()
        if self._critical_phase and (
            "error" in msg_lower or "failed" in msg_lower or "exception" in msg_lower
        ):
            logger.warning(
                f"⚠️ HAL: Critical phase reset due to error in log: "
                f"{msg[:100]}... (previous instruction: {self._current_critical_item})"
            )
            self._critical_phase = False
            self._current_critical_item = ""

    async def _on_sequence_item_started(self, data: dict):
        """
        ИСПРАВЛЕНО (audit 3.3): Установка флага _critical_phase при входе
        в критические инструкции секвенсора.
        """
        item_type = data.get("Type", "")
        item_name = data.get("Name", "")

        # Очищаем тип от namespace (NINA.Legacy.Instructions.X -> X)
        clean_type = item_type.split(".")[-1] if "." in item_type else item_type

        if clean_type in CRITICAL_INSTRUCTION_TYPES:
            self._critical_phase = True
            self._current_critical_item = clean_type
            logger.warning(
                f"🚨 HAL: CRITICAL PHASE entered "
                f"(instruction={clean_type}, name={item_name})"
            )
        else:
            # Для не-критических инструкций сбрасываем флаг, если он был
            if self._critical_phase:
                self._critical_phase = False
                self._current_critical_item = ""
                logger.info("✅ HAL: critical phase exited")

    async def _on_sequence_item_completed(self, data: dict):
        """
        ИСПРАВЛЕНО (audit 3.3): Сброс флага _critical_phase при выходе из
        критической инструкции.
        """
        item_type = data.get("Type", "")
        clean_type = item_type.split(".")[-1] if "." in item_type else item_type

        if clean_type in CRITICAL_INSTRUCTION_TYPES and self._critical_phase:
            self._critical_phase = False
            logger.info(
                f"✅ HAL: CRITICAL PHASE exited (completed instruction={clean_type})"
            )
            self._current_critical_item = ""

    async def _on_sequence_stopped(self, data: dict):
        """Сброс всех флагов при остановке секвенсора."""
        if self._critical_phase:
            logger.info("✅ HAL: critical phase reset on sequence stop")
            self._critical_phase = False
            self._current_critical_item = ""

    def validate_slew(self, target_alt: float = None) -> tuple:
        """
        Валидация команды slew (перемещение монтировки).
        Возвращает: (is_safe: bool, reason: str)
        """
        if not settings.hal.enabled:
            return True, "HAL disabled"

        if self._safety_status == "UNSAFE":
            return False, "Safety Monitor is UNSAFE"

        if self._critical_phase:
            return (
                False,
                f"Critical phase in progress: {self._current_critical_item}",
            )

        # Проверка лимитов высоты
        alt_to_check = target_alt if target_alt is not None else self._current_altitude
        if alt_to_check < settings.hal.min_altitude_limit:
            return (
                False,
                f"Target altitude {alt_to_check:.2f} below limit "
                f"{settings.hal.min_altitude_limit}",
            )

        return True, "OK"

    def validate_trigger_injection(self, trigger_name: str) -> tuple:
        """
        Валидация инжекта триггера через N.I.N.A. Advanced API.
        Возвращает: (is_safe: bool, reason: str)
        """
        if not settings.hal.enabled:
            return True, "HAL disabled"

        if self._safety_status == "UNSAFE":
            return False, "Safety Monitor is UNSAFE"

        # Во время критической фазы разрешены только триггеры прерывания
        # (например, InterruptWhenRMSAbove от PHD2Tools)
        if self._critical_phase:
            safe_during_critical = {
                "InterruptWhenRMSAbove",
                "RestartWhenSaturated",
            }
            if trigger_name not in safe_during_critical:
                return (
                    False,
                    f"Critical phase in progress: {self._current_critical_item}",
                )

        # Проверка занятости камеры
        if self._camera_busy and trigger_name not in ["InterruptWhenRMSAbove"]:
            return False, "Camera is busy (exposure in progress)"

        # Проверка приближения к Shutdown
        if state_tracker.state.is_approaching_shutdown:
            return False, "Approaching shutdown"

        return True, "OK"

    def get_status(self) -> dict:
        """Возвращает текущий статус HAL (для API /health)."""
        return {
            "enabled": settings.hal.enabled,
            "safety_status": self._safety_status,
            "camera_busy": self._camera_busy,
            "critical_phase": self._critical_phase,
            "current_critical_item": self._current_critical_item,
            "current_altitude": self._current_altitude,
            "min_altitude_limit": settings.hal.min_altitude_limit,
        }


hal = HAL()
