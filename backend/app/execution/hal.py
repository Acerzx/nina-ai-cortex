import logging
from app.shadow_engine.state_tracker import state_tracker
from app.core.events import event_bus
from app.core.config import settings

logger = logging.getLogger("HAL")


class HAL:
    def __init__(self):
        self._safety_status = "UNKNOWN"
        self._camera_busy = False
        self._critical_phase = False
        self._current_altitude = 90.0

    async def start(self):
        event_bus.subscribe("PROMETHEUS_UPDATE", self._on_metrics)
        event_bus.subscribe("LOG_EVENT", self._on_log)

    async def _on_metrics(self, data: dict):
        if "mount_altitude" in data and data["mount_altitude"] is not None:
            self._current_altitude = data["mount_altitude"]

    async def _on_log(self, data: dict):
        msg = data.get("message", "")
        if "Safety Monitor" in msg and "UNSAFE" in msg:
            self._safety_status = "UNSAFE"
        elif "Safety Monitor" in msg and "SAFE" in msg:
            self._safety_status = "SAFE"
        if "Starting exposure" in msg:
            self._camera_busy = True
        elif "Exposure completed" in msg:
            self._camera_busy = False

    def validate_slew(self, target_alt: float = None) -> tuple[bool, str]:
        if not settings.hal.enabled:
            return True, "HAL disabled"
        if self._safety_status == "UNSAFE":
            return False, "Safety Monitor is UNSAFE"
        if self._critical_phase:
            return False, "Critical phase in progress"

        # Проверка лимитов высоты
        alt_to_check = target_alt if target_alt is not None else self._current_altitude
        if alt_to_check < settings.hal.min_altitude_limit:
            return (
                False,
                f"Target altitude {alt_to_check} below limit {settings.hal.min_altitude_limit}",
            )

        return True, "OK"

    def validate_trigger_injection(self, trigger_name: str) -> tuple[bool, str]:
        if not settings.hal.enabled:
            return True, "HAL disabled"
        if self._camera_busy and trigger_name not in ["InterruptWhenRMSAbove"]:
            return False, "Camera is busy"
        if state_tracker.state.is_approaching_shutdown:
            return False, "Approaching shutdown"
        return True, "OK"


hal = HAL()
