import logging
from app.core.events import event_bus
from app.shadow_engine.state_tracker import state_tracker
from app.execution.python_bridge import python_bridge

logger = logging.getLogger("SafetyInterceptor")


class SafetyInterceptor:
    def __init__(self):
        self._user_active = False

    async def start(self):
        event_bus.subscribe("SEQUENCE_ITEM_STARTED", self._handle_item)

    async def _handle_item(self, data: dict):
        item_type = data.get("Type", "")
        if "ShutdownPcInstruction" in item_type or "ShutdownNina" in item_type:
            path = state_tracker.state.container_path
            # Проверяем, находимся ли мы в финальном блоке (EndAreaContainer / Деактивация)
            is_final_stage = any(
                "EndArea" in p or "Деактивация" in p or "Отключение" in p for p in path
            )

            if is_final_stage:
                logger.warning("⚠️ SHUTDOWN INSTRUCTION IN FINAL STAGE DETECTED!")
                if self._user_active:
                    await self._intercept()

    async def _intercept(self):
        await python_bridge.inject_shutdown_intercept(delay_minutes=10)
        await event_bus.publish(
            "ALERT", {"level": "CRITICAL", "message": "Shutdown intercepted"}
        )


safety_interceptor = SafetyInterceptor()
