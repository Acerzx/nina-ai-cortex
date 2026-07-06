import logging
from typing import Any
from app.execution.nina_client import nina_client
from app.shadow_engine.state_tracker import state_tracker

logger = logging.getLogger("GlobalVarInjector")


class GlobalVarInjector:
    """
    Изменяет глобальные переменные Sequencer+ через Advanced API.
    """

    async def set_variable(
        self, name: str, value: Any, reason: str = "AI Optimization"
    ) -> bool:
        """
        Устанавливает новое значение для глобальной переменной.
        """
        logger.info(f"🔧 Setting global variable: {name} = {value} (Reason: {reason})")

        # Проверка: существует ли переменная в теневом графе?
        if name not in state_tracker.state.global_variables:
            logger.warning(f"⚠️ Variable '{name}' not found in sequence shadow graph")
            # Все равно пытаемся отправить, вдруг она динамическая

        # Проверка критической фазы
        if state_tracker.state.is_approaching_shutdown:
            logger.warning(
                f"🛑 BLOCKED: Variable change ignored - approaching shutdown"
            )
            return False

        try:
            # Advanced API эндпоинт для Sequencer+
            response = await nina_client.post(
                "sequence/global-variable",
                json_data={"name": name, "value": str(value)},
            )

            # Обновляем локальный стейт
            state_tracker.state.global_variables[name] = str(value)

            logger.info(f"✅ Variable '{name}' updated successfully")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to set variable '{name}': {e}")
            return False

    async def get_variable(self, name: str) -> Any:
        """Возвращает текущее значение переменной из стейта."""
        return state_tracker.state.global_variables.get(name)


global_var_injector = GlobalVarInjector()
