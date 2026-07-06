import logging
from typing import Optional
from app.execution.nina_client import nina_client
from app.shadow_engine.state_tracker import state_tracker
from app.core.config import settings

logger = logging.getLogger("TriggerEmulator")


class TriggerEmulator:
    """
    Эмулирует срабатывание триггеров через Advanced API.
    Учитывает FLAT_MODE и отсутствие плагинов (Graceful Degradation).
    """

    # Маппинг наших внутренних имен на ID триггеров в N.I.N.A.
    TRIGGER_MAP = {
        "autofocus": "InjectAutofocusTrigger",
        "dither": "DitherAfterExposures",
        "guider_calibration": "StartGuiding",
        "phd2_settle": "Phd2SettleTrigger",
        "flexure_compensation": "FlexureCompensatorTrigger",
    }

    async def fire_trigger(
        self, trigger_name: str, reason: str = "AI Agent Decision"
    ) -> bool:
        """
        Эмулирует срабатывание триггера.
        Возвращает True если успешно, False если заблокировано или плагин отсутствует.
        """
        logger.info(f"🔥 Attempting to fire trigger: {trigger_name} (Reason: {reason})")

        # 1. Проверка FLAT_MODE (Жесткий блок для автофокуса и гидирования)
        if state_tracker.state.is_flat_mode:
            if trigger_name in ["autofocus", "dither", "guider_calibration"]:
                logger.warning(
                    f"🛑 BLOCKED: Trigger '{trigger_name}' ignored during FLAT_MODE"
                )
                return False

        # 2. Проверка отсутствия плагинов (Архитектурное решение №1)
        if (
            trigger_name == "dither"
            and settings.plugins_status.dither_inject == "NOT_INSTALLED"
        ):
            logger.warning(
                "[MOCK EXECUTION] Dither plugin not yet installed. Action simulated."
            )
            return False

        if (
            trigger_name == "guider_calibration"
            and settings.plugins_status.guider_calibration == "NOT_INSTALLED"
        ):
            logger.warning(
                "[MOCK EXECUTION] Guider calibration plugin not yet installed. Action simulated."
            )
            return False

        # 3. Проверка критической фазы секвенсора
        if state_tracker.state.is_approaching_shutdown:
            logger.warning(
                f"🛑 BLOCKED: Trigger '{trigger_name}' ignored - approaching shutdown"
            )
            return False

        # 4. Вызов Advanced API
        nina_trigger_name = self.TRIGGER_MAP.get(trigger_name, trigger_name)
        try:
            # Эндпоинт Advanced API для эмуляции триггеров (зависит от реализации плагина Advanced API)
            # Обычно это POST /advanced/trigger/fire с именем триггера
            response = await nina_client.post(
                "trigger/fire", json_data={"triggerName": nina_trigger_name}
            )
            logger.info(f"✅ Trigger '{trigger_name}' fired successfully: {response}")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to fire trigger '{trigger_name}': {e}")
            return False


trigger_emulator = TriggerEmulator()
