import logging
from typing import Any, Optional
from app.execution.nina_client import nina_client

logger = logging.getLogger("DeviceCommander")


class DeviceCommander:
    """
    Отправляет ASCOM команды напрямую в оборудование через плагин Device Commands.
    """

    VALID_DEVICES = [
        "Camera",
        "Mount",
        "Focuser",
        "FilterWheel",
        "Rotator",
        "Guider",
        "Dome",
        "Switch",
    ]

    async def send_action(
        self, device: str, action_name: str, raw_params: str = ""
    ) -> dict:
        """
        Вызывает ASCOM Action() для указанного устройства.
        """
        if device not in self.VALID_DEVICES:
            logger.error(f"❌ Invalid device: {device}")
            return {"status": "error", "message": "Invalid device"}

        logger.info(f"🎮 Sending ASCOM Action: {device}.{action_name}({raw_params})")

        try:
            payload = {"device": device, "action": action_name, "rawParams": raw_params}
            response = await nina_client.post("device/action", json_data=payload)
            logger.info(f"✅ Action executed: {response}")
            return response
        except Exception as e:
            logger.error(f"❌ Failed to send action: {e}")
            return {"status": "error", "message": str(e)}

    async def command_blind(
        self, device: str, command: str, raw_params: str = ""
    ) -> dict:
        """Вызывает ASCOM CommandBlind() (без ожидания ответа)."""
        logger.info(f"🎮 Sending ASCOM CommandBlind: {device} -> {command}")
        try:
            payload = {"device": device, "command": command, "rawParams": raw_params}
            return await nina_client.post("device/command-blind", json_data=payload)
        except Exception as e:
            logger.error(f"❌ Failed to send command blind: {e}")
            return {"status": "error", "message": str(e)}

    async def command_string(
        self, device: str, command: str, raw_params: str = ""
    ) -> dict:
        """Вызывает ASCOM CommandString() (с возвратом строки)."""
        logger.info(f"🎮 Sending ASCOM CommandString: {device} -> {command}")
        try:
            payload = {"device": device, "command": command, "rawParams": raw_params}
            return await nina_client.post("device/command-string", json_data=payload)
        except Exception as e:
            logger.error(f"❌ Failed to send command string: {e}")
            return {"status": "error", "message": str(e)}


device_commander = DeviceCommander()
