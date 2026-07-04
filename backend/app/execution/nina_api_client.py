"""
N.I.N.A. REST API Client (ninaAPI v2)
Отправляет команды на http://localhost:1888/v2/api
"""

import logging
import aiohttp
from typing import Dict, Any, Optional
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class NinaAPIClient:
    """Клиент для отправки команд в N.I.N.A. через REST API."""

    def __init__(self):
        self.settings = get_settings()
        self.base_url = self.settings.network.nina_api_host
        self.timeout = aiohttp.ClientTimeout(total=10)

    async def _request(
        self, method: str, endpoint: str, **kwargs
    ) -> Optional[Dict[str, Any]]:
        """Выполняет HTTP-запрос к ninaAPI."""
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        try:
            async with aiohttp.ClientSession(timeout=self.timeout) as session:
                async with session.request(method, url, **kwargs) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    else:
                        logger.error(
                            f"❌ API request failed: {response.status} - {await response.text()}"
                        )
                        return None
        except aiohttp.ClientError as e:
            logger.error(f"❌ API connection error: {e}")
            return None
        except Exception as e:
            logger.error(f"❌ Unexpected API error: {e}")
            return None

    async def get_equipment(self) -> Optional[Dict[str, Any]]:
        """Получает статус всего оборудования."""
        return await self._request("GET", "equipment")

    async def get_sequence_status(self) -> Optional[Dict[str, Any]]:
        """Получает статус секвенсора."""
        return await self._request("GET", "sequence")

    async def start_sequence(self) -> bool:
        """Запускает секвенсор."""
        result = await self._request("POST", "sequence/start")
        if result and result.get("Success"):
            logger.info("▶️ Sequence start command sent")
            return True
        return False

    async def stop_sequence(self) -> bool:
        """Останавливает секвенсор."""
        result = await self._request("POST", "sequence/stop")
        if result and result.get("Success"):
            logger.info("⏹️ Sequence stop command sent")
            return True
        return False

    async def set_global_variable(self, name: str, value: Any) -> bool:
        """Изменяет глобальную переменную Sequencer+."""
        payload = {"Name": name, "Value": str(value)}
        result = await self._request("POST", "sequence/global-variable", json=payload)
        if result and result.get("Success"):
            logger.info(f"🔄 Global variable '{name}' set to {value}")
            return True
        logger.error(f"❌ Failed to set global variable '{name}'")
        return False

    async def fire_trigger(self, trigger_name: str) -> bool:
        """Эмулирует срабатывание триггера (например, InjectAutofocus)."""
        payload = {"Name": trigger_name}
        result = await self._request("POST", "sequence/trigger", json=payload)
        if result and result.get("Success"):
            logger.info(f"⚡ Trigger '{trigger_name}' fired")
            return True
        logger.error(f"❌ Failed to fire trigger '{trigger_name}'")
        return False
