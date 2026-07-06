import logging
import asyncio
from typing import Dict, Any, Optional
import httpx

from app.core.config import settings

logger = logging.getLogger("HomeAssistantBridge")


class HomeAssistantBridge:
    """
    Интеграция с Home Assistant для управления умным домом обсерватории.
    Устраняет Упрощение #26.
    """

    def __init__(self):
        # Настройки из settings.yaml (добавить в конфиг!)
        self.ha_url = getattr(settings, "home_assistant", {}).get(
            "url", "http://homeassistant.local:8123"
        )
        self.ha_token = getattr(settings, "home_assistant", {}).get("token", "")
        self.enabled = bool(self.ha_token)
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            headers = {
                "Authorization": f"Bearer {self.ha_token}",
                "Content-Type": "application/json",
            }
            self._client = httpx.AsyncClient(
                base_url=self.ha_url, headers=headers, timeout=10.0
            )
        return self._client

    async def call_service(
        self,
        domain: str,
        service: str,
        entity_id: str,
        service_data: Optional[Dict] = None,
        reason: str = "AI Action",
    ) -> bool:
        """
        Вызывает сервис Home Assistant.
        Пример: call_service("switch", "turn_on", "switch.observatory_flat_panel")
        """
        if not self.enabled:
            logger.debug("Home Assistant integration disabled (no token configured)")
            return False

        logger.info(f"🏠 HA: {domain}.{service} -> {entity_id} (Reason: {reason})")

        try:
            client = await self._get_client()
            payload = {"entity_id": entity_id, **(service_data or {})}

            response = await client.post(
                f"/api/services/{domain}/{service}", json=payload
            )
            response.raise_for_status()

            logger.info(f"✅ HA service called successfully")
            return True
        except httpx.ConnectError:
            logger.error("❌ Cannot connect to Home Assistant")
            return False
        except Exception as e:
            logger.error(f"❌ HA service call failed: {e}")
            return False

    async def get_entity_state(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Получает текущее состояние сущности HA."""
        if not self.enabled:
            return None

        try:
            client = await self._get_client()
            response = await client.get(f"/api/states/{entity_id}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Failed to get HA entity state: {e}")
            return None

    # ===== Специфичные методы для обсерватории =====

    async def turn_on_flat_panel(self, brightness: int = 100) -> bool:
        """Включает Flat Panel (через switch или light)."""
        return await self.call_service(
            "light",
            "turn_on",
            "light.observatory_flat_panel",
            {"brightness_pct": brightness},
            reason="Starting Flat sequence",
        )

    async def turn_off_flat_panel(self) -> bool:
        """Выключает Flat Panel."""
        return await self.call_service(
            "light",
            "turn_off",
            "light.observatory_flat_panel",
            reason="Flat sequence completed",
        )

    async def turn_on_dew_heater(self) -> bool:
        """Включает обогреватель (защита от росы)."""
        return await self.call_service(
            "switch",
            "turn_on",
            "switch.observatory_dew_heater",
            reason="Humidity above threshold",
        )

    async def power_off_observatory(self) -> bool:
        """Полное отключение питания обсерватории (через умное реле)."""
        return await self.call_service(
            "switch",
            "turn_off",
            "switch.observatory_main_power",
            reason="Session completed, safe power down",
        )

    async def publish_nina_status(self, status: str, details: str = "") -> bool:
        """Публикует статус N.I.N.A. в HA (через input_text или MQTT)."""
        return await self.call_service(
            "input_text",
            "set_value",
            "input_text.nina_status",
            {"value": f"{status}: {details}"},
            reason="Status update",
        )


home_assistant_bridge = HomeAssistantBridge()
