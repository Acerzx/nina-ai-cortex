"""
Home Assistant Bridge — интеграция с Home Assistant для управления умным домом
обсерватории.

ИСПРАВЛЕНО (audit 3.2): settings.home_assistant — это объект HomeAssistantConfig
(Pydantic BaseModel), а не dict. Заменили getattr(...).get(...) на прямой доступ
через атрибуты .url и .token.
"""

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
        # ИСПРАВЛЕНО (audit 3.2): прямой доступ к атрибутам Pydantic-модели.
        # settings.home_assistant — это HomeAssistantConfig с полями
        # enabled, url, token.
        self.ha_url = settings.home_assistant.url
        self.ha_token = settings.home_assistant.token
        # Интеграция активна, только если:
        # 1. В конфиге явно включена (settings.home_assistant.enabled)
        # 2. Задан непустой токен аутентификации
        self.enabled = settings.home_assistant.enabled and bool(self.ha_token)

        if not self.enabled:
            logger.info(
                "ℹ️ Home Assistant integration disabled (enabled=%s, token_present=%s)",
                settings.home_assistant.enabled,
                bool(self.ha_token),
            )
        else:
            logger.info(
                "🏠 Home Assistant Bridge initialized (url=%s)",
                self.ha_url,
            )

        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        """Возвращает (или создаёт) HTTP-клиент с заголовком авторизации."""
        if self._client is None or self._client.is_closed:
            headers = {
                "Authorization": f"Bearer {self.ha_token}",
                "Content-Type": "application/json",
            }
            self._client = httpx.AsyncClient(
                base_url=self.ha_url,
                headers=headers,
                timeout=10.0,
            )
        return self._client

    async def close(self):
        """Корректное закрытие HTTP-клиента."""
        if self._client and not self._client.is_closed:
            try:
                await self._client.aclose()
            except Exception as e:
                logger.debug(f"Error closing HA client: {e}")
            finally:
                self._client = None

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
        except httpx.HTTPStatusError as e:
            logger.error(
                f"❌ HA service returned {e.response.status_code}: "
                f"{e.response.text[:200]}"
            )
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
        except httpx.ConnectError:
            logger.error("❌ Cannot connect to Home Assistant")
            return None
        except httpx.HTTPStatusError as e:
            logger.error(f"❌ HA get_state returned {e.response.status_code}")
            return None
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

    async def turn_off_dew_heater(self) -> bool:
        """Выключает обогреватель."""
        return await self.call_service(
            "switch",
            "turn_off",
            "switch.observatory_dew_heater",
            reason="Humidity normalized",
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
