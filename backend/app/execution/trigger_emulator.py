"""
Trigger Emulator v2
Эмулирует срабатывание триггеров через N.I.N.A. Advanced API.

ИСПРАВЛЕНО v2: Использует РЕАЛЬНЫЕ пути из OpenAPI спецификации:
- Autofocus: GET /v2/api/equipment/focuser/auto-focus
- Guider: GET /v2/api/equipment/guider/start?calibrate=...
- Mount: GET /v2/api/equipment/mount/park, /home, /flip
- Sequence: GET /v2/api/sequence/start, /stop

Базовый URL из спецификации: http://localhost:1888/v2/api
"""

import logging
from typing import Optional, Dict, Any
import httpx
from app.core.config import settings
from app.shadow_engine.state_tracker import state_tracker
from app.core.events import event_bus

logger = logging.getLogger("TriggerEmulator")


class TriggerEmulator:
    """
    Эмулирует срабатывание триггеров через N.I.N.A. Advanced API.

    Использует РЕАЛЬНЫЕ эндпоинты из спецификации christian-photo/ninaAPI v2.
    """

    # Маппинг внутренних триггеров на РЕАЛЬНЫЕ пути Advanced API
    # Все пути относительные к base_url = /v2/api
    TRIGGER_MAP = {
        # === Autofocus ===
        "autofocus": {
            "method": "GET",
            "path": "/equipment/focuser/auto-focus",
            "params": {},
            "description": "Start autofocus",
        },
        "autofocus_cancel": {
            "method": "GET",
            "path": "/equipment/focuser/auto-focus",
            "params": {"cancel": True},
            "description": "Cancel running autofocus",
        },
        # === Guider (PHD2) ===
        "guider_start": {
            "method": "GET",
            "path": "/equipment/guider/start",
            "params": {"calibrate": False},
            "description": "Start guiding (without calibration)",
        },
        "guider_calibrate": {
            "method": "GET",
            "path": "/equipment/guider/start",
            "params": {"calibrate": True},
            "description": "Start guiding WITH force calibration",
        },
        "guider_stop": {
            "method": "GET",
            "path": "/equipment/guider/stop",
            "params": {},
            "description": "Stop guiding",
        },
        "guider_clear_calibration": {
            "method": "GET",
            "path": "/equipment/guider/clear-calibration",
            "params": {},
            "description": "Clear guider calibration data",
        },
        # === Sequence ===
        "sequence_start": {
            "method": "GET",
            "path": "/sequence/start",
            "params": {},
            "description": "Start Advanced Sequence",
        },
        "sequence_stop": {
            "method": "GET",
            "path": "/sequence/stop",
            "params": {},
            "description": "Stop Advanced Sequence",
        },
        "sequence_skip": {
            "method": "GET",
            "path": "/sequence/skip",
            "params": {"type": "CurrentItems"},
            "description": "Skip current sequence items",
        },
        "sequence_reset": {
            "method": "GET",
            "path": "/sequence/reset",
            "params": {},
            "description": "Reset sequence counters",
        },
        # === Mount ===
        "mount_park": {
            "method": "GET",
            "path": "/equipment/mount/park",
            "params": {},
            "description": "Park the mount",
        },
        "mount_unpark": {
            "method": "GET",
            "path": "/equipment/mount/unpark",
            "params": {},
            "description": "Unpark the mount",
        },
        "mount_home": {
            "method": "GET",
            "path": "/equipment/mount/home",
            "params": {},
            "description": "Home the mount",
        },
        "meridian_flip": {
            "method": "GET",
            "path": "/equipment/mount/flip",
            "params": {},
            "description": "Perform meridian flip (if needed)",
        },
        # === Dome ===
        "dome_park": {
            "method": "GET",
            "path": "/equipment/dome/park",
            "params": {},
            "description": "Park the dome",
        },
        "dome_open": {
            "method": "GET",
            "path": "/equipment/dome/open",
            "params": {},
            "description": "Open dome shutter",
        },
        "dome_close": {
            "method": "GET",
            "path": "/equipment/dome/close",
            "params": {},
            "description": "Close dome shutter",
        },
        # === Camera ===
        "camera_connect": {
            "method": "GET",
            "path": "/equipment/camera/connect",
            "params": {},
            "description": "Connect to camera",
        },
        "camera_disconnect": {
            "method": "GET",
            "path": "/equipment/camera/disconnect",
            "params": {},
            "description": "Disconnect camera",
        },
        "camera_cool": {
            "method": "GET",
            "path": "/equipment/camera/cool",
            "params": {"temperature": -15.0, "minutes": 10},
            "description": "Cool camera to target temp",
        },
        "camera_warm": {
            "method": "GET",
            "path": "/equipment/camera/warm",
            "params": {"minutes": 10},
            "description": "Warm camera",
        },
        # === Flat Panel ===
        "flat_light_on": {
            "method": "GET",
            "path": "/equipment/flatdevice/set-light",
            "params": {"on": True},
            "description": "Turn on flat panel light",
        },
        "flat_light_off": {
            "method": "GET",
            "path": "/equipment/flatdevice/set-light",
            "params": {"on": False},
            "description": "Turn off flat panel light",
        },
        # === LiveStack ===
        "livestack_start": {
            "method": "GET",
            "path": "/livestack/start",
            "params": {},
            "description": "Start LiveStack",
        },
        "livestack_stop": {
            "method": "GET",
            "path": "/livestack/stop",
            "params": {},
            "description": "Stop LiveStack",
        },
        # === Application ===
        "switch_tab_equipment": {
            "method": "GET",
            "path": "/application/switch-tab",
            "params": {"tab": "equipment"},
            "description": "Switch to Equipment tab",
        },
        "switch_tab_imaging": {
            "method": "GET",
            "path": "/application/switch-tab",
            "params": {"tab": "imaging"},
            "description": "Switch to Imaging tab",
        },
    }

    # Маппинг упрощённых имён агентов на реальные триггеры
    AGENT_ALIASES = {
        "autofocus": "autofocus",
        "dither": "guider_start",  # Dither делается через guider
        "guider_calibration": "guider_calibrate",
        "phd2_settle": "guider_start",
        "emergency_park": "mount_park",
    }

    def __init__(self):
        # Базовый URL из спецификации
        self.base_url = settings.network.nina_api_host.rstrip("/")

        # Проверяем, что URL содержит /v2/api
        if not self.base_url.endswith("/v2/api"):
            # Если не содержит, добавляем
            if self.base_url.endswith("/v2"):
                self.base_url = f"{self.base_url}/api"
            elif not self.base_url.endswith("/api"):
                self.base_url = f"{self.base_url}/v2/api"

        logger.info(f"🎯 TriggerEmulator initialized with base URL: {self.base_url}")

    async def fire_trigger(
        self,
        trigger_name: str,
        reason: str = "AI Agent Decision",
        extra_params: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Эмулирует срабатывание триггера через Advanced API.

        Args:
            trigger_name: Имя триггера (autofocus, guider_start, mount_park, etc.)
            reason: Причина срабатывания (для логов)
            extra_params: Дополнительные query параметры

        Returns:
            True если триггер успешно отправлен, False в противном случае
        """
        # Разрешаем алиасы
        actual_trigger = self.AGENT_ALIASES.get(trigger_name, trigger_name)

        logger.info(
            f"🔥 Firing trigger: {trigger_name}"
            f"{' (aliased to ' + actual_trigger + ')' if actual_trigger != trigger_name else ''}"
            f" (Reason: {reason})"
        )

        # 1. Проверка FLAT_MODE
        if state_tracker.state.is_flat_mode:
            if actual_trigger in ["autofocus", "guider_start", "guider_calibrate"]:
                logger.warning(
                    f"🛑 BLOCKED: Trigger '{trigger_name}' ignored during FLAT_MODE"
                )
                return False

        # 2. Проверка критической фазы
        if state_tracker.state.is_approaching_shutdown:
            logger.warning(
                f"🛑 BLOCKED: Trigger '{trigger_name}' ignored - approaching shutdown"
            )
            return False

        # 3. Получаем конфигурацию триггера
        trigger_config = self.TRIGGER_MAP.get(actual_trigger)
        if not trigger_config:
            logger.error(
                f"❌ Unknown trigger: '{trigger_name}'. "
                f"Available: {', '.join(sorted(self.TRIGGER_MAP.keys()))}"
            )
            return False

        # 4. Формируем URL и параметры
        url = f"{self.base_url}{trigger_config['path']}"
        params = {**trigger_config["params"]}
        if extra_params:
            params.update(extra_params)

        # 5. Выполняем запрос
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                if trigger_config["method"] == "GET":
                    response = await client.get(url, params=params)
                elif trigger_config["method"] == "POST":
                    response = await client.post(url, json=params)
                else:
                    logger.error(f"❌ Unsupported method: {trigger_config['method']}")
                    return False

                # Обрабатываем ответ
                if response.status_code == 200:
                    try:
                        data = response.json()
                        success = data.get("Success", False)
                        api_response = data.get("Response", "")
                        error = data.get("Error", "")

                        if success:
                            logger.info(
                                f"✅ Trigger '{trigger_name}' fired successfully: {api_response}"
                            )

                            # Публикуем событие
                            await event_bus.publish(
                                "TRIGGER_FIRED",
                                {
                                    "trigger": trigger_name,
                                    "reason": reason,
                                    "response": api_response,
                                },
                            )

                            return True
                        else:
                            logger.warning(
                                f"⚠️ Trigger '{trigger_name}' returned error: {error}"
                            )
                            return False

                    except Exception as e:
                        # Не JSON ответ, но 200 OK
                        logger.info(
                            f"✅ Trigger '{trigger_name}' fired (non-JSON response)"
                        )
                        return True

                elif response.status_code == 409:
                    # Конфликт (оборудование не подключено, уже запущено и т.д.)
                    try:
                        data = response.json()
                        error = data.get("Error", "Conflict")
                    except:
                        error = "Conflict"

                    logger.warning(
                        f"⚠️ Trigger '{trigger_name}' conflict (409): {error}"
                    )
                    return False

                elif response.status_code == 404:
                    logger.error(
                        f"❌ Trigger '{trigger_name}' endpoint not found (404): {url}\n"
                        f"   Проверьте, что Advanced API плагин установлен и запущен.\n"
                        f"   Установите: N.I.N.A. → Options → Plugins → Advanced API"
                    )
                    return False

                else:
                    logger.error(
                        f"❌ Trigger '{trigger_name}' failed with status "
                        f"{response.status_code}: {response.text[:200]}"
                    )
                    return False

        except httpx.ConnectError:
            logger.error(
                f"❌ Cannot connect to N.I.N.A. Advanced API at {self.base_url}\n"
                f"   Проверьте, что N.I.N.A. запущена и Advanced API включен."
            )
            return False

        except httpx.TimeoutException:
            logger.error(f"❌ Timeout firing trigger '{trigger_name}'")
            return False

        except Exception as e:
            logger.error(f"❌ Unexpected error firing trigger '{trigger_name}': {e}")
            return False

    def list_available_triggers(self) -> Dict[str, Dict[str, Any]]:
        """Возвращает список всех доступных триггеров."""
        return {
            name: {
                "method": config["method"],
                "path": config["path"],
                "params": config["params"],
                "description": config["description"],
                "full_url": f"{self.base_url}{config['path']}",
            }
            for name, config in self.TRIGGER_MAP.items()
        }


# Singleton instance
trigger_emulator = TriggerEmulator()
