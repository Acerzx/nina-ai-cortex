"""
Trigger Emulator v2
Эмулирует срабатывание триггеров через N.I.N.A. Advanced API.

ИСПРАВЛЕНО v2: Использует РЕАЛЬНЫЕ пути из OpenAPI спецификации:
- Autofocus: GET /v2/api/equipment/focuser/auto-focus
- Guider: GET /v2/api/equipment/guider/start?calibrate=...
- Mount: GET /v2/api/equipment/mount/park, /home, /flip
- Sequence: GET /v2/api/sequence/start, /stop

Базовый URL из спецификации: http://localhost:1888/v2/api

ИСПРАВЛЕНО (audit 4.5):
- Внедрён список PROTECTED_PARAMS для предотвращения перезаписи
  критических параметров через extra_params
- Добавлена валидация значений параметров (температура, время,
  координаты) для защиты оборудования от опасных команд
- Попытки перезаписи защищённых параметров логируются с уровнем WARNING
- Добавлена проверка FLAT_MODE и критической фазы из HAL
"""

import logging
from typing import Optional, Dict, Any, List, Set
import httpx
from app.core.config import settings
from app.shadow_engine.state_tracker import state_tracker
from app.core.events import event_bus

logger = logging.getLogger("TriggerEmulator")


# ============================================================================
# ЗАЩИЩЁННЫЕ ПАРАМЕТРЫ И ВАЛИДАЦИЯ
# ============================================================================

# Параметры, которые НЕ могут быть перезаписаны через extra_params.
# Эти параметры критичны для безопасности оборудования.
PROTECTED_PARAMS: Set[str] = {
    "cancel",  # Отмена операций (может прервать критические процессы)
    "skipValidation",  # Пропуск валидации секвенсора
}

# Диапазоны допустимых значений для параметров оборудования.
# Значения вне этих диапазонов будут отклонены для защиты оборудования.
PARAMETER_RANGES: Dict[str, Dict[str, Any]] = {
    # Температура камеры (°C)
    "temperature": {"min": -40.0, "max": 30.0},
    # Время охлаждения/нагрева камеры (минуты)
    "minutes": {"min": 0, "max": 120},
    # Яркость плоской панели (0-100)
    "brightness": {"min": 0, "max": 100},
    # Количество кадров (для flats)
    "count": {"min": 1, "max": 100},
    # Экспозиция (секунды)
    "exposureTime": {"min": 0.001, "max": 3600.0},
    "minExposure": {"min": 0.0, "max": 300.0},
    "maxExposure": {"min": 0.0, "max": 600.0},
    # Азимут купола (градусы)
    "azimuth": {"min": 0.0, "max": 360.0},
    # Координаты монтировки
    "ra": {"min": 0.0, "max": 360.0},
    "dec": {"min": -90.0, "max": 90.0},
    # Позиция ротатора (градусы)
    "position": {"min": 0.0, "max": 360.0},
    "rotationAngle": {"min": -360.0, "max": 360.0},
    # Гистограмма (0-1)
    "histogramMean": {"min": 0.0, "max": 1.0},
    "meanTolerance": {"min": 0.0, "max": 1.0},
    # Gain и Offset
    "gain": {"min": 0, "max": 10000},
    "offset": {"min": -1000, "max": 10000},
    # Filter ID
    "filterId": {"min": 0, "max": 20},
}


class TriggerEmulator:
    """
    Эмулирует срабатывание триггеров через N.I.N.A. Advanced API.
    Использует РЕАЛЬНЫЕ эндпоинты из спецификации christian-photo/ninaAPI v2.

    ИСПРАВЛЕНО (audit 4.5):
    - Защита от перезаписи критических параметров
    - Валидация значений для защиты оборудования
    - Полное логирование всех операций
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
            "category": "focuser",
            "risk_level": "LOW",
        },
        "autofocus_cancel": {
            "method": "GET",
            "path": "/equipment/focuser/auto-focus",
            "params": {"cancel": True},
            "description": "Cancel running autofocus",
            "category": "focuser",
            "risk_level": "LOW",
        },
        # === Guider (PHD2) ===
        "guider_start": {
            "method": "GET",
            "path": "/equipment/guider/start",
            "params": {"calibrate": False},
            "description": "Start guiding (without calibration)",
            "category": "guider",
            "risk_level": "LOW",
        },
        "guider_calibrate": {
            "method": "GET",
            "path": "/equipment/guider/start",
            "params": {"calibrate": True},
            "description": "Start guiding WITH force calibration",
            "category": "guider",
            "risk_level": "MEDIUM",
        },
        "guider_stop": {
            "method": "GET",
            "path": "/equipment/guider/stop",
            "params": {},
            "description": "Stop guiding",
            "category": "guider",
            "risk_level": "LOW",
        },
        "guider_clear_calibration": {
            "method": "GET",
            "path": "/equipment/guider/clear-calibration",
            "params": {},
            "description": "Clear guider calibration data",
            "category": "guider",
            "risk_level": "MEDIUM",
        },
        # === Sequence ===
        "sequence_start": {
            "method": "GET",
            "path": "/sequence/start",
            "params": {},
            "description": "Start Advanced Sequence",
            "category": "sequence",
            "risk_level": "HIGH",
        },
        "sequence_stop": {
            "method": "GET",
            "path": "/sequence/stop",
            "params": {},
            "description": "Stop Advanced Sequence",
            "category": "sequence",
            "risk_level": "MEDIUM",
        },
        "sequence_skip": {
            "method": "GET",
            "path": "/sequence/skip",
            "params": {"type": "CurrentItems"},
            "description": "Skip current sequence items",
            "category": "sequence",
            "risk_level": "LOW",
        },
        "sequence_reset": {
            "method": "GET",
            "path": "/sequence/reset",
            "params": {},
            "description": "Reset sequence counters",
            "category": "sequence",
            "risk_level": "MEDIUM",
        },
        # === Mount ===
        "mount_park": {
            "method": "GET",
            "path": "/equipment/mount/park",
            "params": {},
            "description": "Park the mount",
            "category": "mount",
            "risk_level": "HIGH",
        },
        "mount_unpark": {
            "method": "GET",
            "path": "/equipment/mount/unpark",
            "params": {},
            "description": "Unpark the mount",
            "category": "mount",
            "risk_level": "MEDIUM",
        },
        "mount_home": {
            "method": "GET",
            "path": "/equipment/mount/home",
            "params": {},
            "description": "Home the mount",
            "category": "mount",
            "risk_level": "HIGH",
        },
        "meridian_flip": {
            "method": "GET",
            "path": "/equipment/mount/flip",
            "params": {},
            "description": "Perform meridian flip (if needed)",
            "category": "mount",
            "risk_level": "HIGH",
        },
        # === Dome ===
        "dome_park": {
            "method": "GET",
            "path": "/equipment/dome/park",
            "params": {},
            "description": "Park the dome",
            "category": "dome",
            "risk_level": "MEDIUM",
        },
        "dome_open": {
            "method": "GET",
            "path": "/equipment/dome/open",
            "params": {},
            "description": "Open dome shutter",
            "category": "dome",
            "risk_level": "HIGH",
        },
        "dome_close": {
            "method": "GET",
            "path": "/equipment/dome/close",
            "params": {},
            "description": "Close dome shutter",
            "category": "dome",
            "risk_level": "MEDIUM",
        },
        # === Camera ===
        "camera_connect": {
            "method": "GET",
            "path": "/equipment/camera/connect",
            "params": {},
            "description": "Connect to camera",
            "category": "camera",
            "risk_level": "LOW",
        },
        "camera_disconnect": {
            "method": "GET",
            "path": "/equipment/camera/disconnect",
            "params": {},
            "description": "Disconnect camera",
            "category": "camera",
            "risk_level": "MEDIUM",
        },
        "camera_cool": {
            "method": "GET",
            "path": "/equipment/camera/cool",
            "params": {"temperature": -15.0, "minutes": 10},
            "description": "Cool camera to target temp",
            "category": "camera",
            "risk_level": "MEDIUM",
        },
        "camera_warm": {
            "method": "GET",
            "path": "/equipment/camera/warm",
            "params": {"minutes": 10},
            "description": "Warm camera",
            "category": "camera",
            "risk_level": "MEDIUM",
        },
        # === Flat Panel ===
        "flat_light_on": {
            "method": "GET",
            "path": "/equipment/flatdevice/set-light",
            "params": {"on": True},
            "description": "Turn on flat panel light",
            "category": "flat",
            "risk_level": "LOW",
        },
        "flat_light_off": {
            "method": "GET",
            "path": "/equipment/flatdevice/set-light",
            "params": {"on": False},
            "description": "Turn off flat panel light",
            "category": "flat",
            "risk_level": "LOW",
        },
        # === LiveStack ===
        "livestack_start": {
            "method": "GET",
            "path": "/livestack/start",
            "params": {},
            "description": "Start LiveStack",
            "category": "livestack",
            "risk_level": "LOW",
        },
        "livestack_stop": {
            "method": "GET",
            "path": "/livestack/stop",
            "params": {},
            "description": "Stop LiveStack",
            "category": "livestack",
            "risk_level": "LOW",
        },
        # === Application ===
        "switch_tab_equipment": {
            "method": "GET",
            "path": "/application/switch-tab",
            "params": {"tab": "equipment"},
            "description": "Switch to Equipment tab",
            "category": "application",
            "risk_level": "LOW",
        },
        "switch_tab_imaging": {
            "method": "GET",
            "path": "/application/switch-tab",
            "params": {"tab": "imaging"},
            "description": "Switch to Imaging tab",
            "category": "application",
            "risk_level": "LOW",
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
        # Нормализация URL
        if not self.base_url.endswith("/v2/api"):
            if self.base_url.endswith("/v2"):
                self.base_url = f"{self.base_url}/api"
            elif not self.base_url.endswith("/api"):
                self.base_url = f"{self.base_url}/v2/api"

        # Статистика
        self._stats = {
            "total_triggers_fired": 0,
            "successful_triggers": 0,
            "failed_triggers": 0,
            "blocked_by_flat_mode": 0,
            "blocked_by_critical_phase": 0,
            "blocked_by_hal": 0,
            "blocked_by_validation": 0,
            "protected_params_rejected": 0,
        }

        # История последних триггеров (для аудита)
        self._trigger_history: List[Dict[str, Any]] = []
        self._history_max_size: int = 100

        logger.info(f"🎯 TriggerEmulator initialized with base URL: {self.base_url}")
        logger.info(f"   Protected parameters: {', '.join(sorted(PROTECTED_PARAMS))}")
        logger.info(
            f"   Parameter ranges validated: {len(PARAMETER_RANGES)} parameters"
        )

    def _validate_parameter_value(
        self, param_name: str, value: Any
    ) -> tuple[bool, Optional[str]]:
        """
        Валидирует значение параметра против допустимых диапазонов.

        Args:
            param_name: Имя параметра
            value: Значение для проверки

        Returns:
            Tuple (is_valid, error_message)
        """
        if param_name not in PARAMETER_RANGES:
            # Параметр без ограничений — считаем валидным
            return True, None

        range_spec = PARAMETER_RANGES[param_name]
        min_val = range_spec.get("min")
        max_val = range_spec.get("max")

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return (
                False,
                f"Parameter '{param_name}' must be numeric, got {type(value).__name__}",
            )

        if min_val is not None and numeric_value < min_val:
            return (
                False,
                f"Parameter '{param_name}' = {numeric_value} "
                f"is below minimum {min_val}",
            )

        if max_val is not None and numeric_value > max_val:
            return (
                False,
                f"Parameter '{param_name}' = {numeric_value} exceeds maximum {max_val}",
            )

        return True, None

    def _merge_params_safely(
        self,
        base_params: Dict[str, Any],
        extra_params: Optional[Dict[str, Any]],
    ) -> tuple[Dict[str, Any], List[str]]:
        """
        Безопасно объединяет базовые и дополнительные параметры.

        ИСПРАВЛЕНО (audit 4.5): Защищённые параметры не могут быть
        перезаписаны через extra_params. Все попытки перезаписи
        логируются с уровнем WARNING.

        Args:
            base_params: Базовые параметры из TRIGGER_MAP
            extra_params: Дополнительные параметры от вызывающего кода

        Returns:
            Tuple (merged_params, list_of_rejected_params)
        """
        if not extra_params:
            return dict(base_params), []

        merged = dict(base_params)
        rejected: List[str] = []

        for key, value in extra_params.items():
            # Проверка 1: Защищённый параметр?
            if key in PROTECTED_PARAMS:
                logger.warning(
                    f"🛡️ BLOCKED: Attempt to override protected parameter "
                    f"'{key}' with value '{value}'. Original value preserved."
                )
                rejected.append(key)
                self._stats["protected_params_rejected"] += 1
                continue

            # Проверка 2: Валидация значения
            is_valid, error_msg = self._validate_parameter_value(key, value)
            if not is_valid:
                logger.warning(
                    f"🛡️ BLOCKED: Invalid value for parameter '{key}': {error_msg}"
                )
                rejected.append(key)
                self._stats["blocked_by_validation"] += 1
                continue

            # Параметр принят
            merged[key] = value

        return merged, rejected

    def _add_to_history(
        self,
        trigger_name: str,
        reason: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Добавляет запись в историю триггеров с ограничением размера."""
        from datetime import datetime

        record = {
            "timestamp": datetime.now().isoformat(),
            "trigger": trigger_name,
            "reason": reason,
            "status": status,
            "details": details or {},
        }

        self._trigger_history.append(record)

        # Ограничиваем размер истории
        if len(self._trigger_history) > self._history_max_size:
            self._trigger_history = self._trigger_history[-self._history_max_size :]

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
                         (защищённые параметры будут отклонены)

        Returns:
            True если триггер успешно отправлен, False в противном случае
        """
        self._stats["total_triggers_fired"] += 1

        # Разрешаем алиасы
        actual_trigger = self.AGENT_ALIASES.get(trigger_name, trigger_name)
        alias_note = (
            f" (aliased to '{actual_trigger}')"
            if actual_trigger != trigger_name
            else ""
        )

        logger.info(
            f"🔥 Firing trigger: '{trigger_name}'{alias_note} (Reason: {reason})"
        )

        # === ПРОВЕРКА 1: FLAT_MODE ===
        if state_tracker.state.is_flat_mode:
            blocked_in_flat = {
                "autofocus",
                "guider_start",
                "guider_calibrate",
                "sequence_start",
            }
            if actual_trigger in blocked_in_flat:
                logger.warning(
                    f"🛑 BLOCKED: Trigger '{trigger_name}' ignored during FLAT_MODE"
                )
                self._stats["blocked_by_flat_mode"] += 1
                self._add_to_history(trigger_name, reason, "BLOCKED_FLAT_MODE")
                return False

        # === ПРОВЕРКА 2: Критическая фаза (shutdown) ===
        if state_tracker.state.is_approaching_shutdown:
            # Во время shutdown разрешены только безопасные операции
            allowed_during_shutdown = {
                "mount_park",
                "dome_close",
                "camera_warm",
                "guider_stop",
                "livestack_stop",
                "sequence_stop",
            }
            if actual_trigger not in allowed_during_shutdown:
                logger.warning(
                    f"🛑 BLOCKED: Trigger '{trigger_name}' ignored - "
                    f"approaching shutdown"
                )
                self._stats["blocked_by_critical_phase"] += 1
                self._add_to_history(trigger_name, reason, "BLOCKED_CRITICAL_PHASE")
                return False

        # === ПРОВЕРКА 3: HAL валидация ===
        try:
            from app.execution.hal import hal

            is_safe, hal_reason = hal.validate_trigger_injection(actual_trigger)
            if not is_safe:
                logger.warning(
                    f"🛑 BLOCKED by HAL: Trigger '{trigger_name}' - {hal_reason}"
                )
                self._stats["blocked_by_hal"] += 1
                self._add_to_history(
                    trigger_name,
                    reason,
                    "BLOCKED_HAL",
                    {"hal_reason": hal_reason},
                )
                return False
        except ImportError:
            logger.debug("HAL not available, skipping HAL validation")

        # === ПРОВЕРКА 4: Получение конфигурации триггера ===
        trigger_config = self.TRIGGER_MAP.get(actual_trigger)
        if not trigger_config:
            available = ", ".join(sorted(self.TRIGGER_MAP.keys()))
            logger.error(
                f"❌ Unknown trigger: '{trigger_name}'. Available: {available}"
            )
            self._add_to_history(trigger_name, reason, "FAILED_UNKNOWN_TRIGGER")
            return False

        # === ПРОВЕРКА 5: Безопасное объединение параметров ===
        params, rejected = self._merge_params_safely(
            trigger_config["params"], extra_params
        )

        # Если были отклонены критические параметры — логируем, но продолжаем
        if rejected:
            logger.warning(
                f"⚠️ Trigger '{trigger_name}' had {len(rejected)} parameters "
                f"rejected: {rejected}. Proceeding with safe parameters."
            )

        # === ВЫПОЛНЕНИЕ ЗАПРОСА ===
        url = f"{self.base_url}{trigger_config['path']}"

        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                if trigger_config["method"] == "GET":
                    response = await client.get(url, params=params)
                elif trigger_config["method"] == "POST":
                    response = await client.post(url, json=params)
                else:
                    logger.error(f"❌ Unsupported method: {trigger_config['method']}")
                    self._add_to_history(
                        trigger_name, reason, "FAILED_UNSUPPORTED_METHOD"
                    )
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
                                f"✅ Trigger '{trigger_name}' fired "
                                f"successfully: {api_response}"
                            )
                            self._stats["successful_triggers"] += 1
                            self._add_to_history(
                                trigger_name,
                                reason,
                                "SUCCESS",
                                {
                                    "response": api_response,
                                    "params": params,
                                    "rejected_params": rejected,
                                },
                            )

                            # Публикуем событие
                            await event_bus.publish(
                                "TRIGGER_FIRED",
                                {
                                    "trigger": trigger_name,
                                    "actual_trigger": actual_trigger,
                                    "reason": reason,
                                    "response": api_response,
                                    "params": params,
                                    "category": trigger_config.get(
                                        "category", "unknown"
                                    ),
                                    "risk_level": trigger_config.get(
                                        "risk_level", "UNKNOWN"
                                    ),
                                },
                            )
                            return True
                        else:
                            logger.warning(
                                f"⚠️ Trigger '{trigger_name}' returned error: {error}"
                            )
                            self._stats["failed_triggers"] += 1
                            self._add_to_history(
                                trigger_name,
                                reason,
                                "FAILED_API_ERROR",
                                {"error": error},
                            )
                            return False

                    except Exception as e:
                        # Не JSON ответ, но 200 OK
                        logger.info(
                            f"✅ Trigger '{trigger_name}' fired (non-JSON response)"
                        )
                        self._stats["successful_triggers"] += 1
                        self._add_to_history(trigger_name, reason, "SUCCESS_NON_JSON")
                        return True

                elif response.status_code == 409:
                    # Конфликт (оборудование не подключено, уже запущено и т.д.)
                    try:
                        data = response.json()
                        error = data.get("Error", "Conflict")
                    except Exception:
                        error = "Conflict"
                    logger.warning(
                        f"⚠️ Trigger '{trigger_name}' conflict (409): {error}"
                    )
                    self._stats["failed_triggers"] += 1
                    self._add_to_history(
                        trigger_name,
                        reason,
                        "FAILED_CONFLICT",
                        {"error": error},
                    )
                    return False

                elif response.status_code == 404:
                    logger.error(
                        f"❌ Trigger '{trigger_name}' endpoint not found "
                        f"(404): {url}\n"
                        f"   Проверьте, что Advanced API плагин установлен "
                        f"и запущен.\n"
                        f"   Установите: N.I.N.A. → Options → Plugins → "
                        f"Advanced API"
                    )
                    self._stats["failed_triggers"] += 1
                    self._add_to_history(trigger_name, reason, "FAILED_NOT_FOUND")
                    return False

                else:
                    logger.error(
                        f"❌ Trigger '{trigger_name}' failed with status "
                        f"{response.status_code}: {response.text[:200]}"
                    )
                    self._stats["failed_triggers"] += 1
                    self._add_to_history(
                        trigger_name,
                        reason,
                        "FAILED_HTTP_ERROR",
                        {
                            "status_code": response.status_code,
                            "response": response.text[:500],
                        },
                    )
                    return False

        except httpx.ConnectError:
            logger.error(
                f"❌ Cannot connect to N.I.N.A. Advanced API at "
                f"{self.base_url}\n"
                f"   Проверьте, что N.I.N.A. запущена и Advanced API "
                f"включен."
            )
            self._stats["failed_triggers"] += 1
            self._add_to_history(trigger_name, reason, "FAILED_CONNECTION_ERROR")
            return False

        except httpx.TimeoutException:
            logger.error(f"❌ Timeout firing trigger '{trigger_name}'")
            self._stats["failed_triggers"] += 1
            self._add_to_history(trigger_name, reason, "FAILED_TIMEOUT")
            return False

        except Exception as e:
            logger.error(f"❌ Unexpected error firing trigger '{trigger_name}': {e}")
            self._stats["failed_triggers"] += 1
            self._add_to_history(
                trigger_name,
                reason,
                "FAILED_UNEXPECTED",
                {"error": str(e)},
            )
            return False

    def list_available_triggers(self) -> Dict[str, Dict[str, Any]]:
        """
        Возвращает список всех доступных триггеров с детальной информацией.

        Включает:
        - Метод HTTP и путь
        - Параметры и их ограничения
        - Категорию и уровень риска
        - Защищённые параметры
        """
        result = {}
        for name, config in self.TRIGGER_MAP.items():
            # Детализация параметров с ограничениями
            param_details = {}
            for param_name, default_value in config["params"].items():
                param_info = {
                    "default": default_value,
                    "protected": param_name in PROTECTED_PARAMS,
                }
                if param_name in PARAMETER_RANGES:
                    param_info["range"] = PARAMETER_RANGES[param_name]
                param_details[param_name] = param_info

            result[name] = {
                "method": config["method"],
                "path": config["path"],
                "description": config["description"],
                "category": config.get("category", "unknown"),
                "risk_level": config.get("risk_level", "UNKNOWN"),
                "params": param_details,
                "full_url": f"{self.base_url}{config['path']}",
            }

        return result

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику TriggerEmulator.

        Включает:
        - Общее количество триггеров
        - Количество успешных/неуспешных
        - Причины блокировок
        - Последние триггеры из истории
        """
        success_rate = (
            self._stats["successful_triggers"]
            / max(self._stats["total_triggers_fired"], 1)
        ) * 100

        return {
            **self._stats,
            "success_rate_percent": round(success_rate, 2),
            "base_url": self.base_url,
            "total_available_triggers": len(self.TRIGGER_MAP),
            "protected_params": sorted(PROTECTED_PARAMS),
            "validated_parameters": len(PARAMETER_RANGES),
            "history_size": len(self._trigger_history),
            "recent_triggers": self._trigger_history[-10:],
        }

    def get_trigger_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Возвращает историю последних триггеров.

        Args:
            limit: Максимальное количество записей

        Returns:
            Список записей истории (от новых к старым)
        """
        return list(reversed(self._trigger_history[-limit:]))


# Singleton instance
trigger_emulator = TriggerEmulator()
