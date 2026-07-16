"""
Trigger Emulator v3 (OpenAPI-Driven Edition)
Эмулирует срабатывание триггеров через N.I.N.A. Advanced API.
КЛЮЧЕВЫЕ УЛУЧШЕНИЯ (рефакторинг v3):
- Динамическое построение реестра триггеров из OpenAPI спецификации
- AGENT_ALIASES читаются из settings.execution.agent_aliases (если есть)
- PARAMETER_RANGES извлекаются автоматически из OpenAPI (minimum/maximum)
- Устранён хардкод путей — всё резолвится из spec
- Сохранена вся бизнес-логика: FLAT_MODE, critical phase, HAL, protected params
- Полная статистика и история всех операций
- Детальная обработка всех HTTP-ответов (200, 409, 404, errors)
- Fallback: можно вызвать любой эндпоинт N.I.N.A. API напрямую
ИСПРАВЛЕНО (v4.1):
- Исправлены отступы в __init__
- Удалён недостижимый код с неопределённой переменной api_response
ИСПРАВЛЕНО (Спринт 5 — 1.5):
- OpenTelemetry spans для trigger.fire и trigger.http_request
- Атрибуты: trigger.name, category, risk_level, status, duration_ms, blocked_by
"""

import logging
import time
from typing import Optional, Dict, Any, List, Set, Tuple
from datetime import datetime
from pydantic import BaseModel, Field
from app.core.config import settings
from app.core.http_client import http_client_manager
from app.shadow_engine.state_tracker import state_tracker
from app.core.events import event_bus
from app.execution.openapi_client import get_nina_api_client, DynamicAPIClient

# Спринт 5: OpenTelemetry tracing
from app.core.tracing import tracing_manager, span_context

logger = logging.getLogger("TriggerEmulator")


# ============================================================================
# МОДЕЛИ ДАННЫХ
# ============================================================================


class TriggerHistoryRecord(BaseModel):
    """Запись в истории триггеров."""

    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    trigger: str
    actual_trigger: str = ""
    reason: str
    status: str  # SUCCESS, FAILED_*, BLOCKED_*
    details: Dict[str, Any] = Field(default_factory=dict)
    rejected_params: List[str] = Field(default_factory=list)


# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

# Защищённые параметры (нельзя перезаписывать через extra_params).
PROTECTED_PARAMS: Set[str] = {
    "cancel",  # Отмена операций
    "skipValidation",  # Пропуск валидации секвенсора
}

# Минимальный маппинг внутренних имён Cortex на паттерны в OpenAPI.
DEFAULT_TRIGGER_PATTERNS: Dict[str, Dict[str, Any]] = {
    # Autofocus
    "autofocus": {
        "method": "GET",
        "path_pattern": "/equipment/focuser/auto-focus",
        "category": "focuser",
        "risk_level": "LOW",
        "description": "Start autofocus",
    },
    "autofocus_cancel": {
        "method": "GET",
        "path_pattern": "/equipment/focuser/auto-focus",
        "default_params": {"cancel": True},
        "category": "focuser",
        "risk_level": "LOW",
        "description": "Cancel running autofocus",
    },
    # Guider
    "guider_start": {
        "method": "GET",
        "path_pattern": "/equipment/guider/start",
        "default_params": {"calibrate": False},
        "category": "guider",
        "risk_level": "LOW",
        "description": "Start guiding",
    },
    "guider_calibrate": {
        "method": "GET",
        "path_pattern": "/equipment/guider/start",
        "default_params": {"calibrate": True},
        "category": "guider",
        "risk_level": "MEDIUM",
        "description": "Start guiding with force calibration",
    },
    "guider_stop": {
        "method": "GET",
        "path_pattern": "/equipment/guider/stop",
        "category": "guider",
        "risk_level": "LOW",
        "description": "Stop guiding",
    },
    "guider_clear_calibration": {
        "method": "GET",
        "path_pattern": "/equipment/guider/clear-calibration",
        "category": "guider",
        "risk_level": "MEDIUM",
        "description": "Clear guider calibration",
    },
    # Sequence
    "sequence_start": {
        "method": "GET",
        "path_pattern": "/sequence/start",
        "category": "sequence",
        "risk_level": "HIGH",
        "description": "Start Advanced Sequence",
    },
    "sequence_stop": {
        "method": "GET",
        "path_pattern": "/sequence/stop",
        "category": "sequence",
        "risk_level": "MEDIUM",
        "description": "Stop Advanced Sequence",
    },
    "sequence_skip": {
        "method": "GET",
        "path_pattern": "/sequence/skip",
        "default_params": {"type": "CurrentItems"},
        "category": "sequence",
        "risk_level": "LOW",
        "description": "Skip current sequence items",
    },
    "sequence_reset": {
        "method": "GET",
        "path_pattern": "/sequence/reset",
        "category": "sequence",
        "risk_level": "MEDIUM",
        "description": "Reset sequence counters",
    },
    # Mount
    "mount_park": {
        "method": "GET",
        "path_pattern": "/equipment/mount/park",
        "category": "mount",
        "risk_level": "HIGH",
        "description": "Park the mount",
    },
    "mount_unpark": {
        "method": "GET",
        "path_pattern": "/equipment/mount/unpark",
        "category": "mount",
        "risk_level": "MEDIUM",
        "description": "Unpark the mount",
    },
    "mount_home": {
        "method": "GET",
        "path_pattern": "/equipment/mount/home",
        "category": "mount",
        "risk_level": "HIGH",
        "description": "Home the mount",
    },
    "meridian_flip": {
        "method": "GET",
        "path_pattern": "/equipment/mount/flip",
        "category": "mount",
        "risk_level": "HIGH",
        "description": "Perform meridian flip",
    },
    # Dome
    "dome_park": {
        "method": "GET",
        "path_pattern": "/equipment/dome/park",
        "category": "dome",
        "risk_level": "MEDIUM",
        "description": "Park the dome",
    },
    "dome_open": {
        "method": "GET",
        "path_pattern": "/equipment/dome/open",
        "category": "dome",
        "risk_level": "HIGH",
        "description": "Open dome shutter",
    },
    "dome_close": {
        "method": "GET",
        "path_pattern": "/equipment/dome/close",
        "category": "dome",
        "risk_level": "MEDIUM",
        "description": "Close dome shutter",
    },
    # Camera
    "camera_connect": {
        "method": "GET",
        "path_pattern": "/equipment/camera/connect",
        "category": "camera",
        "risk_level": "LOW",
        "description": "Connect to camera",
    },
    "camera_disconnect": {
        "method": "GET",
        "path_pattern": "/equipment/camera/disconnect",
        "category": "camera",
        "risk_level": "MEDIUM",
        "description": "Disconnect camera",
    },
    "camera_cool": {
        "method": "GET",
        "path_pattern": "/equipment/camera/cool",
        "default_params": {"temperature": -15.0, "minutes": 10},
        "category": "camera",
        "risk_level": "MEDIUM",
        "description": "Cool camera to target temp",
    },
    "camera_warm": {
        "method": "GET",
        "path_pattern": "/equipment/camera/warm",
        "default_params": {"minutes": 10},
        "category": "camera",
        "risk_level": "MEDIUM",
        "description": "Warm camera",
    },
    # Flat Panel
    "flat_light_on": {
        "method": "GET",
        "path_pattern": "/equipment/flatdevice/set-light",
        "default_params": {"on": True},
        "category": "flat",
        "risk_level": "LOW",
        "description": "Turn on flat panel light",
    },
    "flat_light_off": {
        "method": "GET",
        "path_pattern": "/equipment/flatdevice/set-light",
        "default_params": {"on": False},
        "category": "flat",
        "risk_level": "LOW",
        "description": "Turn off flat panel light",
    },
    # LiveStack
    "livestack_start": {
        "method": "GET",
        "path_pattern": "/livestack/start",
        "category": "livestack",
        "risk_level": "LOW",
        "description": "Start LiveStack",
    },
    "livestack_stop": {
        "method": "GET",
        "path_pattern": "/livestack/stop",
        "category": "livestack",
        "risk_level": "LOW",
        "description": "Stop LiveStack",
    },
}


# ============================================================================
# TRIGGER EMULATOR
# ============================================================================


class TriggerEmulator:
    """
    Эмулятор триггеров N.I.N.A. Advanced API (OpenAPI-Driven Edition).

    ИСПРАВЛЕНО (Спринт 5 — 1.5):
    - OpenTelemetry spans для trigger.fire и trigger.http_request
    """

    def __init__(self):
        self.base_url = settings.network.nina_api_host.rstrip("/")

        # Нормализация URL
        if not self.base_url.endswith("/v2/api"):
            if self.base_url.endswith("/v2"):
                self.base_url = f"{self.base_url}/api"
            elif not self.base_url.endswith("/api"):
                self.base_url = f"{self.base_url}/v2/api"

        # Загружаем AGENT_ALIASES из settings (если есть) или используем default
        self._agent_aliases: Dict[str, str] = self._load_agent_aliases()

        # Загружаем trigger patterns из settings (если есть) или используем default
        self._trigger_patterns: Dict[str, Dict[str, Any]] = (
            self._load_trigger_patterns()
        )

        # Dynamic registry: trigger_name -> {method, path, params, ...}
        self._registry: Dict[str, Dict[str, Any]] = {}

        # OpenAPI клиент (lazy init)
        self._openapi_client: Optional[DynamicAPIClient] = None

        # Статистика
        self._stats = {
            "total_triggers_fired": 0,
            "successful_triggers": 0,
            "failed_triggers": 0,
            "blocked_by_flat_mode": 0,
            "blocked_by_critical_phase": 0,
            "blocked_by_hal": 0,
            "blocked_by_validation": 0,
            "blocked_by_manual_mode": 0,
            "protected_params_rejected": 0,
            "openapi_calls": 0,
            "direct_calls": 0,
        }

        # История
        self._trigger_history: List[TriggerHistoryRecord] = []
        self._history_max_size: int = 100

        logger.info(f"🎯 TriggerEmulator v3 initialized (base: {self.base_url})")
        logger.info(
            f"   Agent aliases: {len(self._agent_aliases)}, "
            f"trigger patterns: {len(self._trigger_patterns)}"
        )

        # ИСПРАВЛЕНО (v4.1): храним последние rejected params
        self._last_rejected_params: Optional[List[str]] = None

    # ====================================================================
    # КОНФИГУРАЦИЯ
    # ====================================================================

    def _load_agent_aliases(self) -> Dict[str, str]:
        """Загружает AGENT_ALIASES из settings или возвращает default."""
        try:
            exec_cfg = getattr(settings, "execution", None)
            if exec_cfg:
                aliases = getattr(exec_cfg, "agent_aliases", None)
                if aliases and isinstance(aliases, dict):
                    logger.info(f"✅ Loaded {len(aliases)} agent aliases from settings")
                    return dict(aliases)
        except Exception as e:
            logger.debug(f"Could not load execution config: {e}")

        return {
            "autofocus": "autofocus",
            "dither": "guider_start",
            "guider_calibration": "guider_calibrate",
            "phd2_settle": "guider_start",
            "emergency_park": "mount_park",
        }

    def _load_trigger_patterns(self) -> Dict[str, Dict[str, Any]]:
        """Загружает trigger patterns из settings или возвращает default."""
        try:
            exec_cfg = getattr(settings, "execution", None)
            if exec_cfg:
                patterns = getattr(exec_cfg, "trigger_patterns", None)
                if patterns and isinstance(patterns, dict):
                    logger.info(
                        f"✅ Loaded {len(patterns)} trigger patterns from settings"
                    )
                    merged = dict(DEFAULT_TRIGGER_PATTERNS)
                    merged.update(patterns)
                    return merged
        except Exception as e:
            logger.debug(f"Could not load execution config: {e}")

        return dict(DEFAULT_TRIGGER_PATTERNS)

    async def _ensure_openapi_client(self) -> Optional[DynamicAPIClient]:
        """Гарантирует, что OpenAPI клиент инициализирован."""
        if self._openapi_client is not None:
            return self._openapi_client

        try:
            self._openapi_client = await get_nina_api_client()
            self._build_registry_from_openapi()
            return self._openapi_client
        except Exception as e:
            logger.warning(f"⚠️ OpenAPI client not available: {e}")
            self._build_registry_fallback()
            return None

    def _build_registry_from_openapi(self):
        """Строит реестр триггеров из OpenAPI спецификации + patterns."""
        if not self._openapi_client:
            self._build_registry_fallback()
            return

        for trigger_name, pattern in self._trigger_patterns.items():
            method = pattern.get("method", "GET")
            path_pattern = pattern.get("path_pattern", "")

            # Сначала ищем точное совпадение
            endpoint = self._openapi_client.find_by_path(method, path_pattern)

            # Если точное не найдено — пробуем fuzzy matching
            if not endpoint:
                matches = self._openapi_client.find_by_path_pattern(path_pattern)
                for ep in matches:
                    if ep.method == method.upper():
                        endpoint = ep
                        break

            if not endpoint:
                logger.warning(
                    f"⚠️ Trigger '{trigger_name}' not found in OpenAPI spec "
                    f"(path: {path_pattern}, method: {method})"
                )
                self._registry[trigger_name] = {
                    "method": method,
                    "path": path_pattern,
                    "params": dict(pattern.get("default_params", {})),
                    "description": pattern.get("description", ""),
                    "category": pattern.get("category", "unknown"),
                    "risk_level": pattern.get("risk_level", "UNKNOWN"),
                    "parameter_ranges": {},
                    "protected_params": set(PROTECTED_PARAMS),
                    "from_openapi": False,
                }
                continue

            # Извлекаем parameter ranges из OpenAPI
            parameter_ranges: Dict[str, Dict[str, Any]] = {}
            for param in endpoint.parameters:
                if param.min_value is not None or param.max_value is not None:
                    parameter_ranges[param.name] = {}
                    if param.min_value is not None:
                        parameter_ranges[param.name]["min"] = param.min_value
                    if param.max_value is not None:
                        parameter_ranges[param.name]["max"] = param.max_value

            self._registry[trigger_name] = {
                "method": endpoint.method,
                "path": endpoint.path,
                "params": dict(pattern.get("default_params", {})),
                "description": endpoint.summary or pattern.get("description", ""),
                "category": pattern.get("category", "unknown"),
                "risk_level": pattern.get("risk_level", "UNKNOWN"),
                "parameter_ranges": parameter_ranges,
                "protected_params": set(PROTECTED_PARAMS),
                "from_openapi": True,
                "openapi_endpoint": endpoint,
            }

        logger.info(
            f"✅ Built registry: {len(self._registry)} triggers "
            f"({sum(1 for v in self._registry.values() if v.get('from_openapi'))} "
            f"from OpenAPI)"
        )

    def _build_registry_fallback(self):
        """Строит registry из patterns без OpenAPI spec (fallback)."""
        for trigger_name, pattern in self._trigger_patterns.items():
            self._registry[trigger_name] = {
                "method": pattern.get("method", "GET"),
                "path": pattern.get("path_pattern", ""),
                "params": dict(pattern.get("default_params", {})),
                "description": pattern.get("description", ""),
                "category": pattern.get("category", "unknown"),
                "risk_level": pattern.get("risk_level", "UNKNOWN"),
                "parameter_ranges": {},
                "protected_params": set(PROTECTED_PARAMS),
                "from_openapi": False,
            }

        logger.info(
            f"⚠️ Built fallback registry: {len(self._registry)} triggers "
            f"(no OpenAPI validation)"
        )

    # ====================================================================
    # ВАЛИДАЦИЯ ПАРАМЕТРОВ
    # ====================================================================

    def _validate_parameter_value(
        self,
        trigger_config: Dict[str, Any],
        param_name: str,
        value: Any,
    ) -> Tuple[bool, Optional[str]]:
        """Валидирует значение параметра против OpenAPI схемы."""
        parameter_ranges = trigger_config.get("parameter_ranges", {})

        if param_name not in parameter_ranges:
            return True, None

        range_spec = parameter_ranges[param_name]
        min_val = range_spec.get("min")
        max_val = range_spec.get("max")

        try:
            numeric_value = float(value)
        except (TypeError, ValueError):
            return False, (
                f"Parameter '{param_name}' must be numeric, got {type(value).__name__}"
            )

        if min_val is not None and numeric_value < min_val:
            return False, (
                f"Parameter '{param_name}' = {numeric_value} is below minimum {min_val}"
            )

        if max_val is not None and numeric_value > max_val:
            return False, (
                f"Parameter '{param_name}' = {numeric_value} exceeds maximum {max_val}"
            )

        return True, None

    def _merge_params_safely(
        self,
        trigger_config: Dict[str, Any],
        base_params: Dict[str, Any],
        extra_params: Optional[Dict[str, Any]],
    ) -> Tuple[Dict[str, Any], List[str]]:
        """Безопасно объединяет базовые и дополнительные параметры."""
        if not extra_params:
            return dict(base_params), []

        merged = dict(base_params)
        rejected: List[str] = []
        protected_params = trigger_config.get("protected_params", PROTECTED_PARAMS)

        for key, value in extra_params.items():
            # Проверка 1: Защищённый параметр?
            if key in protected_params:
                logger.warning(
                    f"🛡️ BLOCKED: Attempt to override protected parameter "
                    f"'{key}' with value '{value}'. Original value preserved."
                )
                rejected.append(key)
                self._stats["protected_params_rejected"] += 1
                continue

            # Проверка 2: Валидация значения
            is_valid, error_msg = self._validate_parameter_value(
                trigger_config, key, value
            )
            if not is_valid:
                logger.warning(
                    f"🛡️ BLOCKED: Invalid value for parameter '{key}': {error_msg}"
                )
                rejected.append(key)
                self._stats["blocked_by_validation"] += 1
                continue

            merged[key] = value

        return merged, rejected

    # ====================================================================
    # ИСТОРИЯ
    # ====================================================================

    def _add_to_history(
        self,
        trigger_name: str,
        actual_trigger: str,
        reason: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        rejected: Optional[List[str]] = None,
    ) -> None:
        """Добавляет запись в историю триггеров."""
        record = TriggerHistoryRecord(
            trigger=trigger_name,
            actual_trigger=actual_trigger,
            reason=reason,
            status=status,
            details=details or {},
            rejected_params=rejected or [],
        )
        self._trigger_history.append(record)

        if len(self._trigger_history) > self._history_max_size:
            self._trigger_history = self._trigger_history[-self._history_max_size :]

    # ====================================================================
    # ГЛАВНЫЙ МЕТОД (с OpenTelemetry spans)
    # ====================================================================

    async def fire_trigger(
        self,
        trigger_name: str,
        reason: str = "AI Agent Decision",
        extra_params: Optional[Dict[str, Any]] = None,
        source: str = "agent",
    ) -> bool:
        """
        Эмулирует срабатывание триггера через Advanced API.

        ИСПРАВЛЕНО (Спринт 5 — 1.5):
        - OpenTelemetry span trigger.fire.{trigger_name}
        - Атрибуты: category, risk_level, status, duration_ms, blocked_by

        Args:
            trigger_name: Имя триггера
            reason: Причина вызова
            extra_params: Дополнительные параметры
            source: "agent" (default) или "api"

        Returns:
            True если триггер успешно выполнен
        """
        self._stats["total_triggers_fired"] += 1

        # Разрешаем алиасы
        actual_trigger = self._agent_aliases.get(trigger_name, trigger_name)
        alias_note = (
            f" (aliased to '{actual_trigger}')"
            if actual_trigger != trigger_name
            else ""
        )

        # ИСПРАВЛЕНО (Спринт 5 — 1.5): OpenTelemetry parent span
        async with span_context(
            f"trigger.fire.{trigger_name}",
            attributes={
                "trigger.name": trigger_name,
                "trigger.actual_name": actual_trigger,
                "trigger.reason": reason,
                "trigger.source": source,
                "trigger.alias": alias_note,
            },
        ) as span:
            start_time = time.perf_counter()

            logger.info(
                f"🔥 Firing trigger: '{trigger_name}'{alias_note} "
                f"(Reason: {reason}, Source: {source})"
            )

            # === ПРОВЕРКА 1: Режим MANUAL ===
            from app.core.mode_manager import mode_manager, OperationMode

            if mode_manager.current_mode == OperationMode.MANUAL and source != "api":
                logger.warning(
                    f"🛑 BLOCKED: Trigger '{trigger_name}' ignored — "
                    f"system in MANUAL mode (source: {source})."
                )
                self._stats["blocked_by_manual_mode"] += 1
                self._add_to_history(
                    trigger_name, actual_trigger, reason, "BLOCKED_MANUAL_MODE"
                )

                if span:
                    span.set_attribute("trigger.status", "BLOCKED_MANUAL_MODE")
                    span.set_attribute("trigger.blocked_by", "manual_mode")

                return False

            # === ПРОВЕРКА 2: FLAT_MODE ===
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
                    self._add_to_history(
                        trigger_name, actual_trigger, reason, "BLOCKED_FLAT_MODE"
                    )

                    if span:
                        span.set_attribute("trigger.status", "BLOCKED_FLAT_MODE")
                        span.set_attribute("trigger.blocked_by", "flat_mode")

                    return False

            # === ПРОВЕРКА 3: Критическая фаза (shutdown) ===
            if state_tracker.state.is_approaching_shutdown:
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
                    self._add_to_history(
                        trigger_name,
                        actual_trigger,
                        reason,
                        "BLOCKED_CRITICAL_PHASE",
                    )

                    if span:
                        span.set_attribute("trigger.status", "BLOCKED_CRITICAL_PHASE")
                        span.set_attribute("trigger.blocked_by", "critical_phase")

                    return False

            # === ПРОВЕРКА 4: HAL валидация ===
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
                        actual_trigger,
                        reason,
                        "BLOCKED_HAL",
                        {"hal_reason": hal_reason},
                    )

                    if span:
                        span.set_attribute("trigger.status", "BLOCKED_HAL")
                        span.set_attribute("trigger.blocked_by", "hal")
                        span.set_attribute("trigger.hal_reason", hal_reason)

                    return False
            except ImportError:
                logger.debug("HAL not available, skipping HAL validation")

            # === ПРОВЕРКА 5: Получение конфигурации триггера ===
            if not self._registry:
                await self._ensure_openapi_client()

            trigger_config = self._registry.get(actual_trigger)
            if not trigger_config:
                available = ", ".join(sorted(self._registry.keys()))
                logger.error(
                    f"❌ Unknown trigger: '{trigger_name}'. Available: {available}"
                )
                self._add_to_history(
                    trigger_name, actual_trigger, reason, "FAILED_UNKNOWN_TRIGGER"
                )

                if span:
                    span.set_attribute("trigger.status", "FAILED_UNKNOWN_TRIGGER")

                return False

            # Добавляем атрибуты конфигурации к span
            if span:
                span.set_attribute(
                    "trigger.category",
                    trigger_config.get("category", "unknown"),
                )
                span.set_attribute(
                    "trigger.risk_level",
                    trigger_config.get("risk_level", "UNKNOWN"),
                )
                span.set_attribute(
                    "trigger.from_openapi",
                    trigger_config.get("from_openapi", False),
                )

            # === ПРОВЕРКА 6: Безопасное объединение параметров ===
            params, rejected = self._merge_params_safely(
                trigger_config,
                trigger_config.get("params", {}),
                extra_params,
            )
            self._last_rejected_params = rejected if rejected else None

            if rejected:
                logger.warning(
                    f"⚠️ Trigger '{trigger_name}' had {len(rejected)} "
                    f"parameters rejected: {rejected}. "
                    f"Proceeding with safe parameters."
                )

            # === ВЫПОЛНЕНИЕ ЗАПРОСА ===
            method = trigger_config["method"]

            # Используем OpenAPI клиент если доступен
            if self._openapi_client and trigger_config.get("from_openapi"):
                result = await self._fire_via_openapi(
                    trigger_name,
                    actual_trigger,
                    trigger_config,
                    params,
                    rejected,
                    reason,
                    span,
                )
            else:
                # Fallback: прямой HTTP запрос
                result = await self._fire_direct_http(
                    trigger_name,
                    actual_trigger,
                    method,
                    f"{self.base_url}{trigger_config['path']}",
                    params,
                    rejected,
                    reason,
                    span,
                )

            # Записываем duration в span
            elapsed_ms = (time.perf_counter() - start_time) * 1000
            if span:
                span.set_attribute("trigger.duration_ms", round(elapsed_ms, 2))
                span.set_attribute(
                    "trigger.status",
                    "SUCCESS" if result else "FAILED",
                )

            return result

    async def _fire_via_openapi(
        self,
        trigger_name: str,
        actual_trigger: str,
        trigger_config: Dict[str, Any],
        params: Dict[str, Any],
        rejected: List[str],
        reason: str,
        parent_span=None,
    ) -> bool:
        """Выполняет триггер через OpenAPI клиент (с валидацией)."""
        self._stats["openapi_calls"] += 1

        endpoint = trigger_config.get("openapi_endpoint")
        if not endpoint:
            return await self._fire_direct_http(
                trigger_name,
                actual_trigger,
                trigger_config["method"],
                f"{self.base_url}{trigger_config['path']}",
                params,
                rejected,
                reason,
                parent_span,
            )

        # ИСПРАВЛЕНО (Спринт 5 — 1.5): child span для HTTP вызова
        async with span_context(
            "trigger.http_request",
            attributes={
                "trigger.operation_id": endpoint.operation_id or "",
                "trigger.method": endpoint.method,
                "trigger.path": endpoint.path,
                "trigger.via": "openapi",
            },
        ) as http_span:
            result = (
                await self._openapi_client.call_endpoint(
                    operation_id=endpoint.operation_id or "",
                    params=params,
                    validate=True,
                )
                if endpoint.operation_id
                else await self._openapi_client.call_by_path(
                    method=endpoint.method,
                    path=endpoint.path,
                    params=params,
                    validate=True,
                )
            )

            return self._process_http_result(
                result,
                trigger_name,
                actual_trigger,
                params,
                rejected,
                reason,
                http_span,
            )

    async def _fire_direct_http(
        self,
        trigger_name: str,
        actual_trigger: str,
        method: str,
        url: str,
        params: Dict[str, Any],
        rejected: List[str],
        reason: str,
        parent_span=None,
    ) -> bool:
        """
        Выполняет прямой HTTP запрос (fallback без OpenAPI).
        ИСПРАВЛЕНО (С-15): использует http_client_manager.
        ИСПРАВЛЕНО (Спринт 5 — 1.5): child span для HTTP вызова.
        """
        self._stats["direct_calls"] += 1

        # ИСПРАВЛЕНО (Спринт 5 — 1.5): child span для HTTP вызова
        async with span_context(
            "trigger.http_request",
            attributes={
                "trigger.method": method,
                "trigger.url": url,
                "trigger.via": "direct_http",
                "trigger.params_count": len(params),
            },
        ) as http_span:
            try:
                client = await http_client_manager.get_client(
                    base_url=self.base_url,
                    service="nina",
                )

                if method == "GET":
                    response = await client.get(url, params=params)
                elif method == "POST":
                    response = await client.post(url, json=params)
                else:
                    logger.error(f"❌ Unsupported method: {method}")
                    self._add_to_history(
                        trigger_name,
                        actual_trigger,
                        reason,
                        "FAILED_UNSUPPORTED_METHOD",
                    )
                    if http_span:
                        http_span.set_attribute("trigger.error", "unsupported_method")
                    return False

                # Преобразуем response в dict-формат
                if response.status_code == 200:
                    try:
                        result = response.json()
                    except Exception:
                        result = {"status": "success", "data": response.text}
                else:
                    result = {
                        "status": "error",
                        "code": response.status_code,
                        "message": response.text[:500],
                    }

                if http_span:
                    http_span.set_attribute("http.status_code", response.status_code)

                return self._process_http_result(
                    result,
                    trigger_name,
                    actual_trigger,
                    params,
                    rejected,
                    reason,
                    http_span,
                )

            except Exception as e:
                error_type = type(e).__name__

                if "Connect" in error_type:
                    logger.error(
                        f"❌ Cannot connect to N.I.N.A. Advanced API "
                        f"at {self.base_url}\n"
                        f"   Проверьте, что N.I.N.A. запущена "
                        f"и Advanced API включен."
                    )
                    self._add_to_history(
                        trigger_name,
                        actual_trigger,
                        reason,
                        "FAILED_CONNECTION_ERROR",
                    )
                elif "Timeout" in error_type:
                    logger.error(f"❌ Timeout firing trigger '{trigger_name}'")
                    self._add_to_history(
                        trigger_name,
                        actual_trigger,
                        reason,
                        "FAILED_TIMEOUT",
                    )
                else:
                    logger.error(
                        f"❌ Unexpected error firing trigger '{trigger_name}': {e}"
                    )
                    self._add_to_history(
                        trigger_name,
                        actual_trigger,
                        reason,
                        "FAILED_UNEXPECTED",
                        {"error": str(e)},
                    )

                if http_span:
                    http_span.set_attribute("trigger.error", error_type)
                    http_span.set_attribute("trigger.error_message", str(e))

                self._stats["failed_triggers"] += 1
                return False

    def _process_http_result(
        self,
        result: Dict[str, Any],
        trigger_name: str,
        actual_trigger: str,
        params: Dict[str, Any],
        rejected: List[str],
        reason: str,
        http_span=None,
    ) -> bool:
        """Обрабатывает результат HTTP запроса (унифицированная логика)."""
        status = result.get("status")
        code = result.get("code", result.get("StatusCode"))

        # Успех: status=success или Success=true
        if status == "success" or result.get("Success") is True:
            api_response = result.get("Response", result.get("data", ""))
            logger.info(
                f"✅ Trigger '{trigger_name}' fired successfully: {api_response}"
            )
            self._stats["successful_triggers"] += 1
            self._add_to_history(
                trigger_name,
                actual_trigger,
                reason,
                "SUCCESS",
                {
                    "response": api_response,
                    "params": params,
                    "rejected_params": rejected,
                },
            )

            if http_span:
                http_span.set_attribute("trigger.result", "success")

            # Публикуем событие
            try:
                event_bus.publish(
                    "TRIGGER_FIRED",
                    {
                        "trigger": trigger_name,
                        "actual_trigger": actual_trigger,
                        "reason": reason,
                        "response": api_response,
                        "params": params,
                        "category": self._registry.get(actual_trigger, {}).get(
                            "category", "unknown"
                        ),
                        "risk_level": self._registry.get(actual_trigger, {}).get(
                            "risk_level", "UNKNOWN"
                        ),
                    },
                )
            except Exception as e:
                logger.debug(f"Failed to publish TRIGGER_FIRED event: {e}")

            return True

        # Валидационная ошибка
        if status == "error" and code == "VALIDATION_ERROR":
            errors = result.get("errors", [])
            logger.warning(f"⚠️ Trigger '{trigger_name}' validation failed: {errors}")
            self._stats["failed_triggers"] += 1
            self._add_to_history(
                trigger_name,
                actual_trigger,
                reason,
                "FAILED_VALIDATION",
                {"errors": errors},
            )
            if http_span:
                http_span.set_attribute("trigger.result", "validation_error")
            return False

        # HTTP ошибка
        if status == "error":
            error_msg = result.get("message", result.get("Error", ""))
            logger.warning(
                f"⚠️ Trigger '{trigger_name}' failed (code={code}): {error_msg}"
            )
            self._stats["failed_triggers"] += 1

            if code == 404:
                logger.error(
                    f"❌ Endpoint not found (404) for trigger "
                    f"'{trigger_name}'.\n"
                    f"   Проверьте, что Advanced API плагин "
                    f"установлен и запущен."
                )
                self._add_to_history(
                    trigger_name,
                    actual_trigger,
                    reason,
                    "FAILED_NOT_FOUND",
                    {"error": error_msg},
                )
            elif code == 409:
                self._add_to_history(
                    trigger_name,
                    actual_trigger,
                    reason,
                    "FAILED_CONFLICT",
                    {"error": error_msg},
                )
            else:
                self._add_to_history(
                    trigger_name,
                    actual_trigger,
                    reason,
                    f"FAILED_HTTP_{code}",
                    {"error": error_msg, "code": code},
                )

            if http_span:
                http_span.set_attribute("trigger.result", "http_error")
                http_span.set_attribute("trigger.http_code", code or 0)

            return False

        # Неожиданный формат
        logger.warning(
            f"⚠️ Trigger '{trigger_name}' returned unexpected result: {result}"
        )
        self._stats["failed_triggers"] += 1
        self._add_to_history(
            trigger_name,
            actual_trigger,
            reason,
            "FAILED_UNKNOWN_FORMAT",
            {"result": result},
        )

        if http_span:
            http_span.set_attribute("trigger.result", "unknown_format")

        return False

    # ====================================================================
    # ПРЯМОЙ ВЫЗОВ ЛЮБОГО OPENAPI ЭНДПОИНТА (FALLBACK)
    # ====================================================================

    async def call_openapi_endpoint(
        self,
        operation_id: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Прямой вызов любого OpenAPI эндпоинта по operationId."""
        client = await self._ensure_openapi_client()
        if not client:
            return {
                "status": "error",
                "message": "OpenAPI client not available",
            }
        return await client.call_endpoint(operation_id, params, body)

    async def call_by_path(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Прямой вызов эндпоинта по методу и path."""
        client = await self._ensure_openapi_client()
        if not client:
            return {
                "status": "error",
                "message": "OpenAPI client not available",
            }
        return await client.call_by_path(method, path, params, body)

    # ====================================================================
    # ПУБЛИЧНЫЕ МЕТОДЫ
    # ====================================================================

    def list_available_triggers(self) -> Dict[str, Dict[str, Any]]:
        """Возвращает список всех доступных триггеров."""
        result = {}
        for name, config in self._registry.items():
            param_details = {}
            for param_name, default_value in config.get("params", {}).items():
                param_info = {
                    "default": default_value,
                    "protected": param_name
                    in config.get("protected_params", PROTECTED_PARAMS),
                }
                ranges = config.get("parameter_ranges", {})
                if param_name in ranges:
                    param_info["range"] = ranges[param_name]
                param_details[param_name] = param_info

            result[name] = {
                "method": config["method"],
                "path": config["path"],
                "description": config["description"],
                "category": config["category"],
                "risk_level": config["risk_level"],
                "params": param_details,
                "from_openapi": config.get("from_openapi", False),
                "full_url": f"{self.base_url}{config['path']}",
            }
        return result

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику TriggerEmulator."""
        total = max(self._stats["total_triggers_fired"], 1)
        success_rate = (self._stats["successful_triggers"] / total) * 100
        return {
            **self._stats,
            "success_rate_percent": round(success_rate, 2),
            "base_url": self.base_url,
            "total_available_triggers": len(self._registry),
            "protected_params": sorted(PROTECTED_PARAMS),
            "openapi_available": self._openapi_client is not None,
            "history_size": len(self._trigger_history),
            "recent_triggers": [r.model_dump() for r in self._trigger_history[-10:]],
            "agent_aliases": self._agent_aliases,
            "last_rejected_params": self._last_rejected_params,
        }

    def get_trigger_history(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Возвращает историю последних триггеров."""
        return [r.model_dump() for r in reversed(self._trigger_history[-limit:])]

    async def close(self):
        """Закрывает OpenAPI клиент."""
        if self._openapi_client:
            await self._openapi_client.close()
            self._openapi_client = None


# Singleton instance
trigger_emulator = TriggerEmulator()
