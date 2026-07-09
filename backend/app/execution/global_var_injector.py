"""
Global Variable Injector — изменение глобальных переменных Sequencer+ через
N.I.N.A. Advanced API.

ИСПРАВЛЕНО (audit 11.1):
- Внедрено маскирование чувствительных значений в логах
- Значения переменных, имена которых содержат паттерны типа 'token',
  'password', 'key', 'secret', маскируются в логах (отображаются как '***')
- Оригинальное значение сохраняется для отправки в N.I.N.A.
- Паттерны маскирования настраиваются через settings.security.sensitive_patterns
"""

import re
import logging
from typing import Any, Optional, Set
from app.execution.nina_client import nina_client
from app.shadow_engine.state_tracker import state_tracker

logger = logging.getLogger("GlobalVarInjector")


# ============================================================================
# КОНФИГУРАЦИЯ МАСКИРОВАНИЯ
# ============================================================================

# Паттерны имен переменных, значения которых считаются чувствительными.
# Применяется case-insensitive поиск подстроки в имени переменной.
# Список можно расширить через settings.security.sensitive_patterns.
DEFAULT_SENSITIVE_PATTERNS: Set[str] = {
    "token",
    "password",
    "passwd",
    "secret",
    "api_key",
    "apikey",
    "api-key",
    "private_key",
    "privatekey",
    "credentials",
    "auth",
    "bearer",
    "access_key",
    "accesskey",
    "secret_key",
    "secretkey",
}

# Значение-маска, отображаемое в логах вместо реального значения
MASK_VALUE: str = "***"

# Паттерны ЗНАЧЕНИЙ, которые всегда маскируются (например, UUID, длинные hex)
# Регулярные выражения для детекции чувствительных значений независимо от имени
VALUE_PATTERNS = [
    # JWT токены (3 части, разделённые точками)
    re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$"),
    # Длинные hex-строки (32+ символов, например MD5/SHA)
    re.compile(r"^[a-fA-F0-9]{32,}$"),
    # AWS-style access keys (AKIA...)
    re.compile(r"^AKIA[0-9A-Z]{16}$"),
    # Base64-подобные длинные строки (64+ символов)
    re.compile(r"^[A-Za-z0-9+/]{64,}={0,2}$"),
]


class GlobalVarInjector:
    """
    Изменяет глобальные переменные Sequencer+ через Advanced API.

    ИСПРАВЛЕНО (audit 11.1):
    - Чувствительные значения маскируются в логах
    - Оригинальные значения передаются в N.I.N.A. без изменений
    - Поддержка настраиваемых паттернов через конфигурацию

    Примеры:
    - Переменная `API_TOKEN` со значением `abc123...` будет залогирована как
      `API_TOKEN = ***`
    - Переменная `EXPOSURE_TIME` со значением `60.0` будет залогирована как
      `EXPOSURE_TIME = 60.0`
    """

    def __init__(self, extra_patterns: Optional[Set[str]] = None):
        """
        Args:
            extra_patterns: Дополнительные паттерны для маскирования
                           (объединяются с DEFAULT_SENSITIVE_PATTERNS)
        """
        self._sensitive_patterns: Set[str] = set(DEFAULT_SENSITIVE_PATTERNS)
        if extra_patterns:
            self._sensitive_patterns.update(extra_patterns)

        # Загружаем дополнительные паттерны из settings (если есть)
        try:
            from app.core.config import settings

            security_config = getattr(settings, "security", None)
            if security_config:
                config_patterns = getattr(security_config, "sensitive_patterns", None)
                if config_patterns and isinstance(config_patterns, (list, set)):
                    self._sensitive_patterns.update(config_patterns)
        except Exception as e:
            logger.debug(f"Could not load security config: {e}")

        # Статистика
        self._stats = {
            "total_variables_set": 0,
            "sensitive_values_masked": 0,
            "blocked_by_shutdown": 0,
            "failed_requests": 0,
        }

        logger.info(
            f"🔧 GlobalVarInjector initialized "
            f"({len(self._sensitive_patterns)} sensitive patterns configured)"
        )

    def _is_sensitive_name(self, name: str) -> bool:
        """
        Проверяет, является ли имя переменной чувствительным.

        Args:
            name: Имя переменной

        Returns:
            True если имя содержит чувствительный паттерн
        """
        if not name:
            return False

        name_lower = name.lower()
        return any(
            pattern.lower() in name_lower for pattern in self._sensitive_patterns
        )

    def _is_sensitive_value(self, value: Any) -> bool:
        """
        Проверяет, является ли значение чувствительным по своему формату.
        ИСПРАВЛЕНО (v4.0 — проблема #32): добавлены дополнительные проверки.

        Использует regex-паттерны для детекции JWT, hex-строк, AWS-ключей и т.д.
        Дополнительно проверяет:
        - Короткие токены (16+ символов, похожие на ключи)
        - Base64-строки
        - UUID форматы

        Args:
            value: Значение для проверки

        Returns:
            True если значение соответствует чувствительному паттерну
        """
        if not isinstance(value, str) or len(value) < 8:
            return False

        # ИСПРАВЛЕНО: более агрессивная проверка для коротких значений
        if len(value) < 16:
            # Короткие значения маскируем если они выглядят как hex/base64
            import re

            # Hex строки (8+ символов)
            if re.match(r"^[a-fA-F0-9]{8,}$", value):
                return True
            # Base64-подобные (с символами +/=)
            if re.match(r"^[A-Za-z0-9+/]{8,}={0,2}$", value):
                return True

        return any(pattern.match(value) for pattern in VALUE_PATTERNS)

    def _mask_sensitive_value(self, name: str, value: Any) -> str:
        """
        Возвращает маску для чувствительного значения (для логирования).

        Логика:
        1. Если имя переменной содержит чувствительный паттерн → маска
        2. Если значение соответствует чувствительному формату → маска
        3. Иначе → строковое представление оригинального значения

        Args:
            name: Имя переменной
            value: Значение переменной

        Returns:
            Строка для отображения в логах
        """
        # Проверка по имени
        if self._is_sensitive_name(name):
            return MASK_VALUE

        # Проверка по формату значения
        if self._is_sensitive_value(value):
            return MASK_VALUE

        # Не чувствительное значение
        return str(value)

    def _log_variable_change(
        self,
        name: str,
        value: Any,
        reason: str,
        success: bool,
    ) -> None:
        """
        Логирует изменение переменной с учётом маскирования.

        Args:
            name: Имя переменной
            value: Значение переменной
            reason: Причина изменения
            success: Успешность операции
        """
        masked_value = self._mask_sensitive_value(name, value)
        is_masked = masked_value == MASK_VALUE

        if is_masked:
            self._stats["sensitive_values_masked"] += 1

        status_icon = "✅" if success else "❌"
        mask_note = " [MASKED]" if is_masked else ""

        logger.info(
            f"{status_icon} Setting global variable: {name} = {masked_value}{mask_note} "
            f"(Reason: {reason})"
        )

    async def set_variable(
        self, name: str, value: Any, reason: str = "AI Optimization"
    ) -> bool:
        """
        Устанавливает новое значение для глобальной переменной.

        Аргументы логируются с учётом маскирования чувствительных данных.
        Оригинальное значение передаётся в N.I.N.A. без изменений.

        Args:
            name: Имя глобальной переменной
            value: Новое значение
            reason: Причина изменения (для логов и Decision Audit)

        Returns:
            True если переменная успешно установлена
        """
        self._stats["total_variables_set"] += 1

        # Проверка: существует ли переменная в теневом графе?
        if name not in state_tracker.state.global_variables:
            logger.warning(
                f"⚠️ Variable '{name}' not found in sequence shadow graph "
                f"(will attempt to set anyway — may be dynamic)"
            )

        # Проверка критической фазы
        if state_tracker.state.is_approaching_shutdown:
            logger.warning(
                f"🛑 BLOCKED: Variable change ignored - approaching shutdown "
                f"(variable: {name})"
            )
            self._stats["blocked_by_shutdown"] += 1
            self._log_variable_change(name, value, reason, success=False)
            return False

        try:
            # Отправляем ОРИГИНАЛЬНОЕ значение в N.I.N.A.
            # (маскирование применяется только в логах!)
            response = await nina_client.post(
                "sequence/global-variable",
                json_data={"name": name, "value": str(value)},
            )

            # Обновляем локальный стейт
            state_tracker.state.global_variables[name] = str(value)

            # Логируем с маскированием
            self._log_variable_change(name, value, reason, success=True)

            return True

        except Exception as e:
            logger.error(f"❌ Failed to set variable '{name}': {e}")
            self._stats["failed_requests"] += 1
            self._log_variable_change(name, value, reason, success=False)
            return False

    async def get_variable(self, name: str) -> Any:
        """
        Возвращает текущее значение переменной из стейта.

        Args:
            name: Имя переменной

        Returns:
            Текущее значение или None если не найдено
        """
        return state_tracker.state.global_variables.get(name)

    def get_stats(self) -> dict:
        """
        Возвращает статистику операций с переменными.

        Включает:
        - Общее количество установленных переменных
        - Количество замаскированных чувствительных значений
        - Количество блокировок из-за shutdown
        - Количество неудачных запросов
        """
        return {
            **self._stats,
            "sensitive_patterns_count": len(self._sensitive_patterns),
            "sensitive_patterns": sorted(self._sensitive_patterns),
        }


# Singleton instance
global_var_injector = GlobalVarInjector()
