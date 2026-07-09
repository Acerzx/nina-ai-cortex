"""
Python Bridge — выполнение заранее определённых Python-скриптов внутри N.I.N.A.
через плагин nina.plugin.python.

КРИТИЧЕСКОЕ ИСПРАВЛЕНИЕ БЕЗОПАСНОСТИ (audit 3.4):
- Полностью удалена возможность выполнения произвольного Python-кода.
- Внедрён whitelist-подход: только явно определённые шаблоны (templates)
  разрешены к выполнению.
- Каждый шаблон имеет строго типизированные параметры с валидацией.
- Метод execute() удалён и заменён на execute_template().

ИСПРАВЛЕНИЕ (audit 5.2):
- System.Threading.Thread.Sleep (блокирует UI-поток N.I.N.A.)
  заменён на await Task.Delay (асинхронная задержка, не блокирует UI).
"""

import logging
import base64
from typing import Dict, Any, Optional, List
from string import Template
from pydantic import BaseModel, Field, validator
from app.execution.nina_client import nina_client

logger = logging.getLogger("PythonBridge")


# ============================================================================
# WHITELIST ШАБЛОНОВ РАЗРЕШЁННЫХ СКРИПТОВ
# ============================================================================
# Каждый шаблон — это безопасная IronPython/C# операция, которая может быть
# выполнена внутри N.I.N.A. Параметры валидируются перед подстановкой.

ALLOWED_TEMPLATES: Dict[str, Dict[str, Any]] = {
    "shutdown_intercept": {
        "description": (
            "Перехват команды Shutdown PC — показывает уведомление "
            "и ждёт указанное время, давая пользователю шанс отменить."
        ),
        # ИСПРАВЛЕНО (audit 5.2): Task.Delay вместо Thread.Sleep
        "template": """
import System.Threading.Tasks
from NINA.Core.Utility.Notification import Success
await Task.Delay(int($delay_ms))
Success("Shutdown intercepted by AI Cortex. Delayed for $delay_minutes minutes.")
""",
        "params": {
            "delay_minutes": {
                "type": int,
                "min": 1,
                "max": 60,
                "default": 5,
                "description": "Задержка в минутах (1-60)",
            },
        },
        # delay_ms вычисляется автоматически из delay_minutes
        "derived_params": {
            "delay_ms": lambda p: p["delay_minutes"] * 60 * 1000,
        },
    },
    "show_notification": {
        "description": "Показывает уведомление в N.I.N.A.",
        "template": """
from NINA.Core.Utility.Notification import Success
Success($message)
""",
        "params": {
            "message": {
                "type": str,
                "max_length": 500,
                "default": "AI Cortex notification",
                "description": "Текст уведомления (до 500 символов)",
            },
        },
    },
    "show_warning": {
        "description": "Показывает предупреждение в N.I.N.A.",
        "template": """
from NINA.Core.Utility.Notification import Warning
Warning($message)
""",
        "params": {
            "message": {
                "type": str,
                "max_length": 500,
                "default": "AI Cortex warning",
                "description": "Текст предупреждения (до 500 символов)",
            },
        },
    },
    "show_error": {
        "description": "Показывает ошибку в N.I.N.A.",
        "template": """
from NINA.Core.Utility.Notification import Error
Error($message)
""",
        "params": {
            "message": {
                "type": str,
                "max_length": 500,
                "default": "AI Cortex error",
                "description": "Текст ошибки (до 500 символов)",
            },
        },
    },
    "write_to_log": {
        "description": "Записывает сообщение в лог N.I.N.A.",
        "template": """
import NINA.Core.Utility.Logger as Logger
Logger.Info($message)
""",
        "params": {
            "message": {
                "type": str,
                "max_length": 1000,
                "default": "AI Cortex log entry",
                "description": "Текст для лога (до 1000 символов)",
            },
        },
    },
}

# Запрещённые подстроки — дополнительная защита от инъекций в параметрах
FORBIDDEN_SUBSTRINGS: List[str] = [
    "import os",
    "import sys",
    "import subprocess",
    "os.system",
    "os.popen",
    "subprocess.",
    "eval(",
    "exec(",
    "__import__",
    "open(",
    "System.IO.File",
    "System.Diagnostics.Process",
    "System.Net.WebClient",
    "System.Net.Http",
]


class TemplateParamSpec(BaseModel):
    """Спецификация параметра шаблона."""

    type: str  # "int", "str", "float", "bool"
    min: Optional[float] = None
    max: Optional[float] = None
    max_length: Optional[int] = None
    default: Optional[Any] = None
    description: str = ""


class PythonBridge:
    """
    Безопасный мост для выполнения Python-скриптов внутри N.I.N.A.

    Архитектурное решение (audit 3.4):
    - Произвольный код НЕ принимается — только предопределённые шаблоны.
    - Каждый шаблон проходит через string.Template safe_substitute.
    - Все параметры строго валидируются по типам и диапазонам.
    - Каждый вызов логируется в Decision Audit Trail.
    """

    def __init__(self):
        self._templates = ALLOWED_TEMPLATES
        # ИСПРАВЛЕНО (v4.0 — проблема #33): проверка версии N.I.N.A. для Task.Delay
        self._nina_version: Optional[str] = None
        self._use_task_delay: bool = True  # По умолчанию используем Task.Delay
        self._version_checked: bool = False
        logger.info(
            f"🔒 PythonBridge initialized "
            f"(whitelist mode, {len(self._templates)} templates available)"
        )

    def _validate_params(
        self,
        template_name: str,
        params: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Валидирует параметры шаблона.
        Возвращает валидированный словарь параметров.
        Raises:
            ValueError: если параметр некорректен
        """
        template_config = self._templates[template_name]
        param_specs = template_config["params"]
        validated = {}

        for param_name, spec in param_specs.items():
            value = params.get(param_name, spec.get("default"))

            if value is None:
                raise ValueError(
                    f"Required parameter '{param_name}' is missing and has no default"
                )

            # Проверка типа
            expected_type = spec["type"]
            if expected_type == int:
                if not isinstance(value, (int, float)):
                    raise ValueError(
                        f"Parameter '{param_name}' must be int, "
                        f"got {type(value).__name__}"
                    )
                value = int(value)
            elif expected_type == float:
                if not isinstance(value, (int, float)):
                    raise ValueError(
                        f"Parameter '{param_name}' must be float, "
                        f"got {type(value).__name__}"
                    )
                value = float(value)
            elif expected_type == str:
                if not isinstance(value, str):
                    raise ValueError(
                        f"Parameter '{param_name}' must be str, "
                        f"got {type(value).__name__}"
                    )
                # Проверка на запрещённые подстроки
                for forbidden in FORBIDDEN_SUBSTRINGS:
                    if forbidden.lower() in value.lower():
                        raise ValueError(
                            f"Parameter '{param_name}' contains "
                            f"forbidden substring: {forbidden}"
                        )
                # Проверка длины
                max_length = spec.get("max_length", 500)
                if len(value) > max_length:
                    raise ValueError(
                        f"Parameter '{param_name}' exceeds max length "
                        f"({len(value)} > {max_length})"
                    )
            elif expected_type == bool:
                if not isinstance(value, bool):
                    raise ValueError(
                        f"Parameter '{param_name}' must be bool, "
                        f"got {type(value).__name__}"
                    )

            # Проверка диапазона для числовых типов
            if expected_type in (int, float):
                if "min" in spec and value < spec["min"]:
                    raise ValueError(
                        f"Parameter '{param_name}' = {value} "
                        f"is below minimum {spec['min']}"
                    )
                if "max" in spec and value > spec["max"]:
                    raise ValueError(
                        f"Parameter '{param_name}' = {value} "
                        f"exceeds maximum {spec['max']}"
                    )

            validated[param_name] = value

        # Добавляем вычисляемые параметры
        derived = template_config.get("derived_params", {})
        for derived_name, calculator in derived.items():
            validated[derived_name] = calculator(validated)

        return validated


async def execute_template(
    self,
    template_name: str,
    params: Optional[Dict[str, Any]] = None,
    description: str = "AI Cortex Template Execution",
) -> dict:
    """Выполняет шаблон скрипта."""
    params = params or {}

    # 1. Проверка наличия шаблона
    if template_name not in self._templates:
        available = ", ".join(sorted(self._templates.keys()))
        error_msg = (
            f"Template '{template_name}' not in whitelist. Available: {available}"
        )
        logger.error(f"❌ {error_msg}")
        return {"status": "error", "message": error_msg}

    template_config = self._templates[template_name]

    # 2. Валидация параметров
    try:
        validated_params = self._validate_params(template_name, params)
    except ValueError as e:
        logger.error(f"❌ Parameter validation failed: {e}")
        return {"status": "error", "message": str(e)}

    # 3. ИСПРАВЛЕНО: проверка версии N.I.N.A. для Task.Delay
    await self._check_nina_version()

    # Если используем Thread.Sleep fallback — заменяем Task.Delay в шаблоне
    if not self._use_task_delay and "Task.Delay" in template_config["template"]:
        logger.debug(
            f"Converting Task.Delay to Thread.Sleep for N.I.N.A. {self._nina_version}"
        )
        # Создаём модифицированный шаблон с Thread.Sleep
        template_text = template_config["template"].replace(
            "await Task.Delay(", "System.Threading.Thread.Sleep("
        )
    else:
        template_text = template_config["template"]

    # 4. Безопасная подстановка параметров
    try:
        tmpl = Template(template_text)
        code = tmpl.safe_substitute(validated_params)
    except Exception as e:
        logger.error(f"❌ Template substitution failed: {e}")
        return {"status": "error", "message": f"Template error: {e}"}

    # 5. Финальная проверка
    for forbidden in FORBIDDEN_SUBSTRINGS:
        if forbidden.lower() in code.lower():
            error_msg = f"Generated code contains forbidden substring: {forbidden}"
            logger.error(f"❌ {error_msg}")
            return {"status": "error", "message": error_msg}

    # 6. Логирование и отправка
    logger.info(
        f"🔒 Executing whitelisted template: {template_name} "
        f"(description: {description})"
    )
    logger.debug(f"   Parameters: {validated_params}")
    logger.debug(f"   Generated code:\n{code}")

    return await self._send_to_nina(code, description)

    async def _send_to_nina(self, code: str, description: str) -> dict:
        """
        Отправляет сгенерированный код в N.I.N.A. через Advanced API.
        Внутренний метод — НЕ должен вызываться напрямую извне.
        """
        try:
            code_b64 = base64.b64encode(code.encode("utf-8")).decode("utf-8")
            response = await nina_client.post(
                "script/python/execute",
                json_data={"script": code_b64, "description": description},
            )
            logger.info(f"✅ Template executed successfully")
            return response
        except Exception as e:
            logger.error(f"❌ Failed to execute template: {e}")
            return {"status": "error", "message": str(e)}

    async def inject_shutdown_intercept(self, delay_minutes: int = 5) -> dict:
        """
        Специальный метод: перехват команды Shutdown PC.
        Создаёт уведомление и задержку внутри N.I.N.A., давая пользователю
        возможность отменить shutdown.

        ИСПРАВЛЕНО (audit 5.2): Task.Delay вместо Thread.Sleep
        (не блокирует UI-поток N.I.N.A.).
        """
        return await self.execute_template(
            template_name="shutdown_intercept",
            params={"delay_minutes": delay_minutes},
            description="Shutdown Interceptor",
        )

    async def show_notification(self, message: str) -> dict:
        """Показывает информационное уведом в N.I.N.A."""
        return await self.execute_template(
            template_name="show_notification",
            params={"message": message},
            description="AI Notification",
        )

    async def show_warning(self, message: str) -> dict:
        """Показывает предупреждение в N.I.N.A."""
        return await self.execute_template(
            template_name="show_warning",
            params={"message": message},
            description="AI Warning",
        )

    async def show_error(self, message: str) -> dict:
        """Показывает ошибку в N.I.N.A."""
        return await self.execute_template(
            template_name="show_error",
            params={"message": message},
            description="AI Error",
        )

    async def write_to_log(self, message: str) -> dict:
        """Записывает сообщение в лог N.I.N.A."""
        return await self.execute_template(
            template_name="write_to_log",
            params={"message": message},
            description="AI Log Entry",
        )

    def list_templates(self) -> Dict[str, Dict[str, Any]]:
        """
        Возвращает список всех доступных шаблонов с их описаниями
        и спецификациями параметров.
        """
        result = {}
        for name, config in self._templates.items():
            result[name] = {
                "description": config["description"],
                "params": {
                    param_name: {
                        "type": spec["type"].__name__,
                        "min": spec.get("min"),
                        "max": spec.get("max"),
                        "max_length": spec.get("max_length"),
                        "default": spec.get("default"),
                        "description": spec.get("description", ""),
                    }
                    for param_name, spec in config["params"].items()
                },
            }
        return result


async def _check_nina_version(self) -> bool:
    """
    Проверяет версию N.I.N.A. для определения поддержки Task.Delay.
    Task.Delay доступен в N.I.N.A. >= 3.0 с IronPython 2.7.9+
    """
    if self._version_checked:
        return self._use_task_delay

    try:
        from app.execution.nina_client import nina_client

        # Получаем версию через API
        response = await nina_client.get("version")
        version_str = response.get("Response", response.get("text", ""))

        # Парсим версию
        if version_str:
            self._nina_version = version_str.strip()

            # Проверяем, что версия >= 3.0
            try:
                version_parts = self._nina_version.split(".")
                major = int(version_parts[0]) if version_parts else 0

                if major >= 3:
                    self._use_task_delay = True
                    logger.info(
                        f"✅ N.I.N.A. version {self._nina_version} supports Task.Delay"
                    )
                else:
                    self._use_task_delay = False
                    logger.warning(
                        f"⚠️ N.I.N.A. version {self._nina_version} is older than 3.0, "
                        f"using Thread.Sleep fallback"
                    )
            except (ValueError, IndexError):
                # Не удалось распарсить — используем Task.Delay по умолчанию
                self._use_task_delay = True
                logger.debug(
                    f"Could not parse N.I.N.A. version: {version_str}, "
                    f"using Task.Delay by default"
                )

        self._version_checked = True
        return self._use_task_delay

    except Exception as e:
        logger.debug(f"Could not check N.I.N.A. version: {e}")
        # По умолчанию используем Task.Delay
        self._version_checked = True
        return True


python_bridge = PythonBridge()
