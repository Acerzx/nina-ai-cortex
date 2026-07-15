"""
Automated API Endpoint Tester v3
Автоматическое тестирование всех endpoints из FastAPI OpenAPI spec.

Улучшения v3:
- Умная генерация тестовых данных на основе OpenAPI schema
- Автоматическое обнаружение реальных значений через GET endpoints
- Корректная обработка enum, типов (float/int/bool), required полей
- Специальные значения для астро-параметров (temperature, exposure, gain)
- Увеличенный таймаут для долгих simulation operations
- Правильные JSON тела для POST endpoints
- Подробная разбивка отчёта по типам ошибок
- Исправлен баг с logger.info(..., end=" ")

Использование:
    python test_api_endpoints.py
    python test_api_endpoints.py --filter agents
    python test_api_endpoints.py --method POST
    python test_api_endpoints.py --verbose
    python test_api_endpoints.py --timeout 60
    python test_api_endpoints.py --skip-simulation  # пропустить долгие simulation тесты
"""

import asyncio
import json
import logging
import sys
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from pathlib import Path
import httpx
from dataclasses import dataclass, field

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("APITester")


# ============================================================================
# DATA MODELS
# ============================================================================
@dataclass
class TestResult:
    """Результат тестирования endpoint."""

    path: str
    method: str
    status_code: Optional[int]
    success: bool
    duration_ms: float
    response_preview: str
    error: Optional[str] = None
    request_info: str = ""
    category: str = "unknown"  # success, client_error, server_error, timeout


# ============================================================================
# SMART TEST DATA GENERATOR
# ============================================================================
class SmartTestDataGenerator:
    """
    Умный генератор тестовых данных на основе OpenAPI schema.
    Учитывает: типы, enum, required, default, format, minimum/maximum.
    """

    # Реальные значения для path parameters (будут дополнены через discovery)
    REAL_PATH_VALUES = {
        "session_id": "M31_2026-07-15",
        "filename": "test_archive.json",
        "policy_name": "keep_last_30_days",
        "agent_name": "Watcher",
        "trigger_name": "autofocus",
        "workflow_id": "test_workflow_001",
        "task_name": "decision_audit_cleanup",
    }

    # Валидные значения для enum параметров (по имени параметра)
    VALID_ENUM_VALUES = {
        "mode": "full_ai",
        "anomaly_type": "hfr_spike",
        "workflow_type": "diagnostic",
        "image_type": "DARK",
    }

    # Реальные значения для известных query параметров
    REAL_QUERY_VALUES = {
        "metric": "hfr",
        "agent": "Watcher",
        "decision_type": "ROOT_CAUSE_IDENTIFIED",
        "filter_name": "Ha",
        "prompt": "Explain Hocus Focus",
        "query": "autofocus configuration",
    }

    # Специальные числовые значения для астро-параметров
    SPECIAL_NUMERIC_VALUES = {
        "temperature": -15.0,
        "temp_tolerance": 2.0,
        "exposure": 60.0,
        "exposure_time": 60.0,
        "gain": 85,
        "offset": 10,
        "limit": 10,
        "offset": 0,
        "max_depth": 5,
        "max_tokens": 500,
        "top_k": 3,
        "frames": 5,
        "days": 30,
        "min_quality": 5.0,
        "max_retries": 2,
        "temp_tolerance": 2.0,
    }

    def generate_query_value(
        self,
        param_name: str,
        schema: Dict[str, Any],
    ) -> Any:
        """Генерирует значение для query параметра."""
        param_type = schema.get("type", "string")

        # 1. Enum
        if "enum" in schema:
            return schema["enum"][0]

        # 2. Default
        if "default" in schema:
            return schema["default"]

        # 3. Known real values
        if param_name in self.REAL_QUERY_VALUES:
            return self.REAL_QUERY_VALUES[param_name]

        # 4. Known enum values
        if param_name in self.VALID_ENUM_VALUES:
            return self.VALID_ENUM_VALUES[param_name]

        # 5. Special numeric values (for astro parameters)
        if param_name in self.SPECIAL_NUMERIC_VALUES:
            return self.SPECIAL_NUMERIC_VALUES[param_name]

        # 6. Generate by type
        type_map = {
            "string": "test_value",
            "integer": 1,
            "number": 1.5,
            "boolean": True,
        }

        return type_map.get(param_type, "test")

    def generate_path_value(
        self,
        param_name: str,
        schema: Dict[str, Any],
    ) -> str:
        """Генерирует значение для path параметра."""
        if param_name in self.REAL_PATH_VALUES:
            return self.REAL_PATH_VALUES[param_name]

        param_type = schema.get("type", "string")
        if param_type == "integer":
            return "1"
        return f"test_{param_name}"

    def generate_request_body(
        self,
        request_body: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        """
        Генерирует валидное тело запроса на основе request body schema.
        Корректно обрабатывает все типы и required поля.
        """
        if not request_body:
            return None

        content = request_body.get("content", {})
        json_content = content.get("application/json", {})
        schema = json_content.get("schema", {})

        # Обрабатываем $ref (будет разрешён через components)
        if "$ref" in schema:
            return self._generate_from_schema(schema)

        return self._generate_from_schema(schema)

    def _generate_from_schema(self, schema: Dict[str, Any]) -> Any:
        """Рекурсивно генерирует значение по schema."""
        schema_type = schema.get("type")

        # Enum
        if "enum" in schema:
            return schema["enum"][0]

        # Default
        if "default" in schema:
            return schema["default]"]

        # AnyOf/OneOf — берём первый вариант
        if "anyOf" in schema:
            return self._generate_from_schema(schema["anyOf"][0])
        if "oneOf" in schema:
            return self._generate_from_schema(schema["oneOf"][0])

        # Object
        if schema_type == "object" or "properties" in schema:
            properties = schema.get("properties", {})
            required = schema.get("required", [])
            result = {}

            for prop_name, prop_schema in properties.items():
                # Генерируем только required поля + несколько опциональных
                if prop_name in required or len(result) < 5:
                    result[prop_name] = self._generate_property_value(
                        prop_name, prop_schema
                    )

            return result

        # Array
        if schema_type == "array":
            items = schema.get("items", {})
            return [self._generate_from_schema(items)]

        # Scalar types
        return self._generate_scalar_value(schema)

    def _generate_property_value(
        self,
        prop_name: str,
        prop_schema: Dict[str, Any],
    ) -> Any:
        """Генерирует значение для одного свойства объекта."""
        # Сначала проверяем известные значения
        if prop_name in self.REAL_QUERY_VALUES:
            return self.REAL_QUERY_VALUES[prop_name]
        if prop_name in self.VALID_ENUM_VALUES:
            return self.VALID_ENUM_VALUES[prop_name]
        if prop_name in self.SPECIAL_NUMERIC_VALUES:
            return self.SPECIAL_NUMERIC_VALUES[prop_name]

        # Затем по схеме
        return self._generate_from_schema(prop_schema)

    def _generate_scalar_value(self, schema: Dict[str, Any]) -> Any:
        """Генерирует скалярное значение по типу."""
        param_type = schema.get("type", "string")
        param_format = schema.get("format", "")

        # Boolean
        if param_type == "boolean":
            return True

        # Integer
        if param_type == "integer":
            minimum = schema.get("minimum", 0)
            maximum = schema.get("maximum", 100)
            return max(minimum, min(maximum, 1))

        # Number (float)
        if param_type == "number":
            minimum = schema.get("minimum", 0.0)
            maximum = schema.get("maximum", 100.0)
            return max(minimum, min(maximum, 1.5))

        # String
        if param_type == "string":
            if param_format == "date-time":
                return datetime.now().isoformat()
            if param_format == "date":
                return datetime.now().strftime("%Y-%m-%d")
            if param_format == "email":
                return "test@example.com"
            if param_format == "uri":
                return "http://localhost:8000"
            return "test_value"

        # Fallback
        return None


# ============================================================================
# MAIN API TESTER
# ============================================================================
class APITester:
    """Автоматический тестер API endpoints v3."""

    def __init__(
        self,
        base_url: str = "http://localhost:8000",
        timeout: float = 30.0,
        skip_simulation: bool = False,
    ):
        self.base_url = base_url
        self.timeout = timeout
        self.skip_simulation = skip_simulation
        self.client = httpx.AsyncClient(timeout=timeout)
        self.results: List[TestResult] = []
        self.data_generator = SmartTestDataGenerator()
        self.openapi_spec: Optional[Dict[str, Any]] = None

        # Кэш discovered values
        self._discovered_values: Dict[str, List[str]] = {
            "session_ids": [],
            "trigger_names": [],
            "task_names": [],
        }

    async def close(self):
        await self.client.aclose()

    # ========================================================================
    # OPENAPI SPEC LOADING
    # ========================================================================
    async def fetch_openapi_spec(self) -> Dict[str, Any]:
        """Загружает OpenAPI спецификацию."""
        try:
            response = await self.client.get(f"{self.base_url}/openapi.json")
            response.raise_for_status()
            spec = response.json()
            self.openapi_spec = spec
            logger.info(
                f"✅ Loaded OpenAPI spec: {spec['info']['title']} "
                f"v{spec['info']['version']}"
            )
            return spec
        except Exception as e:
            logger.error(f"❌ Failed to load OpenAPI spec: {e}")
            raise

    def _resolve_schema_ref(self, ref: str) -> Dict[str, Any]:
        """Разрешает $ref ссылку на компонент."""
        # Формат: #/components/schemas/ModelName
        parts = ref.lstrip("#/").split("/")
        result = self.openapi_spec
        for part in parts:
            result = result.get(part, {})
        return result

    def extract_endpoints(self, spec: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Извлекает все endpoints из OpenAPI spec."""
        endpoints = []
        paths = spec.get("paths", {})

        for path, methods in paths.items():
            for method, details in methods.items():
                if method.lower() not in ("get", "post", "put", "delete", "patch"):
                    continue

                endpoint = {
                    "path": path,
                    "method": method.upper(),
                    "summary": details.get("summary", ""),
                    "operation_id": details.get("operationId", ""),
                    "parameters": details.get("parameters", []),
                    "request_body": details.get("requestBody"),
                    "tags": details.get("tags", []),
                }
                endpoints.append(endpoint)

        logger.info(f"📊 Extracted {len(endpoints)} endpoints")
        return endpoints

    # ========================================================================
    # DISCOVERY: ПОЛУЧЕНИЕ РЕАЛЬНЫХ ЗНАЧЕНИЙ
    # ========================================================================
    async def discover_real_values(self):
        """
        Обнаруживает реальные значения через GET endpoints.
        Используется для подстановки в path parameters.
        """
        logger.info("🔍 Discovering real values for path parameters...")

        # 1. Sessions
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/sessions", params={"limit": 5}
            )
            if response.status_code == 200:
                data = response.json()
                sessions = data.get("sessions", [])
                if sessions:
                    self._discovered_values["session_ids"] = [
                        s["session_id"] for s in sessions[:3]
                    ]
                    self.data_generator.REAL_PATH_VALUES["session_id"] = (
                        self._discovered_values["session_ids"][0]
                    )
                    logger.info(
                        f"   Found {len(sessions)} sessions, "
                        f"using: {self._discovered_values['session_ids'][0]}"
                    )
                else:
                    logger.info("   No sessions found (will use placeholder)")
        except Exception as e:
            logger.debug(f"   Could not discover sessions: {e}")

        # 2. Triggers
        try:
            response = await self.client.get(f"{self.base_url}/api/v1/triggers")
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict):
                    self._discovered_values["trigger_names"] = list(data.keys())[:5]
                    if self._discovered_values["trigger_names"]:
                        self.data_generator.REAL_PATH_VALUES["trigger_name"] = (
                            self._discovered_values["trigger_names"][0]
                        )
                        logger.info(
                            f"   Found {len(data)} triggers, "
                            f"using: {self._discovered_values['trigger_names'][0]}"
                        )
        except Exception as e:
            logger.debug(f"   Could not discover triggers: {e}")

        # 3. Background tasks
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/system/background-tasks"
            )
            if response.status_code == 200:
                data = response.json()
                tasks = data.get("tasks", {})
                if tasks:
                    self._discovered_values["task_names"] = list(tasks.keys())[:3]
                    self.data_generator.REAL_PATH_VALUES["task_name"] = (
                        self._discovered_values["task_names"][0]
                    )
                    logger.info(
                        f"   Found {len(tasks)} tasks, "
                        f"using: {self._discovered_values['task_names'][0]}"
                    )
        except Exception as e:
            logger.debug(f"   Could not discover tasks: {e}")

        # 4. Disk policies
        try:
            response = await self.client.get(
                f"{self.base_url}/api/v1/storage/recommendations"
            )
            if response.status_code == 200:
                # Используем реальную политику
                self.data_generator.REAL_PATH_VALUES["policy_name"] = (
                    "keep_last_30_days"
                )
        except Exception as e:
            logger.debug(f"   Could not discover policies: {e}")

    # ========================================================================
    # REQUEST BUILDING
    # ========================================================================
    def build_request_url(
        self,
        path: str,
        parameters: List[Dict[str, Any]],
    ) -> Tuple[str, Dict[str, Any]]:
        """
        Строит URL с path parameters и query parameters.
        Returns: (url, query_params)
        """
        url = path
        query_params = {}

        for param in parameters:
            param_name = param.get("name", "")
            param_in = param.get("in", "query")
            schema = param.get("schema", {})
            required = param.get("required", False)

            if param_in == "path":
                # Path parameter — всегда подставляем
                value = self.data_generator.generate_path_value(param_name, schema)
                url = url.replace(f"{{{param_name}}}", str(value))

            elif param_in == "query":
                # Query parameter
                # Передаём required и некоторые важные опциональные
                important_optional = {
                    "limit",
                    "offset",
                    "days",
                    "include_details",
                    "max_depth",
                    "show_triggers",
                    "show_conditions",
                    "max_retries",
                    "prompt",
                    "agent",
                    "query",
                    "target",
                    "frames",
                    "metric",
                    "max_tokens",
                }

                if not required and param_name not in important_optional:
                    continue

                value = self.data_generator.generate_query_value(param_name, schema)
                if value is not None:
                    query_params[param_name] = value

        return f"{self.base_url}{url}", query_params

    def get_test_timeout(self, path: str) -> float:
        """Возвращает timeout для конкретного endpoint."""
        # Долгие операции
        long_paths = {
            "trigger-meridian-flip": 60.0,  # 30 сек меридиан + запас
            "trigger-autofocus": 30.0,
            "test-llm": 60.0,
            "simulation/start": 10.0,
            "preflight": 15.0,
        }

        for pattern, timeout in long_paths.items():
            if pattern in path:
                return max(self.timeout, timeout)

        return self.timeout

    # ========================================================================
    # SINGLE ENDPOINT TEST
    # ========================================================================
    async def test_endpoint(self, endpoint: Dict[str, Any]) -> TestResult:
        """Тестирует один endpoint."""
        path = endpoint["path"]
        method = endpoint["method"]
        parameters = endpoint.get("parameters", [])
        request_body = endpoint.get("request_body")

        # Skip simulation endpoints if requested
        if self.skip_simulation and "/simulation/" in path:
            return TestResult(
                path=path,
                method=method,
                status_code=None,
                success=True,
                duration_ms=0,
                response_preview="SKIPPED (--skip-simulation)",
                category="skipped",
            )

        # Build URL and query params
        url, query_params = self.build_request_url(path, parameters)

        # Generate body for POST/PUT/PATCH
        json_body = None
        if method in ("POST", "PUT", "PATCH") and request_body:
            json_body = self.data_generator.generate_request_body(request_body)

        # Format request info
        request_info_parts = []
        if query_params:
            # Ограничиваем длину для читаемости
            params_str = ", ".join(
                f"{k}={v}" for k, v in list(query_params.items())[:5]
            )
            if len(query_params) > 5:
                params_str += f" +{len(query_params) - 5} more"
            request_info_parts.append(f"params={{{params_str}}}")
        if json_body:
            body_str = json.dumps(json_body, ensure_ascii=False)
            if len(body_str) > 100:
                body_str = body_str[:100] + "..."
            request_info_parts.append(f"body={body_str}")
        request_info = ", ".join(request_info_parts) if request_info_parts else ""

        # Get timeout for this specific endpoint
        timeout = self.get_test_timeout(path)

        logger.info(f"🧪 Testing: {method} {path}")
        if request_info:
            logger.info(f"   Request: {request_info}")

        start_time = datetime.now()
        try:
            # Выполняем запрос с индивидуальным таймаутом
            async with httpx.AsyncClient(timeout=timeout) as client:
                if method == "GET":
                    response = await client.get(url, params=query_params)
                elif method == "POST":
                    response = await client.post(
                        url, params=query_params, json=json_body
                    )
                elif method == "PUT":
                    response = await client.put(
                        url, params=query_params, json=json_body
                    )
                elif method == "DELETE":
                    response = await client.delete(url, params=query_params)
                elif method == "PATCH":
                    response = await client.patch(
                        url, params=query_params, json=json_body
                    )
                else:
                    return TestResult(
                        path=path,
                        method=method,
                        status_code=None,
                        success=False,
                        duration_ms=0,
                        response_preview="",
                        error=f"Unsupported method: {method}",
                        request_info=request_info,
                        category="error",
                    )

            duration_ms = (datetime.now() - start_time).total_seconds() * 1000

            # Определяем категорию ответа
            status_code = response.status_code
            if 200 <= status_code < 300:
                category = "success"
                success = True
            elif 400 <= status_code < 500:
                category = "client_error"
                success = True  # Endpoint работает, просто данные невалидны
            elif 500 <= status_code < 600:
                category = "server_error"
                success = False
            else:
                category = "unknown"
                success = False

            # Preview ответа
            try:
                response_data = response.json()
                preview = json.dumps(response_data, indent=2, ensure_ascii=False)[:300]
            except Exception:
                preview = response.text[:300] if response.text else ""

            return TestResult(
                path=path,
                method=method,
                status_code=status_code,
                success=success,
                duration_ms=duration_ms,
                response_preview=preview,
                request_info=request_info,
                category=category,
            )

        except httpx.TimeoutException:
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            return TestResult(
                path=path,
                method=method,
                status_code=None,
                success=False,
                duration_ms=duration_ms,
                response_preview="",
                error=f"Timeout after {timeout}s",
                request_info=request_info,
                category="timeout",
            )
        except Exception as e:
            duration_ms = (datetime.now() - start_time).total_seconds() * 1000
            return TestResult(
                path=path,
                method=method,
                status_code=None,
                success=False,
                duration_ms=duration_ms,
                response_preview="",
                error=f"{type(e).__name__}: {str(e)}",
                request_info=request_info,
                category="error",
            )

    # ========================================================================
    # MAIN TEST RUNNER
    # ========================================================================
    async def run_tests(
        self,
        filter_tag: Optional[str] = None,
        filter_method: Optional[str] = None,
        verbose: bool = False,
    ):
        """Запускает тестирование всех endpoints."""
        spec = await self.fetch_openapi_spec()
        endpoints = self.extract_endpoints(spec)

        # Discover реальные значения
        await self.discover_real_values()

        # Фильтрация по тегу
        if filter_tag:
            endpoints = [e for e in endpoints if filter_tag in e.get("tags", [])]
            logger.info(f"🏷️ Filtered by tag '{filter_tag}': {len(endpoints)} endpoints")

        # Фильтрация по методу
        if filter_method:
            endpoints = [e for e in endpoints if e["method"] == filter_method.upper()]
            logger.info(
                f"🔧 Filtered by method '{filter_method}': {len(endpoints)} endpoints"
            )

        logger.info(f"🚀 Starting tests for {len(endpoints)} endpoints...")
        logger.info(f"⏱️ Base timeout: {self.timeout}s")
        if self.skip_simulation:
            logger.info("⏭️ Skipping /simulation/ endpoints")

        for i, endpoint in enumerate(endpoints, 1):
            # Прогресс через stdout (не logger, чтобы не было переноса строки)
            sys.stdout.write(f"[{i}/{len(endpoints)}] ")
            sys.stdout.flush()

            result = await self.test_endpoint(endpoint)
            self.results.append(result)

            # Статус иконка
            if result.category == "skipped":
                status_icon = "⏭️"
            elif result.category == "success":
                status_icon = "✅"
            elif result.category == "client_error":
                status_icon = "⚠️"
            elif result.category == "server_error":
                status_icon = "❌"
            elif result.category == "timeout":
                status_icon = "⏱️"
            else:
                status_icon = "❌"

            # Выводим статус через print (с переносом строки)
            print(
                f"{status_icon} {result.status_code or 'N/A'} "
                f"({result.duration_ms:.0f}ms)"
            )

            if verbose or (not result.success and result.category != "skipped"):
                if result.error:
                    logger.error(f"   Error: {result.error}")
                if result.response_preview:
                    preview_lines = result.response_preview.split("\n")[:3]
                    for line in preview_lines:
                        logger.info(f"   {line}")

    # ========================================================================
    # REPORTING
    # ========================================================================
    def print_report(self):
        """Печатает отчёт о тестировании."""
        total = len(self.results)
        skipped = sum(1 for r in self.results if r.category == "skipped")
        tested = total - skipped

        successful = sum(1 for r in self.results if r.success)
        failed = tested - successful

        # Разбивка по категориям
        success_2xx = sum(
            1 for r in self.results if r.status_code and 200 <= r.status_code < 300
        )
        client_4xx = sum(
            1 for r in self.results if r.status_code and 400 <= r.status_code < 500
        )
        server_5xx = sum(
            1 for r in self.results if r.status_code and 500 <= r.status_code < 600
        )
        timeouts = sum(1 for r in self.results if r.category == "timeout")
        errors = sum(
            1 for r in self.results if r.category == "error" and r.category != "timeout"
        )

        # Средняя длительность (исключая пропущенные)
        tested_results = [r for r in self.results if r.category != "skipped"]
        avg_duration = (
            sum(r.duration_ms for r in tested_results) / len(tested_results)
            if tested_results
            else 0
        )

        logger.info("=" * 80)
        logger.info("📊 TEST REPORT")
        logger.info("=" * 80)
        logger.info(f"Total endpoints: {total}")
        if skipped > 0:
            logger.info(f"Skipped: {skipped} ⏭️")
        logger.info(f"Tested: {tested}")
        logger.info(
            f"Successful (2xx+4xx): {successful} ✅ ({successful / tested * 100:.1f}%)"
            if tested > 0
            else ""
        )
        logger.info(f"Failed (5xx+errors): {failed} ❌")
        logger.info("-" * 80)
        logger.info(f"  2xx (OK): {success_2xx}")
        logger.info(f"  4xx (Client error): {client_4xx}")
        logger.info(f"  5xx (Server error): {server_5xx}")
        logger.info(f"  Timeouts: {timeouts}")
        logger.info(f"  Errors: {errors}")
        logger.info(f"  Average duration: {avg_duration:.0f}ms")
        logger.info("=" * 80)

        # Детали по ошибкам
        failed_results = [
            r for r in self.results if not r.success and r.category != "skipped"
        ]

        if failed_results:
            logger.info(f"\n❌ FAILED ENDPOINTS ({len(failed_results)}):")
            for result in failed_results:
                logger.error(f"  {result.method} {result.path}")
                logger.error(
                    f"    Status: {result.status_code or 'N/A'} ({result.category})"
                )
                if result.request_info:
                    logger.error(f"    Request: {result.request_info}")
                if result.error:
                    logger.error(f"    Error: {result.error}")
                elif result.response_preview:
                    preview = result.response_preview[:200].replace("\n", " ")
                    logger.error(f"    Response: {preview}")

        # Детали по client errors (для анализа)
        client_errors = [r for r in self.results if r.category == "client_error"]
        if client_errors and len(client_errors) > 0:
            logger.info(
                f"\n⚠️ CLIENT ERRORS ({len(client_errors)}) - "
                f"endpoints work, test data may need adjustment:"
            )
            for result in client_errors[:5]:  # Топ-5
                logger.warning(
                    f"  {result.method} {result.path} → {result.status_code}"
                )
            if len(client_errors) > 5:
                logger.warning(f"  ... and {len(client_errors) - 5} more")

        # Сохраняем детальный отчёт
        report_file = Path("api_test_report.json")
        report_data = {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total": total,
                "skipped": skipped,
                "tested": tested,
                "successful": successful,
                "failed": failed,
                "success_rate_percent": (
                    round(successful / tested * 100, 2) if tested > 0 else 0
                ),
                "status_2xx": success_2xx,
                "status_4xx": client_4xx,
                "status_5xx": server_5xx,
                "timeouts": timeouts,
                "errors": errors,
                "avg_duration_ms": round(avg_duration, 2),
            },
            "discovered_values": self._discovered_values,
            "results": [
                {
                    "path": r.path,
                    "method": r.method,
                    "status_code": r.status_code,
                    "success": r.success,
                    "category": r.category,
                    "duration_ms": round(r.duration_ms, 2),
                    "error": r.error,
                    "request_info": r.request_info,
                }
                for r in self.results
            ],
        }

        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report_data, f, indent=2, ensure_ascii=False)

        logger.info(f"\n💾 Detailed report saved to: {report_file}")

        # Финальный вердикт
        if failed == 0 and server_5xx == 0:
            logger.info(f"\n🎉 ALL TESTS PASSED! ({successful}/{tested} successful)")
        elif server_5xx == 0:
            logger.info(
                f"\n✅ NO SERVER ERRORS. "
                f"Some client errors may need test data adjustment."
            )
        else:
            logger.info(
                f"\n⚠️ {server_5xx} SERVER ERROR(S) DETECTED. "
                f"Check server logs for details."
            )


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
async def main():
    import argparse

    parser = argparse.ArgumentParser(description="Automated API Endpoint Tester v3")
    parser.add_argument(
        "--base-url",
        default="http://localhost:8000",
        help="Base URL (default: http://localhost:8000)",
    )
    parser.add_argument(
        "--filter",
        help="Filter by tag (e.g., 'agents', 'observatory', 'simulation')",
    )
    parser.add_argument(
        "--method",
        help="Filter by HTTP method (GET, POST, etc.)",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Verbose output",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Base request timeout in seconds (default: 30)",
    )
    parser.add_argument(
        "--skip-simulation",
        action="store_true",
        help="Skip /simulation/ endpoints (they are slow)",
    )

    args = parser.parse_args()

    tester = APITester(
        base_url=args.base_url,
        timeout=args.timeout,
        skip_simulation=args.skip_simulation,
    )

    try:
        await tester.run_tests(
            filter_tag=args.filter,
            filter_method=args.method,
            verbose=args.verbose,
        )
        tester.print_report()

        # Exit code based on results
        failed = sum(
            1 for r in tester.results if not r.success and r.category != "skipped"
        )
        sys.exit(1 if failed > 0 else 0)

    except KeyboardInterrupt:
        logger.info("\n⚠️ Tests interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"❌ Fatal error: {e}", exc_info=True)
        sys.exit(2)
    finally:
        await tester.close()


if __name__ == "__main__":
    asyncio.run(main())
