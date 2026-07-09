"""
OpenAPI Client Generator для N.I.N.A. Advanced API.
Загружает OpenAPI спецификацию (YAML/JSON) и предоставляет динамический
клиент с типизированными методами и валидацией параметров.

УЛУЧШЕНИЯ (рефакторинг v3):
- Динамический вызов эндпоинтов по operationId или path
- Автоматическая валидация параметров по OpenAPI схеме (min/max, enum, required)
- Кэширование распарсенной спецификации
- Поиск эндпоинтов по тегам, operationId, паттернам в path
- Загрузка из локального файла или URL
- Метод get_nina_api_client() для обратной совместимости
"""

import logging
import json
import hashlib
import yaml
import httpx
import asyncio
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from app.core.config import settings

logger = logging.getLogger("OpenAPIClient")


# ============================================================================
# МОДЕЛИ ДАННЫХ
# ============================================================================


@dataclass
class APIParameter:
    """Параметр API эндпоинта (извлечён из OpenAPI схемы)."""

    name: str
    location: str  # query, path, header
    param_type: str
    required: bool = False
    description: str = ""
    example: Optional[Any] = None
    enum: Optional[List[Any]] = None
    default: Optional[Any] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    format: Optional[str] = None  # int32, double, string, ...

    def validate_value(self, value: Any) -> Tuple[bool, Optional[str]]:
        """
        Валидирует значение параметра по OpenAPI схеме.
        Returns:
            Tuple (is_valid, error_message)
        """
        # Проверка enum
        if self.enum and value not in self.enum:
            return False, (
                f"Parameter '{self.name}' must be one of {self.enum}, got {value}"
            )

        # Числовые типы
        if self.param_type in ("integer", "number"):
            try:
                numeric_value = float(value)
            except (TypeError, ValueError):
                return False, (
                    f"Parameter '{self.name}' must be numeric, "
                    f"got {type(value).__name__}"
                )

            if self.min_value is not None and numeric_value < self.min_value:
                return False, (
                    f"Parameter '{self.name}' = {numeric_value} "
                    f"is below minimum {self.min_value}"
                )
            if self.max_value is not None and numeric_value > self.max_value:
                return False, (
                    f"Parameter '{self.name}' = {numeric_value} "
                    f"exceeds maximum {self.max_value}"
                )

            # Integer constraint
            if self.param_type == "integer":
                try:
                    if float(value) != int(float(value)):
                        return False, (
                            f"Parameter '{self.name}' must be integer, got {value}"
                        )
                except (TypeError, ValueError):
                    pass

        # Boolean
        elif self.param_type == "boolean":
            if not isinstance(value, bool):
                if isinstance(value, str) and value.lower() in ("true", "false"):
                    pass  # Строки "true"/"false" допустимы
                else:
                    return False, (
                        f"Parameter '{self.name}' must be boolean, "
                        f"got {type(value).__name__}"
                    )

        # String
        elif self.param_type == "string":
            if not isinstance(value, (str, int, float, bool)):
                return False, (
                    f"Parameter '{self.name}' must be string, "
                    f"got {type(value).__name__}"
                )

        return True, None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "location": self.location,
            "type": self.param_type,
            "required": self.required,
            "description": self.description,
            "example": self.example,
            "enum": self.enum,
            "default": self.default,
            "min": self.min_value,
            "max": self.max_value,
            "format": self.format,
        }


@dataclass
class APIEndpoint:
    """Полное описание одного API эндпоинта."""

    path: str
    method: str
    operation_id: Optional[str] = None
    summary: str = ""
    description: str = ""
    parameters: List[APIParameter] = field(default_factory=list)
    has_request_body: bool = False
    responses: Dict[str, str] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)

    @property
    def full_signature(self) -> str:
        """Читаемая сигнатура эндпоинта."""
        params_str = []
        for p in self.parameters:
            if p.location == "path":
                params_str.append(f"{{{p.name}}}")
            elif p.location == "query":
                req = "*" if p.required else "?"
                params_str.append(f"{p.name}{req}:{p.param_type}")
        params_part = ", ".join(params_str) if params_str else ""
        return f"{self.method:6s} {self.path} ({params_part})"

    def get_parameter(self, name: str) -> Optional[APIParameter]:
        """Находит параметр по имени."""
        for p in self.parameters:
            if p.name == name:
                return p
        return None

    def validate_params(self, params: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Валидирует набор параметров против OpenAPI схемы.
        Returns:
            Tuple (is_valid, list_of_error_messages)
        """
        errors = []

        # Проверка required параметров
        for p in self.parameters:
            if p.required and p.location == "query":
                if p.name not in params:
                    if p.default is not None:
                        # Есть default — не ошибка
                        continue
                    errors.append(f"Required parameter '{p.name}' is missing")

        # Валидация значений
        for param_name, value in params.items():
            param_spec = self.get_parameter(param_name)
            if param_spec:
                is_valid, error = param_spec.validate_value(value)
                if not is_valid and error:
                    errors.append(error)

        return len(errors) == 0, errors

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path": self.path,
            "method": self.method,
            "operation_id": self.operation_id,
            "summary": self.summary,
            "description": self.description,
            "parameters": [p.to_dict() for p in self.parameters],
            "has_request_body": self.has_request_body,
            "tags": self.tags,
        }


# ============================================================================
# ЗАГРУЗЧИК OPENAPI СПЕЦИФИКАЦИИ
# ============================================================================


class OpenAPILoader:
    """Загружает и парсит OpenAPI спецификацию."""

    @staticmethod
    def load_from_file(file_path: Path) -> Dict[str, Any]:
        """Загружает спецификацию из локального файла (YAML или JSON)."""
        if not file_path.exists():
            raise FileNotFoundError(f"OpenAPI spec not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            if file_path.suffix in [".yaml", ".yml"]:
                data = yaml.safe_load(f)
            else:
                data = json.load(f)

        return data

    @staticmethod
    async def load_from_url(url: str, timeout: float = 30.0) -> Dict[str, Any]:
        """Загружает спецификацию по URL (асинхронно)."""
        async with httpx.AsyncClient(timeout=timeout) as client:
            response = await client.get(url)
            response.raise_for_status()

            content_type = response.headers.get("content-type", "")
            if "yaml" in content_type or url.endswith((".yaml", ".yml")):
                return yaml.safe_load(response.text)
            return response.json()


# ============================================================================
# ДИНАМИЧЕСКИЙ API КЛИЕНТ
# ============================================================================


class DynamicAPIClient:
    """
    Динамический API клиент на основе OpenAPI спецификации.
    Автоматически генерирует методы для всех эндпоинтов с валидацией.
    """

    def __init__(
        self,
        spec_data: Dict[str, Any],
        base_url: Optional[str] = None,
        timeout: float = 30.0,
    ):
        self.spec = spec_data
        self.info = spec_data.get("info", {})
        self.servers = spec_data.get("servers", [])
        self.paths = spec_data.get("paths", {})
        self.components = spec_data.get("components", {})

        # Base URL из аргумента или первого server
        if base_url:
            self.base_url = base_url.rstrip("/")
        elif self.servers:
            self.base_url = self.servers[0].get("url", "").rstrip("/")
        else:
            self.base_url = ""

        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._client_lock = asyncio.Lock()

        # Парсим эндпоинты
        self._endpoints: List[APIEndpoint] = []
        self._by_operation_id: Dict[str, APIEndpoint] = {}
        self._by_tag: Dict[str, List[APIEndpoint]] = {}
        self._parse_all_endpoints()

        logger.info(
            f"📚 DynamicAPIClient initialized: {len(self._endpoints)} endpoints "
            f"(base: {self.base_url})"
        )

    def _parse_all_endpoints(self):
        """Парсит все эндпоинты из спецификации."""
        for path, methods in self.paths.items():
            for method, details in methods.items():
                if method.lower() not in ("get", "post", "put", "delete", "patch"):
                    continue

                endpoint = APIEndpoint(
                    path=path,
                    method=method.upper(),
                    operation_id=details.get("operationId"),
                    summary=details.get("summary", ""),
                    description=details.get("description", ""),
                    has_request_body="requestBody" in details,
                    tags=details.get("tags", []),
                )

                # Парсим параметры
                for param in details.get("parameters", []):
                    schema = param.get("schema", {})
                    endpoint.parameters.append(
                        APIParameter(
                            name=param.get("name", ""),
                            location=param.get("in", "query"),
                            param_type=schema.get("type", "string"),
                            required=param.get("required", False),
                            description=param.get("description", ""),
                            example=schema.get("example"),
                            enum=schema.get("enum"),
                            default=schema.get("default"),
                            min_value=schema.get("minimum"),
                            max_value=schema.get("maximum"),
                            format=schema.get("format"),
                        )
                    )

                # Парсим responses
                for status, resp in details.get("responses", {}).items():
                    endpoint.responses[status] = resp.get("description", "")

                self._endpoints.append(endpoint)

                # Индексы для быстрого поиска
                if endpoint.operation_id:
                    self._by_operation_id[endpoint.operation_id] = endpoint

                for tag in endpoint.tags:
                    if tag not in self._by_tag:
                        self._by_tag[tag] = []
                    self._by_tag[tag].append(endpoint)

    # ====================================================================
    # HTTP КЛИЕНТ
    # ====================================================================

    async def _get_client(self) -> httpx.AsyncClient:
        """Возвращает или создаёт HTTP клиент (thread-safe)."""
        async with self._client_lock:
            # ИСПРАВЛЕНО (v4.0 — проблема #17): Закрываем старый если есть
            if self._client is not None and not self._client.is_closed:
                return self._client

            if self._client is not None and self._client.is_closed:
                logger.debug("Old OpenAPI client was closed, creating new one")
                self._client = None

            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                base_url=self.base_url,
                headers={"Content-Type": "application/json"},
            )
            return self._client

    async def close(self):
        """Закрывает HTTP клиент."""
        async with self._client_lock:
            if self._client and not self._client.is_closed:
                try:
                    await self._client.aclose()
                    logger.info("✅ OpenAPI client closed")
                except Exception as e:
                    logger.debug(f"Error closing OpenAPI client: {e}")
                finally:
                    self._client = None

    # ====================================================================
    # ПОИСК ЭНДПОИНТОВ
    # ====================================================================

    def get_all_endpoints(self) -> List[APIEndpoint]:
        """Возвращает все эндпоинты."""
        return self._endpoints

    def get_endpoints_by_tag(self, tag: str) -> List[APIEndpoint]:
        """Возвращает эндпоинты по тегу."""
        return self._by_tag.get(tag, [])

    def get_all_tags(self) -> List[str]:
        """Возвращает все уникальные теги."""
        return sorted(self._by_tag.keys())

    def find_by_operation_id(self, operation_id: str) -> Optional[APIEndpoint]:
        """Находит эндпоинт по operationId."""
        return self._by_operation_id.get(operation_id)

    def find_by_path(self, method: str, path: str) -> Optional[APIEndpoint]:
        """Находит эндпоинт по HTTP методу и path."""
        for ep in self._endpoints:
            if ep.method == method.upper() and ep.path == path:
                return ep
        return None

    def find_by_path_pattern(self, pattern: str) -> List[APIEndpoint]:
        """Находит эндпоинты по паттерну в path (case-insensitive)."""
        pattern_lower = pattern.lower()
        return [ep for ep in self._endpoints if pattern_lower in ep.path.lower()]

    def search(self, query: str) -> List[APIEndpoint]:
        """Полнотекстовый поиск по всем полям эндпоинтов."""
        query_lower = query.lower()
        results = []
        for ep in self._endpoints:
            searchable = " ".join(
                [
                    ep.path,
                    ep.summary,
                    ep.description,
                    " ".join(ep.tags),
                    " ".join(p.name for p in ep.parameters),
                ]
            ).lower()
            if query_lower in searchable:
                results.append(ep)
        return results

    # ====================================================================
    # ВЫЗОВ ЭНДПОИНТОВ
    # ====================================================================

    async def call_endpoint(
        self,
        operation_id: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
        path_params: Optional[Dict[str, Any]] = None,
        validate: bool = True,
    ) -> Dict[str, Any]:
        """
        Вызывает эндпоинт по operationId с валидацией параметров.
        """
        endpoint = self.find_by_operation_id(operation_id)
        if not endpoint:
            return {
                "status": "error",
                "code": "NOT_FOUND",
                "message": f"Endpoint with operationId '{operation_id}' not found",
            }

        return await self._call_endpoint_internal(
            endpoint, params, body, path_params, validate
        )

    async def call_by_path(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
        validate: bool = True,
    ) -> Dict[str, Any]:
        """Вызывает эндпоинт по методу и path."""
        endpoint = self.find_by_path(method, path)
        if not endpoint:
            # Если эндпоинт не в spec, пытаемся вызвать напрямую
            return await self._call_raw(method, path, params, body)

        return await self._call_endpoint_internal(
            endpoint, params, body, None, validate
        )

    async def _call_endpoint_internal(
        self,
        endpoint: APIEndpoint,
        params: Optional[Dict[str, Any]],
        body: Optional[Any],
        path_params: Optional[Dict[str, Any]],
        validate: bool,
    ) -> Dict[str, Any]:
        """Внутренний метод вызова эндпоинта."""
        params = params or {}
        path_params = path_params or {}

        # 1. Валидация
        if validate:
            is_valid, errors = endpoint.validate_params(params)
            if not is_valid:
                return {
                    "status": "error",
                    "code": "VALIDATION_ERROR",
                    "message": "Parameter validation failed",
                    "errors": errors,
                    "endpoint": endpoint.full_signature,
                }

        # 2. Подстановка path параметров
        url = endpoint.path
        for key, value in path_params.items():
            url = url.replace(f"{{{key}}}", str(value))

        # 3. HTTP запрос
        client = await self._get_client()
        try:
            if endpoint.method == "GET":
                response = await client.get(url, params=params)
            elif endpoint.method == "POST":
                response = await client.post(url, params=params, json=body)
            elif endpoint.method == "PUT":
                response = await client.put(url, params=params, json=body)
            elif endpoint.method == "DELETE":
                response = await client.delete(url, params=params)
            elif endpoint.method == "PATCH":
                response = await client.patch(url, params=params, json=body)
            else:
                return {
                    "status": "error",
                    "message": f"Unsupported method: {endpoint.method}",
                }

            response.raise_for_status()

            # Пытаемся распарсить JSON
            try:
                return response.json()
            except Exception:
                return {"status": "success", "data": response.text}

        except httpx.HTTPStatusError as e:
            logger.error(
                f"HTTP error {e.response.status_code} "
                f"calling {endpoint.method} {endpoint.path}: {e}"
            )
            # Пытаемся получить тело ошибки
            error_body = None
            try:
                error_body = e.response.json()
            except Exception:
                error_body = e.response.text[:500]

            return {
                "status": "error",
                "code": e.response.status_code,
                "message": str(e),
                "response": error_body,
            }
        except httpx.RequestError as e:
            logger.error(f"Request error: {e}")
            return {"status": "error", "message": str(e)}

    async def _call_raw(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]],
        body: Optional[Any],
    ) -> Dict[str, Any]:
        """Прямой вызов без спецификации (fallback)."""
        client = await self._get_client()
        try:
            response = await client.request(
                method.upper(), path, params=params, json=body
            )
            response.raise_for_status()
            try:
                return response.json()
            except Exception:
                return {"status": "success", "data": response.text}
        except httpx.HTTPStatusError as e:
            return {
                "status": "error",
                "code": e.response.status_code,
                "message": str(e),
            }
        except httpx.RequestError as e:
            return {"status": "error", "message": str(e)}

    # ====================================================================
    # СТАТИСТИКА
    # ====================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику по загруженной спецификации."""
        methods_count: Dict[str, int] = {}
        for ep in self._endpoints:
            methods_count[ep.method] = methods_count.get(ep.method, 0) + 1

        return {
            "title": self.info.get("title"),
            "version": self.info.get("version"),
            "base_url": self.base_url,
            "total_endpoints": len(self._endpoints),
            "total_tags": len(self._by_tag),
            "methods": methods_count,
            "tags": self.get_all_tags(),
            "operation_ids_count": len(self._by_operation_id),
        }

    def list_endpoints_summary(self) -> List[Dict[str, str]]:
        """Возвращает краткий список всех эндпоинтов."""
        return [
            {
                "method": ep.method,
                "path": ep.path,
                "operation_id": ep.operation_id or "",
                "summary": ep.summary,
                "tags": ", ".join(ep.tags),
            }
            for ep in self._endpoints
        ]


# ============================================================================
# SINGLETON FACTORY
# ============================================================================

_api_client: Optional[DynamicAPIClient] = None
_client_lock = asyncio.Lock()


async def get_nina_api_client(
    spec_path: Optional[Path] = None,
    base_url: Optional[str] = None,
    force_reload: bool = False,
) -> DynamicAPIClient:
    """
    Получает или создаёт API клиент для N.I.N.A.
    Использует singleton паттерн с thread-safe инициализацией.
    """
    global _api_client

    if _api_client is not None and not force_reload:
        return _api_client

    async with _client_lock:
        # Double-check после захвата lock
        if _api_client is not None and not force_reload:
            return _api_client

        # Base URL из аргумента или settings
        if base_url is None:
            base_url = settings.network.nina_api_host

        # Spec path
        if spec_path is None:
            configured_path = Path(settings.openapi.spec_path)
            if configured_path.exists():
                spec_path = configured_path
            else:
                # Fallback paths
                possible_paths = [
                    Path("config/nina_api_spec.json"),
                    Path("config/nina_api_spec.yaml"),
                    Path("../config/nina_api_spec.json"),
                    Path("../config/nina_api_spec.yaml"),
                ]
                for path in possible_paths:
                    if path.exists():
                        spec_path = path
                        break

        if spec_path is None or not spec_path.exists():
            raise FileNotFoundError(
                "OpenAPI spec not found. Please download from:\n"
                "  https://christian-photo.github.io/github-page/projects/"
                "ninaAPI/v2/doc/api\n"
                "and save to config/nina_api_spec.json"
            )

        logger.info(f"📖 Loading N.I.N.A. API spec from: {spec_path}")
        logger.info(f"🌐 Using base URL: {base_url}")

        spec_data = OpenAPILoader.load_from_file(spec_path)
        _api_client = DynamicAPIClient(spec_data=spec_data, base_url=base_url)

        return _api_client


async def close_nina_api_client():
    """Закрывает singleton клиент."""
    global _api_client
    if _api_client is not None:
        await _api_client.close()
        _api_client = None
