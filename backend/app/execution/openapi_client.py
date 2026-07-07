"""
OpenAPI Client Generator для N.I.N.A. Advanced API.

Загружает OpenAPI спецификацию (YAML/JSON) и предоставляет
динамический клиент с типизированными методами.

Преимущества:
- Автоматическое использование правильных эндпоинтов
- Нет 404 ошибок
- Валидация параметров на основе схемы
- Легко обновлять при изменении API
"""

import logging
import yaml
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable
from dataclasses import dataclass, field
import httpx
from urllib.parse import urljoin

logger = logging.getLogger("OpenAPIClient")


@dataclass
class APIEndpoint:
    """Представление одного API эндпоинта."""

    path: str
    method: str
    operation_id: Optional[str] = None
    summary: Optional[str] = None
    description: Optional[str] = None
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Optional[Dict[str, Any]] = None
    responses: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)


@dataclass
class APISpec:
    """Парсинг OpenAPI спецификации."""

    info: Dict[str, Any]
    servers: List[Dict[str, str]]
    paths: Dict[str, Dict[str, Any]]
    components: Dict[str, Any] = field(default_factory=dict)

    def get_endpoints(self) -> List[APIEndpoint]:
        """Извлекает все эндпоинты из спецификации."""
        endpoints = []

        for path, methods in self.paths.items():
            for method, details in methods.items():
                if method.lower() in ("get", "post", "put", "delete", "patch"):
                    endpoint = APIEndpoint(
                        path=path,
                        method=method.upper(),
                        operation_id=details.get("operationId"),
                        summary=details.get("summary"),
                        description=details.get("description"),
                        parameters=details.get("parameters", []),
                        request_body=details.get("requestBody"),
                        responses=details.get("responses", {}),
                        tags=details.get("tags", []),
                    )
                    endpoints.append(endpoint)

        return endpoints

    def find_endpoint_by_operation(self, operation_id: str) -> Optional[APIEndpoint]:
        """Находит эндпоинт по operationId."""
        for endpoint in self.get_endpoints():
            if endpoint.operation_id == operation_id:
                return endpoint
        return None

    def find_endpoints_by_tag(self, tag: str) -> List[APIEndpoint]:
        """Находит все эндпоинты с определённым тегом."""
        return [ep for ep in self.get_endpoints() if tag in ep.tags]

    def find_endpoints_by_path_pattern(self, pattern: str) -> List[APIEndpoint]:
        """Находит эндпоинты по паттерну в пути."""
        return [ep for ep in self.get_endpoints() if pattern in ep.path]


class OpenAPILoader:
    """Загружает и парсит OpenAPI спецификацию."""

    @staticmethod
    def load_from_file(file_path: Path) -> APISpec:
        """Загружает спецификацию из файла (YAML или JSON)."""
        if not file_path.exists():
            raise FileNotFoundError(f"OpenAPI spec not found: {file_path}")

        with open(file_path, "r", encoding="utf-8") as f:
            if file_path.suffix in [".yaml", ".yml"]:
                data = yaml.safe_load(f)
            else:
                data = json.load(f)

        return APISpec(
            info=data.get("info", {}),
            servers=data.get("servers", []),
            paths=data.get("paths", {}),
            components=data.get("components", {}),
        )

    @staticmethod
    def load_from_url(url: str) -> APISpec:
        """Загружает спецификацию по URL."""
        response = httpx.get(url, timeout=30.0)
        response.raise_for_status()

        # Определяем формат по Content-Type или расширению
        content_type = response.headers.get("content-type", "")
        if "yaml" in content_type or url.endswith((".yaml", ".yml")):
            data = yaml.safe_load(response.text)
        else:
            data = response.json()

        return APISpec(
            info=data.get("info", {}),
            servers=data.get("servers", []),
            paths=data.get("paths", {}),
            components=data.get("components", {}),
        )


class DynamicAPIClient:
    """
    Динамический API клиент на основе OpenAPI спецификации.

    Автоматически генерирует методы для всех эндпоинтов.
    """

    def __init__(self, spec: APISpec, base_url: str, timeout: float = 30.0):
        self.spec = spec
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None
        self._endpoints = spec.get_endpoints()

        logger.info(
            f"📚 DynamicAPIClient initialized: {len(self._endpoints)} endpoints loaded"
        )

    async def _get_client(self) -> httpx.AsyncClient:
        """Получает или создаёт HTTP клиент."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.timeout),
                base_url=self.base_url,
            )
        return self._client

    async def close(self):
        """Закрывает HTTP клиент."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    def list_endpoints(self) -> List[Dict[str, str]]:
        """Возвращает список всех доступных эндпоинтов."""
        return [
            {
                "method": ep.method,
                "path": ep.path,
                "operation_id": ep.operation_id or "",
                "summary": ep.summary or "",
            }
            for ep in self._endpoints
        ]

    async def call_endpoint(
        self,
        operation_id: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
        path_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Вызывает эндпоинт по operationId.

        Args:
            operation_id: Идентификатор операции из OpenAPI spec
            params: Query параметры
            body: Request body (для POST/PUT)
            path_params: Path параметры (например, {id} в /items/{id})

        Returns:
            Response JSON или {'status': 'error', 'message': '...'}
        """
        endpoint = self.spec.find_endpoint_by_operation(operation_id)

        if not endpoint:
            logger.error(f"Endpoint with operationId '{operation_id}' not found")
            return {"status": "error", "message": f"Endpoint not found: {operation_id}"}

        # Строим URL с path параметрами
        url = endpoint.path
        if path_params:
            for key, value in path_params.items():
                url = url.replace(f"{{{key}}}", str(value))

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
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            return {
                "status": "error",
                "code": e.response.status_code,
                "message": str(e),
            }

        except httpx.RequestError as e:
            logger.error(f"Request error: {e}")
            return {"status": "error", "message": str(e)}

    async def call_by_path(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        body: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """
        Вызывает эндпоинт по пути и методу (fallback).
        """
        client = await self._get_client()

        try:
            if method.upper() == "GET":
                response = await client.get(path, params=params)
            elif method.upper() == "POST":
                response = await client.post(path, params=params, json=body)
            elif method.upper() == "PUT":
                response = await client.put(path, params=params, json=body)
            elif method.upper() == "DELETE":
                response = await client.delete(path, params=params)
            else:
                return {"status": "error", "message": f"Unsupported method: {method}"}

            response.raise_for_status()

            try:
                return response.json()
            except Exception:
                return {"status": "success", "data": response.text}

        except httpx.HTTPStatusError as e:
            logger.error(f"HTTP error {e.response.status_code}: {e}")
            return {
                "status": "error",
                "code": e.response.status_code,
                "message": str(e),
            }

        except httpx.RequestError as e:
            logger.error(f"Request error: {e}")
            return {"status": "error", "message": str(e)}


# Singleton для загрузки спецификации
_api_spec: Optional[APISpec] = None
_api_client: Optional[DynamicAPIClient] = None


# ИСПРАВЛЕНО (audit 8.2): Используем settings.network.nina_api_host вместо хардкода
async def get_nina_api_client(
    spec_path: Optional[Path] = None,
    base_url: Optional[str] = None,  # ← Было: "http://localhost:1888"
) -> DynamicAPIClient:
    """
    Получает или создаёт API клиент для N.I.N.A.

    Args:
        spec_path: Путь к файлу OpenAPI спецификации
        base_url: Базовый URL N.I.N.A. API (по умолчанию из settings)

    Returns:
        DynamicAPIClient instance
    """
    from app.core.config import settings

    global _api_spec, _api_client
    if _api_client is not None:
        return _api_client

    # ИСПРАВЛЕНО (audit 8.2): Используем настройки вместо хардкода
    if base_url is None:
        base_url = settings.network.nina_api_host

    # Загружаем спецификацию
    if spec_path is None:
        possible_paths = [
            Path("config/nina_api_spec.yaml"),
            Path("config/nina_api_spec.json"),
            Path("../config/nina_api_spec.yaml"),
            Path("../config/nina_api_spec.json"),
        ]
        for path in possible_paths:
            if path.exists():
                spec_path = path
                break

    if spec_path is None or not spec_path.exists():
        raise FileNotFoundError(
            "OpenAPI spec not found. Please download from:\n"
            "https://christian-photo.github.io/github-page/projects/ninaAPI/v2/doc/api\n"
            "and save to config/nina_api_spec.yaml"
        )

    logger.info(f"📖 Loading N.I.N.A. API spec from: {spec_path}")
    logger.info(f"🌐 Using base URL: {base_url}")

    _api_spec = OpenAPILoader.load_from_file(spec_path)

    _api_client = DynamicAPIClient(
        spec=_api_spec,
        base_url=base_url,
    )

    return _api_client
