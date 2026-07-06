import logging
import asyncio
from typing import Dict, Any, Optional
import httpx
from app.core.config import settings

logger = logging.getLogger("NinaAdvancedClient")


class NinaAdvancedClient:
    """
    Асинхронный клиент для N.I.N.A. Advanced API.
    Все команды Execution Layer проходят через этот клиент.
    """

    def __init__(self):
        self.base_url = settings.network.nina_api_host
        self.advanced_url = f"{self.base_url}/advanced"
        self._client: Optional[httpx.AsyncClient] = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=10.0,
                headers={"Content-Type": "application/json"},
            )
        return self._client

    async def close(self):
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """Базовый метод запроса с обработкой ошибок и ретраями."""
        client = await self._get_client()
        url = (
            f"{self.advanced_url}/{endpoint}"
            if not endpoint.startswith("http")
            else endpoint
        )

        for attempt in range(3):
            try:
                response = await client.request(method, url, **kwargs)
                response.raise_for_status()

                # N.I.N.A. API часто возвращает пустой ответ или простой текст
                if response.headers.get("content-type", "").startswith(
                    "application/json"
                ):
                    return response.json()
                return {"status": "success", "text": response.text}

            except httpx.ConnectError:
                logger.warning(f"N.I.N.A. API not available (attempt {attempt + 1}/3)")
                if attempt == 2:
                    raise ConnectionError("Cannot connect to N.I.N.A. Advanced API")
            except httpx.HTTPStatusError as e:
                logger.error(f"API error {e.response.status_code}: {e.response.text}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error calling {url}: {e}")
                if attempt == 2:
                    raise

            await asyncio.sleep(1.0 * (attempt + 1))  # Экспоненциальная задержка

        return {"status": "error", "message": "Max retries exceeded"}

    async def get(self, endpoint: str, params: Dict = None) -> Dict[str, Any]:
        return await self._request("GET", endpoint, params=params)

    async def post(self, endpoint: str, json_data: Dict = None) -> Dict[str, Any]:
        return await self._request("POST", endpoint, json=json_data)

    async def put(self, endpoint: str, json_data: Dict = None) -> Dict[str, Any]:
        return await self._request("PUT", endpoint, json=json_data)


# Singleton instance
nina_client = NinaAdvancedClient()
