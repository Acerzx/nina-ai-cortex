"""
N.I.N.A. Advanced API Client

Асинхронный клиент для N.I.N.A. Advanced API.
Все команды Execution Layer проходят через этот клиент.

ИСПРАВЛЕНО (audit 10.1):
- Добавлено явное управление жизненным циклом через start()/close()
- Добавлен _client_lock для thread-safe доступа
- Добавлен exponential backoff с jitter для retry
- Клиент корректно закрывается при shutdown
"""

import logging
import asyncio
import random
from typing import Dict, Any, Optional
import httpx
from app.core.config import settings

logger = logging.getLogger("NinaAdvancedClient")


class NinaAdvancedClient:
    """
    Асинхронный клиент для N.I.N.A. Advanced API.

    ИСПРАВЛЕНО (audit 10.1):
    - Явное управление жизненным циклом HTTP-клиента
    - Exponential backoff с jitter для retry
    - Thread-safe доступ к клиенту через asyncio.Lock
    - Graceful shutdown через close()
    """

    # Retry configuration
    MAX_RETRIES: int = 3
    BASE_DELAY: float = 1.0  # seconds
    MAX_DELAY: float = 10.0  # seconds

    def __init__(self):
        self.base_url = settings.network.nina_api_host
        self.advanced_url = f"{self.base_url}/advanced"
        self._client: Optional[httpx.AsyncClient] = None
        self._client_lock = asyncio.Lock()
        self._is_started: bool = False

    async def start(self):
        """Запускает клиент, создаёт HTTP-соединение."""
        # ИСПРАВЛЕНО (v4.0 — проблема #16): Закрываем старый клиент если есть
        async with self._client_lock:
            if self._client is not None and not self._client.is_closed:
                logger.warning(
                    "⚠️ NinaAdvancedClient already started, closing old client"
                )
                try:
                    await self._client.aclose()
                except Exception as e:
                    logger.debug(f"Error closing old client: {e}")

            self._client = httpx.AsyncClient(
                base_url=self.base_url,
                timeout=httpx.Timeout(10.0),
                headers={"Content-Type": "application/json"},
                limits=httpx.Limits(
                    max_connections=20,
                    max_keepalive_connections=10,
                    keepalive_expiry=30,
                ),
            )
            self._is_started = True
            logger.info(f"✅ N.I.N.A. client started (base: {self.base_url})")

    async def _get_client(self) -> httpx.AsyncClient:
        """Получает или создаёт HTTP-клиент. Thread-safe."""
        async with self._client_lock:
            if self._client is None or self._client.is_closed:
                self._client = httpx.AsyncClient(
                    base_url=self.base_url,
                    timeout=httpx.Timeout(10.0),
                    headers={"Content-Type": "application/json"},
                    limits=httpx.Limits(
                        max_connections=20,
                        max_keepalive_connections=10,
                        keepalive_expiry=30,
                    ),
                )
                self._is_started = True
            return self._client

    async def close(self):
        """
        Корректно закрывает HTTP-клиент.
        ИСПРАВЛЕНО (v4.0 — проблема #16): проверка перед закрытием.
        """
        async with self._client_lock:
            if self._client is not None:
                if not self._client.is_closed:
                    try:
                        await self._client.aclose()
                        logger.info("✅ N.I.N.A. client closed")
                    except Exception as e:
                        logger.debug(f"Error closing N.I.N.A. client: {e}")
                self._client = None
                self._is_started = False

    async def health_check(self) -> bool:
        """
        Проверяет доступность N.I.N.A. API.

        ИСПРАВЛЕНО (для audit 12.3): используется APIHealthGate в preflight.py.

        Returns:
            True если API доступен, False в противном случае
        """
        try:
            client = await self._get_client()
            response = await client.get("/v2/api/version")
            return response.status_code == 200
        except httpx.ConnectError:
            logger.debug("N.I.N.A. API not reachable (connection refused)")
            return False
        except httpx.TimeoutException:
            logger.debug("N.I.N.A. API timeout during health check")
            return False
        except Exception as e:
            logger.debug(f"N.I.N.A. health check error: {e}")
            return False

    def _calculate_backoff(self, attempt: int) -> float:
        """
        Вычисляет задержку для exponential backoff с jitter.
        Формула: min(base_delay * 2^attempt + jitter, max_delay)
        """
        exponential_delay = self.BASE_DELAY * (2**attempt)
        jitter = random.uniform(0, self.BASE_DELAY * 0.5)
        return min(exponential_delay + jitter, self.MAX_DELAY)

    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """
        Базовый метод запроса с exponential backoff retry.

        ИСПРАВЛЕНО (audit 10.1):
        - Exponential backoff с jitter
        - Retry только для connection/timeout ошибок
        - HTTP status errors не retry-ятся (они финальные)
        """
        # Auto-start при первом запросе
        if not self._is_started:
            await self.start()

        client = await self._get_client()

        url = (
            f"{self.advanced_url}/{endpoint}"
            if not endpoint.startswith("http")
            else endpoint
        )

        last_error: Optional[Exception] = None

        for attempt in range(self.MAX_RETRIES):
            try:
                response = await client.request(method, url, **kwargs)
                response.raise_for_status()

                # N.I.N.A. API часто возвращает пустой ответ или простой текст
                if response.headers.get("content-type", "").startswith(
                    "application/json"
                ):
                    return response.json()
                return {"status": "success", "text": response.text}

            except httpx.ConnectError as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    delay = self._calculate_backoff(attempt)
                    logger.warning(
                        f"N.I.N.A. API not available "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES}, "
                        f"retrying in {delay:.1f}s): {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"❌ N.I.N.A. API unreachable after {self.MAX_RETRIES} "
                        f"attempts: {e}"
                    )
                    raise ConnectionError(
                        "Cannot connect to N.I.N.A. Advanced API"
                    ) from e

            except httpx.TimeoutException as e:
                last_error = e
                if attempt < self.MAX_RETRIES - 1:
                    delay = self._calculate_backoff(attempt)
                    logger.warning(
                        f"N.I.N.A. API timeout "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES}, "
                        f"retrying in {delay:.1f}s)"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"❌ N.I.N.A. API timeout after {self.MAX_RETRIES} attempts"
                    )
                    raise



            except httpx.HTTPStatusError as e:
                # ИСПРАВЛЕНО (v4.0 — проблема #37): не пробрасываем исключение,
                # а возвращаем словарь с ошибкой для graceful handling
                error_msg = f"API error {e.response.status_code}"
                error_details = e.response.text[:200] if e.response.text else "No details"
                
                logger.error(f"❌ {error_msg}: {error_details}")
                
                # Для 4xx ошибок — возвращаем сразу (они финальные)
                if 400 <= e.response.status_code < 500:
                    return {
                        "status": "error",
                        "code": e.response.status_code,
                        "message": error_msg,
                        "details": error_details,
                        "client_error": True,
                    }
                
                # Для 5xx ошибок — можно retry
                if attempt < self.MAX_RETRIES - 1:
                    delay = self._calculate_backoff(attempt)
                    logger.warning(
                        f"Server error {e.response.status_code}, "
                        f"retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{self.MAX_RETRIES})"
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(
                        f"❌ Server error after {self.MAX_RETRIES} attempts: "
                        f"{e.response.status_code}"
                    )
                    return {
                        "status": "error",
                        "code": e.response.status_code,
                        "message": error_msg,
                        "details": error_details,
                        "server_error": True,
                    }




            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error calling {url}: {e}")
                if attempt == self.MAX_RETRIES - 1:
                    raise

        # Если все retry исчерпаны
        if last_error:
            raise last_error
        return {"status": "error", "message": "Max retries exceeded"}

    async def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """GET-запрос к N.I.N.A. API."""
        return await self._request("GET", endpoint, params=params)

    async def post(
        self, endpoint: str, json_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """POST-запрос к N.I.N.A. API."""
        return await self._request("POST", endpoint, json=json_data)

    async def put(
        self, endpoint: str, json_data: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """PUT-запрос к N.I.N.A. API."""
        return await self._request("PUT", endpoint, json=json_data)

    async def delete(self, endpoint: str) -> Dict[str, Any]:
        """DELETE-запрос к N.I.N.A. API."""
        return await self._request("DELETE", endpoint)

    async def health_check(self) -> bool:
        """
        Проверяет доступность N.I.N.A. API.
        Возвращает True если API отвечает, False в противном случае.
        """
        try:
            response = await self.get("version")
            return True
        except Exception as e:
            logger.debug(f"N.I.N.A. health check failed: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику клиента."""
        return {
            "base_url": self.base_url,
            "advanced_url": self.advanced_url,
            "is_started": self._is_started,
            "client_alive": self._client is not None and not self._client.is_closed,
            "max_retries": self.MAX_RETRIES,
        }


# Singleton instance
nina_client = NinaAdvancedClient()
