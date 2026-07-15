"""
N.I.N.A. Advanced API Client
Асинхронный клиент для N.I.N.A. Advanced API.
Все команды Execution Layer проходят через этот клиент.

ИСПРАВЛЕНО (audit 10.1):
- Добавлен exponential backoff с jitter для retry
- Клиент корректно закрывается при shutdown

ИСПРАВЛЕНО (С-15):
- Миграция на единый HttpClientManager
- Убрано самостоятельное создание httpx.AsyncClient
- Connection pooling через http_client_manager
"""

import logging
import asyncio
import random
from typing import Dict, Any, Optional
import httpx
from app.core.config import settings
from app.core.http_client import http_client_manager

logger = logging.getLogger("NinaAdvancedClient")


class NinaAdvancedClient:
    """
    Асинхронный клиент для N.I.N.A. Advanced API.

    ИСПРАВЛЕНО (С-15):
    - Использует http_client_manager для connection pooling
    - Сервис "nina" для конфигурации таймаутов из settings.http_client.nina
    """

    # Retry configuration
    MAX_RETRIES: int = 3
    BASE_DELAY: float = 1.0  # seconds
    MAX_DELAY: float = 10.0  # seconds

    def __init__(self):
        self.base_url = settings.network.nina_api_host
        self.advanced_url = f"{self.base_url}/advanced"
        # ИСПРАВЛЕНО (С-15): http_client_manager управляет клиентами
        # self._client, self._client_lock, self._is_started — удалены

    async def start(self):
        """
        Запускает клиент.
        ИСПРАВЛЕНО (С-15): http_client_manager лениво создаёт клиенты при первом запросе.
        Этот метод оставлен для обратной совместимости, но больше не создаёт клиент напрямую.
        """
        # Пре-создаём клиент через менеджер для быстрой первой операции
        await http_client_manager.get_client(
            base_url=self.base_url,
            service="nina",
        )
        logger.info(f"✅ N.I.N.A. client started (base: {self.base_url})")

    async def _get_client(self) -> httpx.AsyncClient:
        """
        Получает HTTP-клиент через менеджер. Thread-safe.
        ИСПРАВЛЕНО (С-15): делегирует http_client_manager.
        """
        return await http_client_manager.get_client(
            base_url=self.base_url,
            service="nina",
        )

    async def close(self):
        """
        Корректно закрывает HTTP-клиент через менеджер.
        ИСПРАВЛЕНО (С-15): делегирует http_client_manager.
        """
        closed = await http_client_manager.close_client(
            base_url=self.base_url,
            service="nina",
        )
        if closed:
            logger.info("✅ N.I.N.A. client closed")

    async def health_check(self) -> bool:
        """
        Проверяет доступность N.I.N.A. API.
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
        - Exponential backoff с jitter
        - Retry только для connection/timeout ошибок
        - HTTP status errors не retry-ятся (они финальные)
        """
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
                error_msg = f"API error {e.response.status_code}"
                error_details = (
                    e.response.text[:200] if e.response.text else "No details"
                )
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

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику клиента."""
        # Получаем статистику из менеджера для нашего base_url
        manager_stats = http_client_manager.get_stats()
        cache_key = f"nina:{self.base_url}"
        client_active = cache_key in manager_stats.get("client_keys", [])

        return {
            "base_url": self.base_url,
            "advanced_url": self.advanced_url,
            "client_active": client_active,
            "max_retries": self.MAX_RETRIES,
            "http_client_manager": "active",
        }


# Singleton instance
nina_client = NinaAdvancedClient()
