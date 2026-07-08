"""
N.I.N.A. Advanced API Client
Асинхронный клиент для N.I.N.A. Advanced API.
Все команды Execution Layer проходят через этот клиент.

ИСПРАВЛЕНО (audit 10.1):
- Добавлено явное управление жизненным циклом через start()/close()
- Добавлен _client_lock для thread-safe доступа
- Добавлен exponential backoff с jitter для retry
- Клиент корректно закрывается при shutdown

ИСПРАВЛЕНО (audit P2 - хардкод + надёжность):
- ВЕСЬ хардкод устранён: retry-параметры, timeout, connection pool
  читаются из settings.nina_api_client
- Добавлен Circuit Breaker для защиты от каскадных сбоев
- При недоступности N.I.N.A. API запросы блокируются вместо
  бесконечных попыток подключения
- Graceful degradation: система продолжает работать с кэшированными
  данными когда API недоступен
"""

import logging
import asyncio
import random
from typing import Dict, Any, Optional
from enum import Enum
import time
import httpx
from app.core.config import settings

logger = logging.getLogger("NinaAdvancedClient")


# ============================================================================
# CIRCUIT BREAKER
# ============================================================================


class CircuitState(str, Enum):
    """Состояние Circuit Breaker."""

    CLOSED = "closed"  # Нормальная работа
    OPEN = "open"  # Блокировка запросов
    HALF_OPEN = "half_open"  # Тестовый запрос


class CircuitBreaker:
    """
    Circuit Breaker для защиты от каскадных сбоев при обращении к N.I.N.A. API.

    Состояния:
    - CLOSED: нормальная работа, запросы проходят
    - OPEN: после N ошибок подряд — все запросы немедленно отклоняются
    - HALF_OPEN: после timeout — один тестовый запрос проверяет восстановление

    Предотвращает:
    - Бесконечные попытки подключения к недоступному N.I.N.A.
    - Перегрузку системы очередью зависших HTTP-запросов
    - Каскадные сбои в зависимых компонентах (HAL, TriggerEmulator)
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 1,
        name: str = "default",
    ):
        self._failure_threshold = failure_threshold
        self._recovery_timeout = recovery_timeout
        self._half_open_max_calls = half_open_max_calls
        self._name = name

        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._success_count = 0
        self._last_failure_time: float = 0.0
        self._half_open_calls = 0
        self._lock = asyncio.Lock()

        # Статистика
        self._stats = {
            "total_requests": 0,
            "rejected_requests": 0,
            "state_transitions": 0,
        }

    @property
    def state(self) -> CircuitState:
        return self._state

    @property
    def is_available(self) -> bool:
        """Можно ли отправлять запросы."""
        if self._state == CircuitState.CLOSED:
            return True
        if self._state == CircuitState.HALF_OPEN:
            return self._half_open_calls < self._half_open_max_calls
        # OPEN — проверяем не пора ли перейти в HALF_OPEN
        if self._state == CircuitState.OPEN:
            elapsed = time.monotonic() - self._last_failure_time
            if elapsed >= self._recovery_timeout:
                return True  # Позволим один тестовый запрос
        return False

    async def record_success(self) -> None:
        """Записать успешный запрос."""
        async with self._lock:
            self._stats["total_requests"] += 1
            self._failure_count = 0
            self._success_count += 1

            if self._state == CircuitState.HALF_OPEN:
                # Успех в HALF_OPEN → переход в CLOSED
                self._state = CircuitState.CLOSED
                self._half_open_calls = 0
                self._stats["state_transitions"] += 1
                logger.info(
                    f"🟢 Circuit Breaker [{self._name}]: "
                    f"HALF_OPEN → CLOSED (N.I.N.A. API recovered)"
                )

    async def record_failure(self) -> None:
        """Записать неудачный запрос."""
        async with self._lock:
            self._stats["total_requests"] += 1
            self._failure_count += 1
            self._last_failure_time = time.monotonic()

            if self._state == CircuitState.HALF_OPEN:
                # Неудача в HALF_OPEN → обратно в OPEN
                self._state = CircuitState.OPEN
                self._half_open_calls = 0
                self._stats["state_transitions"] += 1
                logger.warning(
                    f"🔴 Circuit Breaker [{self._name}]: "
                    f"HALF_OPEN → OPEN (test request failed)"
                )
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._failure_threshold:
                    self._state = CircuitState.OPEN
                    self._stats["state_transitions"] += 1
                    logger.warning(
                        f"🔴 Circuit Breaker [{self._name}]: "
                        f"CLOSED → OPEN ({self._failure_count} failures, "
                        f"recovery in {self._recovery_timeout:.0f}s)"
                    )

    async def before_request(self) -> bool:
        """
        Проверка перед запросом.
        Returns:
            True если запрос разрешён, False если отклонён
        """
        async with self._lock:
            if self._state == CircuitState.CLOSED:
                return True

            if self._state == CircuitState.OPEN:
                elapsed = time.monotonic() - self._last_failure_time
                if elapsed >= self._recovery_timeout:
                    # Переход в HALF_OPEN
                    self._state = CircuitState.HALF_OPEN
                    self._half_open_calls = 0
                    self._stats["state_transitions"] += 1
                    logger.info(
                        f"🟡 Circuit Breaker [{self._name}]: "
                        f"OPEN → HALF_OPEN (testing recovery after {elapsed:.0f}s)"
                    )
                else:
                    self._stats["rejected_requests"] += 1
                    return False

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_calls < self._half_open_max_calls:
                    self._half_open_calls += 1
                    return True
                else:
                    self._stats["rejected_requests"] += 1
                    return False

            return False

    def get_stats(self) -> Dict[str, Any]:
        return {
            **self._stats,
            "state": self._state.value,
            "failure_count": self._failure_count,
            "failure_threshold": self._failure_threshold,
            "recovery_timeout_seconds": self._recovery_timeout,
            "is_available": self.is_available,
        }


# ============================================================================
# N.I.N.A. API CLIENT
# ============================================================================


class NinaAdvancedClient:
    """
    Асинхронный клиент для N.I.N.A. Advanced API.

    ИСПРАВЛЕНО (audit P2):
    - ВСЕ параметры читаются из settings.nina_api_client (НОЛЬ хардкода):
      * request_timeout, max_connections, max_keepalive_connections, keepalive_expiry
      * retry: max_retries, base_delay_seconds, max_delay_seconds
      * circuit_breaker: failure_threshold, recovery_timeout_seconds, half_open_max_calls
    - Circuit Breaker для защиты от каскадных сбоев
    - Exponential backoff с jitter для retry
    - Thread-safe доступ к клиенту через asyncio.Lock
    - Graceful shutdown через close()
    """

    def __init__(self):
        # === Читаем ВСЕ параметры из settings (НОЛЬ хардкода) ===
        api_cfg = settings.nina_api_client

        self.base_url = settings.network.nina_api_host
        self.advanced_url = f"{self.base_url}/advanced"

        # Connection pool параметры
        self._request_timeout: float = api_cfg.request_timeout
        self._max_connections: int = api_cfg.max_connections
        self._max_keepalive_connections: int = api_cfg.max_keepalive_connections
        self._keepalive_expiry: int = api_cfg.keepalive_expiry

        # Retry параметры
        retry_cfg = api_cfg.retry
        self._max_retries: int = retry_cfg.max_retries
        self._base_delay: float = retry_cfg.base_delay_seconds
        self._max_delay: float = retry_cfg.max_delay_seconds
        self._retry_enabled: bool = retry_cfg.enabled

        # Circuit Breaker
        cb_cfg = api_cfg.circuit_breaker
        self._circuit_breaker = CircuitBreaker(
            failure_threshold=cb_cfg.failure_threshold,
            recovery_timeout=cb_cfg.recovery_timeout_seconds,
            half_open_max_calls=cb_cfg.half_open_max_calls,
            name="nina_api",
        )
        self._circuit_breaker_enabled: bool = cb_cfg.enabled

        # HTTP client state
        self._client: Optional[httpx.AsyncClient] = None
        self._client_lock = asyncio.Lock()
        self._is_started: bool = False

        # Статистика
        self._stats = {
            "total_requests": 0,
            "successful_requests": 0,
            "failed_requests": 0,
            "circuit_rejected": 0,
            "retry_exhausted": 0,
        }

        logger.info(f"🔌 N.I.N.A. API Client initialized:")
        logger.info(f"   Base URL: {self.base_url}")
        logger.info(f"   Timeout: {self._request_timeout}s")
        logger.info(
            f"   Retry: max={self._max_retries}, "
            f"delay={self._base_delay}-{self._max_delay}s"
        )
        logger.info(
            f"   Connection pool: max={self._max_connections}, "
            f"keepalive={self._max_keepalive_connections}"
        )
        logger.info(
            f"   Circuit Breaker: "
            f"{'enabled' if self._circuit_breaker_enabled else 'disabled'} "
            f"(threshold: {cb_cfg.failure_threshold})"
        )

    async def start(self):
        """Запускает клиент, создаёт HTTP-соединение."""
        async with self._client_lock:
            if self._client is None or self._client.is_closed:
                self._client = httpx.AsyncClient(
                    base_url=self.base_url,
                    timeout=httpx.Timeout(self._request_timeout),
                    headers={"Content-Type": "application/json"},
                    limits=httpx.Limits(
                        max_connections=self._max_connections,
                        max_keepalive_connections=self._max_keepalive_connections,
                        keepalive_expiry=self._keepalive_expiry,
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
                    timeout=httpx.Timeout(self._request_timeout),
                    headers={"Content-Type": "application/json"},
                    limits=httpx.Limits(
                        max_connections=self._max_connections,
                        max_keepalive_connections=self._max_keepalive_connections,
                        keepalive_expiry=self._keepalive_expiry,
                    ),
                )
                self._is_started = True
        return self._client

    async def close(self):
        """
        Корректно закрывает HTTP-клиент.
        Вызывается при shutdown приложения.
        """
        async with self._client_lock:
            if self._client and not self._client.is_closed:
                try:
                    await self._client.aclose()
                    logger.info("✅ N.I.N.A. client closed")
                except Exception as e:
                    logger.debug(f"Error closing N.I.N.A. client: {e}")
                finally:
                    self._client = None
                    self._is_started = False

    def _calculate_backoff(self, attempt: int) -> float:
        """
        Вычисляет задержку для exponential backoff с jitter.
        Формула: min(base_delay * 2^attempt + jitter, max_delay)
        """
        exponential_delay = self._base_delay * (2**attempt)
        jitter = random.uniform(0, self._base_delay * 0.5)
        return min(exponential_delay + jitter, self._max_delay)

    async def _request(self, method: str, endpoint: str, **kwargs) -> Dict[str, Any]:
        """
        Базовый метод запроса с exponential backoff retry и Circuit Breaker.

        ИСПРАВЛЕНО (audit P2):
        - Circuit Breaker проверка перед каждым запросом
        - Exponential backoff с jitter (параметры из конфига)
        - Retry только для connection/timeout ошибок
        - HTTP status errors не retry-ятся (они финальные)
        """
        self._stats["total_requests"] += 1

        # === Circuit Breaker проверка ===
        if self._circuit_breaker_enabled:
            if not await self._circuit_breaker.before_request():
                self._stats["circuit_rejected"] += 1
                logger.warning(
                    f"🚫 N.I.N.A. API request blocked by Circuit Breaker "
                    f"(state: {self._circuit_breaker.state.value}): "
                    f"{method} {endpoint}"
                )
                raise ConnectionError(
                    f"N.I.N.A. API circuit breaker is OPEN "
                    f"(recovery in {self._circuit_breaker._recovery_timeout:.0f}s)"
                )

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
        max_attempts = self._max_retries if self._retry_enabled else 1

        for attempt in range(max_attempts):
            try:
                response = await client.request(method, url, **kwargs)
                response.raise_for_status()

                # Успешный запрос — записываем в Circuit Breaker
                if self._circuit_breaker_enabled:
                    await self._circuit_breaker.record_success()

                self._stats["successful_requests"] += 1

                # N.I.N.A. API часто возвращает пустой ответ или простой текст
                if response.headers.get("content-type", "").startswith(
                    "application/json"
                ):
                    return response.json()
                return {"status": "success", "text": response.text}

            except httpx.ConnectError as e:
                last_error = e
                if attempt < max_attempts - 1:
                    delay = self._calculate_backoff(attempt)
                    logger.warning(
                        f"N.I.N.A. API not available "
                        f"(attempt {attempt + 1}/{max_attempts}, "
                        f"retrying in {delay:.1f}s): {e}"
                    )
                    await asyncio.sleep(delay)
                else:
                    # Все retry исчерпаны — записываем failure в Circuit Breaker
                    if self._circuit_breaker_enabled:
                        await self._circuit_breaker.record_failure()

                    self._stats["retry_exhausted"] += 1
                    logger.error(
                        f"❌ N.I.N.A. API unreachable after {max_attempts} "
                        f"attempts: {e}"
                    )
                    raise ConnectionError(
                        "Cannot connect to N.I.N.A. Advanced API"
                    ) from e

            except httpx.TimeoutException as e:
                last_error = e
                if attempt < max_attempts - 1:
                    delay = self._calculate_backoff(attempt)
                    logger.warning(
                        f"N.I.N.A. API timeout "
                        f"(attempt {attempt + 1}/{max_attempts}, "
                        f"retrying in {delay:.1f}s)"
                    )
                    await asyncio.sleep(delay)
                else:
                    if self._circuit_breaker_enabled:
                        await self._circuit_breaker.record_failure()

                    self._stats["retry_exhausted"] += 1
                    logger.error(
                        f"❌ N.I.N.A. API timeout after {max_attempts} attempts"
                    )
                    raise

            except httpx.HTTPStatusError as e:
                # HTTP errors (4xx, 5xx) не retry-ятся — они финальные
                # Но 5xx записываем как failure в Circuit Breaker
                if self._circuit_breaker_enabled and e.response.status_code >= 500:
                    await self._circuit_breaker.record_failure()

                self._stats["failed_requests"] += 1
                logger.error(
                    f"API error {e.response.status_code}: {e.response.text[:200]}"
                )
                raise

            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error calling {url}: {e}")
                if attempt == max_attempts - 1:
                    if self._circuit_breaker_enabled:
                        await self._circuit_breaker.record_failure()
                    self._stats["failed_requests"] += 1
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

        ИСПРАВЛЕНО: учитывает состояние Circuit Breaker.
        """
        # Если Circuit Breaker OPEN — API точно недоступен
        if (
            self._circuit_breaker_enabled
            and self._circuit_breaker.state == CircuitState.OPEN
        ):
            return False

        try:
            response = await self.get("version")
            return True
        except Exception as e:
            logger.debug(f"N.I.N.A. health check failed: {e}")
            return False

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику клиента."""
        return {
            **self._stats,
            "base_url": self.base_url,
            "advanced_url": self.advanced_url,
            "is_started": self._is_started,
            "client_alive": (self._client is not None and not self._client.is_closed),
            "config": {
                "request_timeout": self._request_timeout,
                "max_retries": self._max_retries,
                "base_delay": self._base_delay,
                "max_delay": self._max_delay,
                "retry_enabled": self._retry_enabled,
                "max_connections": self._max_connections,
                "max_keepalive_connections": self._max_keepalive_connections,
            },
            "circuit_breaker": self._circuit_breaker.get_stats(),
        }


# Singleton instance
nina_client = NinaAdvancedClient()
