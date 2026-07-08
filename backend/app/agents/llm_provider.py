"""
LLM Provider — работа с Ollama (локальная + облачная модели).
Архитектура:
- Основная модель: из settings.ai_settings.primary_model
- Fallback модель: из settings.ai_settings.fallback_model
- Обе модели вызываются через Ollama API
ИСПРАВЛЕНО (audit P2):
- Убран ВЕСЬ хардкод — все значения из settings.yaml
- Добавлен Circuit Breaker для защиты от каскадных сбоев
- Пул клиентов по таймаутам (исправление Race Condition)
- Graceful закрытие при shutdown
"""

import logging
import asyncio
import os
import time
from typing import Optional, Dict, Any
from datetime import datetime
from enum import Enum
import httpx
from pydantic import BaseModel, Field

logger = logging.getLogger("LLMProvider")


# ============================================================================
# CIRCUIT BREAKER (P2)
# ============================================================================


class CircuitState(str, Enum):
    """Состояние Circuit Breaker."""

    CLOSED = "closed"  # Нормальная работа
    OPEN = "open"  # Блокировка запросов
    HALF_OPEN = "half_open"  # Тестовый запрос


class CircuitBreaker:
    """
    Circuit Breaker для защиты от каскадных сбоев.

    Состояния:
    - CLOSED: нормальная работа, запросы проходят
    - OPEN: после N ошибок подряд — все запросы немедленно отклоняются
    - HALF_OPEN: после timeout — один тестовый запрос проверяет восстановление

    Предотвращает:
    - Бесконечные попытки подключения к недоступному сервису
    - Перегрузку системы очередью зависших запросов
    - Каскадные сбои в зависимых компонентах
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
                logger.info(f"🟢 Circuit Breaker [{self._name}]: HALF_OPEN → CLOSED")

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
                    f"🔴 Circuit Breaker [{self._name}]: HALF_OPEN → OPEN "
                    f"(test request failed)"
                )
            elif self._state == CircuitState.CLOSED:
                if self._failure_count >= self._failure_threshold:
                    self._state = CircuitState.OPEN
                    self._stats["state_transitions"] += 1
                    logger.warning(
                        f"🔴 Circuit Breaker [{self._name}]: CLOSED → OPEN "
                        f"({self._failure_count} failures, "
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
                        f"🟡 Circuit Breaker [{self._name}]: OPEN → HALF_OPEN "
                        f"(testing recovery after {elapsed:.0f}s)"
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
# MODELS
# ============================================================================


class LLMResponse(BaseModel):
    """Ответ от LLM."""

    content: str
    model: str
    tokens_used: Optional[int] = None
    latency_ms: float = 0.0
    from_fallback: bool = False


# ============================================================================
# LLM PROVIDER
# ============================================================================


class LLMProvider:
    """
    LLM провайдер через Ollama с автоматическим fallback.

    ИСПРАВЛЕНО (P2):
    - Все параметры читаются из settings.ai_settings (НОЛЬ хардкода)
    - Circuit Breaker для защиты от каскадных сбоев
    - Пул клиентов по таймаутам (исправление Race Condition)
    - Graceful закрытие при shutdown

    Workflow:
    1. Пытается использовать primary модель
    2. При таймауте или ошибке → fallback на fallback модель
    3. Если Circuit Breaker OPEN → немедленный отказ
    4. Если обе модели недоступны → возвращает None
    """

    def __init__(self):
        # === Читаем ВСЕ параметры из settings (НОЛЬ хардкода) ===
        from app.core.config import settings as app_settings

        ai = app_settings.ai_settings

        self._ollama_host: str = ai.ollama_host
        self._primary_model: str = ai.primary_model
        self._fallback_model: str = ai.fallback_model
        self._primary_timeout: float = ai.primary_timeout
        self._fallback_timeout: float = ai.fallback_timeout
        self._max_tokens: int = ai.max_tokens
        self._temperature: float = ai.temperature
        self._fallback_enabled: bool = ai.fallback_enabled

        # Connection pool параметры
        conn = ai.connection
        self._max_connections: int = conn.max_connections
        self._max_keepalive_connections: int = conn.max_keepalive_connections
        self._keepalive_expiry: int = conn.keepalive_expiry

        # Circuit Breaker
        cb = ai.circuit_breaker
        self._primary_circuit = CircuitBreaker(
            failure_threshold=cb.failure_threshold,
            recovery_timeout=cb.recovery_timeout_seconds,
            half_open_max_calls=cb.half_open_max_calls,
            name="primary_llm",
        )
        self._fallback_circuit = CircuitBreaker(
            failure_threshold=cb.failure_threshold,
            recovery_timeout=cb.recovery_timeout_seconds,
            half_open_max_calls=cb.half_open_max_calls,
            name="fallback_llm",
        )
        self._circuit_breaker_enabled: bool = cb.enabled

        # Пул клиентов по таймаутам (исправление Race Condition из P2)
        self._clients_pool: Dict[float, httpx.AsyncClient] = {}
        self._client_lock = asyncio.Lock()
        self._is_started: bool = False

        # Статистика
        self._stats = {
            "total_requests": 0,
            "primary_success": 0,
            "fallback_success": 0,
            "failed_requests": 0,
            "circuit_rejected": 0,
            "total_latency_ms": 0.0,
            "clients_created": 0,
        }

        logger.info(f"🤖 LLM Provider initialized:")
        logger.info(
            f"   Primary: {self._primary_model} (timeout: {self._primary_timeout}s)"
        )
        logger.info(
            f"   Fallback: {self._fallback_model} (timeout: {self._fallback_timeout}s)"
        )
        logger.info(f"   Host: {self._ollama_host}")
        logger.info(
            f"   Circuit Breaker: "
            f"{'enabled' if self._circuit_breaker_enabled else 'disabled'} "
            f"(threshold: {cb.failure_threshold})"
        )

    async def start(self):
        """Запускает LLM Provider."""
        if self._is_started:
            return
        async with self._client_lock:
            # Pre-create клиенты для основных таймаутов
            await self._get_or_create_client(self._primary_timeout)
            await self._get_or_create_client(self._fallback_timeout)
            self._is_started = True
        logger.info("✅ LLM Provider started")

    async def _get_or_create_client(self, timeout: float) -> httpx.AsyncClient:
        """
        Получает или создаёт клиент для конкретного таймаута.
        ИСПРАВЛЕНО (P2): каждый таймаут имеет свой клиент в пуле.
        """
        async with self._client_lock:
            client = self._clients_pool.get(timeout)
            if client is not None and not client.is_closed:
                return client

            client = httpx.AsyncClient(
                timeout=httpx.Timeout(timeout),
                limits=httpx.Limits(
                    max_connections=self._max_connections,
                    max_keepalive_connections=self._max_keepalive_connections,
                    keepalive_expiry=self._keepalive_expiry,
                ),
            )
            self._clients_pool[timeout] = client
            self._stats["clients_created"] += 1
            self._is_started = True

            logger.debug(
                f"   Created HTTP client for timeout={timeout}s "
                f"(total clients: {len(self._clients_pool)})"
            )
            return client

    async def close(self):
        """Корректно закрывает ВСЕ HTTP клиенты из пула."""
        async with self._client_lock:
            closed_count = 0
            for timeout, client in list(self._clients_pool.items()):
                if client and not client.is_closed:
                    try:
                        await client.aclose()
                        closed_count += 1
                    except Exception as e:
                        logger.debug(f"Error closing client (timeout={timeout}s): {e}")
            self._clients_pool.clear()
            self._is_started = False
            logger.info(f"✅ LLM Provider closed ({closed_count} clients closed)")

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> Optional[LLMResponse]:
        """Генерирует ответ от LLM с автоматическим fallback и Circuit Breaker."""
        if not self._is_started:
            await self.start()

        self._stats["total_requests"] += 1
        start_time = datetime.now()

        # === Попытка 1: Primary модель ===
        response = await self._try_model(
            model=self._primary_model,
            timeout=self._primary_timeout,
            circuit=self._primary_circuit,
            prompt=prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens or self._max_tokens,
            temperature=temperature or self._temperature,
            is_primary=True,
        )

        if response:
            latency = (datetime.now() - start_time).total_seconds() * 1000
            response.latency_ms = latency
            response.from_fallback = False
            self._stats["primary_success"] += 1
            self._stats["total_latency_ms"] += latency
            logger.info(
                f"✅ LLM response from {self._primary_model} "
                f"in {latency:.0f}ms ({response.tokens_used or '?'} tokens)"
            )
            return response

        # === Попытка 2: Fallback модель ===
        if self._fallback_enabled:
            response = await self._try_model(
                model=self._fallback_model,
                timeout=self._fallback_timeout,
                circuit=self._fallback_circuit,
                prompt=prompt,
                system_prompt=system_prompt,
                max_tokens=max_tokens or self._max_tokens,
                temperature=temperature or self._temperature,
                is_primary=False,
            )

            if response:
                latency = (datetime.now() - start_time).total_seconds() * 1000
                response.latency_ms = latency
                response.from_fallback = True
                self._stats["fallback_success"] += 1
                self._stats["total_latency_ms"] += latency
                logger.warning(
                    f"⚠️ LLM response from FALLBACK {self._fallback_model} "
                    f"in {latency:.0f}ms"
                )
                return response

        self._stats["failed_requests"] += 1
        logger.error("❌ All LLM models failed")
        return None

    async def _try_model(
        self,
        model: str,
        timeout: float,
        circuit: CircuitBreaker,
        prompt: str,
        system_prompt: Optional[str],
        max_tokens: int,
        temperature: float,
        is_primary: bool,
    ) -> Optional[LLMResponse]:
        """
        Пытается получить ответ от конкретной модели с учётом Circuit Breaker.
        """
        model_label = "primary" if is_primary else "fallback"

        # Circuit Breaker проверка
        if self._circuit_breaker_enabled:
            if not await circuit.before_request():
                self._stats["circuit_rejected"] += 1
                logger.warning(
                    f"🚫 {model_label} model blocked by Circuit Breaker "
                    f"(state: {circuit.state.value})"
                )
                return None

        try:
            response = await self._call_ollama(
                model=model,
                timeout=timeout,
                prompt=prompt,
                system_prompt=system_prompt,
                max_tokens=max_tokens,
                temperature=temperature,
            )

            if response:
                if self._circuit_breaker_enabled:
                    await circuit.record_success()
                return response
            else:
                if self._circuit_breaker_enabled:
                    await circuit.record_failure()
                return None

        except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
            logger.warning(f"⚠️ {model_label} model {model} failed: {type(e).__name__}")
            if self._circuit_breaker_enabled:
                await circuit.record_failure()
            return None

        except Exception as e:
            logger.error(f"❌ Unexpected error from {model_label} model: {e}")
            if self._circuit_breaker_enabled:
                await circuit.record_failure()
            return None

    async def _call_ollama(
        self,
        model: str,
        timeout: float,
        prompt: str,
        system_prompt: Optional[str],
        max_tokens: int,
        temperature: float,
    ) -> Optional[LLMResponse]:
        """Вызывает Ollama API."""
        client = await self._get_or_create_client(timeout)

        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})

        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "options": {
                "temperature": temperature,
                "num_predict": max_tokens,
            },
        }

        response = await client.post(
            f"{self._ollama_host}/api/chat",
            json=payload,
        )
        response.raise_for_status()
        data = response.json()

        content = data.get("message", {}).get("content", "")
        tokens_used = data.get("eval_count") or data.get("prompt_eval_count")

        return LLMResponse(
            content=content,
            model=model,
            tokens_used=tokens_used,
        )

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику использования LLM."""
        total_success = self._stats["primary_success"] + self._stats["fallback_success"]
        avg_latency = (
            self._stats["total_latency_ms"] / total_success
            if total_success > 0
            else 0.0
        )
        return {
            **self._stats,
            "avg_latency_ms": avg_latency,
            "success_rate": (
                total_success / self._stats["total_requests"] * 100
                if self._stats["total_requests"] > 0
                else 0.0
            ),
            "current_config": {
                "primary_model": self._primary_model,
                "fallback_model": self._fallback_model,
                "fallback_enabled": self._fallback_enabled,
                "primary_timeout": self._primary_timeout,
                "fallback_timeout": self._fallback_timeout,
                "ollama_host": self._ollama_host,
            },
            "client_state": {
                "is_started": self._is_started,
                "active_clients": len(self._clients_pool),
                "client_timeouts": list(self._clients_pool.keys()),
            },
            "circuit_breaker": {
                "enabled": self._circuit_breaker_enabled,
                "primary": self._primary_circuit.get_stats(),
                "fallback": self._fallback_circuit.get_stats(),
            },
        }

    async def check_availability(self) -> Dict[str, bool]:
        """Проверяет фактическую доступность обеих моделей в Ollama."""
        result = {"primary": False, "fallback": False}
        try:
            if not self._is_started:
                await self.start()
            client = await self._get_or_create_client(5.0)
            response = await client.get(f"{self._ollama_host}/api/tags")
            if response.status_code == 200:
                data = response.json()
                models = [m["name"] for m in data.get("models", [])]
                result["primary"] = any(self._primary_model in m for m in models)
                result["fallback"] = any(self._fallback_model in m for m in models)
                logger.info(
                    f"🔍 Model availability: "
                    f"primary={result['primary']}, fallback={result['fallback']}"
                )
            else:
                logger.warning(f"⚠️ Ollama /api/tags returned {response.status_code}")
        except httpx.ConnectError:
            logger.warning("⚠️ Cannot connect to Ollama")
        except Exception as e:
            logger.error(f"Failed to check model availability: {e}")
        return result


# Singleton instance
llm_provider = LLMProvider()
