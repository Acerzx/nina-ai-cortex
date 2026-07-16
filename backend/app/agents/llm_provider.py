"""
LLM Provider — работа с Ollama (локальная + облачная модели).
Архитектура:
- Основная модель: gemma4:31b-cloud (облачная, мощная)
- Fallback модель: gemma4:e4b (локальная, быстрая)
- Обе модели вызываются через Ollama API
Ollama автоматически маршрутизирует cloud модели на свои серверы,
а локальные модели выполняются на вашем железе.
ИСПРАВЛЕНО (audit 10.1): добавлено корректное управление жизненным циклом
HTTP-клиента через методы start()/close() и контекстный менеджер.
Клиент теперь имеет явное состояние и корректно закрывается при shutdown.
ИСПРАВЛЕНО (С-15):
- Миграция на единый HttpClientManager
- Убрано самостоятельное создание httpx.AsyncClient
- Connection pooling через http_client_manager
ИСПРАВЛЕНО (Спринт 5 — Фаза 2):
- Добавлены OpenTelemetry spans для LLM запросов
- Parent span `llm.generate` с атрибутами (model, prompt_length, max_tokens)
- Child span `llm.call_ollama` для каждого HTTP вызова
- Атрибуты: from_fallback, latency_ms, tokens_used, status
"""

import logging
import asyncio
import os
from typing import Optional, Dict, Any
from datetime import datetime
import httpx
from pydantic import BaseModel, Field
from app.core.config import settings
from app.core.http_client import http_client_manager

# Спринт 5: OpenTelemetry tracing
from app.core.tracing import tracing_manager, span_context

logger = logging.getLogger("LLMProvider")


class LLMConfig(BaseModel):
    """Конфигурация LLM провайдера."""

    ollama_host: str = "http://localhost:11434"
    # Основная модель (облачная)
    primary_model: str = "gemma4:31b-cloud"
    # Fallback модель (локальная)
    fallback_model: str = "gemma4:e4b"
    # Таймауты
    primary_timeout: float = 30.0  # Cloud может быть медленнее
    fallback_timeout: float = 15.0  # Local быстрее
    # Параметры генерации
    max_tokens: int = 1500
    temperature: float = 0.3
    # Fallback включен
    fallback_enabled: bool = True


class LLMResponse(BaseModel):
    """Ответ от LLM."""

    content: str
    model: str
    tokens_used: Optional[int] = None
    latency_ms: float = 0.0
    from_fallback: bool = False


class LLMProvider:
    """
    LLM провайдер через Ollama с автоматическим fallback.
    Workflow:
    1. Пытается использовать gemma4:31b-cloud (облачная)
    2. При таймауте или ошибке → fallback на gemma4:e4b (локальная)
    3. Если и fallback не работает → возвращает None
    ИСПРАВЛЕНО (С-15):
    - Использует http_client_manager для connection pooling
    - Убраны _client, _client_lock, _is_started
    ИСПРАВЛЕНО (Спринт 5 — Фаза 2):
    - OpenTelemetry spans для observability
    """

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or self._load_config()
        # ИСПРАВЛЕНО (С-15): http_client_manager управляет клиентами
        # self._client, self._client_lock, self._is_started — удалены
        self._stats = {
            "total_requests": 0,
            "primary_success": 0,
            "fallback_success": 0,
            "failed_requests": 0,
            "total_latency_ms": 0.0,
        }
        logger.info(f"🤖 LLM Provider initialized:")
        logger.info(f"   Primary: {self.config.primary_model} (cloud)")
        logger.info(f"   Fallback: {self.config.fallback_model} (local)")
        logger.info(f"   Host: {self.config.ollama_host}")

    def _load_config(self) -> LLMConfig:
        """Загружает конфигурацию из переменных окружения."""
        return LLMConfig(
            ollama_host=os.getenv("OLLAMA_HOST", settings.ai_settings.ollama_host),
            primary_model=os.getenv(
                "LLM_PRIMARY_MODEL", settings.ai_settings.primary_model
            ),
            fallback_model=os.getenv(
                "LLM_FALLBACK_MODEL", settings.ai_settings.fallback_model
            ),
            primary_timeout=float(os.getenv("LLM_PRIMARY_TIMEOUT", "30.0")),
            fallback_timeout=float(os.getenv("LLM_FALLBACK_TIMEOUT", "15.0")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "1500")),
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.3")),
            fallback_enabled=os.getenv("LLM_FALLBACK_ENABLED", "true").lower()
            == "true",
        )

    async def start(self):
        """
        Запускает LLM Provider.
        ИСПРАВЛЕНО (С-15): pre-creates клиент через менеджер.
        """
        await http_client_manager.get_client(
            base_url=self.config.ollama_host,
            service="ollama",
        )
        logger.info("✅ LLM Provider started (via HttpClientManager)")

    async def _get_client(self, timeout: float) -> httpx.AsyncClient:
        """
        Получает HTTP клиент через менеджер.
        ИСПРАВЛЕНО (С-15): делегирует http_client_manager.
        Args:
            timeout: Таймаут для запроса (используется per-request,
            не влияет на конфигурацию клиента в менеджере)
        """
        return await http_client_manager.get_client(
            base_url=self.config.ollama_host,
            service="ollama",
        )

    async def close(self):
        """
        Корректно закрывает HTTP клиент.
        ИСПРАВЛЕНО (С-15): делегирует http_client_manager.
        """
        closed = await http_client_manager.close_client(
            base_url=self.config.ollama_host,
            service="ollama",
        )
        if closed:
            logger.info("✅ LLM Provider HTTP client closed")

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> Optional[LLMResponse]:
        """
        Генерирует ответ от LLM с автоматическим fallback.
        ИСПРАВЛЕНО (Спринт 5 — Фаза 2): OpenTelemetry span с атрибутами.
        """
        self._stats["total_requests"] += 1
        start_time = datetime.now()

        # Спринт 5: OpenTelemetry parent span
        async with span_context(
            "llm.generate",
            attributes={
                "llm.provider": "ollama",
                "llm.primary_model": self.config.primary_model,
                "llm.fallback_model": self.config.fallback_model,
                "llm.prompt_length": len(prompt) if prompt else 0,
                "llm.system_prompt_length": len(system_prompt) if system_prompt else 0,
                "llm.max_tokens": max_tokens or self.config.max_tokens,
                "llm.temperature": temperature or self.config.temperature,
                "llm.fallback_enabled": self.config.fallback_enabled,
            },
        ) as span:
            # Попытка 1: Основная модель (cloud)
            try:
                response = await self._call_ollama(
                    model=self.config.primary_model,
                    timeout=self.config.primary_timeout,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    max_tokens=max_tokens or self.config.max_tokens,
                    temperature=temperature or self.config.temperature,
                )
                if response:
                    latency = (datetime.now() - start_time).total_seconds() * 1000
                    response.latency_ms = latency
                    response.from_fallback = False
                    self._stats["primary_success"] += 1
                    self._stats["total_latency_ms"] += latency

                    # Спринт 5: Устанавливаем атрибуты span
                    if span:
                        span.set_attribute("llm.model", response.model)
                        span.set_attribute("llm.from_fallback", False)
                        span.set_attribute("llm.latency_ms", latency)
                        span.set_attribute("llm.tokens_used", response.tokens_used or 0)
                        span.set_attribute("llm.status", "success")

                    logger.info(
                        f"✅ LLM response from {self.config.primary_model} "
                        f"in {latency:.0f}ms ({response.tokens_used or '?'} tokens)"
                    )
                    return response

            except (httpx.TimeoutException, httpx.ConnectError, httpx.ReadError) as e:
                logger.warning(
                    f"⚠️ Primary model {self.config.primary_model} failed: "
                    f"{type(e).__name__}"
                )
                if span:
                    span.set_attribute("llm.primary_error", type(e).__name__)

            except Exception as e:
                logger.error(f"❌ Unexpected error from primary model: {e}")
                if span:
                    span.record_exception(e)
                    span.set_attribute("llm.primary_error", type(e).__name__)

            # Попытка 2: Fallback модель (local)
            if self.config.fallback_enabled:
                try:
                    logger.info(f"🔄 Trying fallback to {self.config.fallback_model}")
                    response = await self._call_ollama(
                        model=self.config.fallback_model,
                        timeout=self.config.fallback_timeout,
                        prompt=prompt,
                        system_prompt=system_prompt,
                        max_tokens=max_tokens or self.config.max_tokens,
                        temperature=temperature or self.config.temperature,
                    )
                    if response:
                        latency = (datetime.now() - start_time).total_seconds() * 1000
                        response.latency_ms = latency
                        response.from_fallback = True
                        self._stats["fallback_success"] += 1
                        self._stats["total_latency_ms"] += latency

                        # Спринт 5: Устанавливаем атрибуты span
                        if span:
                            span.set_attribute("llm.model", response.model)
                            span.set_attribute("llm.from_fallback", True)
                            span.set_attribute("llm.latency_ms", latency)
                            span.set_attribute(
                                "llm.tokens_used", response.tokens_used or 0
                            )
                            span.set_attribute("llm.status", "fallback_success")

                        logger.warning(
                            f"⚠️ LLM response from FALLBACK {self.config.fallback_model} "
                            f"in {latency:.0f}ms"
                        )
                        return response

                except Exception as e:
                    logger.error(f"❌ Fallback model also failed: {e}")
                    if span:
                        span.record_exception(e)
                        span.set_attribute("llm.fallback_error", type(e).__name__)

            # Все модели недоступны
            self._stats["failed_requests"] += 1

            if span:
                span.set_attribute("llm.status", "failed")

            logger.error("❌ All LLM models failed")
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
        """
        Вызывает Ollama API.
        ИСПРАВЛЕНО (С-15): Таймаут применяется per-request через httpx.Timeout.
        ИСПРАВЛЕНО (Спринт 5 — Фаза 2): OpenTelemetry child span.
        """
        # Спринт 5: Child span для HTTP вызова
        async with span_context(
            "llm.call_ollama",
            attributes={
                "llm.model": model,
                "llm.timeout": timeout,
                "llm.max_tokens": max_tokens,
                "llm.temperature": temperature,
                "llm.endpoint": f"{self.config.ollama_host}/api/chat",
            },
        ) as span:
            client = await self._get_client(timeout)

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

            # ИСПРАВЛЕНО (С-15): Таймаут применяется per-request
            response = await client.post(
                f"{self.config.ollama_host}/api/chat",
                json=payload,
                timeout=httpx.Timeout(timeout),  # Per-request timeout
            )
            response.raise_for_status()
            data = response.json()

            # Извлекаем контент
            content = data.get("message", {}).get("content", "")

            # Извлекаем количество токенов (если доступно)
            tokens_used = data.get("eval_count") or data.get("prompt_eval_count")

            # Спринт 5: Устанавливаем атрибуты child span
            if span:
                span.set_attribute("llm.tokens_used", tokens_used or 0)
                span.set_attribute("llm.response_length", len(content))
                span.set_attribute("http.status_code", response.status_code)

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

        # ИСПРАВЛЕНО (С-15): Читаем статус клиента из менеджера
        cache_key = f"ollama:{self.config.ollama_host}"
        manager_stats = http_client_manager.get_stats()
        client_active = cache_key in manager_stats.get("client_keys", [])

        return {
            **self._stats,
            "avg_latency_ms": avg_latency,
            "success_rate": (
                total_success / self._stats["total_requests"] * 100
                if self._stats["total_requests"] > 0
                else 0.0
            ),
            "current_config": {
                "primary_model": self.config.primary_model,
                "fallback_model": self.config.fallback_model,
                "fallback_enabled": self.config.fallback_enabled,
            },
            "client_state": {
                "client_active": client_active,
                "http_client_manager": "active",
            },
        }

    async def check_availability(self) -> Dict[str, bool]:
        """
        Проверяет фактическую доступность обеих моделей в Ollama.
        Выполняет реальный HTTP-запрос к /api/tags.
        """
        result = {
            "primary": False,
            "fallback": False,
        }
        try:
            client = await self._get_client(5.0)
            # ИСПРАВЛЕНО (С-15): Per-request timeout для health check
            response = await client.get(
                f"{self.config.ollama_host}/api/tags",
                timeout=httpx.Timeout(5.0),
            )
            if response.status_code == 200:
                data = response.json()
                models = [m["name"] for m in data.get("models", [])]
                # Проверяем наличие моделей
                result["primary"] = any(self.config.primary_model in m for m in models)
                result["fallback"] = any(
                    self.config.fallback_model in m for m in models
                )
                logger.info(
                    f"🔍 Model availability: "
                    f"primary={result['primary']}, fallback={result['fallback']}"
                )
            else:
                logger.warning(f"⚠️ Ollama /api/tags returned {response.status_code}")
        except httpx.ConnectError:
            logger.warning("⚠️ Cannot connect to Ollama (check if it's running)")
        except Exception as e:
            logger.error(f"Failed to check model availability: {e}")

        return result


# Singleton instance
llm_provider = LLMProvider()
