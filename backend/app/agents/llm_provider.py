"""
LLM Provider — работа с Ollama (локальная + облачная модели).

Архитектура:
- Основная модель: gemma4:31b-cloud (облачная, мощная)
- Fallback модель: gemma4:e4b (локальная, быстрая)
- Обе модели вызываются через Ollama API

Ollama автоматически маршрутизирует cloud модели на свои серверы,
а локальные модели выполняются на вашем железе.
"""

import logging
import asyncio
import os
from typing import Optional, Dict, Any
from datetime import datetime
import httpx
from pydantic import BaseModel, Field

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
    """

    def __init__(self, config: Optional[LLMConfig] = None):
        self.config = config or self._load_config()
        self._client: Optional[httpx.AsyncClient] = None
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
            ollama_host=os.getenv("OLLAMA_HOST", "http://localhost:11434"),
            primary_model=os.getenv("LLM_PRIMARY_MODEL", "gemma4:31b-cloud"),
            fallback_model=os.getenv("LLM_FALLBACK_MODEL", "gemma4:e4b"),
            primary_timeout=float(os.getenv("LLM_PRIMARY_TIMEOUT", "30.0")),
            fallback_timeout=float(os.getenv("LLM_FALLBACK_TIMEOUT", "15.0")),
            max_tokens=int(os.getenv("LLM_MAX_TOKENS", "1500")),
            temperature=float(os.getenv("LLM_TEMPERATURE", "0.3")),
            fallback_enabled=os.getenv("LLM_FALLBACK_ENABLED", "true").lower()
            == "true",
        )

    async def _get_client(self, timeout: float) -> httpx.AsyncClient:
        """Получает или создаёт HTTP клиент с нужным таймаутом."""
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(timeout),
                limits=httpx.Limits(
                    max_connections=10,
                    max_keepalive_connections=5,
                    keepalive_expiry=30,
                ),
            )
        return self._client

    async def close(self):
        """Закрывает HTTP клиент."""
        if self._client and not self._client.is_closed:
            await self._client.aclose()
            self._client = None

    async def generate(
        self,
        prompt: str,
        system_prompt: Optional[str] = None,
        max_tokens: Optional[int] = None,
        temperature: Optional[float] = None,
    ) -> Optional[LLMResponse]:
        """
        Генерирует ответ от LLM с автоматическим fallback.

        Args:
            prompt: Пользовательский промпт
            system_prompt: Системный промпт (роль агента)
            max_tokens: Максимальное количество токенов
            temperature: Температура генерации

        Returns:
            LLMResponse если успешно, None если все модели недоступны
        """
        self._stats["total_requests"] += 1
        start_time = datetime.now()

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
        except Exception as e:
            logger.error(f"❌ Unexpected error from primary model: {e}")

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

                    logger.warning(
                        f"⚠️ LLM response from FALLBACK {self.config.fallback_model} "
                        f"in {latency:.0f}ms"
                    )
                    return response

            except Exception as e:
                logger.error(f"❌ Fallback model also failed: {e}")

        # Все модели недоступны
        self._stats["failed_requests"] += 1
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
        """Вызывает Ollama API."""
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

        response = await client.post(
            f"{self.config.ollama_host}/api/chat",
            json=payload,
        )
        response.raise_for_status()

        data = response.json()

        # Извлекаем контент
        content = data.get("message", {}).get("content", "")

        # Извлекаем количество токенов (если доступно)
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
                "primary_model": self.config.primary_model,
                "fallback_model": self.config.fallback_model,
                "fallback_enabled": self.config.fallback_enabled,
            },
        }

    async def check_availability(self) -> Dict[str, bool]:
        """Проверяет доступность обеих моделей."""
        result = {
            "primary": False,
            "fallback": False,
        }

        try:
            client = await self._get_client(5.0)
            response = await client.get(f"{self.config.ollama_host}/api/tags")

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

        except Exception as e:
            logger.error(f"Failed to check model availability: {e}")

        return result


# Singleton instance
llm_provider = LLMProvider()
