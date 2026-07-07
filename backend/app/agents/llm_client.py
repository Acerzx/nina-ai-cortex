"""
LLM Client — интеграция с Ollama для AI-агентов.
Обеспечивает единый интерфейс для всех агентов с обработкой ошибок и fallback.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
import httpx
from app.core.config import settings

logger = logging.getLogger("LLMClient")


class LLMClient:
    """
    Клиент для взаимодействия с LLM (Ollama).

    Features:
    - Автоматический retry при ошибках
    - Timeout для долгих запросов
    - Fallback на safe mode при недоступности
    - Кэширование частых запросов
    - Структурированные промпты для каждого агента
    """

    def __init__(self):
        self.ollama_host = settings.ai_settings.ollama_host
        self.model_name = settings.ai_settings.model_name
        self._client: Optional[httpx.AsyncClient] = None
        self._available = True
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_ttl = 300  # 5 минут

        # Системные промпты для разных агентов
        self.system_prompts = {
            "Watcher": """Ты — агент мониторинга обсерватории. 
Твоя задача — анализировать метрики качества (HFR, FWHM, RMS) и обнаруживать аномалии.
Отвечай кратко и по делу. Формат: АНОМАЛИЯ: [описание] | ПРИЧИНА: [гипотеза]""",
            "Guardian": """Ты — агент безопасности обсерватории.
Твоя задача — обеспечивать безопасность оборудования и данных.
Приоритет: Safety > Quality > Optimization.
Отвечай: ДЕЙСТВИЕ: [что сделать] | ПРИЧИНА: [почему]""",
            "Diagnostician": """Ты — агент диагностики проблем.
Твоя задача — находить root cause проблем через анализ корреляций.
Используй исторические данные из RAG для поиска похожих кейсов.
Формат: КОРНЕВАЯ ПРИЧИНА: [описание] | УВЕРЕННОСТЬ: [0-100%] | РЕШЕНИЕ: [предложение]""",
            "Strategist": """Ты — агент оптимизации параметров съемки.
Твоя задача — предлагать оптимальные параметры на основе SNR и условий.
Формат: ОПТИМИЗАЦИЯ: [параметр] | СТАРОЕ: [значение] | НОВОЕ: [значение] | ОЖИДАЕМЫЙ ЭФФЕКТ: [описание]""",
            "Auditor": """Ты — агент post-mortem анализа сессий.
Твоя задача — генерировать Session Digest с выводами и рекомендациями.
Формат: Markdown-отчет с секциями: Параметры, Результаты, Проблемы, Рекомендации""",
            "Calibrator": """Ты — агент управления калибровочными кадрами.
Твоя задача — проверять свежесть мастеров и предлагать обновление.
Формат: СТАТУС: [свежий/устарел] | ДЕЙСТВИЕ: [что сделать]""",
            "Scheduler": """Ты — агент планирования сессий.
Твоя задача — оптимизировать порядок целей на основе погоды и видимости.
Формат: ПЛАН: [цель1, цель2, ...] | ПРИЧИНА: [почему такой порядок]""",
            "Copilot": """Ты — интерактивный помощник для ручных шагов.
Твоя задача — предоставлять пошаговые инструкции для MessageBox, 2PA, OAG Focus.
Формат: пошаговые инструкции с визуальными подсказками""",
            "MemoryManager": """Ты — агент управления контекстом и памятью.
Твоя задача — управлять краткосрочной и долгосрочной памятью.
Формат: КЛЮЧ: [имя] | ЗНАЧЕНИЕ: [данные] | TTL: [секунды]""",
        }

    async def initialize(self):
        """Инициализирует HTTP клиент."""
        self._client = httpx.AsyncClient(timeout=60.0)
        await self._check_availability()

        if self._available:
            logger.info(f"✅ LLM Client initialized (model: {self.model_name})")
        else:
            logger.warning("⚠️ LLM not available, agents will work in safe mode")

    async def close(self):
        """Закрывает HTTP клиент."""
        if self._client:
            try:
                await self._client.aclose()
                logger.info("✅ LLM Client closed")
            except Exception as e:
                logger.debug(f"Error closing LLM client: {e}")
            finally:
                self._client = None

    async def _check_availability(self):
        """Проверяет доступность Ollama."""
        try:
            response = await self._client.get(f"{self.ollama_host}/api/tags")
            self._available = response.status_code == 200
        except Exception as e:
            logger.warning(f"Ollama not available: {e}")
            self._available = False

    async def generate(
        self,
        agent_name: str,
        prompt: str,
        context: Optional[str] = None,
        max_tokens: int = 1000,
        temperature: float = 0.3,
    ) -> Optional[str]:
        """
        Генерирует ответ от LLM.

        Args:
            agent_name: Имя агента (для выбора системного промпта)
            prompt: Пользовательский промпт
            context: Дополнительный контекст из RAG
            max_tokens: Максимальное количество токенов
            temperature: Температура генерации

        Returns:
            Ответ LLM или None при ошибке
        """
        if not self._available:
            logger.warning(f"LLM not available for {agent_name}")
            return None

        # Формируем полный промпт
        system_prompt = self.system_prompts.get(agent_name, "Ты — AI-ассистент.")

        full_prompt = f"СИСТЕМА: {system_prompt}\n\n"

        if context:
            full_prompt += f"КОНТЕКСТ:\n{context}\n\n"

        full_prompt += f"ЗАПРОС:\n{prompt}\n\nОТВЕТ:"

        # Проверяем кэш
        cache_key = f"{agent_name}:{hash(full_prompt)}"
        cached = self._get_from_cache(cache_key)
        if cached:
            logger.debug(f"Cache hit for {agent_name}")
            return cached

        try:
            # Запрос к Ollama
            response = await self._client.post(
                f"{self.ollama_host}/api/generate",
                json={
                    "model": self.model_name,
                    "prompt": full_prompt,
                    "stream": False,
                    "options": {
                        "temperature": temperature,
                        "num_predict": max_tokens,
                    },
                },
            )

            response.raise_for_status()
            data = response.json()

            result = data.get("response", "")

            # Кэшируем результат
            self._add_to_cache(cache_key, result)

            logger.info(
                f"🤖 [{agent_name}] LLM response generated ({len(result)} chars)"
            )

            return result

        except httpx.ConnectError:
            logger.error(f"Cannot connect to Ollama at {self.ollama_host}")
            self._available = False
            return None

        except httpx.TimeoutException:
            logger.error(f"LLM request timeout for {agent_name}")
            return None

        except Exception as e:
            logger.error(f"LLM generation error for {agent_name}: {e}")
            return None

    async def analyze_anomaly(
        self,
        agent_name: str,
        metric: str,
        current_value: float,
        baseline_value: float,
        history: List[float],
        context: Optional[str] = None,
    ) -> Optional[str]:
        """Специализированный метод для анализа аномалий."""
        prompt = f"""Аномалия обнаружена:
- Метрика: {metric}
- Текущее значение: {current_value:.2f}
- Базовое значение: {baseline_value:.2f}
- Отклонение: {((current_value - baseline_value) / baseline_value * 100):.1f}%
- Последние 10 значений: {[f"{v:.2f}" for v in history[-10:]]}

Проанализируй причину аномалии и предложи решение."""

        return await self.generate(agent_name, prompt, context)

    async def generate_session_digest(
        self,
        session_data: Dict[str, Any],
        problems: List[Dict[str, str]],
        context: Optional[str] = None,
    ) -> Optional[str]:
        """Генерирует Session Digest."""
        prompt = f"""Сгенерируй Session Digest для сессии:
- Цель: {session_data.get("target")}
- Фильтр: {session_data.get("filter")}
- Экспозиция: {session_data.get("exposure_time")}s
- Отснято кадров: {session_data.get("frames_total")}
- Принято: {session_data.get("frames_accepted")}
- Средний HFR: {session_data.get("avg_hfr")}
- Средний RMS: {session_data.get("avg_rms_ra")}" (RA), {session_data.get("avg_rms_dec")}" (Dec)

Проблемы:
{chr(10).join(f"- {p.get('time')}: {p.get('issue')} → {p.get('solution')}" for p in problems)}

Сгенерируй Markdown-отчет с рекомендациями."""

        return await self.generate("Auditor", prompt, context, max_tokens=2000)

    def _get_from_cache(self, key: str) -> Optional[str]:
        """Получает значение из кэша."""
        if key not in self._cache:
            return None

        entry = self._cache[key]
        age = (datetime.now() - entry["timestamp"]).total_seconds()

        if age > self._cache_ttl:
            del self._cache[key]
            return None

        return entry["value"]

    def _add_to_cache(self, key: str, value: str):
        """Добавляет значение в кэш."""
        self._cache[key] = {"value": value, "timestamp": datetime.now()}

        # Ограничиваем размер кэша
        if len(self._cache) > 1000:
            # Удаляем самые старые записи
            oldest_keys = sorted(
                self._cache.keys(), key=lambda k: self._cache[k]["timestamp"]
            )[:100]
            for key in oldest_keys:
                del self._cache[key]

    def is_available(self) -> bool:
        """Проверяет доступность LLM."""
        return self._available


# Singleton instance
llm_client = LLMClient()
