"""
LLM Client — обёртка над LLM Provider для совместимости с агентами.
Использует LLM Provider (gemma4:31b-cloud → gemma4:e4b через Ollama).
"""

import logging
from typing import Dict, Any, Optional
from app.agents.llm_provider import llm_provider

logger = logging.getLogger("LLMClient")


class LLMClient:
    """
    Клиент для взаимодействия с LLM через Ollama.

    Системные промпты для разных агентов:
    - Watcher: мониторинг и детекция аномалий
    - Guardian: безопасность оборудования
    - Diagnostician: root cause analysis
    - Strategist: оптимизация параметров
    - Auditor: post-mortem анализ
    - Copilot: интерактивная помощь
    """

    def __init__(self):
        self.system_prompts = {
            "Watcher": "Ты — агент мониторинга обсерватории. Анализируй метрики качества (HFR, FWHM, RMS) и обнаруживай аномалии. Отвечай кратко на русском.",
            "Guardian": "Ты — агент безопасности обсерватории. Обеспечивай безопасность оборудования. Приоритет: Safety > Quality > Optimization. Отвечай на русском.",
            "Diagnostician": "Ты — агент диагностики проблем. Находи root cause через анализ корреляций и исторических данных. Отвечай кратко на русском.",
            "Strategist": "Ты — агент оптимизации параметров съемки. Предлагай оптимальные параметры на основе SNR и условий. Отвечай на русском.",
            "Auditor": "Ты — агент post-mortem анализа сессий. Генерируй Session Digest с выводами и рекомендациями. Отвечай на русском.",
            "Copilot": "Ты — интерактивный помощник. Предоставляй пошаговые инструкции для ручных шагов. Отвечай на русском.",
        }

    async def initialize(self):
        """Инициализирует LLM Provider."""
        availability = await llm_provider.check_availability()

        if availability["primary"]:
            logger.info(
                f"✅ LLM Client initialized (primary: {llm_provider.config.primary_model})"
            )
        elif availability["fallback"]:
            logger.warning(
                f"⚠️ LLM Client initialized with FALLBACK only ({llm_provider.config.fallback_model})"
            )
        else:
            logger.error("❌ No LLM models available in Ollama")

    async def close(self):
        """Закрывает LLM Provider."""
        await llm_provider.close()

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
        system_prompt = self.system_prompts.get(
            agent_name, "Ты — AI-ассистент обсерватории. Отвечай кратко на русском."
        )

        full_prompt = prompt
        if context:
            full_prompt = f"КОНТЕКСТ:\n{context}\n\nЗАПРОС:\n{prompt}"

        response = await llm_provider.generate(
            prompt=full_prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
        )

        if response:
            return response.content

        return None

    def is_available(self) -> bool:
        """Проверяет доступность LLM."""
        # Проверяем, что хотя бы одна модель настроена
        return llm_provider.config.primary_model or llm_provider.config.fallback_model

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику LLM."""
        return llm_provider.get_stats()


# Singleton instance
llm_client = LLMClient()
