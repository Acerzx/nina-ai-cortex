"""
LLM Client — обёртка над LLM Provider для совместимости с агентами.
Использует LLM Provider (gemma4:31b-cloud → gemma4:e4b через Ollama).

ИСПРАВЛЕНО (audit 4.4):
- Метод is_available() теперь выполняет реальную проверку доступности моделей
  через llm_provider.check_availability()
- Внедрён кэш результатов доступности с TTL (30 секунд) для предотвращения
  излишней нагрузки на Ollama API
- Добавлены синхронная и асинхронная версии проверки:
  - is_available() — синхронная, с кэшированием (для health check)
  - is_available_async() — асинхронная, без кэширования (для агентов)
- Добавлен метод refresh_availability() для принудительного обновления кэша
"""
import logging
import asyncio
import time
from typing import Dict, Any, Optional, Tuple
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

    ИСПРАВЛЕНО (audit 4.4):
    - Реальная проверка доступности моделей через Ollama API
    - Кэширование результатов с TTL для оптимизации
    """

    # Время жизни кэша доступности (секунды)
    AVAILABILITY_CACHE_TTL: int = 30

    def __init__(self):
        self.system_prompts = {
            "Watcher": (
                "Ты — агент мониторинга обсерватории. "
                "Анализируй метрики качества (HFR, FWHM, RMS) "
                "и обнаруживай аномалии. Отвечай кратко на русском."
            ),
            "Guardian": (
                "Ты — агент безопасности обсерватории. "
                "Обеспечивай безопасность оборудования. "
                "Приоритет: Safety > Quality > Optimization. "
                "Отвечай на русском."
            ),
            "Diagnostician": (
                "Ты — агент диагностики проблем. "
                "Находи root cause через анализ корреляций "
                "и исторических данных. Отвечай кратко на русском."
            ),
            "Strategist": (
                "Ты — агент оптимизации параметров съемки. "
                "Предлагай оптимальные параметры на основе SNR "
                "и условий. Отвечай на русском."
            ),
            "Auditor": (
                "Ты — агент post-mortem анализа сессий. "
                "Генерируй Session Digest с выводами "
                "и рекомендациями. Отвечай на русском."
            ),
            "Copilot": (
                "Ты — интерактивный помощник. "
                "Предоставляй пошаговые инструкции "
                "для ручных шагов. Отвечай на русском."
            ),
        }

        # ИСПРАВЛЕНО (audit 4.4): Кэш доступности моделей
        # Формат: (any_available: bool, primary_available: bool,
        #           fallback_available: bool, timestamp: float)
        self._availability_cache: Optional[Tuple[bool, bool, bool, float]] = None
        self._availability_lock = asyncio.Lock()

        # Статистика
        self._stats = {
            "total_generate_calls": 0,
            "successful_generations": 0,
            "failed_generations": 0,
            "availability_checks": 0,
            "availability_cache_hits": 0,
        }

    async def initialize(self):
        """Инициализирует LLM Provider и выполняет первичную проверку доступности."""
        # ИСПРАВЛЕНО (audit 4.4): Реальная проверка доступности моделей
        availability = await llm_provider.check_availability()

        if availability["primary"]:
            logger.info(
                f"✅ LLM Client initialized "
                f"(primary: {llm_provider.config.primary_model})"
            )
        elif availability["fallback"]:
            logger.warning(
                f"⚠️ LLM Client initialized with FALLBACK only "
                f"({llm_provider.config.fallback_model})"
            )
        else:
            logger.error(
                "❌ No LLM models available in Ollama. "
                f"Please run: ollama pull {llm_provider.config.primary_model} "
                f"or ollama pull {llm_provider.config.fallback_model}"
            )

        # Обновляем кэш доступности
        self._update_availability_cache(availability)

    async def close(self):
        """Закрывает LLM Provider."""
        await llm_provider.close()
        # Сбрасываем кэш при закрытии
        self._availability_cache = None
        logger.info("🛑 LLM Client closed")

    def _update_availability_cache(self, availability: Dict[str, bool]) -> None:
        """
        Обновляет кэш доступности.

        Args:
            availability: Словарь с ключами 'primary' и 'fallback'
        """
        any_available = availability["primary"] or availability["fallback"]
        self._availability_cache = (
            any_available,
            availability["primary"],
            availability["fallback"],
            time.time(),
        )

    def _is_cache_valid(self) -> bool:
        """Проверяет, валиден ли кэш доступности (не истёк TTL)."""
        if self._availability_cache is None:
            return False
        _, _, _, cached_time = self._availability_cache
        return (time.time() - cached_time) < self.AVAILABILITY_CACHE_TTL

    async def is_available_async(self, force_refresh: bool = False) -> bool:
        """
        Асинхронная проверка доступности LLM.

        ИСПРАВЛЕНО (audit 4.4): Выполняет реальную проверку через Ollama API
        с использованием кэша для оптимизации.

        Args:
            force_refresh: Принудительно обновить кэш (игнорировать TTL)

        Returns:
            True если хотя бы одна модель доступна
        """
        self._stats["availability_checks"] += 1

        # Проверяем валидность кэша
        if not force_refresh and self._is_cache_valid():
            self._stats["availability_cache_hits"] += 1
            any_available, _, _, _ = self._availability_cache
            return any_available

        # Кэш невалиден или требуется принудительное обновление
        async with self._availability_lock:
            # Двойная проверка после захвата блокировки
            if not force_refresh and self._is_cache_valid():
                self._stats["availability_cache_hits"] += 1
                any_available, _, _, _ = self._availability_cache
                return any_available

            try:
                availability = await llm_provider.check_availability()
                self._update_availability_cache(availability)

                any_available = availability["primary"] or availability["fallback"]

                if not any_available:
                    logger.warning(
                        "⚠️ No LLM models available in Ollama. "
                        "System will operate in degraded mode."
                    )

                return any_available

            except Exception as e:
                logger.error(f"Failed to check LLM availability: {e}")
                # В случае ошибки возвращаем последнее известное состояние
                if self._availability_cache is not None:
                    any_available, _, _, _ = self._availability_cache
                    return any_available
                return False

    def is_available(self) -> bool:
        """
        Синхронная проверка доступности LLM (для health check и быстрых проверок).

        ИСПРАВЛЕНО (audit 4.4): Теперь возвращает реальное состояние,
        основанное на кэшированной проверке через Ollama API.

        Если кэш невалиден, возвращает последнее известное состояние
        и планирует асинхронное обновление в фоне.

        Returns:
            True если хотя бы одна модель доступна (по последним данным)
        """
        self._stats["availability_checks"] += 1

        # Если кэш валиден — возвращаем актуальный результат
        if self._is_cache_valid():
            self._stats["availability_cache_hits"] += 1
            any_available, _, _, _ = self._availability_cache
            return any_available

        # Кэш невалиден — возвращаем последнее известное состояние
        if self._availability_cache is not None:
            any_available, _, _, _ = self._availability_cache
            # Планируем асинхронное обновление кэша в фоне
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._background_refresh_availability())
            except RuntimeError:
                # Нет запущенного event loop — это нормально при импорте
                pass
            return any_available

        # Кэш полностью отсутствует — проверяем хотя бы конфиг
        # (fallback для случаев, когда initialize() ещё не вызван)
        return bool(
            llm_provider.config.primary_model
            or llm_provider.config.fallback_model
        )

    async def _background_refresh_availability(self) -> None:
        """
        Фоновое обновление кэша доступности.
        Вызывается из is_available() когда кэш истёк.
        """
        try:
            await self.is_available_async(force_refresh=True)
        except Exception as e:
            logger.debug(f"Background availability refresh failed: {e}")

    async def refresh_availability(self) -> Dict[str, bool]:
        """
        Принудительно обновляет кэш доступности и возвращает детальный статус.

        Используется Mode Manager для проверки здоровья LLM API.

        Returns:
            Словарь {'primary': bool, 'fallback': bool, 'any': bool}
        """
        availability = await llm_provider.check_availability()
        self._update_availability_cache(availability)

        return {
            "primary": availability["primary"],
            "fallback": availability["fallback"],
            "any": availability["primary"] or availability["fallback"],
        }

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
        self._stats["total_generate_calls"] += 1

        # Проверяем доступность перед генерацией
        if not await self.is_available_async():
            logger.warning(
                f"LLM not available, skipping generation for agent '{agent_name}'"
            )
            self._stats["failed_generations"] += 1
            return None

        # Формируем системный промпт
        system_prompt = self.system_prompts.get(
            agent_name,
            "Ты — AI-ассистент обсерватории. Отвечай кратко на русском.",
        )

        # Формируем полный промпт с контекстом
        full_prompt = prompt
        if context:
            full_prompt = f"КОНТЕКСТ:\n{context}\n\nЗАПРОС:\n{prompt}"

        # Вызываем LLM Provider
        response = await llm_provider.generate(
            prompt=full_prompt,
            system_prompt=system_prompt,
            max_tokens=max_tokens,
            temperature=temperature,
        )

        if response and response.content:
            self._stats["successful_generations"] += 1
            return response.content

        self._stats["failed_generations"] += 1
        return None

    async def generate_session_digest(
        self,
        session_data: Dict[str, Any],
        problems: list,
        context: Optional[str] = None,
    ) -> Optional[str]:
        """
        Генерирует расширенный Session Digest с использованием LLM.

        Args:
            session_data: Данные сессии
            problems: Список проблем
            context: Контекст из RAG

        Returns:
            Текстовый отчёт или None
        """
        prompt = f"""Проанализируй астрофотографическую сессию и создай краткий отчёт.

Данные сессии:
- Цель: {session_data.get('target', 'Unknown')}
- Фильтр: {session_data.get('filter', 'Unknown')}
- Экспозиция: {session_data.get('exposure_time', 0)}s
- Кадров всего: {session_data.get('frames_total', 0)}
- Кадров принято: {session_data.get('frames_accepted', 0)}
- Средний HFR: {session_data.get('avg_hfr', 'N/A')}
- Средний RMS RA: {session_data.get('avg_rms_ra', 'N/A')}
- Средний RMS Dec: {session_data.get('avg_rms_dec', 'N/A')}

Проблемы во время сессии:
{chr(10).join(f'- {p.get("time", "")}: {p.get("issue", "")}' for p in problems) if problems else 'Нет серьёзных проблем'}

Создай краткий отчёт (3-5 предложений) с выводами и рекомендациями для будущих сессий.
Отвечай на русском языке."""

        return await self.generate(
            agent_name="Auditor",
            prompt=prompt,
            context=context,
            max_tokens=800,
            temperature=0.3,
        )

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику LLM Client.

        Включает:
        - Статистику генераций (успешные/неуспешные)
        - Статистику проверок доступности
        - Текущее состояние кэша доступности
        - Статистику от LLM Provider
        """
        # Детализация кэша доступности
        availability_cache_info = {
            "cached": self._availability_cache is not None,
            "valid": self._is_cache_valid(),
        }

        if self._availability_cache is not None:
            any_available, primary, fallback, cached_time = self._availability_cache
            availability_cache_info.update(
                {
                    "any_available": any_available,
                    "primary_available": primary,
                    "fallback_available": fallback,
                    "age_seconds": round(time.time() - cached_time, 2),
                    "ttl_seconds": self.AVAILABILITY_CACHE_TTL,
                }
            )

        # Статистика от LLM Provider
        provider_stats = llm_provider.get_stats()

        return {
            **self._stats,
            "availability_cache": availability_cache_info,
            "provider": provider_stats,
            "config": {
                "primary_model": llm_provider.config.primary_model,
                "fallback_model": llm_provider.config.fallback_model,
                "cache_ttl_seconds": self.AVAILABILITY_CACHE_TTL,
            },
        }

    def get_cache_stats(self) -> Dict[str, Any]:
        """Возвращает детальную статистику кэша доступности."""
        return {
            "cached": self._availability_cache is not None,
            "valid": self._is_cache_valid(),
            "ttl_seconds": self.AVAILABILITY_CACHE_TTL,
            "checks_total": self._stats["availability_checks"],
            "cache_hits": self._stats["availability_cache_hits"],
            "hit_rate_percent": (
                round(
                    self._stats["availability_cache_hits"]
                    / max(self._stats["availability_checks"], 1)
                    * 100,
                    2,
                )
            ),
        }


# Singleton instance
llm_client = LLMClient()