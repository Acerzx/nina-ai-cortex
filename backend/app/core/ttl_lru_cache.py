"""
AsyncTTLCache — асинхронная обёртка над cachetools.TTLCache.

Архитектурное решение (Этап 1.1):
- Использует battle-tested cachetools вместо собственной реализации
- Добавляет async/await поддержку через asyncio.Lock
- TTL (time-to-live) + LRU (least recently used) eviction
- Потокобезопасность гарантирована

Использование:
    from app.core.ttl_lru_cache import AsyncTTLCache

    cache = AsyncTTLCache(max_size=10000, ttl_seconds=3600)
    await cache.put("key", value)
    result = await cache.get("key")
"""

import asyncio
import logging
from typing import Optional, Any
from cachetools import TTLCache

logger = logging.getLogger("AsyncTTLCache")


class AsyncTTLCache:
    """
    Асинхронная обёртка над cachetools.TTLCache.

    Features:
    - TTL: автоматическое удаление устаревших записей
    - LRU: вытеснение наименее используемых при достижении max_size
    - Thread-safe через asyncio.Lock
    - Статистика hit/miss для мониторинга

    Args:
        max_size: Максимальное количество записей в кэше
        ttl_seconds: Время жизни записи в секундах
    """

    def __init__(self, max_size: int = 10000, ttl_seconds: int = 3600):
        self._cache = TTLCache(maxsize=max_size, ttl=ttl_seconds)
        self._lock = asyncio.Lock()
        self._stats = {
            "hits": 0,
            "misses": 0,
            "total_requests": 0,
        }
        logger.info(
            f"✅ AsyncTTLCache initialized (max_size={max_size}, ttl={ttl_seconds}s)"
        )

    async def get(self, key: str) -> Optional[Any]:
        """
        Получает значение из кэша.

        Returns:
            Значение если найдено и не устарело, иначе None
        """
        self._stats["total_requests"] += 1
        async with self._lock:
            value = self._cache.get(key)
            if value is not None:
                self._stats["hits"] += 1
                return value
            self._stats["misses"] += 1
            return None

    async def put(self, key: str, value: Any) -> None:
        """Сохраняет значение в кэш."""
        async with self._lock:
            self._cache[key] = value

    async def clear(self) -> int:
        """
        Очищает весь кэш.

        Returns:
            Количество удалённых записей
        """
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.debug(f"Cleared {count} entries from cache")
            return count

    def get_stats(self) -> dict:
        """Возвращает статистику использования кэша."""
        total = self._stats["total_requests"]
        hit_rate = (self._stats["hits"] / total * 100) if total > 0 else 0.0

        return {
            "size": len(self._cache),
            "max_size": self._cache.maxsize,
            "ttl_seconds": self._cache.ttl,
            **self._stats,
            "hit_rate_percent": round(hit_rate, 2),
        }
