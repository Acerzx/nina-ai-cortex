"""
HTTP Client Manager — единый менеджер HTTP клиентов для Cortex.
Устраняет проблему С-15: множественные HTTP клиенты без connection pooling.

Архитектура:
- Отдельный httpx.AsyncClient на каждый base_url
- Кэширование клиентов по base_url для connection pooling
- Конфигурация таймаутов и лимитов из settings.http_client
- Thread-safe через asyncio.Lock
- Graceful shutdown через close_all()

Использование:
    from app.core.http_client import http_client_manager

    # Получить клиент для N.I.N.A. API
    client = await http_client_manager.get_client(
        base_url="http://localhost:1888",
        service="nina",
    )
    response = await client.get("/v2/api/version")

    # При shutdown
    await http_client_manager.close_all()

Сервисы:
- "nina" — N.I.N.A. Advanced API
- "ollama" — Ollama LLM
- "prometheus" — Prometheus Scraper
- "embeddings" — Ollama embeddings API
- "default" — глобальные дефолты
"""

import asyncio
import logging
from typing import Dict, Optional, Any
import httpx
from app.core.config import settings

logger = logging.getLogger("HttpClientManager")


class HttpClientManager:
    """
    Единый менеджер HTTP клиентов.

    Features:
    - Кэширование httpx.AsyncClient по base_url
    - Конфигурация из settings.http_client
    - Thread-safe через asyncio.Lock
    - Graceful shutdown через close_all()
    - Статистика использования
    """

    def __init__(self):
        self._clients: Dict[str, httpx.AsyncClient] = {}
        self._lock = asyncio.Lock()
        self._stats = {
            "clients_created": 0,
            "clients_reused": 0,
            "clients_closed": 0,
        }

        # Загружаем конфигурацию
        self._config = getattr(settings, "http_client", None)

        logger.info(
            f"🌐 HttpClientManager initialized "
            f"(config: {'loaded' if self._config else 'defaults'})"
        )

    def _get_service_config(self, service: str) -> Dict[str, Any]:
        """
        Получает конфигурацию для сервиса.

        Args:
            service: Имя сервиса ("nina", "ollama", "prometheus", "embeddings", "default")

        Returns:
            Dict с timeout_seconds, max_connections, max_keepalive, keepalive_expiry
        """
        if self._config is None:
            # Fallback на глобальные дефолты
            return {
                "timeout_seconds": 30.0,
                "max_connections": 20,
                "max_keepalive": 10,
                "keepalive_expiry": 30,
            }

        # Пробуем получить конфигурацию сервиса
        service_cfg = getattr(self._config, service, None)
        if service_cfg is not None:
            return {
                "timeout_seconds": getattr(
                    service_cfg, "timeout_seconds", self._config.default_timeout_seconds
                ),
                "max_connections": getattr(
                    service_cfg, "max_connections", self._config.default_max_connections
                ),
                "max_keepalive": getattr(
                    service_cfg, "max_keepalive", self._config.default_max_keepalive
                ),
                "keepalive_expiry": getattr(
                    service_cfg,
                    "keepalive_expiry",
                    self._config.default_keepalive_expiry,
                ),
            }

        # Fallback на глобальные дефолты из конфига
        return {
            "timeout_seconds": self._config.default_timeout_seconds,
            "max_connections": self._config.default_max_connections,
            "max_keepalive": self._config.default_max_keepalive,
            "keepalive_expiry": self._config.default_keepalive_expiry,
        }

    async def get_client(
        self,
        base_url: str,
        service: str = "default",
        headers: Optional[Dict[str, str]] = None,
    ) -> httpx.AsyncClient:
        """
        Получает или создаёт HTTP клиент для base_url.

        Кэширует клиенты по ключу "{service}:{base_url}".
        Thread-safe через asyncio.Lock.

        Args:
            base_url: Базовый URL сервиса
            service: Имя сервиса для конфигурации ("nina", "ollama", etc.)
            headers: Дополнительные заголовки (опционально)

        Returns:
            httpx.AsyncClient (кэшированный или новый)
        """
        # Нормализуем base_url
        base_url = base_url.rstrip("/")
        cache_key = f"{service}:{base_url}"

        async with self._lock:
            # Проверяем кэш
            if cache_key in self._clients:
                client = self._clients[cache_key]
                if not client.is_closed:
                    self._stats["clients_reused"] += 1
                    return client
                # Клиент закрыт — удаляем из кэша
                del self._clients[cache_key]
                logger.debug(f"Removed closed client from cache: {cache_key}")

            # Создаём новый клиент
            cfg = self._get_service_config(service)

            client = httpx.AsyncClient(
                base_url=base_url,
                timeout=httpx.Timeout(cfg["timeout_seconds"]),
                limits=httpx.Limits(
                    max_connections=cfg["max_connections"],
                    max_keepalive_connections=cfg["max_keepalive"],
                    keepalive_expiry=cfg["keepalive_expiry"],
                ),
                headers=headers or {"Content-Type": "application/json"},
            )

            self._clients[cache_key] = client
            self._stats["clients_created"] += 1

            logger.info(
                f"✅ HTTP client created: {cache_key} "
                f"(timeout={cfg['timeout_seconds']}s, "
                f"max_conn={cfg['max_connections']})"
            )

            return client

    async def close_client(self, base_url: str, service: str = "default") -> bool:
        """
        Закрывает конкретный клиент и удаляет из кэша.

        Args:
            base_url: Базовый URL
            service: Имя сервиса

        Returns:
            True если клиент был закрыт
        """
        base_url = base_url.rstrip("/")
        cache_key = f"{service}:{base_url}"

        async with self._lock:
            if cache_key in self._clients:
                client = self._clients[cache_key]
                if not client.is_closed:
                    try:
                        await client.aclose()
                        logger.debug(f"Closed client: {cache_key}")
                    except Exception as e:
                        logger.debug(f"Error closing client {cache_key}: {e}")
                del self._clients[cache_key]
                self._stats["clients_closed"] += 1
                return True
            return False

    async def close_all(self) -> int:
        """
        Закрывает все кэшированные клиенты.
        Вызывается при shutdown приложения.

        Returns:
            Количество закрытых клиентов
        """
        closed_count = 0

        async with self._lock:
            for cache_key, client in list(self._clients.items()):
                if not client.is_closed:
                    try:
                        await client.aclose()
                        logger.debug(f"Closed client: {cache_key}")
                    except Exception as e:
                        logger.debug(f"Error closing client {cache_key}: {e}")
                closed_count += 1

            self._clients.clear()
            self._stats["clients_closed"] += closed_count

        logger.info(f"🛑 HttpClientManager: closed {closed_count} clients")
        return closed_count

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику менеджера."""
        return {
            **self._stats,
            "active_clients": len(self._clients),
            "client_keys": list(self._clients.keys()),
        }

    def list_active_clients(self) -> list:
        """Возвращает список активных клиентов."""
        return list(self._clients.keys())


# Singleton instance
http_client_manager = HttpClientManager()
