"""
Embeddings через Ollama.
Использует модель nomic-embed-text через Ollama API.
Поддерживает оба эндпоинта:
- /api/embed (новые версии Ollama >= 0.1.26)
- /api/embeddings (старые версии)

Преимущества:
- Нет дополнительных зависимостей (torch не нужен)
- Единый источник для LLM и embeddings
- Работает с Python 3.14 без проблем

ИСПРАВЛЕНО (С-15):
- Миграция на единый HttpClientManager
- Убрано самостоятельное создание httpx.AsyncClient
- Connection pooling через http_client_manager
"""

import logging
import hashlib
from typing import List, Optional, Dict
from pathlib import Path
import pickle
import httpx
from app.core.config import settings
from app.core.http_client import http_client_manager

logger = logging.getLogger("Embeddings")


class OllamaEmbeddings:
    """
    Генерация embeddings через Ollama.
    Модель: nomic-embed-text (768 dim)
    Кэширование: pickle на диск для быстрого рестарта

    ИСПРАВЛЕНО (С-15):
    - Использует http_client_manager для connection pooling
    """

    MODEL = "nomic-embed-text"
    CACHE_FILE = Path("./data/embeddings_cache.pkl")

    def __init__(self):
        self.model_name = self.MODEL
        self._dimension = 768  # nomic-embed-text = 768 dim
        self._cache: Dict[str, List[float]] = {}
        self._initialized = False
        # ИСПРАВЛЕНО (С-15): http_client_manager управляет клиентами
        # self._http_client — удалён
        self._embedding_backend = "ollama"
        self._load_cache()

    def _load_cache(self):
        """Загружает кэш embeddings с диска."""
        if self.CACHE_FILE.exists():
            try:
                with open(self.CACHE_FILE, "rb") as f:
                    self._cache = pickle.load(f)
                logger.info(f"📚 Loaded {len(self._cache)} embeddings from cache")
            except Exception as e:
                logger.warning(f"Failed to load embeddings cache: {e}")
                self._cache = {}

    def _save_cache(self):
        """Сохраняет кэш embeddings на диск."""
        try:
            self.CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(self.CACHE_FILE, "wb") as f:
                pickle.dump(self._cache, f)
        except Exception as e:
            logger.debug(f"Failed to save embeddings cache: {e}")

    async def initialize(self):
        """
        Инициализирует HTTP клиент и проверяет доступность модели.
        ИСПРАВЛЕНО (С-15): pre-creates клиент через менеджер.
        """
        if self._initialized:
            return

        try:
            ollama_host = settings.ai_settings.ollama_host

            # ИСПРАВЛЕНО (С-15): Получаем клиент через менеджер
            client = await http_client_manager.get_client(
                base_url=ollama_host,
                service="embeddings",
            )

            # Проверяем доступность модели
            response = await client.get(
                f"{ollama_host}/api/tags",
                timeout=httpx.Timeout(30.0),
            )
            response.raise_for_status()
            models = response.json().get("models", [])
            model_names = [m["name"] for m in models]

            if any(self.model_name in name for name in model_names):
                logger.info(f"✅ Ollama embeddings ready: {self.model_name}")
            else:
                logger.warning(
                    f"⚠️ Model {self.model_name} not found. "
                    f"Run: ollama pull {self.model_name}"
                )

            self._initialized = True
            logger.info(
                f"✅ Embeddings initialized ({self._dimension} dims, "
                f"{len(self._cache)} cached)"
            )

        except Exception as e:
            logger.error(f"❌ Failed to initialize embeddings: {e}")
            raise

    def _get_cache_key(self, text: str) -> str:
        """Генерирует ключ кэша для текста."""
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    async def embed(self, text: str) -> Optional[List[float]]:
        """
        Генерирует embedding для текста через Ollama.
        Returns:
            Вектор embedding (768 dim) или None при ошибке
        """
        if not self._initialized:
            try:
                await self.initialize()
            except Exception:
                return None

        # Проверяем кэш
        cache_key = self._get_cache_key(text)
        if cache_key in self._cache:
            return self._cache[cache_key]

        ollama_host = settings.ai_settings.ollama_host

        # ИСПРАВЛЕНО (С-15): Получаем клиент через менеджер
        try:
            client = await http_client_manager.get_client(
                base_url=ollama_host,
                service="embeddings",
            )
        except Exception as e:
            logger.error(f"Failed to get HTTP client: {e}")
            return None

        # Пробуем оба эндпоинта Ollama
        endpoints = [
            (f"{ollama_host}/api/embed", True),  # Новый формат
            (f"{ollama_host}/api/embeddings", False),  # Старый формат
        ]

        for endpoint, use_input in endpoints:
            try:
                payload = (
                    {"model": self.model_name, "input": text}
                    if use_input
                    else {"model": self.model_name, "prompt": text}
                )
                response = await client.post(
                    endpoint,
                    json=payload,
                    timeout=httpx.Timeout(30.0),
                )
                response.raise_for_status()
                data = response.json()

                embedding = None
                if "embeddings" in data and data["embeddings"]:
                    embedding = data["embeddings"][0]
                elif "embedding" in data:
                    embedding = data["embedding"]

                if embedding:
                    self._cache[cache_key] = embedding
                    # Периодически сохраняем кэш
                    if len(self._cache) % 100 == 0:
                        self._save_cache()
                    return embedding
                continue

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                logger.error(f"HTTP error from {endpoint}: {e}")
                return None

            except httpx.ConnectError:
                logger.warning(
                    f"Cannot connect to Ollama. "
                    f"Make sure Ollama is running: ollama pull {self.model_name}"
                )
                return None

            except Exception as e:
                logger.debug(f"Endpoint {endpoint} failed: {e}")
                continue

        logger.error(f"Failed to generate embedding for text: {text[:50]}...")
        return None

    async def embed_batch(self, texts: List[str]) -> List[Optional[List[float]]]:
        """Генерирует embeddings для списка текстов."""
        results = []
        for text in texts:
            results.append(await self.embed(text))
        return results

    def get_dimension(self) -> int:
        """Возвращает размерность embedding вектора."""
        return self._dimension

    def get_backend(self) -> str:
        """Возвращает текущий backend embeddings."""
        return self._embedding_backend

    def get_stats(self) -> Dict:
        """Возвращает статистику embeddings."""
        # ИСПРАВЛЕНО (С-15): Читаем статус клиента из менеджера
        ollama_host = settings.ai_settings.ollama_host
        cache_key = f"embeddings:{ollama_host}"
        manager_stats = http_client_manager.get_stats()
        client_active = cache_key in manager_stats.get("client_keys", [])

        return {
            "model": self.model_name,
            "backend": self._embedding_backend,
            "dimension": self._dimension,
            "initialized": self._initialized,
            "cached_embeddings": len(self._cache),
            "client_active": client_active,
            "http_client_manager": "active",
        }

    async def close(self):
        """
        Закрывает HTTP клиент.
        ИСПРАВЛЕНО (С-15): делегирует http_client_manager.
        """
        ollama_host = settings.ai_settings.ollama_host
        closed = await http_client_manager.close_client(
            base_url=ollama_host,
            service="embeddings",
        )
        if closed:
            logger.info("✅ Embeddings HTTP client closed")


# Singleton instance
local_embeddings = OllamaEmbeddings()
