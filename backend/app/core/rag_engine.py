"""
RAG Engine (Retrieval-Augmented Generation)
Предоставляет AI-агентам доступ к документации и истории сессий через векторный поиск.

Архитектура embeddings (гибридный подход):
1. Primary: sentence-transformers (локально, быстро, оффлайн)
2. Fallback: Ollama (nomic-embed-text) через HTTP

Автоматический fallback обеспечивает работоспособность RAG даже если
sentence-transformers не установлен (например, на Python 3.14).

ИСПРАВЛЕНО (audit 6.2):
- Внедрён LRU-кэш эмбеддингов на основе OrderedDict
- Ограничение размера кэша: 10000 записей
- Хеш SHA-256 используется как ключ для детерминированного доступа
- Добавлена статистика hit/miss для мониторинга эффективности кэша
- Автоматическое вытеснение старых записей при достижении лимита
"""

import asyncio
import logging
import hashlib
import json
from typing import List, Dict, Any, Optional, OrderedDict as OrderedDictType
from collections import OrderedDict
from datetime import datetime
from pathlib import Path
import httpx
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
)
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("RAGEngine")


class EmbeddingCache:
    """
    LRU-кэш для эмбеддингов.

    Использует OrderedDict для эффективной реализации LRU-eviction:
    - При доступе к элементу он перемещается в конец (MRU)
    - При превышении лимита удаляется первый элемент (LRU)
    - Все операции O(1)

    Потокобезопасность обеспечивается asyncio.Lock.
    """

    def __init__(self, max_size: int = 10000):
        """
        Args:
            max_size: Максимальное количество записей в кэше
        """
        self.max_size = max_size
        self._cache: OrderedDictType[str, List[float]] = OrderedDict()
        self._lock = asyncio.Lock()

        # Статистика
        self._stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "total_requests": 0,
        }

    @staticmethod
    def _make_key(text: str) -> str:
        """
        Создаёт детерминированный ключ для текста.

        Используется SHA-256 для:
        - Фиксированной длины ключа (64 hex символа)
        - Минимизации коллизий
        - Детерминированности (одинаковые тексты → одинаковые ключи)
        """
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    async def get(self, text: str) -> Optional[List[float]]:
        """
        Получает эмбеддинг из кэша.

        Args:
            text: Текст для поиска

        Returns:
            Вектор эмбеддинга или None если не найден
        """
        self._stats["total_requests"] += 1
        key = self._make_key(text)

        async with self._lock:
            if key in self._cache:
                # Перемещаем в конец (MRU)
                self._cache.move_to_end(key)
                self._stats["hits"] += 1
                return self._cache[key]

        self._stats["misses"] += 1
        return None

    async def put(self, text: str, embedding: List[float]) -> None:
        """
        Сохраняет эмбеддинг в кэш.

        При превышении лимита автоматически удаляет самую старую запись (LRU).

        Args:
            text: Текст (используется для создания ключа)
            embedding: Вектор эмбеддинга
        """
        key = self._make_key(text)

        async with self._lock:
            # Если ключ уже есть — обновляем и перемещаем в конец
            if key in self._cache:
                self._cache.move_to_end(key)
                self._cache[key] = embedding
                return

            # Добавляем новую запись
            self._cache[key] = embedding

            # Проверяем лимит и вытесняем LRU при необходимости
            while len(self._cache) > self.max_size:
                # popitem(last=False) удаляет первый (самый старый) элемент
                self._cache.popitem(last=False)
                self._stats["evictions"] += 1

    async def clear(self) -> int:
        """
        Очищает весь кэш.

        Returns:
            Количество удалённых записей
        """
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            return count

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику кэша.

        Включает:
        - Количество записей
        - Hit/miss ratio
        - Количество evictions
        """
        total = self._stats["total_requests"]
        hit_rate = round(self._stats["hits"] / max(total, 1) * 100, 2)

        return {
            "size": len(self._cache),
            "max_size": self.max_size,
            "utilization_percent": round(
                len(self._cache) / max(self.max_size, 1) * 100, 2
            ),
            **self._stats,
            "hit_rate_percent": hit_rate,
        }


class RAGEngine:
    """
    RAG-система для предоставления AI-агентам контекста из:
    1. Документации N.I.N.A. и плагинов
    2. Истории сессий (Session_Digest.md)
    3. Логов ошибок и решений

    Архитектура:
    - Qdrant для хранения векторов и метаданных
    - Гибридные embeddings (sentence-transformers → Ollama fallback)
    - Автоматическое пополнение через EventBus
    - LRU-кэш эмбеддингов (10000 записей)

    ИСПРАВЛЕНО (audit 6.2):
    - Добавлен EmbeddingCache для оптимизации повторных запросов
    - Статистика кэша доступна через get_stats()
    """

    CHUNK_SIZES = {
        "documentation": 1000,
        "session": 500,
        "error_log": 300,
    }

    # Максимальный размер кэша эмбеддингов
    EMBEDDING_CACHE_MAX_SIZE: int = 10000

    def __init__(self):
        self.qdrant_url = settings.qdrant.url
        self.collection_name = settings.qdrant.collection_name
        self.embedding_model = settings.qdrant.embedding_model
        self.ollama_host = settings.ai_settings.ollama_host

        self._client: Optional[AsyncQdrantClient] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._initialized = False
        self._vector_size = 384  # По умолчанию под MiniLM

        # Бэкенд embeddings
        self._embedding_backend: str = "unknown"

        # ИСПРАВЛЕНО (audit 6.2): LRU-кэш эмбеддингов
        self._embedding_cache = EmbeddingCache(max_size=self.EMBEDDING_CACHE_MAX_SIZE)

        # Статистика
        self._stats = {
            "documents_added": 0,
            "chunks_added": 0,
            "searches_performed": 0,
            "embedding_calls": 0,
            "embedding_failures": 0,
            "embedding_cache_hits": 0,
        }

    async def initialize(self):
        """
        Инициализирует подключения к Qdrant и embeddings.

        Порядок:
        1. Инициализация LocalEmbeddings (определяет backend)
        2. Определение размерности векторов из embeddings
        3. Создание/проверка коллекции Qdrant
        """
        if self._initialized:
            return

        try:
            # 1. Инициализация гибридных embeddings
            from app.core.embeddings import local_embeddings

            await local_embeddings.initialize()

            # 2. Получаем backend и dimension от embeddings
            self._embedding_backend = local_embeddings.get_backend()
            self._vector_size = local_embeddings.get_dimension()

            # Если embeddings не работают — fallback на Ollama HTTP
            if self._embedding_backend == "none":
                logger.warning(
                    "⚠️ LocalEmbeddings unavailable. "
                    "Will use direct Ollama HTTP fallback (768 dim)."
                )
                self._embedding_backend = "ollama_direct"
                self._vector_size = 768  # nomic-embed-text

            # 3. Подключение к Qdrant
            self._client = AsyncQdrantClient(url=self.qdrant_url)

            # 4. Проверка/создание коллекции
            collections = await self._client.get_collections()
            collection_names = [c.name for c in collections.collections]

            if self.collection_name in collection_names:
                # Проверяем размерность существующей коллекции
                collection_info = await self._client.get_collection(
                    self.collection_name
                )
                existing_size = collection_info.config.params.vectors.size

                if existing_size != self._vector_size:
                    logger.warning(
                        f"⚠️ Collection {self.collection_name} has dimension "
                        f"{existing_size}, but embeddings produce "
                        f"{self._vector_size}. "
                        f"Recreating collection..."
                    )
                    # Удаляем коллекцию и создаём заново
                    await self._client.delete_collection(self.collection_name)
                    collection_names.remove(self.collection_name)

            if self.collection_name not in collection_names:
                logger.info(
                    f"Creating Qdrant collection: {self.collection_name} "
                    f"(dim={self._vector_size})"
                )
                await self._client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self._vector_size,
                        distance=Distance.COSINE,
                    ),
                )

            # 5. HTTP клиент для прямого Ollama fallback
            self._http_client = httpx.AsyncClient(timeout=30.0)

            # 6. Подписка на события для автоматического пополнения
            event_bus.subscribe("SESSION_COMPLETED", self._on_session_completed)
            event_bus.subscribe("NIGHT_SUMMARY", self._on_night_summary)

            self._initialized = True
            logger.info(
                f"✅ RAG Engine initialized "
                f"(Qdrant: {self.qdrant_url}, "
                f"Backend: {self._embedding_backend}, "
                f"Dim: {self._vector_size}, "
                f"Cache: {self.EMBEDDING_CACHE_MAX_SIZE} entries)"
            )

        except Exception as e:
            logger.error(f"❌ Failed to initialize RAG Engine: {e}")

    async def close(self):
        """Корректно закрывает все подключения и очищает кэш."""
        # 1. Отписка от событий
        try:
            event_bus.unsubscribe("SESSION_COMPLETED", self._on_session_completed)
            event_bus.unsubscribe("NIGHT_SUMMARY", self._on_night_summary)
        except Exception:
            pass

        # 2. Закрытие HTTP клиента
        if self._http_client:
            try:
                await self._http_client.aclose()
            except Exception as e:
                logger.debug(f"Error closing HTTP client: {e}")
            finally:
                self._http_client = None

        # 3. ИСПРАВЛЕНО (v4.0 — проблема #18): Закрытие Qdrant клиента
        if self._client:
            try:
                await self._client.close()
                logger.info("✅ Qdrant client closed")
            except Exception as e:
                logger.debug(f"Error closing Qdrant client: {e}")
            finally:
                self._client = None

        # 4. Очистка кэша эмбеддингов
        cleared = await self._embedding_cache.clear()
        if cleared > 0:
            logger.debug(f"Cleared {cleared} entries from embedding cache")

        self._initialized = False
        logger.info("🛑 RAG Engine closed")

    async def _get_embedding(self, text: str) -> Optional[List[float]]:
        """
        Получает embedding вектор для текста с использованием LRU-кэша.

        ИСПРАВЛЕНО (audit 6.2): Сначала проверяет кэш, и только при
        промахе выполняет реальное вычисление. Результат сохраняется
        в кэш для повторного использования.

        Стратегия (приоритет):
        1. Проверка LRU-кэша (O(1))
        2. LocalEmbeddings (sentence-transformers или Ollama через него)
        3. Прямой HTTP запрос к Ollama (fallback)

        Args:
            text: Текст для получения эмбеддинга

        Returns:
            Вектор эмбеддинга или None при ошибке
        """
        if not self._initialized:
            logger.warning("RAG Engine not initialized")
            return None

        # ИСПРАВЛЕНО (audit 6.2): Проверка кэша
        cached = await self._embedding_cache.get(text)
        if cached is not None:
            self._stats["embedding_cache_hits"] += 1
            return cached

        # Кэш не сработал — вычисляем эмбеддинг
        self._stats["embedding_calls"] += 1

        embedding = await self._compute_embedding(text)

        # Сохраняем в кэш при успешном вычислении
        if embedding is not None:
            await self._embedding_cache.put(text, embedding)

        return embedding

    async def _compute_embedding(self, text: str) -> Optional[List[float]]:
        """
        Вычисляет эмбеддинг для текста (без кэширования).

        Внутренний метод, вызывается из _get_embedding() при cache miss.
        """
        # === Попытка 1: LocalEmbeddings ===
        try:
            from app.core.embeddings import local_embeddings

            if local_embeddings._initialized:
                embedding = await local_embeddings.embed(text)
                if embedding is not None:
                    # Проверяем размерность
                    if len(embedding) != self._vector_size:
                        logger.warning(
                            f"Embedding dimension mismatch: "
                            f"got {len(embedding)}, "
                            f"expected {self._vector_size}"
                        )
                        return None
                    return embedding
        except ImportError:
            logger.debug("LocalEmbeddings not available")
        except Exception as e:
            logger.debug(f"LocalEmbeddings failed: {e}")

        # === Попытка 2: Прямой HTTP запрос к Ollama ===
        if not self._http_client:
            logger.error("HTTP client not available for Ollama fallback")
            self._stats["embedding_failures"] += 1
            return None

        endpoints = [
            (f"{self.ollama_host}/api/embed", True),
            (f"{self.ollama_host}/api/embeddings", False),
        ]

        for endpoint, use_input in endpoints:
            try:
                payload = (
                    {"model": self.embedding_model, "input": text}
                    if use_input
                    else {"model": self.embedding_model, "prompt": text}
                )
                response = await self._http_client.post(endpoint, json=payload)
                response.raise_for_status()
                data = response.json()

                embedding = None
                if "embeddings" in data and data["embeddings"]:
                    embedding = data["embeddings"][0]
                elif "embedding" in data:
                    embedding = data["embedding"]

                if embedding:
                    if len(embedding) != self._vector_size:
                        logger.warning(
                            f"Ollama embedding dimension mismatch: "
                            f"got {len(embedding)}, "
                            f"expected {self._vector_size}"
                        )
                        continue
                    return embedding

            except httpx.HTTPStatusError as e:
                if e.response.status_code == 404:
                    continue
                logger.error(f"HTTP error from {endpoint}: {e}")
                break
            except httpx.ConnectError:
                logger.warning(
                    f"Cannot connect to Ollama for embeddings. "
                    f"Make sure Ollama is running: "
                    f"ollama pull {self.embedding_model}"
                )
                break
            except Exception as e:
                logger.debug(f"Failed to get embedding from {endpoint}: {e}")
                continue

        self._stats["embedding_failures"] += 1
        logger.error(f"All embedding methods failed for text: {text[:50]}...")
        return None

    def _generate_point_id(self, text: str, metadata: Dict) -> str:
        """Генерирует уникальный ID для точки."""
        content = f"{text}_{json.dumps(metadata, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()

    def _chunk_text(self, text: str, chunk_type: str = "documentation") -> List[str]:
        """Разбивает текст на чанки."""
        chunk_size = self.CHUNK_SIZES.get(chunk_type, 500)
        overlap = chunk_size // 4

        sentences = text.replace("\n", " ").split(". ")
        chunks = []
        current_chunk = []
        current_length = 0

        for sentence in sentences:
            sentence = sentence.strip() + ". "
            sentence_length = len(sentence)

            if current_length + sentence_length > chunk_size and current_chunk:
                chunks.append("".join(current_chunk))

                overlap_text = "".join(current_chunk)
                if len(overlap_text) > overlap:
                    current_chunk = [overlap_text[-overlap:]]
                    current_length = overlap
                else:
                    current_chunk = []
                    current_length = 0

            current_chunk.append(sentence)
            current_length += sentence_length

        if current_chunk:
            chunks.append("".join(current_chunk))

        return chunks

    async def add_document(
        self,
        text: str,
        metadata: Dict[str, Any],
        chunk_type: str = "documentation",
    ) -> int:
        """
        Добавляет документ в векторную базу.
        ИСПРАВЛЕНО (v4.0 — проблема #40): логирование пропущенных чанков.
        """
        if not self._initialized:
            logger.warning("RAG Engine not initialized")
            return 0

        if not text or not text.strip():
            logger.warning("Empty text provided to add_document")
            return 0

        chunks = self._chunk_text(text, chunk_type)
        points = []
        skipped_chunks = 0  # ИСПРАВЛЕНО: счётчик пропущенных чанков

        for i, chunk in enumerate(chunks):
            embedding = await self._get_embedding(chunk)

            if not embedding:
                # ИСПРАВЛЕНО: логируем пропущенные чанки
                skipped_chunks += 1
                logger.debug(
                    f"Skipped chunk {i + 1}/{len(chunks)} (embedding generation failed)"
                )
                continue

            chunk_metadata = {
                **metadata,
                "chunk_index": i,
                "total_chunks": len(chunks),
                "chunk_type": chunk_type,
                "added_at": datetime.now().isoformat(),
            }

            point_id = self._generate_point_id(chunk, chunk_metadata)

            points.append(
                PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={"text": chunk, **chunk_metadata},
                )
            )

        # ИСПРАВЛЕНО: логируем статистику пропущенных чанков
        if skipped_chunks > 0:
            logger.warning(
                f"⚠️ Skipped {skipped_chunks}/{len(chunks)} chunks "
                f"from {metadata.get('source', 'unknown')} "
                f"(embedding generation failed)"
            )

        if not points:
            logger.warning(
                f"No embeddings generated for document: "
                f"{metadata.get('source', 'unknown')}"
            )
            return 0

        try:
            # Поддержка обоих API Qdrant
            try:
                await self._client.upsert(
                    collection_name=self.collection_name,
                    points=points,
                )
            except TypeError:
                await self._client.upsert(
                    collection_name=self.collection_name,
                    points=points,
                    wait=True,
                )

            self._stats["documents_added"] += 1
            self._stats["chunks_added"] += len(points)

            logger.info(
                f"✅ Added {len(points)}/{len(chunks)} chunks from "
                f"{metadata.get('source', 'unknown')}"
            )
            return len(points)

        except Exception as e:
            logger.error(f"Failed to add document to Qdrant: {e}")
            return 0

    async def add_session_digest(self, session_data: Dict[str, Any]) -> int:
        """Добавляет Session_Digest в базу знаний."""
        digest_text = f"""
Сессия {session_data.get("date")}: {session_data.get("target")}
Параметры: Фильтр {session_data.get("filter")},
Экспозиция {session_data.get("exposure_time")}s,
Gain {session_data.get("gain")}, Температура {session_data.get("temperature")}°C
Результаты: Отснято {session_data.get("frames_total")} кадров,
принято {session_data.get("frames_accepted")}
Средний HFR: {session_data.get("avg_hfr")}px,
RMS: {session_data.get("avg_rms_ra")}" (RA), {session_data.get("avg_rms_dec")}" (Dec)
"""

        problems = session_data.get("problems", [])
        if problems:
            digest_text += "\nПроблемы и решения:\n"
            for p in problems:
                digest_text += (
                    f"- {p.get('time')}: {p.get('issue')} → {p.get('solution')}\n"
                )

        recommendations = session_data.get("recommendations", [])
        if recommendations:
            digest_text += "\nРекомендации:\n"
            for r in recommendations:
                digest_text += f"- {r}\n"

        detailed_report = session_data.get("detailed_report")
        if detailed_report:
            digest_text += f"\nДетальный анализ:\n{detailed_report}\n"

        metadata = {
            "source": "session_digest",
            "session_id": session_data.get("session_id"),
            "target": session_data.get("target"),
            "date": session_data.get("date"),
            "filter": session_data.get("filter"),
            "temperature": session_data.get("temperature"),
            "quality_score": session_data.get("quality_score"),
        }

        return await self.add_document(digest_text, metadata, chunk_type="session")

    async def search(
        self,
        query: str,
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Семантический поиск по базе знаний.
        ИСПРАВЛЕНО (v4.0 — проблема #41): логирование ошибок поиска.
        """
        if not self._initialized:
            return []

        if not query or not query.strip():
            return []

        self._stats["searches_performed"] += 1

        # Получаем эмбеддинг запроса (с кэшированием)
        query_embedding = await self._get_embedding(query)

        if not query_embedding:
            # ИСПРАВЛЕНО: логируем ошибку генерации эмбеддинга
            logger.warning(
                f"⚠️ RAG search failed: could not generate embedding "
                f"for query: {query[:50]}..."
            )
            return []

        # Строим фильтр
        query_filter = None
        if filters:
            conditions = []
            for key, value in filters.items():
                if value is not None:
                    conditions.append(
                        FieldCondition(key=key, match=MatchValue(value=value))
                    )
            if conditions:
                query_filter = Filter(must=conditions)

        try:
            # Поддержка обоих API qdrant-client
            results = []

            try:
                # Новый API (>= 1.7.0)
                response = await self._client.query_points(
                    collection_name=self.collection_name,
                    query=query_embedding,
                    limit=top_k,
                    query_filter=query_filter,
                    with_payload=True,
                )
                results = response.points if hasattr(response, "points") else []
            except AttributeError:
                # Старый API
                results = await self._client.search(
                    collection_name=self.collection_name,
                    query_vector=query_embedding,
                    limit=top_k,
                    query_filter=query_filter,
                )

            # Форматируем результаты
            formatted_results = []
            for result in results:
                payload = result.payload if hasattr(result, "payload") else {}
                payload = payload or {}
                formatted_results.append(
                    {
                        "text": payload.get("text", ""),
                        "score": (result.score if hasattr(result, "score") else 0.0),
                        "metadata": {k: v for k, v in payload.items() if k != "text"},
                    }
                )

            return formatted_results

        except Exception as e:
            # ИСПРАВЛЕНО: логируем ошибку поиска
            logger.error(
                f"❌ RAG search failed for query '{query[:50]}...': "
                f"{type(e).__name__}: {e}"
            )
            return []

    async def get_context(
        self,
        query: str,
        max_tokens: int = 2000,
        filters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Получает контекст для LLM на основе запроса."""
        results = await self.search(query, top_k=10, filters=filters)

        if not results:
            return "Контекст не найден в базе знаний."

        context_parts = []
        current_length = 0

        for result in results:
            text = result["text"]
            score = result["score"]
            metadata = result["metadata"]

            source = metadata.get("source", "unknown")
            target = metadata.get("target", "")
            date = metadata.get("date", "")

            header = f"[Источник: {source}"
            if target:
                header += f", Цель: {target}"
            if date:
                header += f", Дата: {date}"
            header += f", Релевантность: {score:.2f}]\n"

            chunk = f"{header}{text}\n"

            if current_length + len(chunk) > max_tokens * 4:
                break

            context_parts.append(chunk)
            current_length += len(chunk)

        return "\n".join(context_parts)

    async def _on_session_completed(self, data: Dict[str, Any]):
        """Обработчик события завершения сессии."""
        try:
            session_digest = {
                "session_id": data.get("session_id"),
                "target": data.get("target_name"),
                "date": datetime.now().strftime("%Y-%m-%d"),
                "filter": data.get("filter"),
                "exposure_time": data.get("exposure_time"),
                "gain": data.get("gain"),
                "temperature": data.get("temperature"),
                "frames_total": data.get("frames_total", 0),
                "frames_accepted": data.get("frames_accepted", 0),
                "avg_hfr": data.get("avg_hfr"),
                "avg_rms_ra": data.get("avg_rms_ra"),
                "avg_rms_dec": data.get("avg_rms_dec"),
                "problems": data.get("problems", []),
                "recommendations": data.get("recommendations", []),
                "quality_score": data.get("quality_score"),
                "detailed_report": data.get("detailed_report"),
            }
            await self.add_session_digest(session_digest)
            logger.info(f"Session {session_digest['session_id']} indexed in RAG")
        except Exception as e:
            logger.error(f"Failed to index session in RAG: {e}")

    async def _on_night_summary(self, data: Dict[str, Any]):
        """Обработчик события Night Summary."""
        try:
            summary_text = json.dumps(data, indent=2, ensure_ascii=False)
            metadata = {
                "source": "night_summary",
                "session_id": data.get("session_id"),
                "date": datetime.now().strftime("%Y-%m-%d"),
            }
            await self.add_document(summary_text, metadata, chunk_type="session")
        except Exception as e:
            logger.error(f"Failed to index night summary in RAG: {e}")

    async def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает полную статистику RAG-системы.

        Включает:
        - Статистику коллекции Qdrant
        - Статистику эмбеддингов (успешные/неуспешные)
        - Статистику LRU-кэша (hit rate, размер, evictions)
        - Информацию о бэкенде и размерности
        """
        if not self._initialized:
            return {
                "status": "not_initialized",
                "backend": self._embedding_backend,
            }

        # Статистика от Qdrant
        try:
            collection_info = await self._client.get_collection(self.collection_name)
            qdrant_stats = {
                "points_count": collection_info.points_count,
                "vectors_count": collection_info.vectors_count,
            }
        except Exception as e:
            qdrant_stats = {"error": str(e)}

        # Статистика кэша эмбеддингов
        cache_stats = self._embedding_cache.get_stats()

        # Hit rate для эмбеддингов (с учётом кэша)
        total_embedding_requests = (
            self._stats["embedding_calls"] + self._stats["embedding_cache_hits"]
        )
        embedding_hit_rate = round(
            self._stats["embedding_cache_hits"]
            / max(total_embedding_requests, 1)
            * 100,
            2,
        )

        return {
            "status": "active",
            "collection": self.collection_name,
            **qdrant_stats,
            "embedding_backend": self._embedding_backend,
            "embedding_model": (
                "sentence-transformers/all-MiniLM-L6-v2"
                if self._embedding_backend == "local"
                else self.embedding_model
            ),
            "vector_dimension": self._vector_size,
            "operations": self._stats,
            "embedding_cache": cache_stats,
            "embedding_effective_hit_rate_percent": embedding_hit_rate,
            "cache_config": {
                "max_size": self.EMBEDDING_CACHE_MAX_SIZE,
                "enabled": True,
            },
        }


# Singleton instance
rag_engine = RAGEngine()
