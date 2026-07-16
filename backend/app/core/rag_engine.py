"""
RAG Engine (Retrieval-Augmented Generation)
Предоставляет AI-агентам доступ к документации и истории сессий через векторный поиск.
Архитектура embeddings (гибридный подход):
1. Primary: sentence-transformers (локально, быстро, оффлайн)
2. Fallback: Ollama (nomic-embed-text) через HTTP
Автоматический fallback обеспечивает работоспособность RAG даже если
sentence-transformers не установлен (например, на Python 3.14).
ИСПРАВЛЕНО (Этап 1.1):
- EmbeddingCache заменён на AsyncTTLCache (cachetools wrapper)
- Упрощение с 80+ строк до 30 строк
- Battle-tested алгоритмы вместо собственной реализации
ИСПРАВЛЕНО (Спринт 5 — Фаза 2):
- Добавлены OpenTelemetry spans для RAG операций
- Parent span `rag.search` с атрибутами (query_length, top_k, results_count)
- Child span `rag.compute_embedding` для вычисления эмбеддингов
- Parent span `rag.add_document` для индексации документов
"""

import asyncio
import logging
import hashlib
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from pathlib import Path
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    Filter,
    FieldCondition,
    MatchValue,
)
from app.core.http_client import http_client_manager
from app.core.config import settings
from app.core.events import event_bus
from app.core.ttl_lru_cache import AsyncTTLCache

# Спринт 5: OpenTelemetry tracing
from app.core.tracing import tracing_manager, span_context

logger = logging.getLogger("RAGEngine")


class RAGEngine:
    """
    RAG-система для предоставления AI-агентам контекста из:
    1. Документации N.I.N.A. и плагинов
    2. Истории сессий (Session_Digest.md)
    3. Логов ошибок и решений
    ИСПРАВЛЕНО (Спринт 5 — Фаза 2):
    - OpenTelemetry spans для observability
    """

    CHUNK_SIZES = {
        "documentation": 1000,
        "session": 500,
        "error_log": 300,
    }

    def __init__(self):
        self.qdrant_url = settings.qdrant.url
        self.collection_name = settings.qdrant.collection_name
        self.embedding_model = settings.qdrant.embedding_model
        self.ollama_host = settings.ai_settings.ollama_host

        self._client: Optional[AsyncQdrantClient] = None
        self._initialized = False
        self._vector_size = 384
        self._embedding_backend: str = "unknown"

        # ИСПРАВЛЕНО (Этап 1.1): Используем AsyncTTLCache вместо EmbeddingCache
        rag_cfg = getattr(settings, "rag", None)
        cache_max_size = 10000
        if rag_cfg:
            cache_max_size = getattr(rag_cfg, "embedding_cache_max_size", 10000)

        self._embedding_cache = AsyncTTLCache(
            max_size=cache_max_size,
            ttl_seconds=3600,  # 1 час
        )

        self._stats = {
            "documents_added": 0,
            "chunks_added": 0,
            "searches_performed": 0,
            "embedding_calls": 0,
            "embedding_failures": 0,
            "embedding_cache_hits": 0,
        }

    async def initialize(self):
        """Инициализирует подключения к Qdrant и embeddings."""
        if self._initialized:
            return

        try:
            from app.core.embeddings import local_embeddings

            await local_embeddings.initialize()
            self._embedding_backend = local_embeddings.get_backend()
            self._vector_size = local_embeddings.get_dimension()

            if self._embedding_backend == "none":
                logger.warning(
                    "LocalEmbeddings unavailable. "
                    "Will use direct Ollama HTTP fallback (768 dim)."
                )
                self._embedding_backend = "ollama_direct"
                self._vector_size = 768

            self._client = AsyncQdrantClient(url=self.qdrant_url)

            collections = await self._client.get_collections()
            collection_names = [c.name for c in collections.collections]

            if self.collection_name in collection_names:
                collection_info = await self._client.get_collection(
                    self.collection_name
                )
                existing_size = collection_info.config.params.vectors.size
                if existing_size != self._vector_size:
                    logger.warning(
                        f"Collection {self.collection_name} has dimension "
                        f"{existing_size}, but embeddings produce "
                        f"{self._vector_size}. Recreating collection..."
                    )
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

            event_bus.subscribe("SESSION_COMPLETED", self._on_session_completed)
            event_bus.subscribe("NIGHT_SUMMARY", self._on_night_summary)

            self._initialized = True
            logger.info(
                f"RAG Engine initialized "
                f"(Qdrant: {self.qdrant_url}, "
                f"Backend: {self._embedding_backend}, "
                f"Dim: {self._vector_size}, "
                f"Cache: {self._embedding_cache.get_stats()['max_size']} entries, "
                f"TTL: {self._embedding_cache.get_stats()['ttl_seconds']}s)"
            )

        except Exception as e:
            logger.error(f"Failed to initialize RAG Engine: {e}")

    async def close(self):
        """
        Корректно закрывает все подключения и очищает кэш.
        ИСПРАВЛЕНО (С-15):
        - HTTP клиент больше не закрывается здесь (это делает http_client_manager)
        - Закрывается только Qdrant клиент и очищается кэш эмбеддингов
        """
        try:
            event_bus.unsubscribe("SESSION_COMPLETED", self._on_session_completed)
            event_bus.unsubscribe("NIGHT_SUMMARY", self._on_night_summary)
        except Exception:
            pass

        # ИСПРАВЛЕНО (С-15): HTTP клиент закрывается через менеджер при shutdown
        # Здесь только закрываем Qdrant и очищаем кэш
        if self._client:
            try:
                await self._client.close()
                logger.info("Qdrant client closed")
            except Exception as e:
                logger.debug(f"Error closing Qdrant client: {e}")
            finally:
                self._client = None

        # Очищаем кэш эмбеддингов
        cleared = await self._embedding_cache.clear()
        if cleared > 0:
            logger.debug(f"Cleared {cleared} entries from embedding cache")

        self._initialized = False
        logger.info("RAG Engine closed")

    async def _get_embedding(self, text: str) -> Optional[List[float]]:
        """Получает embedding вектор для текста с использованием кэша."""
        if not self._initialized:
            logger.warning("RAG Engine not initialized")
            return None

        # Проверяем кэш
        cached = await self._embedding_cache.get(text)
        if cached is not None:
            self._stats["embedding_cache_hits"] += 1
            return cached

        # Вычисляем новый embedding
        self._stats["embedding_calls"] += 1
        embedding = await self._compute_embedding(text)
        if embedding is not None:
            await self._embedding_cache.put(text, embedding)
        return embedding

    async def _compute_embedding(self, text: str) -> Optional[List[float]]:
        """
        Вычисляет эмбеддинг для текста (без кэширования).
        ИСПРАВЛЕНО (Спринт 5 — Фаза 2): OpenTelemetry span.
        """
        # Спринт 5: OpenTelemetry span
        async with span_context(
            "rag.compute_embedding",
            attributes={
                "rag.text_length": len(text),
                "rag.backend": self._embedding_backend,
                "rag.dimension": self._vector_size,
            },
        ) as span:
            # Попытка 1: Local embeddings
            try:
                from app.core.embeddings import local_embeddings

                if local_embeddings._initialized:
                    embedding = await local_embeddings.embed(text)
                    if embedding is not None:
                        if len(embedding) != self._vector_size:
                            logger.warning(
                                f"Embedding dimension mismatch: "
                                f"got {len(embedding)}, expected {self._vector_size}"
                            )
                            if span:
                                span.set_attribute("rag.status", "dimension_mismatch")
                            return None

                        if span:
                            span.set_attribute("rag.status", "success")
                            span.set_attribute("rag.backend_used", "local")

                        return embedding
            except ImportError:
                logger.debug("LocalEmbeddings not available")
            except Exception as e:
                logger.debug(f"LocalEmbeddings failed: {e}")
                if span:
                    span.set_attribute("rag.local_error", type(e).__name__)

            # Попытка 2: Ollama HTTP fallback
            # ИСПРАВЛЕНО (С-15): Получаем клиент через http_client_manager
            try:
                client = await http_client_manager.get_client(
                    base_url=self.ollama_host,
                    service="embeddings",
                )
            except Exception as e:
                logger.error(f"Failed to get HTTP client for Ollama fallback: {e}")
                self._stats["embedding_failures"] += 1
                if span:
                    span.set_attribute("rag.status", "http_client_error")
                    span.record_exception(e)
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
                    response = await client.post(endpoint, json=payload)
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
                                f"got {len(embedding)}, expected {self._vector_size}"
                            )
                            continue

                        if span:
                            span.set_attribute("rag.status", "success")
                            span.set_attribute("rag.backend_used", "ollama")
                            span.set_attribute("rag.endpoint", endpoint)

                        return embedding

                except httpx.HTTPStatusError as e:
                    if e.response.status_code == 404:
                        continue
                    logger.error(f"HTTP error from {endpoint}: {e}")
                    if span:
                        span.set_attribute("rag.http_error", e.response.status_code)
                    break
                except httpx.ConnectError:
                    logger.warning(
                        f"Cannot connect to Ollama for embeddings. "
                        f"Make sure Ollama is running: ollama pull {self.embedding_model}"
                    )
                    if span:
                        span.set_attribute("rag.status", "connection_error")
                    break
                except Exception as e:
                    logger.debug(f"Failed to get embedding from {endpoint}: {e}")
                    continue

            self._stats["embedding_failures"] += 1

            if span:
                span.set_attribute("rag.status", "all_methods_failed")

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
        ИСПРАВЛЕНО (Спринт 5 — Фаза 2): OpenTelemetry span.
        """
        if not self._initialized:
            logger.warning("RAG Engine not initialized")
            return 0

        if not text or not text.strip():
            logger.warning("Empty text provided to add_document")
            return 0

        # Спринт 5: OpenTelemetry span
        async with span_context(
            "rag.add_document",
            attributes={
                "rag.chunk_type": chunk_type,
                "rag.text_length": len(text),
                "rag.source": metadata.get("source", "unknown"),
                "rag.session_id": metadata.get("session_id", ""),
            },
        ) as span:
            chunks = self._chunk_text(text, chunk_type)
            points = []
            skipped_chunks = 0

            for i, chunk in enumerate(chunks):
                embedding = await self._get_embedding(chunk)
                if not embedding:
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

            if skipped_chunks > 0:
                logger.warning(
                    f"Skipped {skipped_chunks}/{len(chunks)} chunks "
                    f"from {metadata.get('source', 'unknown')} "
                    f"(embedding generation failed)"
                )

            if not points:
                logger.warning(
                    f"No embeddings generated for document: "
                    f"{metadata.get('source', 'unknown')}"
                )
                if span:
                    span.set_attribute("rag.status", "no_embeddings")
                    span.set_attribute("rag.chunks_count", 0)
                return 0

            try:
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

                if span:
                    span.set_attribute("rag.status", "success")
                    span.set_attribute("rag.chunks_count", len(points))
                    span.set_attribute("rag.skipped_chunks", skipped_chunks)

                logger.info(
                    f"Added {len(points)}/{len(chunks)} chunks from "
                    f"{metadata.get('source', 'unknown')}"
                )
                return len(points)

            except Exception as e:
                logger.error(f"Failed to add document to Qdrant: {e}")
                if span:
                    span.set_attribute("rag.status", "qdrant_error")
                    span.record_exception(e)
                return 0

    async def add_session_digest(self, session_data: Dict[str, Any]) -> int:
        """Добавляет Session_Digest в базу знаний."""
        date_str = session_data.get("date", "unknown")
        target_str = session_data.get("target", "unknown")
        filter_str = session_data.get("filter", "unknown")
        exposure_str = session_data.get("exposure_time", 0)
        gain_str = session_data.get("gain", 0)
        temp_str = session_data.get("temperature", 0)
        frames_total_str = session_data.get("frames_total", 0)
        frames_accepted_str = session_data.get("frames_accepted", 0)
        avg_hfr_str = session_data.get("avg_hfr", "N/A")
        avg_rms_ra_str = session_data.get("avg_rms_ra", "N/A")
        avg_rms_dec_str = session_data.get("avg_rms_dec", "N/A")

        digest_text = (
            f"Сессия {date_str}: {target_str}\n"
            f"Параметры: Фильтр {filter_str}, "
            f"Экспозиция {exposure_str}s, "
            f"Gain {gain_str}, Температура {temp_str} degC\n"
            f"Результаты: Отснято {frames_total_str} кадров, "
            f"принято {frames_accepted_str}\n"
            f"Средний HFR: {avg_hfr_str}px, "
            f'RMS: {avg_rms_ra_str}" (RA), {avg_rms_dec_str}" (Dec)\n'
        )

        problems = session_data.get("problems", [])
        if problems:
            digest_text += "\nПроблемы и решения:\n"
            for p in problems:
                time_str = p.get("time", "")
                issue_str = p.get("issue", "")
                solution_str = p.get("solution", "")
                digest_text += f"- {time_str}: {issue_str} -> {solution_str}\n"

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

    async def cleanup_old_documents(self, max_age_days: int = 365) -> int:
        """Удаляет старые документы из Qdrant."""
        if not self._initialized:
            logger.warning("RAG Engine not initialized")
            return 0

        cutoff_date = datetime.now() - timedelta(days=max_age_days)
        deleted_count = 0

        try:
            offset = None
            while True:
                response = await self._client.scroll(
                    collection_name=self.collection_name,
                    scroll_filter=None,
                    limit=100,
                    offset=offset,
                    with_payload=True,
                    with_vectors=False,
                )

                points = response[0]
                next_offset = response[1]

                if not points:
                    break

                old_point_ids = []
                for point in points:
                    payload = point.payload or {}
                    added_at = payload.get("added_at")
                    if added_at:
                        try:
                            point_date = datetime.fromisoformat(added_at)
                            if point_date < cutoff_date:
                                old_point_ids.append(point.id)
                        except (ValueError, TypeError):
                            pass

                if old_point_ids:
                    await self._client.delete(
                        collection_name=self.collection_name,
                        points_selector=old_point_ids,
                        wait=True,
                    )
                    deleted_count += len(old_point_ids)
                    logger.debug(
                        f"Deleted {len(old_point_ids)} old documents from Qdrant"
                    )

                if next_offset is None:
                    break
                offset = next_offset

            logger.info(
                f"RAG cleanup complete: {deleted_count} documents older than "
                f"{max_age_days} days deleted"
            )
            return deleted_count

        except Exception as e:
            logger.error(f"RAG cleanup failed: {e}")
            return deleted_count

    async def search(
        self,
        query: str,
        top_k: int = 5,
        filters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Семантический поиск по базе знаний.
        ИСПРАВЛЕНО (Спринт 5 — Фаза 2): OpenTelemetry span.
        """
        if not self._initialized:
            return []

        if not query or not query.strip():
            return []

        self._stats["searches_performed"] += 1

        # Спринт 5: OpenTelemetry span
        async with span_context(
            "rag.search",
            attributes={
                "rag.query_length": len(query),
                "rag.top_k": top_k,
                "rag.has_filters": filters is not None,
                "rag.filters_count": len(filters) if filters else 0,
            },
        ) as span:
            query_embedding = await self._get_embedding(query)
            if not query_embedding:
                logger.warning(
                    f"RAG search failed: could not generate embedding "
                    f"for query: {query[:50]}..."
                )
                if span:
                    span.set_attribute("rag.status", "embedding_failed")
                    span.set_attribute("rag.results_count", 0)
                return []

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
                results = []
                try:
                    response = await self._client.query_points(
                        collection_name=self.collection_name,
                        query=query_embedding,
                        limit=top_k,
                        query_filter=query_filter,
                        with_payload=True,
                    )
                    results = response.points if hasattr(response, "points") else []
                except AttributeError:
                    results = await self._client.search(
                        collection_name=self.collection_name,
                        query_vector=query_embedding,
                        limit=top_k,
                        query_filter=query_filter,
                    )

                formatted_results = []
                top_score = 0.0

                for result in results:
                    payload = result.payload if hasattr(result, "payload") else {}
                    payload = payload or {}
                    score = result.score if hasattr(result, "score") else 0.0

                    if score > top_score:
                        top_score = score

                    formatted_results.append(
                        {
                            "text": payload.get("text", ""),
                            "score": score,
                            "metadata": {
                                k: v for k, v in payload.items() if k != "text"
                            },
                        }
                    )

                # Спринт 5: Устанавливаем атрибуты span
                if span:
                    span.set_attribute("rag.status", "success")
                    span.set_attribute("rag.results_count", len(formatted_results))
                    span.set_attribute("rag.top_score", top_score)

                return formatted_results

            except Exception as e:
                logger.error(
                    f"RAG search failed for query '{query[:50]}...': "
                    f"{type(e).__name__}: {e}"
                )
                if span:
                    span.set_attribute("rag.status", "qdrant_error")
                    span.record_exception(e)
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
                "target_name": data.get("target_name"),
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
        """Возвращает полную статистику RAG-системы."""
        if not self._initialized:
            return {
                "status": "not_initialized",
                "backend": self._embedding_backend,
            }

        try:
            collection_info = await self._client.get_collection(self.collection_name)
            qdrant_stats = {
                "points_count": collection_info.points_count,
                "vectors_count": collection_info.vectors_count,
            }
        except Exception as e:
            qdrant_stats = {"error": str(e)}

        cache_stats = self._embedding_cache.get_stats()

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
                "max_size": cache_stats["max_size"],
                "ttl_seconds": cache_stats["ttl_seconds"],
                "enabled": True,
            },
        }


# Singleton instance
rag_engine = RAGEngine()
