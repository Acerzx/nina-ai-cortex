"""
RAG Engine (Retrieval-Augmented Generation)
Предоставляет AI-агентам доступ к документации и истории сессий через векторный поиск.
Устраняет Упрощение #18.
"""

import asyncio
import logging
import hashlib
import json
from typing import List, Dict, Any, Optional
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
    PointIdsList,
)

from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("RAGEngine")


class RAGEngine:
    """
    RAG-система для предоставления AI-агентам контекста из:
    1. Документации N.I.N.A. и плагинов
    2. Истории сессий (Session_Digest.md)
    3. Логов ошибок и решений

    Архитектура:
    - Qdrant для хранения векторов и метаданных
    - Ollama (nomic-embed-text) для генерации embeddings
    - Автоматическое пополнение через EventBus
    """

    # Размеры чанков для разных типов документов
    CHUNK_SIZES = {
        "documentation": 1000,  # 1000 символов для документации
        "session": 500,  # 500 символов для сессий
        "error_log": 300,  # 300 символов для логов ошибок
    }

    def __init__(self):
        self.qdrant_url = settings.qdrant.url
        self.collection_name = settings.qdrant.collection_name
        self.embedding_model = settings.qdrant.embedding_model
        self.ollama_host = settings.ai_settings.ollama_host

        self._client: Optional[AsyncQdrantClient] = None
        self._http_client: Optional[httpx.AsyncClient] = None
        self._initialized = False
        self._vector_size = 768  # nomic-embed-text возвращает 764-dim векторы

    async def initialize(self):
        """Инициализирует подключения к Qdrant и Ollama."""
        if self._initialized:
            return

        try:
            # 1. Подключение к Qdrant
            self._client = AsyncQdrantClient(url=self.qdrant_url)

            # Проверяем существование коллекции
            collections = await self._client.get_collections()
            collection_names = [c.name for c in collections.collections]

            if self.collection_name not in collection_names:
                logger.info(f"Creating Qdrant collection: {self.collection_name}")
                await self._client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self._vector_size, distance=Distance.COSINE
                    ),
                )

            # 2. HTTP клиент для Ollama
            self._http_client = httpx.AsyncClient(timeout=30.0)

            # 3. Подписка на события для автоматического пополнения
            event_bus.subscribe("SESSION_COMPLETED", self._on_session_completed)
            event_bus.subscribe("NIGHT_SUMMARY", self._on_night_summary)

            self._initialized = True
            logger.info(
                f"✅ RAG Engine initialized (Qdrant: {self.qdrant_url}, Model: {self.embedding_model})"
            )

        except Exception as e:
            logger.error(f"❌ Failed to initialize RAG Engine: {e}")
            # Graceful degradation: система работает без RAG

    async def close(self):
        """Закрывает подключения."""
        # Закрываем HTTP клиент (Ollama)
        if self._http_client:
            try:
                await self._http_client.aclose()
                logger.debug("RAG HTTP client closed")
            except Exception as e:
                logger.debug(f"Error closing RAG HTTP client: {e}")
            finally:
                self._http_client = None

        # Закрываем Qdrant клиент (использует aiohttp внутри)
        if self._client:
            try:
                await self._client.close()
                logger.debug("RAG Qdrant client closed")
            except Exception as e:
                logger.debug(f"Error closing RAG Qdrant client: {e}")
            finally:
                self._client = None

        # Отписываемся от событий
        try:
            event_bus.unsubscribe("SESSION_COMPLETED", self._on_session_completed)
            event_bus.unsubscribe("NIGHT_SUMMARY", self._on_night_summary)
        except Exception:
            pass

        logger.info("🛑 RAG Engine closed")

    async def _get_embedding(self, text: str) -> Optional[List[float]]:
        """
        Получает embedding вектор для текста через Ollama.
        Использует модель nomic-embed-text (768 dimensions).
        """
        if not self._http_client:
            return None

        try:
            response = await self._http_client.post(
                f"{self.ollama_host}/api/embeddings",
                json={"model": self.embedding_model, "prompt": text},
            )
            response.raise_for_status()
            data = response.json()
            return data.get("embedding")
        except httpx.ConnectError:
            logger.warning("Ollama not available for embeddings")
            return None
        except Exception as e:
            logger.error(f"Failed to get embedding: {e}")
            return None

    def _generate_point_id(self, text: str, metadata: Dict) -> str:
        """Генерирует уникальный ID для точки на основе текста и метаданных."""
        content = f"{text}_{json.dumps(metadata, sort_keys=True)}"
        return hashlib.md5(content.encode()).hexdigest()

    def _chunk_text(self, text: str, chunk_type: str = "documentation") -> List[str]:
        """
        Разбивает текст на чанки с учетом типа документа.
        Использует простой подход: разбиение по предложениям с перекрытием.
        """
        chunk_size = self.CHUNK_SIZES.get(chunk_type, 500)
        overlap = chunk_size // 4  # 25% перекрытие

        # Разбиение по предложениям
        sentences = text.replace("\n", " ").split(". ")

        chunks = []
        current_chunk = []
        current_length = 0

        for sentence in sentences:
            sentence = sentence.strip() + ". "
            sentence_length = len(sentence)

            if current_length + sentence_length > chunk_size and current_chunk:
                # Сохраняем текущий чанк
                chunks.append("".join(current_chunk))

                # Оставляем перекрытие
                overlap_text = "".join(current_chunk)
                if len(overlap_text) > overlap:
                    # Берем последние N символов для перекрытия
                    current_chunk = [overlap_text[-overlap:]]
                    current_length = overlap
                else:
                    current_chunk = []
                    current_length = 0

            current_chunk.append(sentence)
            current_length += sentence_length

        # Добавляем последний чанк
        if current_chunk:
            chunks.append("".join(current_chunk))

        return chunks

    async def add_document(
        self, text: str, metadata: Dict[str, Any], chunk_type: str = "documentation"
    ) -> int:
        """
        Добавляет документ в векторную базу.
        Автоматически разбивает на чанки и векторизует.

        Returns:
            Количество добавленных точек
        """
        if not self._initialized:
            logger.warning("RAG Engine not initialized")
            return 0

        chunks = self._chunk_text(text, chunk_type)
        points = []

        for i, chunk in enumerate(chunks):
            embedding = await self._get_embedding(chunk)
            if not embedding:
                continue

            # Уникальный ID для каждого чанка
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

        if not points:
            logger.warning(
                f"No embeddings generated for document: {metadata.get('source', 'unknown')}"
            )
            return 0

        try:
            # Batch upsert в Qdrant
            await self._client.upsert(
                collection_name=self.collection_name, points=points
            )
            logger.info(
                f"✅ Added {len(points)} chunks from {metadata.get('source', 'unknown')}"
            )
            return len(points)
        except Exception as e:
            logger.error(f"Failed to add document to Qdrant: {e}")
            return 0

    async def add_session_digest(self, session_data: Dict[str, Any]) -> int:
        """
        Добавляет Session_Digest в базу знаний.
        Автоматически вызывается при завершении сессии.

        Формат session_data:
        {
            "session_id": "M31_2026-07-06",
            "target": "M31",
            "date": "2026-07-06",
            "filter": "SV220_Ha-Oiii_7nm",
            "exposure_time": 60,
            "gain": 85,
            "temperature": -15,
            "frames_total": 45,
            "frames_accepted": 42,
            "avg_hfr": 2.1,
            "avg_rms_ra": 0.8,
            "avg_rms_dec": 0.9,
            "problems": [
                {"time": "03:15", "issue": "Ветер 12 м/с", "solution": "Переключились на M42"}
            ],
            "recommendations": ["Оптимальная экспозиция 60-90s при Луне < 50%"]
        }
        """
        # Генерируем текстовое представление для векторизации
        digest_text = f"""
Сессия {session_data.get("date")}: {session_data.get("target")}
Параметры: Фильтр {session_data.get("filter")}, Экспозиция {session_data.get("exposure_time")}s, 
Gain {session_data.get("gain")}, Температура {session_data.get("temperature")}°C
Результаты: Отснято {session_data.get("frames_total")} кадров, принято {session_data.get("frames_accepted")}
Средний HFR: {session_data.get("avg_hfr")}px, RMS: {session_data.get("avg_rms_ra")}" (RA), {session_data.get("avg_rms_dec")}" (Dec)
"""

        # Добавляем проблемы и решения
        problems = session_data.get("problems", [])
        if problems:
            digest_text += "\nПроблемы и решения:\n"
            for p in problems:
                digest_text += (
                    f"- {p.get('time')}: {p.get('issue')} → {p.get('solution')}\n"
                )

        # Добавляем рекомендации
        recommendations = session_data.get("recommendations", [])
        if recommendations:
            digest_text += "\nРекомендации:\n"
            for r in recommendations:
                digest_text += f"- {r}\n"

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
        self, query: str, top_k: int = 5, filters: Optional[Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """
        Семантический поиск по базе знаний.

        Args:
            query: Поисковый запрос
            top_k: Количество результатов
            filters: Дополнительные фильтры (например, {"target": "M31"})

        Returns:
            Список найденных документов с метаданными
        """
        if not self._initialized:
            return []

        # Получаем embedding для запроса
        query_embedding = await self._get_embedding(query)
        if not query_embedding:
            return []

        # Строим фильтр
        query_filter = None
        if filters:
            conditions = []
            for key, value in filters.items():
                conditions.append(
                    FieldCondition(key=key, match=MatchValue(value=value))
                )
            if conditions:
                query_filter = Filter(must=conditions)

        try:
            # Поиск в Qdrant
            results = await self._client.search(
                collection_name=self.collection_name,
                query_vector=query_embedding,
                limit=top_k,
                query_filter=query_filter,
            )

            # Форматируем результаты
            formatted_results = []
            for result in results:
                formatted_results.append(
                    {
                        "text": result.payload.get("text", ""),
                        "score": result.score,
                        "metadata": {
                            k: v for k, v in result.payload.items() if k != "text"
                        },
                    }
                )

            return formatted_results

        except Exception as e:
            logger.error(f"RAG search failed: {e}")
            return []

    async def get_context(
        self,
        query: str,
        max_tokens: int = 2000,
        filters: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Получает контекст для LLM на основе запроса.
        Используется AI-агентами для принятия решений.

        Returns:
            Строка с релевантным контекстом
        """
        results = await self.search(query, top_k=10, filters=filters)

        if not results:
            return "Контекст не найден в базе знаний."

        # Собираем контекст, не превышая max_tokens
        context_parts = []
        current_length = 0

        for result in results:
            text = result["text"]
            score = result["score"]
            metadata = result["metadata"]

            # Форматируем с метаданными
            source = metadata.get("source", "unknown")
            target = metadata.get("target", "")
            date = metadata.get("date", "")

            header = f"[Источник: {source}"
            if target:
                header += f", Цель: {target}"
            if date:
                header += f", Дата: {date}"
            header += f", Релевантность: {score:.2f}]\n"

            chunk = f"{header}{text}\n\n"

            # Проверка лимита токенов (примерно 4 символа на токен)
            if current_length + len(chunk) > max_tokens * 4:
                break

            context_parts.append(chunk)
            current_length += len(chunk)

        return "\n".join(context_parts)

    async def _on_session_completed(self, data: Dict[str, Any]):
        """Обработчик события завершения сессии."""
        try:
            # Генерируем Session_Digest из данных сессии
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
            }

            await self.add_session_digest(session_digest)
            logger.info(f"Session {session_digest['session_id']} indexed in RAG")

        except Exception as e:
            logger.error(f"Failed to index session in RAG: {e}")

    async def _on_night_summary(self, data: Dict[str, Any]):
        """Обработчик события Night Summary."""
        try:
            # Извлекаем ключевую информацию из Night Summary
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
        """Возвращает статистику RAG-базы."""
        if not self._initialized:
            return {"status": "not_initialized"}

        try:
            collection_info = await self._client.get_collection(self.collection_name)
            return {
                "status": "active",
                "collection": self.collection_name,
                "points_count": collection_info.points_count,
                "vectors_count": collection_info.vectors_count,
                "embedding_model": self.embedding_model,
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}


# Singleton instance
rag_engine = RAGEngine()
