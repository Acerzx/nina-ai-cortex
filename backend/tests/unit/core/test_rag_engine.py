"""
Unit tests for RAG Engine.
Тестирует векторный поиск, кэширование эмбеддингов и интеграцию с Qdrant.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pathlib import Path
import asyncio

from app.core.rag_engine import RAGEngine


class TestRAGEngine:
    """Тесты RAG Engine."""

    @pytest.fixture
    async def rag_engine(self):
        """Создаёт изолированный RAG Engine для тестов."""
        engine = RAGEngine()

        # Мокаем зависимости
        with patch("app.core.rag_engine.AsyncQdrantClient") as mock_qdrant:
            mock_client = AsyncMock()
            mock_qdrant.return_value = mock_client

            # Мокаем collections
            mock_collections = MagicMock()
            mock_collections.collections = []
            mock_client.get_collections = AsyncMock(return_value=mock_collections)
            mock_client.create_collection = AsyncMock()

            # Мокаем embeddings
            with patch("app.core.rag_engine.local_embeddings") as mock_embeddings:
                mock_embeddings._initialized = True
                mock_embeddings.embed = AsyncMock(return_value=[0.1] * 768)

                await engine.initialize()
                yield engine

                await engine.close()

    @pytest.mark.asyncio
    async def test_rag_engine_initialization(self, rag_engine):
        """Тест инициализации RAG Engine."""
        assert rag_engine._initialized is True
        assert rag_engine._client is not None

    @pytest.mark.asyncio
    async def test_embedding_cache_hit(self, rag_engine):
        """Тест кэширования эмбеддингов."""
        text = "Test text for embedding"

        # Первый вызов должен вычислить эмбеддинг
        with patch("app.core.rag_engine.local_embeddings") as mock_embeddings:
            mock_embeddings.embed = AsyncMock(return_value=[0.1] * 768)

            embedding1 = await rag_engine._get_embedding(text)
            assert embedding1 is not None

            # Второй вызов должен использовать кэш
            embedding2 = await rag_engine._get_embedding(text)
            assert embedding2 == embedding1

            # Проверяем статистику кэша
            cache_stats = rag_engine._embedding_cache.get_stats()
            assert cache_stats["hits"] >= 1

    @pytest.mark.asyncio
    async def test_add_document_to_qdrant(self, rag_engine):
        """Тест добавления документа в Qdrant."""
        text = "Test document content"
        metadata = {"source": "test", "session_id": "test_session"}

        with patch.object(rag_engine._client, "upsert") as mock_upsert:
            mock_upsert.return_value = True

            chunks_added = await rag_engine.add_document(text, metadata)

            assert chunks_added > 0
            mock_upsert.assert_called_once()

    @pytest.mark.asyncio
    async def test_search_in_qdrant(self, rag_engine):
        """Тест поиска в Qdrant."""
        query = "test query"

        # Мокаем результаты поиска
        mock_result = MagicMock()
        mock_result.payload = {"text": "result text", "source": "test"}
        mock_result.score = 0.95

        with patch.object(rag_engine._client, "query_points") as mock_query:
            mock_response = MagicMock()
            mock_response.points = [mock_result]
            mock_query.return_value = mock_response

            results = await rag_engine.search(query, top_k=5)

            assert len(results) > 0
            assert results[0]["text"] == "result text"
            assert results[0]["score"] == 0.95

    @pytest.mark.asyncio
    async def test_get_context_for_llm(self, rag_engine):
        """Тест получения контекста для LLM."""
        query = "test query"

        with patch.object(rag_engine, "search") as mock_search:
            mock_search.return_value = [
                {
                    "text": "Context 1",
                    "score": 0.9,
                    "metadata": {"source": "session_digest"},
                },
                {
                    "text": "Context 2",
                    "score": 0.8,
                    "metadata": {"source": "documentation"},
                },
            ]

            context = await rag_engine.get_context(query, max_tokens=2000)

            assert "Context 1" in context
            assert "Context 2" in context
            assert "session_digest" in context

    @pytest.mark.asyncio
    async def test_add_session_digest(self, rag_engine):
        """Тест добавления Session Digest."""
        session_data = {
            "session_id": "test_session",
            "target": "M31",
            "date": "2026-07-08",
            "filter": "Ha",
            "exposure_time": 60.0,
            "frames_total": 100,
            "frames_accepted": 95,
            "avg_hfr": 2.5,
            "quality_score": 8.5,
        }

        with patch.object(rag_engine, "add_document") as mock_add:
            mock_add.return_value = 5

            chunks = await rag_engine.add_session_digest(session_data)

            assert chunks == 5
            mock_add.assert_called_once()

            # Проверяем, что документ содержит правильные данные
            call_args = mock_add.call_args
            text = call_args[0][0]
            metadata = call_args[0][1]

            assert "M31" in text
            assert "Ha" in text
            assert metadata["session_id"] == "test_session"
            assert metadata["source"] == "session_digest"

    @pytest.mark.asyncio
    async def test_chunk_text(self, rag_engine):
        """Тест разбиения текста на чанки."""
        long_text = " ".join([f"Sentence {i}." for i in range(100)])

        chunks = rag_engine._chunk_text(long_text, chunk_type="documentation")

        assert len(chunks) > 1
        assert all(len(chunk) > 0 for chunk in chunks)

    @pytest.mark.asyncio
    async def test_embedding_cache_eviction(self, rag_engine):
        """Тест вытеснения старых записей из кэша."""
        # Устанавливаем маленький размер кэша для теста
        rag_engine._embedding_cache.max_size = 5

        with patch("app.core.rag_engine.local_embeddings") as mock_embeddings:
            # Генерируем больше записей, чем размер кэша
            for i in range(10):
                mock_embeddings.embed = AsyncMock(return_value=[0.1 * i] * 768)
                await rag_engine._get_embedding(f"Text {i}")

            # Проверяем, что размер кэша не превышает лимит
            cache_stats = rag_engine._embedding_cache.get_stats()
            assert cache_stats["size"] <= 5
            assert cache_stats["evictions"] > 0

    @pytest.mark.asyncio
    async def test_get_stats(self, rag_engine):
        """Тест получения статистики RAG Engine."""
        stats = await rag_engine.get_stats()

        assert "status" in stats
        assert stats["status"] == "active"
        assert "embedding_cache" in stats
        assert "embedding_backend" in stats

    @pytest.mark.asyncio
    async def test_close_rag_engine(self):
        """Тест корректного закрытия RAG Engine."""
        engine = RAGEngine()

        with patch("app.core.rag_engine.AsyncQdrantClient") as mock_qdrant:
            mock_client = AsyncMock()
            mock_qdrant.return_value = mock_client
            mock_client.close = AsyncMock()

            with patch("app.core.rag_engine.local_embeddings"):
                await engine.initialize()
                await engine.close()

                assert engine._initialized is False
                mock_client.close.assert_called_once()
