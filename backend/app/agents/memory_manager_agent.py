"""
Memory Manager Agent — управление долгосрочной памятью и контекстом для всех агентов.
Обеспечивает согласованность контекста и очистку старых данных.

ИСПРАВЛЕНО (audit 5.1): Сохранение ссылки на cleanup задачу.
"""

import asyncio
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.rag_engine import rag_engine

logger = logging.getLogger("MemoryManagerAgent")


class MemoryEntry(BaseModel):
    """Запись в памяти."""

    key: str
    value: Any
    ttl_seconds: Optional[int] = None
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    expires_at: Optional[str] = None
    tags: List[str] = Field(default_factory=list)


class MemoryManagerAgent(BaseAgent):
    """
    Агент управления памятью и контекстом.

    Responsibilities:
    - Управление краткосрочной памятью (текущая сессия)
    - Управление среднесрочной памятью (последние 30 дней)
    - Управление долгосрочной памятью (вся история через RAG)
    - Очистка устаревших записей
    - Предоставление контекста другим агентам

    ИСПРАВЛЕНО (audit 5.1): Сохранение ссылки на cleanup задачу.
    """

    def __init__(self):
        super().__init__(name="MemoryManager", role="Context Management")

        # Краткосрочная память (текущая сессия)
        self._short_term: Dict[str, MemoryEntry] = {}

        # Среднесрочная память (последние 30 дней)
        self._medium_term: Dict[str, MemoryEntry] = {}

        # ИСПРАВЛЕНО (audit 5.1): Хранение ссылки на cleanup задачу
        self._cleanup_task: Optional[asyncio.Task] = None

    async def initialize(self):
        """Инициализация агента управления памятью."""
        await super().initialize()

        # Подписываемся на события
        event_bus.subscribe("SEQUENCE_STARTED", self._on_sequence_started)
        event_bus.subscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)

        # ИСПРАВЛЕНО (audit 5.1): Сохраняем ссылку на задачу
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

        logger.info("✅ Memory Manager Agent initialized")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("SEQUENCE_STARTED", self._on_sequence_started)
        event_bus.unsubscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)

        # ИСПРАВЛЕНО (audit 5.1): Корректная отмена cleanup задачи
        if self._cleanup_task:
            if not self._cleanup_task.done():
                self._cleanup_task.cancel()
                try:
                    await self._cleanup_task
                except asyncio.CancelledError:
                    pass
                logger.debug("Cleanup task cancelled")
            self._cleanup_task = None

        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """Memory Manager не принимает автономных решений."""
        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Memory Manager не выполняет автономных действий."""
        return False

    async def _on_sequence_started(self, data: Dict[str, Any]) -> None:
        """Обработка начала новой сессии - очистка краткосрочной памяти."""
        logger.info("🧠 New session started, clearing short-term memory")
        self._short_term.clear()

    async def _on_sequence_stopped(self, data: Dict[str, Any]) -> None:
        """Обработка завершения сессии."""
        logger.info("🧠 Session ended")

    async def _periodic_cleanup(self):
        """Периодическая очистка устаревших записей."""
        while True:
            try:
                await asyncio.sleep(3600)  # Каждый час
                now = datetime.now()

                # Очищаем краткосрочную память
                expired_short = [
                    key
                    for key, entry in self._short_term.items()
                    if entry.expires_at
                    and datetime.fromisoformat(entry.expires_at) < now
                ]
                for key in expired_short:
                    del self._short_term[key]

                # Очищаем среднесрочную память
                expired_medium = [
                    key
                    for key, entry in self._medium_term.items()
                    if entry.expires_at
                    and datetime.fromisoformat(entry.expires_at) < now
                ]
                for key in expired_medium:
                    del self._medium_term[key]

                if expired_short or expired_medium:
                    logger.debug(
                        f"Memory cleanup: {len(expired_short)} short-term, "
                        f"{len(expired_medium)} medium-term entries removed"
                    )

            except asyncio.CancelledError:
                logger.debug("Periodic cleanup cancelled")
                break
            except Exception as e:
                logger.error(f"Error in periodic cleanup: {e}")

    async def store_memory(
        self,
        key: str,
        value: Any,
        ttl: Optional[int] = None,
        memory_type: str = "short",
        tags: List[str] = None,
    ) -> bool:
        """
        Сохраняет запись в память.

        Args:
            key: Уникальный ключ
            value: Значение (любой тип)
            ttl: Время жизни в секундах (None = бесконечно)
            memory_type: "short", "medium", "long"
            tags: Теги для поиска
        """
        entry = MemoryEntry(key=key, value=value, ttl_seconds=ttl, tags=tags or [])

        if ttl:
            expires_at = datetime.now() + timedelta(seconds=ttl)
            entry.expires_at = expires_at.isoformat()

        if memory_type == "short":
            self._short_term[key] = entry
        elif memory_type == "medium":
            self._medium_term[key] = entry
        elif memory_type == "long":
            # Для долгосрочной памяти используем RAG
            try:
                await rag_engine.add_document(
                    text=str(value),
                    metadata={"key": key, "tags": tags or [], "memory_type": "long"},
                    chunk_type="session",
                )
                logger.debug(f"Stored in long-term memory (RAG): {key}")
                return True
            except Exception as e:
                logger.error(f"Failed to store in long-term memory: {e}")
                return False
        else:
            logger.warning(f"Unknown memory type: {memory_type}")
            return False

        logger.debug(f"Stored in {memory_type}-term memory: {key}")
        return True

    async def retrieve_memory(
        self, key: str, memory_type: str = "short"
    ) -> Optional[Any]:
        """
        Извлекает запись из памяти.

        Args:
            key: Ключ записи
            memory_type: "short", "medium", "long"

        Returns:
            Значение или None если не найдено
        """
        if memory_type == "short":
            entry = self._short_term.get(key)
        elif memory_type == "medium":
            entry = self._medium_term.get(key)
        elif memory_type == "long":
            # Ищем в RAG
            try:
                results = await rag_engine.search(
                    query=key, top_k=1, filters={"key": key}
                )
                if results:
                    return results[0]["text"]
                return None
            except Exception as e:
                logger.error(f"Failed to retrieve from long-term memory: {e}")
                return None
        else:
            return None

        if not entry:
            return None

        # Проверяем, не истек ли TTL
        if entry.expires_at:
            expires_at = datetime.fromisoformat(entry.expires_at)
            if expires_at < datetime.now():
                # Запись устарела, удаляем
                if memory_type == "short":
                    del self._short_term[key]
                elif memory_type == "medium":
                    del self._medium_term[key]
                return None

        return entry.value

    async def get_context(
        self, query: str, max_tokens: int = 2000, memory_types: List[str] = None
    ) -> str:
        """
        Получает контекст из всех типов памяти.

        Args:
            query: Поисковый запрос
            max_tokens: Максимальное количество токенов
            memory_types: Типы памяти для поиска (по умолчанию все)

        Returns:
            Строка с релевантным контекстом
        """
        if memory_types is None:
            memory_types = ["short", "medium", "long"]

        context_parts = []

        # 1. Краткосрочная память
        if "short" in memory_types:
            for key, entry in self._short_term.items():
                if query.lower() in key.lower() or any(
                    query.lower() in tag.lower() for tag in entry.tags
                ):
                    context_parts.append(f"[Short-term: {key}] {entry.value}")

        # 2. Среднесрочная память
        if "medium" in memory_types:
            for key, entry in self._medium_term.items():
                if query.lower() in key.lower() or any(
                    query.lower() in tag.lower() for tag in entry.tags
                ):
                    context_parts.append(f"[Medium-term: {key}] {entry.value}")

        # 3. Долгосрочная память (RAG)
        if "long" in memory_types:
            try:
                rag_context = await rag_engine.get_context(
                    query=query,
                    max_tokens=max_tokens // 2,  # Оставляем половину для RAG
                )
                if rag_context and rag_context != "Контекст не найден в базе знаний.":
                    context_parts.append(f"[Long-term RAG]\n{rag_context}")
            except Exception as e:
                logger.error(f"Failed to get RAG context: {e}")

        return "\n".join(context_parts) if context_parts else "Контекст не найден"

    async def prune_old_memories(self) -> int:
        """
        Очищает устаревшие записи.

        Returns:
            Количество удаленных записей
        """
        now = datetime.now()
        removed = 0

        # Краткосрочная память
        expired_short = [
            key
            for key, entry in self._short_term.items()
            if entry.expires_at and datetime.fromisoformat(entry.expires_at) < now
        ]
        for key in expired_short:
            del self._short_term[key]
            removed += 1

        # Среднесрочная память
        expired_medium = [
            key
            for key, entry in self._medium_term.items()
            if entry.expires_at and datetime.fromisoformat(entry.expires_at) < now
        ]
        for key in expired_medium:
            del self._medium_term[key]
            removed += 1

        logger.info(f"Pruned {removed} expired memory entries")
        return removed

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику памяти."""
        return {
            "short_term_count": len(self._short_term),
            "medium_term_count": len(self._medium_term),
            "short_term_keys": list(self._short_term.keys())[:10],
            "medium_term_keys": list(self._medium_term.keys())[:10],
            "cleanup_task_running": (
                self._cleanup_task is not None and not self._cleanup_task.done()
            ),
        }
