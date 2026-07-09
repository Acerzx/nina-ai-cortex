"""
RAG Updater — автоматическое обновление векторной базы знаний.
Реализация идеи 1.2: автоматическое пополнение RAG при появлении новых данных.

Источники обновлений:
1. Decision Audit Trail — новые Session Digest (SESSION_DIGEST_GENERATED)
2. Локальная документация — изменения в docs/ директории
3. N.I.N.A. документация — проверка обновлений (stub, требует API)

Архитектура:
- Работает как фоновая задача в BackgroundTaskManager
- Интервал проверок настраивается через settings
- Feature flag для включения/выключения
- Идемпотентность: отслеживает уже индексированные элементы

Использование:
    from app.core.rag_updater import rag_updater

    # В BackgroundTaskManager.register()
    background_tasks.register(
        name="rag_auto_update",
        coro=rag_updater.update,
        interval_seconds=6 * 3600,  # каждые 6 часов
        enabled=settings.feature_flags.rag.auto_update_enabled,
    )
"""

import logging
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Set, List
from datetime import datetime, timedelta

from app.core.config import settings
from app.core.rag_engine import rag_engine
from app.storage.decision_audit import decision_audit
from app.core.events import event_bus

logger = logging.getLogger("RAGUpdater")


class RAGUpdater:
    """
    Автоматический апдейтер RAG-базы знаний.

    Features:
    - Идемпотентность (не индексирует повторно)
    - Инкрементальное обновление (только новые данные)
    - Интеграция с Decision Audit Trail
    - Feature flag для управления
    - Подробное логирование
    """

    # Директория с документацией (относительно корня проекта)
    DOCS_DIR = Path("./docs")

    # Паттерны файлов документации
    DOC_EXTENSIONS = {".md", ".txt", ".rst"}

    # Максимальное количество документов за один цикл обновления
    MAX_DOCS_PER_RUN = 50

    def __init__(self):
        # Отслеживание уже индексированных элементов
        self._indexed_session_ids: Set[str] = set()
        self._indexed_doc_hashes: Set[str] = set()

        # Статистика
        self._stats = {
            "total_update_runs": 0,
            "successful_runs": 0,
            "failed_runs": 0,
            "sessions_indexed": 0,
            "docs_indexed": 0,
            "last_update_time": None,
            "last_update_duration_seconds": 0.0,
        }

        # Флаг инициализации
        self._initialized = False

        # Загружаем конфигурацию
        self._enabled = self._load_enabled_flag()
        self._check_interval_hours = self._load_check_interval()

        logger.info(
            f"🔄 RAG Updater initialized "
            f"(enabled: {self._enabled}, "
            f"interval: {self._check_interval_hours}h)"
        )

    def _load_enabled_flag(self) -> bool:
        """Загружает feature flag из settings."""
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                rag_ff = getattr(ff, "rag", None)
                if rag_ff:
                    return getattr(rag_ff, "auto_update_enabled", False)
        except Exception as e:
            logger.debug(f"Could not load RAG feature flag: {e}")
        return False

    def _load_check_interval(self) -> float:
        """Загружает интервал проверок из settings (часы)."""
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                rag_ff = getattr(ff, "rag", None)
                if rag_ff:
                    return getattr(rag_ff, "auto_update_interval_hours", 6.0)
        except Exception as e:
            logger.debug(f"Could not load RAG interval: {e}")
        return 6.0

    async def initialize(self) -> bool:
        """
        Инициализирует RAG Updater.
        Загружает список уже индексированных элементов из RAG.

        Returns:
            True если инициализация успешна
        """
        if self._initialized:
            return True

        try:
            # Проверяем, что RAG engine инициализирован
            if not rag_engine._initialized:
                logger.warning("RAG Engine not initialized, RAG Updater will skip")
                return False

            # Загружаем уже индексированные session_ids из Qdrant
            await self._load_indexed_sessions()

            # Загружаем хэши индексированных документов
            await self._load_indexed_doc_hashes()

            self._initialized = True
            logger.info(
                f"✅ RAG Updater initialized "
                f"({len(self._indexed_session_ids)} sessions, "
                f"{len(self._indexed_doc_hashes)} docs already indexed)"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize RAG Updater: {e}")
            return False

    async def _load_indexed_sessions(self):
        """
        Загружает список уже индексированных session_ids из Qdrant.
        Используется для идемпотентности — не индексируем дважды.
        """
        try:
            # Ищем все документы с source=session_digest
            # Используем большой top_k чтобы получить максимум
            results = await rag_engine.search(
                query="session",
                top_k=1000,
                filters={"source": "session_digest"},
            )

            for result in results:
                metadata = result.get("metadata", {})
                session_id = metadata.get("session_id")
                if session_id:
                    self._indexed_session_ids.add(session_id)

            logger.debug(
                f"Loaded {len(self._indexed_session_ids)} indexed sessions from RAG"
            )

        except Exception as e:
            logger.warning(f"Could not load indexed sessions: {e}")

    async def _load_indexed_doc_hashes(self):
        """
        Загружает хэши уже индексированных документов.
        Используется для детекции изменений в документации.
        """
        try:
            results = await rag_engine.search(
                query="documentation",
                top_k=1000,
                filters={"source": "documentation"},
            )

            for result in results:
                metadata = result.get("metadata", {})
                content_hash = metadata.get("content_hash")
                if content_hash:
                    self._indexed_doc_hashes.add(content_hash)

            logger.debug(
                f"Loaded {len(self._indexed_doc_hashes)} indexed doc hashes from RAG"
            )

        except Exception as e:
            logger.warning(f"Could not load indexed doc hashes: {e}")

    async def update(self) -> Dict[str, Any]:
        """
        Основной метод обновления RAG.
        Вызывается периодически из BackgroundTaskManager.

        Returns:
            Статистика обновления
        """
        if not self._enabled:
            logger.debug("RAG auto-update is disabled, skipping")
            return {"status": "disabled"}

        # Инициализация при первом запуске
        if not self._initialized:
            if not await self.initialize():
                return {"status": "init_failed"}

        start_time = datetime.now()
        self._stats["total_update_runs"] += 1

        result = {
            "status": "success",
            "sessions_indexed": 0,
            "docs_indexed": 0,
            "errors": [],
            "timestamp": start_time.isoformat(),
        }

        try:
            # 1. Индексируем новые Session Digest
            sessions_result = await self._index_unindexed_sessions()
            result["sessions_indexed"] = sessions_result["indexed"]
            result["errors"].extend(sessions_result.get("errors", []))

            # 2. Проверяем обновления документации
            docs_result = await self._check_documentation_updates()
            result["docs_indexed"] = docs_result["indexed"]
            result["errors"].extend(docs_result.get("errors", []))

            # 3. Stub: проверка обновлений N.I.N.A. (требует GitHub API)
            # nina_result = await self._check_nina_updates()

            # Обновляем статистику
            self._stats["successful_runs"] += 1
            self._stats["sessions_indexed"] += result["sessions_indexed"]
            self._stats["docs_indexed"] += result["docs_indexed"]

            # Логируем результат
            duration = (datetime.now() - start_time).total_seconds()
            self._stats["last_update_time"] = start_time.isoformat()
            self._stats["last_update_duration_seconds"] = duration

            if result["sessions_indexed"] > 0 or result["docs_indexed"] > 0:
                logger.info(
                    f"✅ RAG update complete in {duration:.1f}s: "
                    f"{result['sessions_indexed']} sessions, "
                    f"{result['docs_indexed']} docs indexed"
                )
            else:
                logger.debug(f"RAG update: no new data to index ({duration:.1f}s)")

            # Публикуем событие для WebSocket broadcast
            if result["sessions_indexed"] > 0 or result["docs_indexed"] > 0:
                await event_bus.publish(
                    "RAG_UPDATED",
                    {
                        "sessions_indexed": result["sessions_indexed"],
                        "docs_indexed": result["docs_indexed"],
                        "duration_seconds": duration,
                        "timestamp": start_time.isoformat(),
                    },
                )

            return result

        except Exception as e:
            self._stats["failed_runs"] += 1
            error_msg = f"RAG update failed: {type(e).__name__}: {e}"
            logger.error(error_msg, exc_info=True)
            result["status"] = "error"
            result["errors"].append(error_msg)
            return result

    async def _index_unindexed_sessions(self) -> Dict[str, Any]:
        """
        Индексирует Session Digest из Decision Audit, которые ещё не в RAG.

        Логика:
        1. Получаем решения SESSION_DIGEST_GENERATED из Decision Audit
        2. Фильтруем уже индексированные (по session_id)
        3. Для каждого нового — добавляем в RAG

        Returns:
            {"indexed": int, "errors": list}
        """
        result = {"indexed": 0, "errors": []}

        try:
            # Получаем последние Session Digest решения
            decisions = await decision_audit.get_decisions(
                decision_type="SESSION_DIGEST_GENERATED",
                limit=100,
            )

            if not decisions:
                logger.debug("No SESSION_DIGEST_GENERATED decisions found")
                return result

            # Фильтруем уже индексированные
            new_decisions = []
            for decision in decisions:
                session_id = decision.inputs.get("session_id")
                if session_id and session_id not in self._indexed_session_ids:
                    new_decisions.append(decision)

            if not new_decisions:
                logger.debug("All session digests already indexed")
                return result

            logger.info(
                f"🔄 Indexing {len(new_decisions)} new Session Digest(s) in RAG..."
            )

            # Индексируем каждый новый Session Digest
            for decision in new_decisions:
                try:
                    digest_data = decision.outputs.get("digest", {})
                    if not digest_data:
                        continue

                    session_id = decision.inputs.get("session_id")

                    # Индексируем в RAG
                    chunks_added = await rag_engine.add_session_digest(digest_data)

                    if chunks_added > 0:
                        result["indexed"] += 1
                        if session_id:
                            self._indexed_session_ids.add(session_id)

                        logger.info(
                            f"📚 Indexed Session Digest: {session_id} "
                            f"({chunks_added} chunks)"
                        )
                    else:
                        logger.warning(
                            f"Failed to index Session Digest for {session_id}: "
                            f"no chunks added"
                        )

                    # Ограничиваем количество за один запуск
                    if result["indexed"] >= self.MAX_DOCS_PER_RUN:
                        logger.info(
                            f"Reached max docs per run ({self.MAX_DOCS_PER_RUN}), "
                            f"stopping session indexing"
                        )
                        break

                except Exception as e:
                    error_msg = (
                        f"Error indexing session {decision.inputs.get('session_id')}: "
                        f"{type(e).__name__}: {e}"
                    )
                    logger.error(error_msg)
                    result["errors"].append(error_msg)

            return result

        except Exception as e:
            error_msg = f"Error in _index_unindexed_sessions: {e}"
            logger.error(error_msg, exc_info=True)
            result["errors"].append(error_msg)
            return result

    async def _check_documentation_updates(self) -> Dict[str, Any]:
        """
        Проверяет изменения в локальной документации и индексирует новые/изменённые файлы.

        Логика:
        1. Сканирует docs/ директорию
        2. Для каждого файла вычисляет SHA-256 хэш содержимого
        3. Если хэш новый — индексирует файл в RAG

        Returns:
            {"indexed": int, "errors": list}
        """
        result = {"indexed": 0, "errors": []}

        # Проверяем существование директории
        if not self.DOCS_DIR.exists():
            logger.debug(f"Docs directory does not exist: {self.DOCS_DIR}")
            return result

        try:
            # Собираем все файлы документации
            doc_files: List[Path] = []
            for ext in self.DOC_EXTENSIONS:
                doc_files.extend(self.DOCS_DIR.rglob(f"*{ext}"))

            if not doc_files:
                logger.debug("No documentation files found")
                return result

            logger.debug(f"Scanning {len(doc_files)} documentation files...")

            # Обрабатываем каждый файл
            for doc_path in doc_files:
                try:
                    # Читаем содержимое
                    content = doc_path.read_text(encoding="utf-8")
                    if not content.strip():
                        continue

                    # Вычисляем хэш содержимого
                    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()

                    # Проверяем, индексирован ли уже
                    if content_hash in self._indexed_doc_hashes:
                        continue

                    # Индексируем документ
                    metadata = {
                        "source": "documentation",
                        "file_path": str(doc_path),
                        "file_name": doc_path.name,
                        "content_hash": content_hash,
                        "updated_at": datetime.now().isoformat(),
                    }

                    chunks_added = await rag_engine.add_document(
                        text=content,
                        metadata=metadata,
                        chunk_type="documentation",
                    )

                    if chunks_added > 0:
                        result["indexed"] += 1
                        self._indexed_doc_hashes.add(content_hash)

                        logger.info(
                            f"📖 Indexed documentation: {doc_path.name} "
                            f"({chunks_added} chunks)"
                        )
                    else:
                        logger.warning(
                            f"Failed to index doc {doc_path.name}: no chunks added"
                        )

                    # Ограничиваем количество за один запуск
                    if result["indexed"] >= self.MAX_DOCS_PER_RUN:
                        logger.info(
                            f"Reached max docs per run ({self.MAX_DOCS_PER_RUN}), "
                            f"stopping doc indexing"
                        )
                        break

                except Exception as e:
                    error_msg = (
                        f"Error processing doc {doc_path.name}: {type(e).__name__}: {e}"
                    )
                    logger.error(error_msg)
                    result["errors"].append(error_msg)

            return result

        except Exception as e:
            error_msg = f"Error in _check_documentation_updates: {e}"
            logger.error(error_msg, exc_info=True)
            result["errors"].append(error_msg)
            return result

    async def _check_nina_updates(self) -> Dict[str, Any]:
        """
        STUB: Проверка обновлений N.I.N.A. документации.

        Требует:
        - GitHub API для проверки новых релизов
        - Скачивания и парсинга release notes
        - Индексации в RAG

        Текущая реализация — заглушка, возвращает пустой результат.
        Полная реализация будет добавлена при необходимости.

        Returns:
            {"indexed": int, "errors": list}
        """
        logger.debug("N.I.N.A. update check: stub (not implemented yet)")
        return {"indexed": 0, "errors": []}

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику RAG Updater."""
        return {
            **self._stats,
            "enabled": self._enabled,
            "check_interval_hours": self._check_interval_hours,
            "initialized": self._initialized,
            "indexed_sessions_count": len(self._indexed_session_ids),
            "indexed_docs_count": len(self._indexed_doc_hashes),
        }

    async def force_update(self) -> Dict[str, Any]:
        """
        Принудительное обновление RAG (вне зависимости от интервала).
        Используется для ручного запуска через API.

        Returns:
            Результат обновления
        """
        logger.info("🔄 Force RAG update triggered")
        return await self.update()


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
rag_updater = RAGUpdater()
