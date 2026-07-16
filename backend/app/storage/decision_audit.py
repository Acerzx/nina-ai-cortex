"""
Decision Audit Trail — хранение всех решений AI-агентов с hindsight verdict.
Основан на архитектуре Atlas для полной объяснимости AI-решений.
ИСПРАВЛЕНО (audit 14):
- Добавлена политика хранения с автоматической очисткой старых записей
- Добавлен экспорт решений в JSON-архив перед удалением
ИСПРАВЛЕНО (v4.0 — проблема #10):
- Переход с синхронного sqlite3 на aiosqlite
- Все DB-операции теперь асинхронные, не блокируют event loop
ИСПРАВЛЕНО (С-3): Batch insert с постоянным подключением
- Постоянное подключение к SQLite (не открывается/закрывается на каждый insert)
- Batch-буфер на N записей (по умолчанию 50)
- executemany + один commit вместо N отдельных insert'ов
- Периодический flush по таймеру (если буфер не полон)
- Final flush при shutdown
- Ожидаемый выигрыш: ~50x ускорение для операций записи
"""

import asyncio
import logging
import aiosqlite
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from app.core.executors import run_io

logger = logging.getLogger("DecisionAudit")


class DecisionRecord(BaseModel):
    """Запись о решении агента."""

    id: Optional[int] = None
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    agent: str
    decision_type: str
    inputs: Dict[str, Any] = Field(default_factory=dict)
    outputs: Dict[str, Any] = Field(default_factory=dict)
    rationale: str
    confidence: float = Field(ge=0.0, le=1.0)
    outcome: Optional[str] = None
    hindsight_verdict: Optional[str] = None
    session_id: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)


class RetentionConfig(BaseModel):
    """Конфигурация политики хранения решений."""

    keep_last_days: int = 90
    max_records: int = 100000
    archive_before_delete: bool = True
    archive_path: str = "./data/decision_archives"
    auto_cleanup_enabled: bool = True
    auto_cleanup_interval_hours: int = 24


class BatchConfig(BaseModel):
    """Конфигурация batch insert (С-3)."""

    batch_size: int = 50  # Записей в буфере перед flush
    flush_interval_seconds: float = 5.0  # Принудительный flush по таймеру
    enabled: bool = True


class DecisionAuditTrail:
    """
    Хранилище всех решений AI-агентов.

    ИСПРАВЛЕНО (v4.0 — проблема #10): aiosqlite вместо sqlite3
    ИСПРАВЛЕНО (С-3): Batch insert с постоянным подключением
    """

    def __init__(
        self,
        db_path: Path,
        config: Optional[RetentionConfig] = None,
        batch_config: Optional[BatchConfig] = None,
    ):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or self._load_config()
        self.batch_config = batch_config or BatchConfig()

        if self.config.archive_before_delete:
            Path(self.config.archive_path).mkdir(parents=True, exist_ok=True)

        # Флаг инициализации БД
        self._db_initialized = False

        # С-3: Постоянное подключение и batch-буфер
        self._db_connection: Optional[aiosqlite.Connection] = None
        self._batch_buffer: List[DecisionRecord] = []
        self._batch_lock = asyncio.Lock()
        self._flush_task: Optional[asyncio.Task] = None
        self._last_flush_time: Optional[datetime] = None

        # Статистика batch-операций
        self._batch_stats = {
            "records_buffered": 0,
            "batch_flushes": 0,
            "records_flushed": 0,
            "timer_flushes": 0,
            "manual_flushes": 0,
        }

        logger.info(
            f"📝 Decision Audit Trail initialized "
            f"(retention: {self.config.keep_last_days} days, "
            f"max records: {self.config.max_records}, "
            f"async: aiosqlite, "
            f"batch: {'enabled' if self.batch_config.enabled else 'disabled'} "
            f"(size={self.batch_config.batch_size}, "
            f"interval={self.batch_config.flush_interval_seconds}s))"
        )

    def _load_config(self) -> RetentionConfig:
        """Загружает конфигурацию retention из settings."""
        try:
            from app.core.config import settings

            if hasattr(settings, "decision_audit"):
                da_config = settings.decision_audit
                return RetentionConfig(
                    keep_last_days=getattr(da_config, "keep_last_days", 90),
                    max_records=getattr(da_config, "max_records", 100000),
                    archive_before_delete=getattr(
                        da_config, "archive_before_delete", True
                    ),
                    archive_path=getattr(
                        da_config, "archive_path", "./data/decision_archives"
                    ),
                    auto_cleanup_enabled=getattr(
                        da_config, "auto_cleanup_enabled", True
                    ),
                    auto_cleanup_interval_hours=getattr(
                        da_config, "auto_cleanup_interval_hours", 24
                    ),
                )
        except Exception as e:
            logger.debug(f"Could not load decision_audit config: {e}")
        return RetentionConfig()

    async def _ensure_db_initialized(self) -> None:
        """
        Гарантирует, что БД инициализирована и подключение открыто.
        С-3: Использует постоянное подключение вместо per-request.
        """
        if self._db_initialized and self._db_connection is not None:
            return

        # Закрываем старое подключение если есть
        if self._db_connection is not None:
            try:
                await self._db_connection.close()
            except Exception:
                pass

        # Открываем новое постоянное подключение
        self._db_connection = await aiosqlite.connect(self.db_path)

        # Создаём таблицы если нужно
        await self._db_connection.execute("""
            CREATE TABLE IF NOT EXISTS decisions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                agent TEXT NOT NULL,
                decision_type TEXT NOT NULL,
                inputs TEXT NOT NULL,
                outputs TEXT NOT NULL,
                rationale TEXT NOT NULL,
                confidence REAL NOT NULL,
                outcome TEXT,
                hindsight_verdict TEXT,
                session_id TEXT,
                context TEXT NOT NULL
            )
        """)

        await self._db_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent ON decisions(agent)"
        )
        await self._db_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_decision_type ON decisions(decision_type)"
        )
        await self._db_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_session_id ON decisions(session_id)"
        )
        await self._db_connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_timestamp ON decisions(timestamp)"
        )

        await self._db_connection.commit()
        self._db_initialized = True

        # Запускаем периодический flush если batch включен
        if self.batch_config.enabled and self._flush_task is None:
            self._flush_task = asyncio.create_task(self._periodic_flush_loop())

        logger.debug("✅ Decision Audit DB connection established (persistent)")

    async def _periodic_flush_loop(self) -> None:
        """
        Периодически flush'ит batch-буфер по таймеру.
        С-3: Гарантирует, что записи не застрянут в буфере надолго.
        """
        try:
            while True:
                await asyncio.sleep(self.batch_config.flush_interval_seconds)

                async with self._batch_lock:
                    if self._batch_buffer:
                        await self._flush_batch_locked(reason="timer")
                        self._batch_stats["timer_flushes"] += 1
        except asyncio.CancelledError:
            # Финальный flush при отмене задачи
            async with self._batch_lock:
                if self._batch_buffer:
                    await self._flush_batch_locked(reason="shutdown")
        except Exception as e:
            logger.error(f"Periodic flush loop error: {e}")

    async def log_decision(self, record: DecisionRecord) -> int:
        """
        Логирует решение в базу данных.

        ИСПРАВЛЕНО (С-3): Batch insert вместо per-request.
        - Если batch включен: добавляет в буфер, flush при достижении размера
        - Если batch выключен: немедленная запись (как раньше)

        Returns:
            ID записи (или 0 если batched — ID будет присвоен при flush)
        """
        await self._ensure_db_initialized()

        # Если batch выключен — используем старую логику
        if not self.batch_config.enabled:
            return await self._log_decision_immediate(record)

        # С-3: Batch insert
        async with self._batch_lock:
            self._batch_buffer.append(record)
            self._batch_stats["records_buffered"] += 1

            # Flush если буфер полон
            if len(self._batch_buffer) >= self.batch_config.batch_size:
                await self._flush_batch_locked(reason="batch_full")
                self._batch_stats["batch_flushes"] += 1

        # ID будет присвоен при flush, возвращаем 0
        return 0

    async def _log_decision_immediate(self, record: DecisionRecord) -> int:
        """
        Немедленная запись решения (fallback если batch выключен).
        Сохраняет обратную совместимость с оригинальной логикой.
        """
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO decisions (
                    timestamp, agent, decision_type, inputs, outputs,
                    rationale, confidence, outcome, hindsight_verdict,
                    session_id, context
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.timestamp,
                    record.agent,
                    record.decision_type,
                    json.dumps(record.inputs, ensure_ascii=False),
                    json.dumps(record.outputs, ensure_ascii=False),
                    record.rationale,
                    record.confidence,
                    record.outcome,
                    record.hindsight_verdict,
                    record.session_id,
                    json.dumps(record.context, ensure_ascii=False),
                ),
            )
            decision_id = cursor.lastrowid
            record.id = decision_id
            await db.commit()

            logger.debug(
                f"Decision logged (immediate): [{record.agent}] {record.decision_type} "
                f"(ID: {decision_id}, confidence: {record.confidence:.2f})"
            )
            return decision_id

    async def _flush_batch_locked(self, reason: str = "unknown") -> None:
        """
        Flush batch-буфера в БД одним executemany.

        ВАЖНО: Должен вызываться ПОД _batch_lock!

        С-3: executemany + один commit вместо N отдельных insert'ов.
        """
        if not self._batch_buffer or self._db_connection is None:
            return

        # Снимаем snapshot буфера
        records_to_flush = list(self._batch_buffer)
        self._batch_buffer.clear()

        # Подготавливаем данные для executemany
        batch_data = [
            (
                r.timestamp,
                r.agent,
                r.decision_type,
                json.dumps(r.inputs, ensure_ascii=False),
                json.dumps(r.outputs, ensure_ascii=False),
                r.rationale,
                r.confidence,
                r.outcome,
                r.hindsight_verdict,
                r.session_id,
                json.dumps(r.context, ensure_ascii=False),
            )
            for r in records_to_flush
        ]

        try:
            # С-3: Один executemany вместо N insert'ов
            cursor = await self._db_connection.executemany(
                """
                INSERT INTO decisions (
                    timestamp, agent, decision_type, inputs, outputs,
                    rationale, confidence, outcome, hindsight_verdict,
                    session_id, context
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                batch_data,
            )

            # Один commit для всего batch'а
            await self._db_connection.commit()

            flushed_count = (
                cursor.rowcount if cursor.rowcount else len(records_to_flush)
            )
            self._batch_stats["records_flushed"] += flushed_count
            self._last_flush_time = datetime.now()

            logger.debug(f"Batch flushed: {flushed_count} records (reason: {reason})")

        except Exception as e:
            # При ошибке возвращаем записи обратно в буфер
            logger.error(f"Batch flush failed: {e}. Returning records to buffer.")
            self._batch_buffer = records_to_flush + self._batch_buffer
            raise

    async def flush(self) -> int:
        """
        Принудительный flush batch-буфера.

        С-3: Используется для гарантии записи перед чтением или shutdown.

        Returns:
            Количество записей, записанных в БД
        """
        if not self.batch_config.enabled:
            return 0

        async with self._batch_lock:
            if not self._batch_buffer:
                return 0

            count = len(self._batch_buffer)
            await self._flush_batch_locked(reason="manual")
            self._batch_stats["manual_flushes"] += 1
            return count

    async def update_outcome(
        self, decision_id: int, outcome: str, hindsight_verdict: str
    ) -> bool:
        """Обновляет outcome и hindsight verdict решения."""
        # С-3: Перед update flush'им буфер для консистентности
        await self.flush()

        await self._ensure_db_initialized()

        cursor = await self._db_connection.execute(
            """
            UPDATE decisions
            SET outcome = ?, hindsight_verdict = ?
            WHERE id = ?
            """,
            (outcome, hindsight_verdict, decision_id),
        )

        updated = cursor.rowcount > 0
        await self._db_connection.commit()

        if updated:
            logger.info(
                f"Decision {decision_id} updated: "
                f"outcome={outcome}, verdict={hindsight_verdict}"
            )
        return updated

    async def get_decisions(
        self,
        agent: Optional[str] = None,
        decision_type: Optional[str] = None,
        session_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[DecisionRecord]:
        """Получает решения с фильтрацией."""
        # С-3: Перед чтением flush'им буфер для консистентности
        await self.flush()

        await self._ensure_db_initialized()

        self._db_connection.row_factory = aiosqlite.Row

        query = "SELECT * FROM decisions WHERE 1=1"
        params: List[Any] = []

        if agent:
            query += " AND agent = ?"
            params.append(agent)

        if decision_type:
            query += " AND decision_type = ?"
            params.append(decision_type)

        if session_id:
            query += " AND session_id = ?"
            params.append(session_id)

        query += " ORDER BY timestamp DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])

        cursor = await self._db_connection.execute(query, params)
        rows = await cursor.fetchall()

        records = []
        for row in rows:
            record = DecisionRecord(
                id=row["id"],
                timestamp=row["timestamp"],
                agent=row["agent"],
                decision_type=row["decision_type"],
                inputs=json.loads(row["inputs"]),
                outputs=json.loads(row["outputs"]),
                rationale=row["rationale"],
                confidence=row["confidence"],
                outcome=row["outcome"],
                hindsight_verdict=row["hindsight_verdict"],
                session_id=row["session_id"],
                context=json.loads(row["context"]),
            )
            records.append(record)

        return records

    async def get_decision(self, decision_id: int) -> Optional[DecisionRecord]:
        """Получает решение по ID."""
        # С-3: Перед чтением flush'им буфер
        await self.flush()

        await self._ensure_db_initialized()

        self._db_connection.row_factory = aiosqlite.Row

        cursor = await self._db_connection.execute(
            "SELECT * FROM decisions WHERE id = ?", (decision_id,)
        )
        row = await cursor.fetchone()

        if not row:
            return None

        return DecisionRecord(
            id=row["id"],
            timestamp=row["timestamp"],
            agent=row["agent"],
            decision_type=row["decision_type"],
            inputs=json.loads(row["inputs"]),
            outputs=json.loads(row["outputs"]),
            rationale=row["rationale"],
            confidence=row["confidence"],
            outcome=row["outcome"],
            hindsight_verdict=row["hindsight_verdict"],
            session_id=row["session_id"],
            context=json.loads(row["context"]),
        )

    async def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику решений."""
        # С-3: Перед статистикой flush'им буфер
        await self.flush()

        await self._ensure_db_initialized()

        cursor = await self._db_connection.execute("SELECT COUNT(*) FROM decisions")
        row = await cursor.fetchone()
        total = row[0]

        cursor = await self._db_connection.execute("""
            SELECT agent, COUNT(*) as count
            FROM decisions
            GROUP BY agent
            ORDER BY count DESC
        """)
        rows = await cursor.fetchall()
        by_agent = {row[0]: row[1] for row in rows}

        cursor = await self._db_connection.execute("""
            SELECT decision_type, COUNT(*) as count
            FROM decisions
            GROUP BY decision_type
            ORDER BY count DESC
            LIMIT 10
        """)
        rows = await cursor.fetchall()
        by_type = {row[0]: row[1] for row in rows}

        cursor = await self._db_connection.execute("""
            SELECT hindsight_verdict, COUNT(*) as count
            FROM decisions
            WHERE hindsight_verdict IS NOT NULL
            GROUP BY hindsight_verdict
        """)
        rows = await cursor.fetchall()
        by_verdict = {row[0]: row[1] for row in rows}

        cursor = await self._db_connection.execute(
            "SELECT AVG(confidence) FROM decisions"
        )
        row = await cursor.fetchone()
        avg_confidence = row[0] or 0.0

        cursor = await self._db_connection.execute(
            "SELECT MIN(timestamp), MAX(timestamp) FROM decisions"
        )
        row = await cursor.fetchone()
        oldest, newest = row

        return {
            "total_decisions": total,
            "by_agent": by_agent,
            "by_type": by_type,
            "by_verdict": by_verdict,
            "avg_confidence": avg_confidence,
            "oldest_decision": oldest,
            "newest_decision": newest,
            "db_path": str(self.db_path),
            "retention_config": self.config.model_dump(),
            "async_engine": "aiosqlite",
            # С-3: Статистика batch-операций
            "batch_stats": self._batch_stats,
            "batch_config": self.batch_config.model_dump(),
            "buffer_size": len(self._batch_buffer),
            "last_flush": (
                self._last_flush_time.isoformat() if self._last_flush_time else None
            ),
        }

    async def export_for_session(self, session_id: str) -> List[Dict[str, Any]]:
        """Экспортирует все решения для конкретной сессии."""
        records = await self.get_decisions(session_id=session_id, limit=10000)
        return [r.model_dump() for r in records]

    async def cleanup_old_decisions(
        self,
        keep_last_days: Optional[int] = None,
        max_records: Optional[int] = None,
        archive: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Очищает старые решения согласно политике хранения."""
        # С-3: Перед очисткой flush'им буфер
        await self.flush()

        days = (
            keep_last_days if keep_last_days is not None else self.config.keep_last_days
        )
        max_rec = max_records if max_records is not None else self.config.max_records
        do_archive = (
            archive if archive is not None else self.config.archive_before_delete
        )

        await self._ensure_db_initialized()

        result = {
            "deleted_by_age": 0,
            "deleted_by_count": 0,
            "archived": 0,
            "archive_file": None,
        }

        # 1. Удаление по возрасту
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff_date.isoformat()

        cursor = await self._db_connection.execute(
            "SELECT COUNT(*) FROM decisions WHERE timestamp < ?", (cutoff_str,)
        )
        row = await cursor.fetchone()
        to_delete_by_age = row[0]

        if to_delete_by_age > 0:
            if do_archive:
                archived = await self._archive_old_decisions(
                    self._db_connection, cutoff_str, "age"
                )
                result["archived"] = archived
                result["archive_file"] = self._get_last_archive_path()

            cursor = await self._db_connection.execute(
                "DELETE FROM decisions WHERE timestamp < ?", (cutoff_str,)
            )
            result["deleted_by_age"] = cursor.rowcount

            logger.info(
                f"🗑️ Deleted {result['deleted_by_age']} decisions older than {days} days"
            )

        # 2. Удаление по количеству
        cursor = await self._db_connection.execute("SELECT COUNT(*) FROM decisions")
        row = await cursor.fetchone()
        total = row[0]

        if total > max_rec:
            cursor = await self._db_connection.execute(
                """
                SELECT id FROM decisions
                ORDER BY timestamp DESC
                LIMIT 1 OFFSET ?
                """,
                (max_rec,),
            )
            row = await cursor.fetchone()

            if row:
                cutoff_id = row[0]

                if do_archive:
                    archived = await self._archive_old_decisions_by_id(
                        self._db_connection, cutoff_id
                    )
                    result["archived"] += archived

                cursor = await self._db_connection.execute(
                    "DELETE FROM decisions WHERE id < ?", (cutoff_id,)
                )
                result["deleted_by_count"] = cursor.rowcount

                logger.info(
                    f"🗑️ Deleted {result['deleted_by_count']} excess decisions "
                    f"(limit: {max_rec})"
                )

        await self._db_connection.commit()

        total_deleted = result["deleted_by_age"] + result["deleted_by_count"]
        if total_deleted > 0:
            logger.info(
                f"✅ Retention cleanup complete: "
                f"{total_deleted} deleted, {result['archived']} archived"
            )

        return result

    async def _archive_old_decisions(
        self, db: aiosqlite.Connection, cutoff_str: str, reason: str
    ) -> int:
        """Экспортирует старые решения в JSON-архив."""
        cursor = await db.execute(
            """
            SELECT id, timestamp, agent, decision_type, inputs, outputs,
                   rationale, confidence, outcome, hindsight_verdict,
                   session_id, context
            FROM decisions
            WHERE timestamp < ?
            ORDER BY timestamp ASC
            """,
            (cutoff_str,),
        )
        rows = await cursor.fetchall()

        if not rows:
            return 0

        archive_data = {
            "archive_date": datetime.now().isoformat(),
            "reason": reason,
            "cutoff_date": cutoff_str,
            "record_count": len(rows),
            "decisions": [],
        }

        for row in rows:
            archive_data["decisions"].append(
                {
                    "id": row[0],
                    "timestamp": row[1],
                    "agent": row[2],
                    "decision_type": row[3],
                    "inputs": json.loads(row[4]),
                    "outputs": json.loads(row[5]),
                    "rationale": row[6],
                    "confidence": row[7],
                    "outcome": row[8],
                    "hindsight_verdict": row[9],
                    "session_id": row[10],
                    "context": json.loads(row[11]),
                }
            )

        archive_path = Path(self.config.archive_path)
        archive_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"decisions_archive_{reason}_{timestamp}.json"
        filepath = archive_path / filename

        # Асинхронная запись через run_io
        def _write_archive():
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(archive_data, f, indent=2, ensure_ascii=False)

        await run_io(_write_archive)
        self._last_archive_path = str(filepath)

        logger.info(f"📦 Archived {len(rows)} decisions to {filepath}")
        return len(rows)

    async def _archive_old_decisions_by_id(
        self, db: aiosqlite.Connection, cutoff_id: int
    ) -> int:
        """Экспортирует старые решения в JSON-архив (по ID)."""
        cursor = await db.execute(
            """
            SELECT id, timestamp, agent, decision_type, inputs, outputs,
                   rationale, confidence, outcome, hindsight_verdict,
                   session_id, context
            FROM decisions
            WHERE id < ?
            ORDER BY timestamp ASC
            """,
            (cutoff_id,),
        )
        rows = await cursor.fetchall()

        if not rows:
            return 0

        archive_data = {
            "archive_date": datetime.now().isoformat(),
            "reason": "count_limit",
            "cutoff_id": cutoff_id,
            "record_count": len(rows),
            "decisions": [],
        }

        for row in rows:
            archive_data["decisions"].append(
                {
                    "id": row[0],
                    "timestamp": row[1],
                    "agent": row[2],
                    "decision_type": row[3],
                    "inputs": json.loads(row[4]),
                    "outputs": json.loads(row[5]),
                    "rationale": row[6],
                    "confidence": row[7],
                    "outcome": row[8],
                    "hindsight_verdict": row[9],
                    "session_id": row[10],
                    "context": json.loads(row[11]),
                }
            )

        archive_path = Path(self.config.archive_path)
        archive_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"decisions_archive_count_{timestamp}.json"
        filepath = archive_path / filename

        def _write_archive():
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(archive_data, f, indent=2, ensure_ascii=False)

        await run_io(_write_archive)
        self._last_archive_path = str(filepath)

        logger.info(f"📦 Archived {len(rows)} decisions to {filepath}")
        return len(rows)

    def _get_last_archive_path(self) -> Optional[str]:
        """Возвращает путь к последнему созданному архиву."""
        return getattr(self, "_last_archive_path", None)

    async def get_archives(self) -> List[Dict[str, Any]]:
        """Возвращает список всех архивов."""
        archive_path = Path(self.config.archive_path)

        if not archive_path.exists():
            return []

        archives = []
        for filepath in sorted(archive_path.glob("decisions_archive_*.json")):
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    data = json.load(f)

                archives.append(
                    {
                        "file": filepath.name,
                        "path": str(filepath),
                        "archive_date": data.get("archive_date"),
                        "reason": data.get("reason"),
                        "record_count": data.get("record_count", 0),
                        "size_mb": filepath.stat().st_size / (1024 * 1024),
                    }
                )
            except Exception as e:
                logger.warning(f"Failed to read archive {filepath}: {e}")

        return archives

    async def close(self) -> None:
        """
        Корректно закрывает Decision Audit Trail.

        С-3: Выполняет финальный flush batch-буфера и закрывает подключение.
        Должен вызываться при shutdown приложения.
        """
        logger.info("🛑 Closing Decision Audit Trail...")

        # 1. Останавливаем периодический flush
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
            self._flush_task = None

        # 2. Финальный flush batch-буфера
        if self.batch_config.enabled:
            async with self._batch_lock:
                if self._batch_buffer:
                    try:
                        await self._flush_batch_locked(reason="shutdown")
                        logger.info(
                            f"✅ Final batch flush: {len(self._batch_buffer)} records"
                        )
                    except Exception as e:
                        logger.error(f"❌ Final batch flush failed: {e}")

        # 3. Закрываем подключение
        if self._db_connection is not None:
            try:
                await self._db_connection.close()
                logger.info("✅ Decision Audit DB connection closed")
            except Exception as e:
                logger.debug(f"Error closing DB connection: {e}")
            finally:
                self._db_connection = None
                self._db_initialized = False

        logger.info(
            f"✅ Decision Audit Trail closed "
            f"(total flushed: {self._batch_stats['records_flushed']})"
        )


# Singleton instance
decision_audit = DecisionAuditTrail(Path("./data/decision_audit.db"))
