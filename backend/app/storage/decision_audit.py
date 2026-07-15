"""
Decision Audit Trail — хранение всех решений AI-агентов с hindsight verdict.
Основан на архитектуре Atlas для полной объяснимости AI-решений.
ИСПРАВЛЕНО (audit 14):
- Добавлена политика хранения с автоматической очисткой старых записей
- Добавлен экспорт решений в JSON-архив перед удалением
ИСПРАВЛЕНО (v4.0 — проблема #10):
- Переход с синхронного sqlite3 на aiosqlite
- Все DB-операции теперь асинхронные, не блокируют event loop
"""

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


class DecisionAuditTrail:
    """
    Хранилище всех решений AI-агентов.
    ИСПРАВЛЕНО (v4.0 — проблема #10): aiosqlite вместо sqlite3
    """

    def __init__(self, db_path: Path, config: Optional[RetentionConfig] = None):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.config = config or self._load_config()

        if self.config.archive_before_delete:
            Path(self.config.archive_path).mkdir(parents=True, exist_ok=True)

        # Флаг инициализации БД
        self._db_initialized = False

        logger.info(
            f"📝 Decision Audit Trail initialized "
            f"(retention: {self.config.keep_last_days} days, "
            f"max records: {self.config.max_records}, "
            f"async: aiosqlite)"
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
        """Гарантирует, что БД инициализирована (ленивая инициализация)."""
        if self._db_initialized:
            return

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
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

            await db.execute("CREATE INDEX IF NOT EXISTS idx_agent ON decisions(agent)")
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_decision_type ON decisions(decision_type)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_session_id ON decisions(session_id)"
            )
            await db.execute(
                "CREATE INDEX IF NOT EXISTS idx_timestamp ON decisions(timestamp)"
            )

            await db.commit()

        self._db_initialized = True
        logger.debug("✅ Decision Audit DB initialized")

    async def log_decision(self, record: DecisionRecord) -> int:
        """
        Логирует решение в базу данных (АСИНХРОННО).
        Returns: ID записи
        """
        await self._ensure_db_initialized()

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
            f"Decision logged: [{record.agent}] {record.decision_type} "
            f"(ID: {decision_id}, confidence: {record.confidence:.2f})"
        )
        return decision_id

    async def update_outcome(
        self, decision_id: int, outcome: str, hindsight_verdict: str
    ) -> bool:
        """Обновляет outcome и hindsight verdict решения (АСИНХРОННО)."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                UPDATE decisions
                SET outcome = ?, hindsight_verdict = ?
                WHERE id = ?
                """,
                (outcome, hindsight_verdict, decision_id),
            )
            updated = cursor.rowcount > 0
            await db.commit()

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
        """Получает решения с фильтрацией (АСИНХРОННО)."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            query = "SELECT * FROM decisions WHERE 1=1"
            params = []

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

            cursor = await db.execute(query, params)
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
        """Получает решение по ID (АСИНХРОННО)."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
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
        """Возвращает статистику решений (АСИНХРОННО)."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute("SELECT COUNT(*) FROM decisions")
            row = await cursor.fetchone()
            total = row[0]

            cursor = await db.execute("""
                SELECT agent, COUNT(*) as count
                FROM decisions
                GROUP BY agent
                ORDER BY count DESC
            """)
            rows = await cursor.fetchall()
            by_agent = {row[0]: row[1] for row in rows}

            cursor = await db.execute("""
                SELECT decision_type, COUNT(*) as count
                FROM decisions
                GROUP BY decision_type
                ORDER BY count DESC
                LIMIT 10
            """)
            rows = await cursor.fetchall()
            by_type = {row[0]: row[1] for row in rows}

            cursor = await db.execute("""
                SELECT hindsight_verdict, COUNT(*) as count
                FROM decisions
                WHERE hindsight_verdict IS NOT NULL
                GROUP BY hindsight_verdict
            """)
            rows = await cursor.fetchall()
            by_verdict = {row[0]: row[1] for row in rows}

            cursor = await db.execute("SELECT AVG(confidence) FROM decisions")
            row = await cursor.fetchone()
            avg_confidence = row[0] or 0.0

            cursor = await db.execute(
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
        """Очищает старые решения согласно политике хранения (АСИНХРОННО)."""
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

        async with aiosqlite.connect(self.db_path) as db:
            # 1. Удаление по возрасту
            cutoff_date = datetime.now() - timedelta(days=days)
            cutoff_str = cutoff_date.isoformat()

            cursor = await db.execute(
                "SELECT COUNT(*) FROM decisions WHERE timestamp < ?", (cutoff_str,)
            )
            row = await cursor.fetchone()
            to_delete_by_age = row[0]

            if to_delete_by_age > 0:
                if do_archive:
                    archived = await self._archive_old_decisions(db, cutoff_str, "age")
                    result["archived"] = archived
                    result["archive_file"] = self._get_last_archive_path()

                cursor = await db.execute(
                    "DELETE FROM decisions WHERE timestamp < ?", (cutoff_str,)
                )
                result["deleted_by_age"] = cursor.rowcount
                logger.info(
                    f"🗑️ Deleted {result['deleted_by_age']} decisions older than {days} days"
                )

            # 2. Удаление по количеству
            cursor = await db.execute("SELECT COUNT(*) FROM decisions")
            row = await cursor.fetchone()
            total = row[0]

            if total > max_rec:
                cursor = await db.execute(
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
                            db, cutoff_id
                        )
                        result["archived"] += archived

                    cursor = await db.execute(
                        "DELETE FROM decisions WHERE id < ?", (cutoff_id,)
                    )
                    result["deleted_by_count"] = cursor.rowcount
                    logger.info(
                        f"🗑️ Deleted {result['deleted_by_count']} excess decisions "
                        f"(limit: {max_rec})"
                    )

            await db.commit()

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

        # ИСПРАВЛЕНО (С-12): асинхронная запись через run_io
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

        # ИСПРАВЛЕНО (С-12): асинхронная запись через run_io
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


# Singleton instance
decision_audit = DecisionAuditTrail(Path("./data/decision_audit.db"))
