"""
Decision Audit Trail — хранение всех решений AI-агентов с hindsight verdict.
Основан на архитектуре Atlas для полной объяснимости AI-решений.

ИСПРАВЛЕНО (audit 14):
- Добавлена политика хранения с автоматической очисткой старых записей
- Добавлен экспорт решений в JSON-архив перед удалением
- Настройка retention через settings
"""

import logging
import sqlite3
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

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
    outcome: Optional[str] = None  # SUCCESS, FAILED, PARTIAL
    hindsight_verdict: Optional[str] = None  # CORRECT, WRONG, SUBOPTIMAL
    session_id: Optional[str] = None
    context: Dict[str, Any] = Field(default_factory=dict)


class RetentionConfig(BaseModel):
    """Конфигурация политики хранения решений."""

    # Хранить решения за последние N дней
    keep_last_days: int = 90
    # Максимальное количество записей в БД
    max_records: int = 100000
    # Экспортировать удаляемые записи в JSON перед удалением
    archive_before_delete: bool = True
    # Путь для архивов
    archive_path: str = "./data/decision_archives"
    # Автоматическая очистка (запускать periodically)
    auto_cleanup_enabled: bool = True
    # Интервал автоматической очистки (часы)
    auto_cleanup_interval_hours: int = 24


class DecisionAuditTrail:
    """
    Хранилище всех решений AI-агентов.

    Features:
    - SQLite для persistence
    - Hindsight verdict (оценка решения постфактум)
    - Поиск по агенту, типу решения, сессии
    - Экспорт для анализа

    ИСПРАВЛЕНО (audit 14):
    - Добавлена политика хранения с автоматической очисткой
    - Экспорт старых решений в JSON-архивы перед удалением
    """

    def __init__(self, db_path: Path, config: Optional[RetentionConfig] = None):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Загружаем конфиг из settings или используем дефолтный
        self.config = config or self._load_config()

        # Создаём директорию для архивов
        if self.config.archive_before_delete:
            Path(self.config.archive_path).mkdir(parents=True, exist_ok=True)

        self._init_db()
        logger.info(
            f"📝 Decision Audit Trail initialized "
            f"(retention: {self.config.keep_last_days} days, "
            f"max records: {self.config.max_records})"
        )

    def _load_config(self) -> RetentionConfig:
        """Загружает конфигурацию retention из settings."""
        try:
            from app.core.config import settings

            # Если есть секция decision_audit в settings
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

    def _init_db(self):
        """Инициализирует базу данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Создаём таблицу решений
        cursor.execute("""
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

        # Создаём индексы для быстрого поиска
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_agent ON decisions(agent)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_decision_type ON decisions(decision_type)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_session_id ON decisions(session_id)
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_timestamp ON decisions(timestamp)
        """)

        conn.commit()
        conn.close()

    async def log_decision(self, record: DecisionRecord) -> int:
        """
        Логирует решение в базу данных.
        Returns:
            ID записи
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
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

        conn.commit()
        conn.close()

        logger.debug(
            f"Decision logged: [{record.agent}] {record.decision_type} "
            f"(ID: {decision_id}, confidence: {record.confidence:.2f})"
        )

        return decision_id

    async def update_outcome(
        self, decision_id: int, outcome: str, hindsight_verdict: str
    ) -> bool:
        """
        Обновляет outcome и hindsight verdict решения.
        Вызывается после выполнения действия и оценки результата.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute(
            """
            UPDATE decisions
            SET outcome = ?, hindsight_verdict = ?
            WHERE id = ?
            """,
            (outcome, hindsight_verdict, decision_id),
        )

        updated = cursor.rowcount > 0

        conn.commit()
        conn.close()

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
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

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

        cursor.execute(query, params)
        rows = cursor.fetchall()
        conn.close()

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
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        cursor.execute("SELECT * FROM decisions WHERE id = ?", (decision_id,))
        row = cursor.fetchone()
        conn.close()

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
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Общее количество решений
        cursor.execute("SELECT COUNT(*) FROM decisions")
        total = cursor.fetchone()[0]

        # Решения по агентам
        cursor.execute("""
            SELECT agent, COUNT(*) as count
            FROM decisions
            GROUP BY agent
            ORDER BY count DESC
        """)
        by_agent = {row[0]: row[1] for row in cursor.fetchall()}

        # Решения по типам
        cursor.execute("""
            SELECT decision_type, COUNT(*) as count
            FROM decisions
            GROUP BY decision_type
            ORDER BY count DESC
            LIMIT 10
        """)
        by_type = {row[0]: row[1] for row in cursor.fetchall()}

        # Hindsight verdicts
        cursor.execute("""
            SELECT hindsight_verdict, COUNT(*) as count
            FROM decisions
            WHERE hindsight_verdict IS NOT NULL
            GROUP BY hindsight_verdict
        """)
        by_verdict = {row[0]: row[1] for row in cursor.fetchall()}

        # Средняя уверенность
        cursor.execute("SELECT AVG(confidence) FROM decisions")
        avg_confidence = cursor.fetchone()[0] or 0.0

        # Самая старая и самая новая запись
        cursor.execute("SELECT MIN(timestamp), MAX(timestamp) FROM decisions")
        oldest, newest = cursor.fetchone()

        conn.close()

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
        }

    async def export_for_session(self, session_id: str) -> List[Dict[str, Any]]:
        """Экспортирует все решения для конкретной сессии."""
        records = await self.get_decisions(session_id=session_id, limit=10000)
        return [r.model_dump() for r in records]

    # ========================================================================
    # ИСПРАВЛЕНО (audit 14): Retention Policy
    # ========================================================================

    async def cleanup_old_decisions(
        self,
        keep_last_days: Optional[int] = None,
        max_records: Optional[int] = None,
        archive: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """
        ИСПРАВЛЕНО (audit 14): Очищает старые решения согласно политике хранения.

        Args:
            keep_last_days: Хранить решения за последние N дней (override config)
            max_records: Максимальное количество записей (override config)
            archive: Экспортировать перед удалением (override config)

        Returns:
            Статистика очистки
        """
        days = (
            keep_last_days if keep_last_days is not None else self.config.keep_last_days
        )
        max_rec = max_records if max_records is not None else self.config.max_records
        do_archive = (
            archive if archive is not None else self.config.archive_before_delete
        )

        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        result = {
            "deleted_by_age": 0,
            "deleted_by_count": 0,
            "archived": 0,
            "archive_file": None,
        }

        # 1. Удаление по возрасту
        cutoff_date = datetime.now() - timedelta(days=days)
        cutoff_str = cutoff_date.isoformat()

        # Считаем, сколько будет удалено
        cursor.execute(
            "SELECT COUNT(*) FROM decisions WHERE timestamp < ?", (cutoff_str,)
        )
        to_delete_by_age = cursor.fetchone()[0]

        if to_delete_by_age > 0:
            # Экспортируем перед удалением
            if do_archive:
                archived = await self._archive_old_decisions(conn, cutoff_str, "age")
                result["archived"] = archived
                result["archive_file"] = self._get_last_archive_path()

            # Удаляем
            cursor.execute("DELETE FROM decisions WHERE timestamp < ?", (cutoff_str,))
            result["deleted_by_age"] = cursor.rowcount
            logger.info(
                f"🗑️ Deleted {result['deleted_by_age']} decisions older than {days} days"
            )

        # 2. Удаление по количеству (если превышен лимит)
        cursor.execute("SELECT COUNT(*) FROM decisions")
        total = cursor.fetchone()[0]

        if total > max_rec:
            excess = total - max_rec

            # Находим ID самой старой записи, которую нужно оставить
            cursor.execute(
                """
                SELECT id FROM decisions
                ORDER BY timestamp DESC
                LIMIT 1 OFFSET ?
                """,
                (max_rec,),
            )
            row = cursor.fetchone()

            if row:
                cutoff_id = row[0]

                if do_archive:
                    archived = await self._archive_old_decisions_by_id(conn, cutoff_id)
                    result["archived"] += archived

                cursor.execute("DELETE FROM decisions WHERE id < ?", (cutoff_id,))
                result["deleted_by_count"] = cursor.rowcount
                logger.info(
                    f"🗑️ Deleted {result['deleted_by_count']} excess decisions "
                    f"(limit: {max_rec})"
                )

        conn.commit()
        conn.close()

        total_deleted = result["deleted_by_age"] + result["deleted_by_count"]
        if total_deleted > 0:
            logger.info(
                f"✅ Retention cleanup complete: "
                f"{total_deleted} deleted, {result['archived']} archived"
            )

        return result

    async def _archive_old_decisions(
        self, conn: sqlite3.Connection, cutoff_str: str, reason: str
    ) -> int:
        """Экспортирует старые решения в JSON-архив."""
        cursor = conn.cursor()
        cursor.execute(
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

        rows = cursor.fetchall()
        if not rows:
            return 0

        # Формируем архив
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

        # Сохраняем в файл
        archive_path = Path(self.config.archive_path)
        archive_path.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"decisions_archive_{reason}_{timestamp}.json"
        filepath = archive_path / filename

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(archive_data, f, indent=2, ensure_ascii=False)

        self._last_archive_path = str(filepath)
        logger.info(f"📦 Archived {len(rows)} decisions to {filepath}")

        return len(rows)

    async def _archive_old_decisions_by_id(
        self, conn: sqlite3.Connection, cutoff_id: int
    ) -> int:
        """Экспортирует старые решения в JSON-архив (по ID)."""
        cursor = conn.cursor()
        cursor.execute(
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

        rows = cursor.fetchall()
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

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(archive_data, f, indent=2, ensure_ascii=False)

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
