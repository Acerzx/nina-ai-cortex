"""
Decision Audit Trail — хранение всех решений AI-агентов с hindsight verdict.
Основан на архитектуре Atlas для полной объяснимости AI-решений.
"""

import logging
import sqlite3
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
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


class DecisionAuditTrail:
    """
    Хранилище всех решений AI-агентов.

    Features:
    - SQLite для persistence
    - Hindsight verdict (оценка решения постфактум)
    - Поиск по агенту, типу решения, сессии
    - Экспорт для анализа
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

        logger.info(f"📝 Decision Audit Trail initialized ({self.db_path})")

    def _init_db(self):
        """Инициализирует базу данных."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Создаем таблицу решений
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

        # Создаем индексы для быстрого поиска
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

        conn.close()

        return {
            "total_decisions": total,
            "by_agent": by_agent,
            "by_type": by_type,
            "by_verdict": by_verdict,
            "avg_confidence": avg_confidence,
            "db_path": str(self.db_path),
        }

    async def export_for_session(self, session_id: str) -> List[Dict[str, Any]]:
        """Экспортирует все решения для конкретной сессии."""
        records = await self.get_decisions(session_id=session_id, limit=10000)
        return [r.model_dump() for r in records]


# Singleton instance
decision_audit = DecisionAuditTrail(Path("./data/decision_audit.db"))
