"""
Sessions Metadata Storage — хранение всех структурированных метаданных сессий.
Реализация идеи 1.3: SQLite для точных запросов + RAG для семантического поиска.

Архитектура разделения ролей:
- **SQLite**: структурированные данные (каждый кадр, метрики, тренды)
  → быстрые SQL-запросы, агрегации, экспорт в CSV/VOTable
  → обучение ML-моделей на полных наборах данных

- **RAG (Qdrant)**: текстовые дайджесты и документация
  → семантический поиск для Diagnostician/Copilot
  → контекст из истории похожих сессий

Таблицы:
- sessions: общая информация (target, date, filter, quality_score, ...)
- frames: данные по каждому кадру (HFR, FWHM, RMS, gain, offset, binning, ...)
- metrics_history: агрегированные тренды (по минутам/кадрам)
- session_problems: выявленные проблемы и их решения
- session_recommendations: рекомендации для будущих сессий

Использование:
    from app.storage.sessions_metadata import sessions_metadata

    # Логирование кадра
    await sessions_metadata.log_frame(session_id, frame_data)

    # Получение статистики сессии
    stats = await sessions_metadata.get_session_stats(session_id)

    # Экспорт в CSV
    await sessions_metadata.export_session_csv(session_id, output_path)
"""

import logging
import aiosqlite
import json
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field

logger = logging.getLogger("SessionsMetadata")


# ============================================================================
# PYDANTIC MODELS
# ============================================================================
class FrameRecord(BaseModel):
    """Запись о кадре."""

    session_id: str
    frame_index: int
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())

    # Параметры съёмки
    exposure_time: Optional[float] = None
    filter_name: Optional[str] = None
    gain: Optional[int] = None
    offset: Optional[int] = None
    binning: Optional[str] = None  # "1x1", "2x2", ...
    temperature: Optional[float] = None

    # Метрики качества
    hfr: Optional[float] = None
    fwhm: Optional[float] = None
    eccentricity: Optional[float] = None
    star_count: Optional[int] = None
    median_adu: Optional[float] = None

    # Гидирование
    rms_ra: Optional[float] = None
    rms_dec: Optional[float] = None
    rms_total: Optional[float] = None

    # Тип кадра
    image_type: str = "LIGHT"  # LIGHT, FLAT, DARK, BIAS


class SessionRecord(BaseModel):
    """Запись о сессии."""

    session_id: str
    target_name: Optional[str] = None
    date: str = Field(default_factory=lambda: datetime.now().strftime("%Y-%m-%d"))

    # Параметры
    filter_name: Optional[str] = None
    exposure_time: Optional[float] = None
    gain: Optional[int] = None
    temperature_setpoint: Optional[float] = None

    # Статистика
    frames_total: int = 0
    frames_accepted: int = 0
    frames_rejected: int = 0
    acceptance_rate: float = 0.0

    # Средние метрики
    avg_hfr: Optional[float] = None
    avg_fwhm: Optional[float] = None
    avg_rms_ra: Optional[float] = None
    avg_rms_dec: Optional[float] = None
    avg_temperature: Optional[float] = None

    # Качество
    quality_score: Optional[float] = None  # 0-10

    # Время
    start_time: Optional[str] = None
    end_time: Optional[str] = None
    duration_minutes: Optional[float] = None

    # Погода (средние за сессию)
    avg_wind_speed: Optional[float] = None
    avg_humidity: Optional[float] = None
    avg_cloud_cover: Optional[float] = None

    # Связь с RAG
    rag_indexed: bool = False
    rag_point_ids: str = "[]"  # JSON array of point IDs


class SessionProblem(BaseModel):
    """Выявленная проблема во время сессии."""

    session_id: str
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    problem_type: str  # HFR_DEGRADATION, RMS_SPIKE, GUIDING_LOST, ...
    severity: str  # LOW, MEDIUM, HIGH, CRITICAL
    description: str
    solution: Optional[str] = None
    resolved: bool = False


class SessionsMetadataStorage:
    """
    Хранилище структурированных метаданных сессий.

    Features:
    - aiosqlite для асинхронного доступа
    - Инкрементальное обновление (добавление кадров по мере поступления)
    - Автоматический расчёт средних метрик
    - Экспорт в CSV для внешнего анализа
    - Синхронизация с RAG после завершения сессии
    """

    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Флаг инициализации БД
        self._db_initialized = False

        # Кэш активных сессий (для быстрого доступа)
        self._active_sessions: Dict[str, SessionRecord] = {}

        logger.info(
            f"📊 Sessions Metadata Storage initialized "
            f"(db: {self.db_path}, async: aiosqlite)"
        )

    async def _ensure_db_initialized(self) -> None:
        """Гарантирует, что БД инициализирована (ленивая инициализация)."""
        if self._db_initialized:
            return

        async with aiosqlite.connect(self.db_path) as db:
            # Таблица sessions
            await db.execute("""
                CREATE TABLE IF NOT EXISTS sessions (
                    session_id TEXT PRIMARY KEY,
                    target_name TEXT,
                    date TEXT NOT NULL,
                    filter_name TEXT,
                    exposure_time REAL,
                    gain INTEGER,
                    temperature_setpoint REAL,
                    frames_total INTEGER DEFAULT 0,
                    frames_accepted INTEGER DEFAULT 0,
                    frames_rejected INTEGER DEFAULT 0,
                    acceptance_rate REAL DEFAULT 0.0,
                    avg_hfr REAL,
                    avg_fwhm REAL,
                    avg_rms_ra REAL,
                    avg_rms_dec REAL,
                    avg_temperature REAL,
                    quality_score REAL,
                    start_time TEXT,
                    end_time TEXT,
                    duration_minutes REAL,
                    avg_wind_speed REAL,
                    avg_humidity REAL,
                    avg_cloud_cover REAL,
                    rag_indexed INTEGER DEFAULT 0,
                    rag_point_ids TEXT DEFAULT '[]'
                )
            """)

            # Таблица frames
            await db.execute("""
                CREATE TABLE IF NOT EXISTS frames (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    frame_index INTEGER NOT NULL,
                    timestamp TEXT NOT NULL,
                    exposure_time REAL,
                    filter_name TEXT,
                    gain INTEGER,
                    offset INTEGER,
                    binning TEXT,
                    temperature REAL,
                    hfr REAL,
                    fwhm REAL,
                    eccentricity REAL,
                    star_count INTEGER,
                    median_adu REAL,
                    rms_ra REAL,
                    rms_dec REAL,
                    rms_total REAL,
                    image_type TEXT DEFAULT 'LIGHT',
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """)

            # Таблица session_problems
            await db.execute("""
                CREATE TABLE IF NOT EXISTS session_problems (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    problem_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    description TEXT NOT NULL,
                    solution TEXT,
                    resolved INTEGER DEFAULT 0,
                    FOREIGN KEY (session_id) REFERENCES sessions(session_id)
                )
            """)

            # Индексы для быстрого поиска
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_frames_session 
                ON frames(session_id, frame_index)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_sessions_date 
                ON sessions(date)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_sessions_target 
                ON sessions(target_name)
            """)
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_problems_session 
                ON session_problems(session_id)
            """)

            await db.commit()

        self._db_initialized = True
        logger.debug("✅ Sessions Metadata DB initialized")

    # ========================================================================
    # SESSION MANAGEMENT
    # ========================================================================
    async def create_session(self, session: SessionRecord) -> str:
        """
        Создаёт новую запись о сессии.

        Args:
            session: Данные сессии

        Returns:
            session_id
        """
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO sessions (
                    session_id, target_name, date, filter_name, exposure_time,
                    gain, temperature_setpoint, start_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    session.session_id,
                    session.target_name,
                    session.date,
                    session.filter_name,
                    session.exposure_time,
                    session.gain,
                    session.temperature_setpoint,
                    session.start_time,
                ),
            )
            await db.commit()

        # Добавляем в кэш активных сессий
        self._active_sessions[session.session_id] = session

        logger.info(
            f"📝 Session created: {session.session_id} "
            f"(target: {session.target_name}, filter: {session.filter_name})"
        )
        return session.session_id

    async def update_session(self, session_id: str, **updates) -> bool:
        """
        Обновляет запись о сессии.

        Args:
            session_id: ID сессии
            **updates: Поля для обновления

        Returns:
            True если обновлено
        """
        await self._ensure_db_initialized()

        if not updates:
            return False

        # Строим SET clause
        set_clause = ", ".join(f"{key} = ?" for key in updates.keys())
        values = list(updates.values()) + [session_id]

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                f"UPDATE sessions SET {set_clause} WHERE session_id = ?", values
            )
            updated = cursor.rowcount > 0
            await db.commit()

        # Обновляем кэш
        if updated and session_id in self._active_sessions:
            for key, value in updates.items():
                setattr(self._active_sessions[session_id], key, value)

        return updated

    async def finalize_session(self, session_id: str) -> Dict[str, Any]:
        """
        Финализирует сессию: рассчитывает средние метрики, quality_score.
        Вызывается при SEQUENCE_STOPPED.

        Returns:
            Статистика финализации
        """
        await self._ensure_db_initialized()

        # Получаем все кадры сессии
        frames = await self.get_frames(session_id)

        if not frames:
            logger.warning(f"No frames found for session {session_id}")
            return {"frames_processed": 0}

        # Рассчитываем средние метрики
        hfr_values = [f.hfr for f in frames if f.hfr is not None]
        fwhm_values = [f.fwhm for f in frames if f.fwhm is not None]
        rms_ra_values = [f.rms_ra for f in frames if f.rms_ra is not None]
        rms_dec_values = [f.rms_dec for f in frames if f.rms_dec is not None]
        temp_values = [f.temperature for f in frames if f.temperature is not None]

        avg_hfr = sum(hfr_values) / len(hfr_values) if hfr_values else None
        avg_fwhm = sum(fwhm_values) / len(fwhm_values) if fwhm_values else None
        avg_rms_ra = sum(rms_ra_values) / len(rms_ra_values) if rms_ra_values else None
        avg_rms_dec = (
            sum(rms_dec_values) / len(rms_dec_values) if rms_dec_values else None
        )
        avg_temp = sum(temp_values) / len(temp_values) if temp_values else None

        # Простой расчёт acceptance rate (кадры с HFR < 3.0 считаются принятыми)
        accepted = sum(1 for h in hfr_values if h < 3.0)
        total = len(hfr_values)
        acceptance_rate = accepted / total if total > 0 else 0.0

        # Рассчитываем quality_score (0-10)
        quality_score = self._calculate_quality_score(
            avg_hfr, avg_fwhm, avg_rms_ra, avg_rms_dec, acceptance_rate
        )

        # Обновляем сессию
        await self.update_session(
            session_id,
            frames_total=len(frames),
            frames_accepted=accepted,
            frames_rejected=total - accepted,
            acceptance_rate=acceptance_rate,
            avg_hfr=avg_hfr,
            avg_fwhm=avg_fwhm,
            avg_rms_ra=avg_rms_ra,
            avg_rms_dec=avg_rms_dec,
            avg_temperature=avg_temp,
            quality_score=quality_score,
            end_time=datetime.now().isoformat(),
        )

        # Удаляем из кэша активных
        self._active_sessions.pop(session_id, None)

        result = {
            "session_id": session_id,
            "frames_processed": len(frames),
            "frames_accepted": accepted,
            "acceptance_rate": acceptance_rate,
            "quality_score": quality_score,
            "avg_hfr": avg_hfr,
            "avg_fwhm": avg_fwhm,
        }

        logger.info(
            f"✅ Session finalized: {session_id} "
            f"(frames: {len(frames)}, accepted: {accepted}, "
            f"quality: {quality_score:.1f}/10)"
        )

        return result

    def _calculate_quality_score(
        self,
        avg_hfr: Optional[float],
        avg_fwhm: Optional[float],
        avg_rms_ra: Optional[float],
        avg_rms_dec: Optional[float],
        acceptance_rate: float,
    ) -> float:
        """
        Рассчитывает quality score через единый модуль app.core.quality.
        ИСПРАВЛЕНО (С-10): устранено дублирование формулы.
        """
        from backend.app.core.quality import calculate_quality_score

        # Вычисляем RMS total из компонент
        avg_rms_total = None
        if avg_rms_ra is not None and avg_rms_dec is not None:
            avg_rms_total = (avg_rms_ra**2 + avg_rms_dec**2) ** 0.5

        return calculate_quality_score(
            avg_hfr=avg_hfr,
            avg_eccentricity=None,  # Не доступно на уровне сессии
            acceptance_rate=acceptance_rate,
            avg_rms_total=avg_rms_total,
            hfr_trend=None,  # Не доступно при финализации
            problems_count=0,  # Будет обновлено позже
        )

    # ========================================================================
    # FRAME MANAGEMENT
    # ========================================================================
    async def log_frame(self, frame: FrameRecord) -> int:
        """
        Логирует кадр в базу данных.

        Args:
            frame: Данные кадра

        Returns:
            ID записи
        """
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO frames (
                    session_id, frame_index, timestamp,
                    exposure_time, filter_name, gain, offset, binning, temperature,
                    hfr, fwhm, eccentricity, star_count, median_adu,
                    rms_ra, rms_dec, rms_total, image_type
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    frame.session_id,
                    frame.frame_index,
                    frame.timestamp,
                    frame.exposure_time,
                    frame.filter_name,
                    frame.gain,
                    frame.offset,
                    frame.binning,
                    frame.temperature,
                    frame.hfr,
                    frame.fwhm,
                    frame.eccentricity,
                    frame.star_count,
                    frame.median_adu,
                    frame.rms_ra,
                    frame.rms_dec,
                    frame.rms_total,
                    frame.image_type,
                ),
            )
            frame_id = cursor.lastrowid
            await db.commit()

        # Инкрементируем счётчик кадров в сессии
        if frame.session_id in self._active_sessions:
            self._active_sessions[frame.session_id].frames_total += 1

        return frame_id

    async def log_frame_from_dict(
        self, session_id: str, frame_data: Dict
    ) -> Optional[int]:
        """
        Логирует кадр из словаря (удобно для интеграции с SessionWatcher).

        Args:
            session_id: ID сессии
            frame_data: Словарь с данными кадра

        Returns:
            ID записи или None
        """
        try:
            frame = FrameRecord(
                session_id=session_id,
                frame_index=frame_data.get("index", frame_data.get("Index", 0)),
                timestamp=datetime.now().isoformat(),
                exposure_time=frame_data.get(
                    "exposure_time", frame_data.get("ExposureTime")
                ),
                filter_name=frame_data.get("filter", frame_data.get("Filter")),
                gain=frame_data.get("gain", frame_data.get("Gain")),
                offset=frame_data.get("offset", frame_data.get("Offset")),
                temperature=frame_data.get(
                    "temperature", frame_data.get("Temperature")
                ),
                hfr=frame_data.get("hfr", frame_data.get("HFR")),
                fwhm=frame_data.get("fwhm", frame_data.get("FWHM")),
                star_count=frame_data.get("stars", frame_data.get("Stars")),
                rms_ra=frame_data.get("rms_ra", frame_data.get("RmsRA")),
                rms_dec=frame_data.get("rms_dec", frame_data.get("RmsDec")),
                rms_total=frame_data.get("rms_total", frame_data.get("RmsTotal")),
                image_type=frame_data.get(
                    "image_type", frame_data.get("ImageType", "LIGHT")
                ),
            )
            return await self.log_frame(frame)
        except Exception as e:
            logger.error(f"Failed to log frame: {e}")
            return None

    async def get_frames(
        self,
        session_id: str,
        image_type: Optional[str] = None,
        limit: int = 10000,
    ) -> List[FrameRecord]:
        """
        Получает кадры сессии.

        Args:
            session_id: ID сессии
            image_type: Фильтр по типу кадра (LIGHT, FLAT, ...)
            limit: Максимальное количество кадров

        Returns:
            Список FrameRecord
        """
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            query = "SELECT * FROM frames WHERE session_id = ?"
            params = [session_id]

            if image_type:
                query += " AND image_type = ?"
                params.append(image_type)

            query += " ORDER BY frame_index ASC LIMIT ?"
            params.append(limit)

            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()

        frames = []
        for row in rows:
            frames.append(
                FrameRecord(
                    session_id=row["session_id"],
                    frame_index=row["frame_index"],
                    timestamp=row["timestamp"],
                    exposure_time=row["exposure_time"],
                    filter_name=row["filter_name"],
                    gain=row["gain"],
                    offset=row["offset"],
                    binning=row["binning"],
                    temperature=row["temperature"],
                    hfr=row["hfr"],
                    fwhm=row["fwhm"],
                    eccentricity=row["eccentricity"],
                    star_count=row["star_count"],
                    median_adu=row["median_adu"],
                    rms_ra=row["rms_ra"],
                    rms_dec=row["rms_dec"],
                    rms_total=row["rms_total"],
                    image_type=row["image_type"],
                )
            )

        return frames

    # ========================================================================
    # PROBLEMS MANAGEMENT
    # ========================================================================
    async def log_problem(self, problem: SessionProblem) -> int:
        """Логирует проблему во время сессии."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO session_problems (
                    session_id, timestamp, problem_type, severity,
                    description, solution, resolved
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
                (
                    problem.session_id,
                    problem.timestamp,
                    problem.problem_type,
                    problem.severity,
                    problem.description,
                    problem.solution,
                    1 if problem.resolved else 0,
                ),
            )
            problem_id = cursor.lastrowid
            await db.commit()

        return problem_id

    async def get_problems(self, session_id: str) -> List[SessionProblem]:
        """Получает все проблемы сессии."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM session_problems WHERE session_id = ? ORDER BY timestamp",
                (session_id,),
            )
            rows = await cursor.fetchall()

        return [
            SessionProblem(
                session_id=row["session_id"],
                timestamp=row["timestamp"],
                problem_type=row["problem_type"],
                severity=row["severity"],
                description=row["description"],
                solution=row["solution"],
                resolved=bool(row["resolved"]),
            )
            for row in rows
        ]

    # ========================================================================
    # QUERY METHODS
    # ========================================================================
    async def get_session(self, session_id: str) -> Optional[SessionRecord]:
        """Получает запись о сессии."""
        # Проверяем кэш
        if session_id in self._active_sessions:
            return self._active_sessions[session_id]

        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM sessions WHERE session_id = ?", (session_id,)
            )
            row = await cursor.fetchone()

        if not row:
            return None

        return SessionRecord(
            session_id=row["session_id"],
            target_name=row["target_name"],
            date=row["date"],
            filter_name=row["filter_name"],
            exposure_time=row["exposure_time"],
            gain=row["gain"],
            temperature_setpoint=row["temperature_setpoint"],
            frames_total=row["frames_total"],
            frames_accepted=row["frames_accepted"],
            frames_rejected=row["frames_rejected"],
            acceptance_rate=row["acceptance_rate"],
            avg_hfr=row["avg_hfr"],
            avg_fwhm=row["avg_fwhm"],
            avg_rms_ra=row["avg_rms_ra"],
            avg_rms_dec=row["avg_rms_dec"],
            avg_temperature=row["avg_temperature"],
            quality_score=row["quality_score"],
            start_time=row["start_time"],
            end_time=row["end_time"],
            duration_minutes=row["duration_minutes"],
            avg_wind_speed=row["avg_wind_speed"],
            avg_humidity=row["avg_humidity"],
            avg_cloud_cover=row["avg_cloud_cover"],
            rag_indexed=bool(row["rag_indexed"]),
            rag_point_ids=row["rag_point_ids"],
        )

    async def get_sessions(
        self,
        target_name: Optional[str] = None,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        min_quality: Optional[float] = None,
        limit: int = 100,
    ) -> List[SessionRecord]:
        """
        Получает список сессий с фильтрацией.

        Args:
            target_name: Фильтр по имени цели
            date_from: Начальная дата (YYYY-MM-DD)
            date_to: Конечная дата (YYYY-MM-DD)
            min_quality: Минимальный quality_score
            limit: Максимальное количество сессий

        Returns:
            Список SessionRecord
        """
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row

            query = "SELECT * FROM sessions WHERE 1=1"
            params = []

            if target_name:
                query += " AND target_name LIKE ?"
                params.append(f"%{target_name}%")

            if date_from:
                query += " AND date >= ?"
                params.append(date_from)

            if date_to:
                query += " AND date <= ?"
                params.append(date_to)

            if min_quality is not None:
                query += " AND quality_score >= ?"
                params.append(min_quality)

            query += " ORDER BY date DESC LIMIT ?"
            params.append(limit)

            cursor = await db.execute(query, params)
            rows = await cursor.fetchall()

        sessions = []
        for row in rows:
            sessions.append(
                SessionRecord(
                    session_id=row["session_id"],
                    target_name=row["target_name"],
                    date=row["date"],
                    filter_name=row["filter_name"],
                    exposure_time=row["exposure_time"],
                    gain=row["gain"],
                    temperature_setpoint=row["temperature_setpoint"],
                    frames_total=row["frames_total"],
                    frames_accepted=row["frames_accepted"],
                    frames_rejected=row["frames_rejected"],
                    acceptance_rate=row["acceptance_rate"],
                    avg_hfr=row["avg_hfr"],
                    avg_fwhm=row["avg_fwhm"],
                    avg_rms_ra=row["avg_rms_ra"],
                    avg_rms_dec=row["avg_rms_dec"],
                    avg_temperature=row["avg_temperature"],
                    quality_score=row["quality_score"],
                    start_time=row["start_time"],
                    end_time=row["end_time"],
                    duration_minutes=row["duration_minutes"],
                    avg_wind_speed=row["avg_wind_speed"],
                    avg_humidity=row["avg_humidity"],
                    avg_cloud_cover=row["avg_cloud_cover"],
                    rag_indexed=bool(row["rag_indexed"]),
                    rag_point_ids=row["rag_point_ids"],
                )
            )

        return sessions

    async def get_session_stats(self, session_id: str) -> Dict[str, Any]:
        """
        Возвращает детальную статистику сессии.

        Включает:
        - Количество кадров по типам
        - Распределение HFR/FWHM
        - Временные тренды
        - Выявленные проблемы
        """
        session = await self.get_session(session_id)
        if not session:
            return {"error": "Session not found"}

        frames = await self.get_frames(session_id)
        problems = await self.get_problems(session_id)

        # Подсчёт по типам кадров
        by_type = {}
        for frame in frames:
            by_type[frame.image_type] = by_type.get(frame.image_type, 0) + 1

        # Распределение HFR
        hfr_values = [f.hfr for f in frames if f.hfr is not None]
        hfr_stats = {}
        if hfr_values:
            hfr_stats = {
                "count": len(hfr_values),
                "min": min(hfr_values),
                "max": max(hfr_values),
                "avg": sum(hfr_values) / len(hfr_values),
                "std": self._calculate_std(hfr_values),
            }

        # Распределение FWHM
        fwhm_values = [f.fwhm for f in frames if f.fwhm is not None]
        fwhm_stats = {}
        if fwhm_values:
            fwhm_stats = {
                "count": len(fwhm_values),
                "min": min(fwhm_values),
                "max": max(fwhm_values),
                "avg": sum(fwhm_values) / len(fwhm_values),
                "std": self._calculate_std(fwhm_values),
            }

        return {
            "session": session.model_dump(),
            "frames": {
                "total": len(frames),
                "by_type": by_type,
            },
            "hfr": hfr_stats,
            "fwhm": fwhm_stats,
            "problems": [p.model_dump() for p in problems],
            "problems_count": len(problems),
            "problems_resolved": sum(1 for p in problems if p.resolved),
        }

    def _calculate_std(self, values: List[float]) -> float:
        """Рассчитывает стандартное отклонение."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / (len(values) - 1)
        return variance**0.5

    # ========================================================================
    # EXPORT METHODS
    # ========================================================================
    async def export_session_csv(
        self,
        session_id: str,
        output_path: Path,
    ) -> bool:
        """
        Экспортирует все кадры сессии в CSV.

        Args:
            session_id: ID сессии
            output_path: Путь к выходному файлу

        Returns:
            True если успешно
        """
        import csv

        frames = await self.get_frames(session_id)
        if not frames:
            logger.warning(f"No frames to export for session {session_id}")
            return False

        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Записываем CSV
        with open(output_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)

            # Заголовок
            writer.writerow(
                [
                    "frame_index",
                    "timestamp",
                    "exposure_time",
                    "filter",
                    "gain",
                    "offset",
                    "binning",
                    "temperature",
                    "hfr",
                    "fwhm",
                    "eccentricity",
                    "star_count",
                    "median_adu",
                    "rms_ra",
                    "rms_dec",
                    "rms_total",
                    "image_type",
                ]
            )

            # Данные
            for frame in frames:
                writer.writerow(
                    [
                        frame.frame_index,
                        frame.timestamp,
                        frame.exposure_time,
                        frame.filter_name,
                        frame.gain,
                        frame.offset,
                        frame.binning,
                        frame.temperature,
                        frame.hfr,
                        frame.fwhm,
                        frame.eccentricity,
                        frame.star_count,
                        frame.median_adu,
                        frame.rms_ra,
                        frame.rms_dec,
                        frame.rms_total,
                        frame.image_type,
                    ]
                )

        logger.info(f"📤 Session exported to CSV: {output_path} ({len(frames)} frames)")
        return True

    # ========================================================================
    # STATISTICS
    # ========================================================================
    async def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику хранилища."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            # Количество сессий
            cursor = await db.execute("SELECT COUNT(*) FROM sessions")
            row = await cursor.fetchone()
            total_sessions = row[0]

            # Количество кадров
            cursor = await db.execute("SELECT COUNT(*) FROM frames")
            row = await cursor.fetchone()
            total_frames = row[0]

            # Количество проблем
            cursor = await db.execute("SELECT COUNT(*) FROM session_problems")
            row = await cursor.fetchone()
            total_problems = row[0]

            # Средний quality_score
            cursor = await db.execute(
                "SELECT AVG(quality_score) FROM sessions WHERE quality_score IS NOT NULL"
            )
            row = await cursor.fetchone()
            avg_quality = row[0] or 0.0

            # Лучшая сессия
            cursor = await db.execute("""
                SELECT session_id, target_name, quality_score 
                FROM sessions 
                WHERE quality_score IS NOT NULL 
                ORDER BY quality_score DESC 
                LIMIT 1
            """)
            row = await cursor.fetchone()
            best_session = None
            if row:
                best_session = {
                    "session_id": row[0],
                    "target": row[1],
                    "quality_score": row[2],
                }

        return {
            "total_sessions": total_sessions,
            "total_frames": total_frames,
            "total_problems": total_problems,
            "active_sessions": len(self._active_sessions),
            "avg_quality_score": round(avg_quality, 2),
            "best_session": best_session,
            "db_path": str(self.db_path),
        }

    async def mark_rag_indexed(
        self,
        session_id: str,
        point_ids: List[str],
    ) -> bool:
        """
        Помечает сессию как индексированную в RAG.

        Args:
            session_id: ID сессии
            point_ids: Список ID точек в Qdrant

        Returns:
            True если обновлено
        """
        return await self.update_session(
            session_id,
            rag_indexed=True,
            rag_point_ids=json.dumps(point_ids),
        )


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
sessions_metadata = SessionsMetadataStorage(Path("./data/sessions_metadata.db"))
