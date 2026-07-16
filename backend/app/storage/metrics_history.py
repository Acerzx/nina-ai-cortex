"""
Metrics History Storage — долгосрочное хранение агрегированных метрик.
Реализация P2: гибридная история (in-memory + SQLite).

Архитектура:
- In-memory (MetricsAggregator.history): последние 100 точек, быстрые тренды
- SQLite (этот модуль): агрегация по минутам, долгосрочные тренды (24+ часа)

Таблица metrics_aggregated:
- minute_key: "2026-07-16 00:15" (ключ минуты)
- metric: "hfr", "fwhm", "rms_ra", etc.
- value: среднее значение за минуту (инкрементально обновляется)
- count: количество точек в агрегации
- min_value, max_value: экстремумы за минуту

Использование:
from app.storage.metrics_history import metrics_history

# Агрегация метрики (вызывается из MetricsAggregator)
await metrics_history.append_metric("hfr", 2.31)

# Получение долгосрочного тренда
trend = await metrics_history.get_trend("hfr", window_minutes=60)

# Получение агрегированных данных
data = await metrics_history.get_aggregated("hfr", hours=24)
"""

import asyncio
import logging
import aiosqlite
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass

logger = logging.getLogger("MetricsHistory")


@dataclass
class AggregatedMetric:
    """Агрегированная метрика за минуту."""

    minute_key: str
    metric: str
    value: float
    count: int
    min_value: float
    max_value: float
    timestamp: str


@dataclass
class LongTermTrend:
    """Долгосрочный тренд метрики."""

    metric: str
    window_minutes: int
    slope: float  # изменение значения в минуту
    r_squared: float  # качество линейной модели
    data_points: int
    first_value: float
    last_value: float
    change_percent: float
    interpretation: str  # "improving", "degrading", "stable"


class MetricsHistoryStorage:
    """
    SQLite хранилище для агрегированных метрик.

    Features:
    - Инкрементальная агрегация по минутам (UPSERT)
    - Автоматическая очистка старых записей
    - Долгосрочный трендовый анализ
    - Thread-safe через asyncio.Lock
    """

    # Метрики, которые агрегируются
    AGGREGATED_METRICS = frozenset(
        {
            "hfr",
            "fwhm",
            "eccentricity",
            "star_count",
            "rms_ra",
            "rms_dec",
            "rms_total",
            "camera_temp",
            "focuser_temp",
            "wind_speed",
            "humidity",
            "cloud_cover",
            "snr",
        }
    )

    def __init__(self, db_path: Path, retention_hours: int = 24):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.retention_hours = retention_hours

        self._db_initialized = False
        self._lock = asyncio.Lock()

        # Кэш текущей минуты для batch-обновлений
        self._current_minute_cache: Dict[str, Dict[str, Any]] = {}

        # Статистика
        self._stats = {
            "appends_total": 0,
            "upserts_new": 0,
            "upserts_update": 0,
            "cleanups_performed": 0,
            "records_deleted": 0,
        }

        logger.info(
            f"📊 Metrics History Storage initialized "
            f"(db: {self.db_path}, retention: {retention_hours}h)"
        )

    async def _ensure_db_initialized(self) -> None:
        """Гарантирует, что БД инициализирована."""
        if self._db_initialized:
            return

        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS metrics_aggregated (
                    minute_key TEXT NOT NULL,
                    metric TEXT NOT NULL,
                    value REAL NOT NULL,
                    count INTEGER NOT NULL,
                    min_value REAL NOT NULL,
                    max_value REAL NOT NULL,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (minute_key, metric)
                )
            """)

            # Индекс для быстрого поиска по метрике и времени
            await db.execute("""
                CREATE INDEX IF NOT EXISTS idx_metric_time
                ON metrics_aggregated(metric, minute_key)
            """)

            await db.commit()

        self._db_initialized = True
        logger.debug("✅ Metrics History DB initialized")

    async def append_metric(self, metric: str, value: Optional[float]) -> bool:
        """
        Добавляет метрику с агрегацией по минутам.

        Алгоритм:
        1. Вычисляем minute_key (текущая минута: "2026-07-16 00:15")
        2. Проверяем кэш текущей минуты
        3. Если минута новая → INSERT
        4. Если минута существует → инкрементальное UPDATE среднего

        Инкрементальная формула среднего:
        new_avg = (old_avg * old_count + new_value) / (old_count + 1)

        Args:
            metric: Имя метрики (hfr, fwhm, etc.)
            value: Значение метрики

        Returns:
            True если метрика агрегирована
        """
        if metric not in self.AGGREGATED_METRICS:
            return False

        if value is None:
            return False

        await self._ensure_db_initialized()

        # Вычисляем ключ текущей минуты
        now = datetime.now()
        minute_key = now.strftime("%Y-%m-%d %H:%M")

        self._stats["appends_total"] += 1

        async with self._lock:
            async with aiosqlite.connect(self.db_path) as db:
                # Проверяем, существует ли запись для этой минуты и метрики
                cursor = await db.execute(
                    """
                    SELECT value, count, min_value, max_value
                    FROM metrics_aggregated
                    WHERE minute_key = ? AND metric = ?
                    """,
                    (minute_key, metric),
                )
                row = await cursor.fetchone()

                if row is None:
                    # Новая запись — INSERT
                    await db.execute(
                        """
                        INSERT INTO metrics_aggregated
                        (minute_key, metric, value, count, min_value, max_value, updated_at)
                        VALUES (?, ?, ?, 1, ?, ?, ?)
                        """,
                        (minute_key, metric, value, value, value, now.isoformat()),
                    )
                    self._stats["upserts_new"] += 1
                else:
                    # Существующая запись — инкрементальное UPDATE
                    old_avg, old_count, old_min, old_max = row
                    new_count = old_count + 1
                    new_avg = (old_avg * old_count + value) / new_count
                    new_min = min(old_min, value)
                    new_max = max(old_max, value)

                    await db.execute(
                        """
                        UPDATE metrics_aggregated
                        SET value = ?, count = ?, min_value = ?, max_value = ?, updated_at = ?
                        WHERE minute_key = ? AND metric = ?
                        """,
                        (
                            new_avg,
                            new_count,
                            new_min,
                            new_max,
                            now.isoformat(),
                            minute_key,
                            metric,
                        ),
                    )
                    self._stats["upserts_update"] += 1

                await db.commit()

        return True

    async def get_aggregated(
        self,
        metric: str,
        hours: float = 1.0,
    ) -> List[AggregatedMetric]:
        """
        Возвращает агрегированные данные за указанный период.

        Args:
            metric: Имя метрики
            hours: Период в часах (default: 1 час)

        Returns:
            Список AggregatedMetric, отсортированный по времени
        """
        await self._ensure_db_initialized()

        cutoff = datetime.now() - timedelta(hours=hours)
        cutoff_key = cutoff.strftime("%Y-%m-%d %H:%M")

        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """
                SELECT minute_key, metric, value, count, min_value, max_value, updated_at
                FROM metrics_aggregated
                WHERE metric = ? AND minute_key >= ?
                ORDER BY minute_key ASC
                """,
                (metric, cutoff_key),
            )
            rows = await cursor.fetchall()

            return [
                AggregatedMetric(
                    minute_key=row["minute_key"],
                    metric=row["metric"],
                    value=row["value"],
                    count=row["count"],
                    min_value=row["min_value"],
                    max_value=row["max_value"],
                    timestamp=row["updated_at"],
                )
                for row in rows
            ]

    async def get_trend(
        self,
        metric: str,
        window_minutes: int = 60,
    ) -> Optional[LongTermTrend]:
        """
        Вычисляет долгосрочный тренд метрики.

        Использует линейную регрессию на агрегированных данных.

        Args:
            metric: Имя метрики
            window_minutes: Окно анализа в минутах (default: 60)

        Returns:
            LongTermTrend или None если недостаточно данных
        """
        hours = window_minutes / 60.0
        data = await self.get_aggregated(metric, hours=hours)

        if len(data) < 3:
            return None

        # Извлекаем значения
        values = [d.value for d in data]

        # Линейная регрессия (индексы как X)
        from app.core.math_utils import linear_regression, calculate_r_squared

        slope, intercept = linear_regression(values)
        r_squared = calculate_r_squared(values, slope, intercept)

        # Интерпретация тренда
        first_value = values[0]
        last_value = values[-1]

        if first_value == 0:
            change_percent = 0.0
        else:
            change_percent = ((last_value - first_value) / abs(first_value)) * 100

        # Интерпретация зависит от метрики
        # Для HFR/FWHM: рост = деградация
        # Для SNR: рост = улучшение
        degrading_metrics = {
            "hfr",
            "fwhm",
            "eccentricity",
            "rms_ra",
            "rms_dec",
            "rms_total",
        }
        improving_metrics = {"snr", "star_count"}

        if metric in degrading_metrics:
            if slope > 0.01:
                interpretation = "degrading"
            elif slope < -0.01:
                interpretation = "improving"
            else:
                interpretation = "stable"
        elif metric in improving_metrics:
            if slope > 0.01:
                interpretation = "improving"
            elif slope < -0.01:
                interpretation = "degrading"
            else:
                interpretation = "stable"
        else:
            # Нейтральные метрики (температура, ветер)
            if abs(slope) > 0.01:
                interpretation = "changing"
            else:
                interpretation = "stable"

        return LongTermTrend(
            metric=metric,
            window_minutes=window_minutes,
            slope=slope,
            r_squared=r_squared,
            data_points=len(values),
            first_value=first_value,
            last_value=last_value,
            change_percent=change_percent,
            interpretation=interpretation,
        )

    async def cleanup_old_records(self, retention_hours: Optional[int] = None) -> int:
        """
        Удаляет записи старше retention периода.

        Args:
            retention_hours: Период хранения (None = из self.retention_hours)

        Returns:
            Количество удалённых записей
        """
        hours = retention_hours if retention_hours is not None else self.retention_hours
        cutoff = datetime.now() - timedelta(hours=hours)
        cutoff_key = cutoff.strftime("%Y-%m-%d %H:%M")

        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM metrics_aggregated WHERE minute_key < ?",
                (cutoff_key,),
            )
            deleted = cursor.rowcount
            await db.commit()

        if deleted > 0:
            self._stats["cleanups_performed"] += 1
            self._stats["records_deleted"] += deleted
            logger.info(
                f"🗑️ Metrics History cleanup: {deleted} records older than {hours}h deleted"
            )

        return deleted

    async def get_all_metrics_summary(self) -> Dict[str, Any]:
        """Возвращает сводку по всем агрегированным метрикам."""
        await self._ensure_db_initialized()

        async with aiosqlite.connect(self.db_path) as db:
            # Количество записей по метрикам
            cursor = await db.execute("""
                SELECT metric, COUNT(*) as count,
                       MIN(minute_key) as oldest,
                       MAX(minute_key) as newest
                FROM metrics_aggregated
                GROUP BY metric
                ORDER BY metric
            """)
            rows = await cursor.fetchall()

            by_metric = {}
            for row in rows:
                by_metric[row[0]] = {
                    "records": row[1],
                    "oldest": row[2],
                    "newest": row[3],
                }

            # Общее количество записей
            cursor = await db.execute("SELECT COUNT(*) FROM metrics_aggregated")
            row = await cursor.fetchone()
            total_records = row[0]

            # Размер БД
            db_size_mb = (
                self.db_path.stat().st_size / (1024 * 1024)
                if self.db_path.exists()
                else 0
            )

        return {
            "total_records": total_records,
            "by_metric": by_metric,
            "db_size_mb": round(db_size_mb, 2),
            "db_path": str(self.db_path),
            "retention_hours": self.retention_hours,
        }

    async def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику хранилища."""
        summary = await self.get_all_metrics_summary()
        return {
            **summary,
            "operations": self._stats,
            "aggregated_metrics": sorted(self.AGGREGATED_METRICS),
        }


# Singleton instance
metrics_history = MetricsHistoryStorage(
    db_path=Path("./data/metrics_history.db"),
    retention_hours=24,
)
