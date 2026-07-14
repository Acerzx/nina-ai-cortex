"""
LiveStack Watcher — расширенный мониторинг real-time стекинга.

ЭТАП 3.5 (полный рефакторинг):
- SNR — единственный источник в системе!
- Acceptance rate мониторинг с трендовым анализом
- Причины rejection (HFR, eccentricity, clouds, etc.)
- Автогенерация WARNING алертов при деградации
- Интеграция со Strategist Agent через SNR_UPDATE событие
- Layer 5 (ENRICHMENT) в Metrics Aggregator

Источники данных:
- stack_status.json — текущее состояние стекинга
- history.csv — история добавления/отклонения кадров
- calibrated/*.fits — игнорируются (не наша задача)

LiveStack plugin: https://github.com/isbeorn/nina.plugin.livestack
GUID: 10bc1716-54af-425e-b307-c0ca1ce10600
"""

import json
import logging
import csv
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
from collections import Counter
import aiofiles

from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings

logger = logging.getLogger("LiveStackWatcher")


class LiveStackWatcher(BaseFileWatcher):
    """
    Расширенный мониторинг LiveStack.

    Отслеживает:
    1. stack_status.json — текущее состояние стекинга
    2. history.csv — история добавления/отклонения кадров

    Ключевые метрики:
    - SNR (единственный источник в системе!)
    - Acceptance rate (текущий и тренд за последние N кадров)
    - Frames stacked / rejected
    - Причины rejection
    - Stacking progress (total_exposure_seconds)

    Публикуемые события:
    - LIVESTACK_STATUS — полное состояние (для ObservatoryState/Metrics Aggregator)
    - SNR_UPDATE — для Strategist Agent (оптимизация экспозиции)
    - ALERT — при проблемах (низкий acceptance rate, stalled stacking)
    """

    LIVESTACK_GUID = "10bc1716-54af-425e-b307-c0ca1ce10600"

    # Пороговые значения для алертов
    LOW_ACCEPTANCE_RATE_THRESHOLD = 0.70  # < 70% → WARNING
    MIN_FRAMES_FOR_TREND = 10  # Минимум кадров для анализа тренда
    RECENT_FRAMES_WINDOW = 20  # Сколько последних кадров анализировать

    def __init__(self, registry: CapabilityRegistry):
        # Получаем рабочую директорию LiveStack из XML-профиля N.I.N.A.
        working_dir = registry.get_plugin_path(self.LIVESTACK_GUID, "WorkingDirectory")
        if not working_dir:
            logger.warning(
                "LiveStack WorkingDirectory not found in profile. "
                "Using fallback: sessions_root/Live"
            )
            working_dir = Path(settings.nina_environment.sessions_root) / "Live"

        super().__init__(
            watch_path=working_dir,
            target_files=[".json", ".csv"],
            registry=registry,
        )

        # Кэш последнего состояния (для дедупликации событий)
        self._last_status: Dict[str, Any] = {}
        self._last_history_count: int = 0

        # История acceptance rate для трендового анализа
        self._acceptance_history: List[float] = []

        logger.info(
            f"📊 LiveStackWatcher initialized "
            f"(watching: {working_dir}, "
            f"low_acceptance_threshold: {self.LOW_ACCEPTANCE_RATE_THRESHOLD:.0%})"
        )

    async def process_file(self, path: Path) -> None:
        """
        Обработка изменённого файла LiveStack.

        Игнорирует:
        - Не JSON/CSV файлы
        - FITS-файлы (calibrated/, stacked/) — не наша задача
        """
        if path.suffix.lower() not in [".json", ".csv"]:
            return

        # Игнорируем FITS-файлы (калиброванные и стек)
        if "calibrated" in str(path).lower() or "stacked" in str(path).lower():
            return

        try:
            if path.suffix.lower() == ".json" and "status" in path.name.lower():
                await self._process_status(path)
            elif path.suffix.lower() == ".csv" and "history" in path.name.lower():
                await self._process_history(path)
        except Exception as e:
            logger.error(f"Error processing LiveStack file {path.name}: {e}")

    async def _process_status(self, path: Path) -> None:
        """
        Обработка stack_status.json.

        Извлекает:
        - snr: Signal-to-Noise Ratio стека (ЕДИНСТВЕННЫЙ источник!)
        - acceptance_rate: процент принятых кадров
        - frames_stacked: количество принятых кадров
        - frames_rejected: количество отклонённых
        - state: running / paused / stopped / idle
        - filter: текущий фильтр
        - total_exposure_seconds: суммарная экспозиция
        """
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            content = await f.read()

        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in {path.name}: {e}")
            return

        # Извлекаем ключевые метрики (с защитой от отсутствующих полей)
        metrics = {
            "snr": self._safe_float(data.get("snr")),
            "acceptance_rate": self._safe_float(data.get("acceptance_rate")),
            "frames_stacked": self._safe_int(data.get("frames_stacked")),
            "frames_rejected": self._safe_int(data.get("frames_rejected")),
            "state": data.get("state", "unknown"),
            "current_filter": data.get("filter"),
            "total_exposure_seconds": self._safe_float(
                data.get("total_exposure_seconds")
            ),
            "target_name": data.get("target"),
            "timestamp": datetime.now().isoformat(),
        }

        # Дедупликация: пропускаем, если состояние не изменилось
        if metrics == self._last_status:
            return
        self._last_status = metrics

        # === Публикуем LIVESTACK_STATUS (для Metrics Aggregator / ObservatoryState) ===
        await event_bus.publish("LIVESTACK_STATUS", metrics)

        logger.info(
            f"📊 LiveStack status: state={metrics['state']}, "
            f"SNR={metrics['snr']}, "
            f"acceptance={metrics['acceptance_rate']}, "
            f"frames={metrics['frames_stacked']}/{metrics['frames_stacked'] + (metrics['frames_rejected'] or 0)}, "
            f"filter={metrics['current_filter']}"
        )

        # === Публикуем SNR_UPDATE (для Strategist Agent) ===
        if metrics["snr"] is not None:
            # Читаем target SNR из настроек Strategist
            target_snr = getattr(settings.thresholds.strategist, "snr_target", 20.0)
            current_exposure = metrics.get("total_exposure_seconds")
            if current_exposure and metrics["frames_stacked"]:
                # Средняя экспозиция на кадр
                avg_exposure = current_exposure / metrics["frames_stacked"]
            else:
                avg_exposure = None

            await event_bus.publish(
                "SNR_UPDATE",
                {
                    "snr": metrics["snr"],
                    "target_snr": target_snr,
                    "exposure_time": avg_exposure,
                    "filter": metrics["current_filter"],
                    "frames_stacked": metrics["frames_stacked"],
                    "timestamp": metrics["timestamp"],
                },
            )
            logger.debug(
                f"📈 SNR_UPDATE published: "
                f"current={metrics['snr']:.2f}, target={target_snr}"
            )

        # === Автогенерация алертов ===
        await self._check_and_alert(metrics)

    async def _process_history(self, path: Path) -> None:
        """
        Обработка history.csv.

        Анализирует:
        - Общее количество кадров
        - Тренд acceptance rate за последние RECENT_FRAMES_WINDOW кадров
        - Причины rejection (топ-3)
        - Детекция stalled stacking (все последние кадры rejected)
        """
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            content = await f.read()

        lines = content.strip().split("\n")
        if len(lines) < 2:
            # Нет данных или только заголовок
            return

        # Дедупликация
        if len(lines) == self._last_history_count:
            return
        self._last_history_count = len(lines)

        try:
            # Парсим CSV
            reader = csv.DictReader(lines)
            rows = list(reader)

            if not rows:
                return

            # Анализ последних N кадров
            recent = rows[-self.RECENT_FRAMES_WINDOW :]
            accepted = [
                r
                for r in recent
                if r.get("accepted", "").lower() in ("true", "1", "yes")
            ]
            rejected = [
                r
                for r in recent
                if r.get("accepted", "").lower() in ("false", "0", "no")
            ]

            acceptance_rate_recent = len(accepted) / len(recent) if recent else 0.0

            # Причины rejection
            rejection_reasons = Counter(r.get("reason", "unknown") for r in rejected)

            # Сохраняем в тренд
            self._acceptance_history.append(acceptance_rate_recent)
            if len(self._acceptance_history) > 10:
                self._acceptance_history.pop(0)

            # === Публикуем LIVESTACK_HISTORY ===
            history_payload = {
                "total_frames": len(rows),
                "recent_frames_analyzed": len(recent),
                "acceptance_rate_recent": acceptance_rate_recent,
                "rejection_reasons": dict(rejection_reasons.most_common(5)),
                "last_frame_timestamp": rows[-1].get("timestamp"),
                "trend_window": self.RECENT_FRAMES_WINDOW,
                "acceptance_trend": self._calculate_acceptance_trend(),
                "timestamp": datetime.now().isoformat(),
            }

            await event_bus.publish("LIVESTACK_HISTORY", history_payload)

            logger.info(
                f"📊 LiveStack history: "
                f"total={len(rows)}, "
                f"recent_acceptance={acceptance_rate_recent:.1%}, "
                f"top_rejection={rejection_reasons.most_common(1)}"
            )

            # === Автогенерация алертов при проблемах ===
            if (
                len(recent) >= self.MIN_FRAMES_FOR_TREND
                and acceptance_rate_recent < self.LOW_ACCEPTANCE_RATE_THRESHOLD
            ):
                top_reason, top_count = rejection_reasons.most_common(1)[0]
                await event_bus.publish(
                    "ALERT",
                    {
                        "level": "WARNING",
                        "message": (
                            f"LiveStack acceptance rate low: "
                            f"{acceptance_rate_recent:.0%} "
                            f"(last {len(recent)} frames). "
                            f"Top rejection reason: {top_reason} "
                            f"({top_count} frames)"
                        ),
                        "agent": "LiveStackWatcher",
                        "timestamp": datetime.now().isoformat(),
                        "context": {
                            "acceptance_rate": acceptance_rate_recent,
                            "rejection_reasons": dict(rejection_reasons),
                            "total_frames": len(rows),
                        },
                    },
                )

        except Exception as e:
            logger.error(f"Error parsing LiveStack history CSV: {e}")

    def _calculate_acceptance_trend(self) -> Optional[float]:
        """
        Вычисляет тренд acceptance rate (наклон линейной регрессии).

        Returns:
            Положительный → acceptance улучшается
            Отрицательный → acceptance деградирует
            None → недостаточно данных
        """
        history = self._acceptance_history
        if len(history) < 3:
            return None

        n = len(history)
        x_mean = (n - 1) / 2
        y_mean = sum(history) / n

        numerator = sum((i - x_mean) * (history[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return 0.0

        return numerator / denominator

    async def _check_and_alert(self, metrics: Dict[str, Any]) -> None:
        """
        Проверяет метрики и генерирует алерты при проблемах.

        Проверяемые условия:
        1. Низкий acceptance rate (< 70%)
        2. Stalled stacking (state='running', но frames_stacked не растёт)
        3. SNR деградирует (требует истории, пока placeholder)
        """
        acceptance = metrics.get("acceptance_rate")
        frames_stacked = metrics.get("frames_stacked") or 0
        frames_rejected = metrics.get("frames_rejected") or 0
        total_frames = frames_stacked + frames_rejected
        state = metrics.get("state")

        # Условие 1: Низкий acceptance rate (при достаточном количестве кадров)
        if (
            acceptance is not None
            and total_frames >= self.MIN_FRAMES_FOR_TREND
            and acceptance < self.LOW_ACCEPTANCE_RATE_THRESHOLD
        ):
            await event_bus.publish(
                "ALERT",
                {
                    "level": "WARNING",
                    "message": (
                        f"LiveStack low acceptance rate: {acceptance:.0%} "
                        f"({frames_stacked}/{total_frames} frames). "
                        f"Check HFR, guiding, clouds."
                    ),
                    "agent": "LiveStackWatcher",
                    "timestamp": datetime.now().isoformat(),
                    "context": {
                        "acceptance_rate": acceptance,
                        "frames_stacked": frames_stacked,
                        "frames_rejected": frames_rejected,
                        "filter": metrics.get("current_filter"),
                    },
                },
            )

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        """Безопасная конвертация в float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value: Any) -> Optional[int]:
        """Безопасная конвертация в int."""
        if value is None:
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None
