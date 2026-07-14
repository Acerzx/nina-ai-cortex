"""
LiveStack Watcher - расширенный мониторинг real-time стекинга.
Мониторит рабочую папку LiveStack (stack_status.json, history.csv).
Устраняет Упрощение #3.

ЭТАП 9 (расширение):
- Парсинг SNR, acceptance_rate, frames_stacked/rejected из stack_status.json
- Trend analysis из history.csv (последние N кадров)
- Публикация расширенного события LIVESTACK_ENHANCED
- Интеграция со Strategist Agent через SNR_UPDATE событие
- Рекомендации на основе SNR и acceptance rate
"""

import json
import logging
import csv
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import aiofiles
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings

logger = logging.getLogger("LiveStackWatcher")


class LiveStackWatcher(BaseFileWatcher):
    """
    Расширенный мониторинг LiveStack.

    Отслеживает:
    - stack_status.json: текущее состояние стекинга (SNR, acceptance_rate, frames)
    - history.csv: история добавления/отклонения кадров

    Публикует события:
    - LIVESTACK_STATUS: базовое состояние (для обратной совместимости)
    - LIVESTACK_ENHANCED: расширенная аналитика (SNR, trends, recommendations)
    - SNR_UPDATE: обновление SNR для Strategist Agent
    """

    LIVESTACK_GUID = "10bc1716-54af-425e-b307-c0ca1ce10600"

    # История последних значений для trend analysis
    SNR_HISTORY_SIZE = 20
    ACCEPTANCE_HISTORY_SIZE = 20

    def __init__(self, registry: CapabilityRegistry):
        working_dir = registry.get_plugin_path(self.LIVESTACK_GUID, "WorkingDirectory")
        if not working_dir:
            logger.warning(
                "LiveStack WorkingDirectory not found in profile. Using fallback."
            )
            working_dir = Path(settings.nina_environment.sessions_root) / "Live"

        super().__init__(
            watch_path=working_dir, target_files=[".json", ".csv"], registry=registry
        )

        # Кэш последних значений для trend analysis
        self._snr_history: List[float] = []
        self._acceptance_history: List[float] = []
        self._last_status_hash: Optional[str] = None

        # Thresholds для рекомендаций
        self.snr_target = getattr(settings.thresholds.strategist, "snr_target", 20.0)
        self.acceptance_threshold = getattr(
            settings.thresholds.strategist, "acceptance_rate_target", 0.90
        )

        logger.info(
            f"LiveStackWatcher initialized (watching: {working_dir}, "
            f"snr_target: {self.snr_target}, acceptance_threshold: {self.acceptance_threshold})"
        )

    async def process_file(self, path: Path) -> None:
        """Обработка измененного файла LiveStack."""
        if path.suffix.lower() not in [".json", ".csv"]:
            return

        # Игнорируем FITS-файлы (калиброванные и стек)
        if "calibrated" in path.name.lower() or "stacked" in path.name.lower():
            return

        try:
            if path.suffix.lower() == ".json" and "status" in path.name.lower():
                await self._process_status(path)
            elif path.suffix.lower() == ".csv" and "history" in path.name.lower():
                await self._process_history(path)
        except Exception as e:
            logger.error(f"Error processing LiveStack file {path.name}: {e}")

    async def _process_status(self, path: Path):
        """
        Обработка stack_status.json с расширенным парсингом.

        Ожидаемые поля:
        - state: текущее состояние (running, paused, stopped)
        - snr: текущий SNR стека
        - frames_stacked: количество принятых кадров
        - frames_rejected: количество отклоненных кадров
        - acceptance_rate: процент принятых кадров (0.0-1.0)
        - total_exposure: суммарная экспозиция в секундах
        - filter: текущий фильтр
        - target: имя цели
        """
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            content = await f.read()

        try:
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.warning(f"Invalid JSON in {path.name}: {e}")
            return

        # Проверка на дубликаты (hash-based)
        status_hash = json.dumps(data, sort_keys=True)
        if status_hash == self._last_status_hash:
            return  # Данные не изменились
        self._last_status_hash = status_hash

        # === Базовое событие (для обратной совместимости) ===
        await event_bus.publish("LIVESTACK_STATUS", data)

        # === Извлечение расширенных метрик ===
        snr = data.get("snr")
        frames_stacked = data.get("frames_stacked", 0)
        frames_rejected = data.get("frames_rejected", 0)
        acceptance_rate = data.get("acceptance_rate")
        total_exposure = data.get("total_exposure", 0.0)
        filter_name = data.get("filter")
        target_name = data.get("target")
        state = data.get("state", "unknown")

        # === Обновление истории для trend analysis ===
        if snr is not None:
            self._snr_history.append(snr)
            if len(self._snr_history) > self.SNR_HISTORY_SIZE:
                self._snr_history.pop(0)

        if acceptance_rate is not None:
            self._acceptance_history.append(acceptance_rate)
            if len(self._acceptance_history) > self.ACCEPTANCE_HISTORY_SIZE:
                self._acceptance_history.pop(0)

        # === Trend analysis ===
        snr_trend = self._calculate_trend(self._snr_history)
        acceptance_trend = self._calculate_trend(self._acceptance_history)

        # === Генерация рекомендаций ===
        recommendations = self._generate_recommendations(
            snr=snr,
            acceptance_rate=acceptance_rate,
            frames_stacked=frames_stacked,
            frames_rejected=frames_rejected,
            snr_trend=snr_trend,
            acceptance_trend=acceptance_trend,
        )

        # === Публикация расширенного события ===
        enhanced_data = {
            "timestamp": datetime.now().isoformat(),
            "state": state,
            "snr": snr,
            "snr_target": self.snr_target,
            "snr_trend": snr_trend,
            "frames_stacked": frames_stacked,
            "frames_rejected": frames_rejected,
            "acceptance_rate": acceptance_rate,
            "acceptance_threshold": self.acceptance_threshold,
            "acceptance_trend": acceptance_trend,
            "total_exposure": total_exposure,
            "filter": filter_name,
            "target": target_name,
            "recommendations": recommendations,
        }

        await event_bus.publish("LIVESTACK_ENHANCED", enhanced_data)

        # === Интеграция со Strategist Agent ===
        if snr is not None:
            await event_bus.publish(
                "SNR_UPDATE",
                {
                    "snr": snr,
                    "snr_target": self.snr_target,
                    "filter": filter_name,
                    "target": target_name,
                    "total_exposure": total_exposure,
                },
            )

        logger.info(
            f"LiveStack status: state={state}, SNR={snr}, "
            f"frames={frames_stacked}/{frames_stacked + frames_rejected}, "
            f"acceptance={acceptance_rate:.2% if acceptance_rate else 'N/A'}, "
            f"filter={filter_name}"
        )

    async def _process_history(self, path: Path):
        """
        Обработка history.csv для детального trend analysis.

        Ожидаемые колонки:
        - timestamp: время кадра
        - frame_index: индекс кадра
        - accepted: принят/отклонен (true/false)
        - reason: причина отклонения (если accepted=false)
        - hfr: HFR кадра
        - fwhm: FWHM кадра
        """
        async with aiofiles.open(path, "r", encoding="utf-8") as f:
            content = await f.read()

        lines = content.strip().split("\n")
        if len(lines) < 2:
            return  # Только заголовок или пусто

        try:
            reader = csv.DictReader(lines)
            rows = list(reader)

            if not rows:
                return

            # Анализ последних N кадров
            recent_frames = rows[-self.ACCEPTANCE_HISTORY_SIZE :]

            # Подсчет acceptance rate для последних кадров
            accepted_count = sum(
                1 for row in recent_frames if row.get("accepted", "").lower() == "true"
            )
            recent_acceptance_rate = accepted_count / len(recent_frames)

            # Анализ причин отклонения
            rejection_reasons = {}
            for row in recent_frames:
                if row.get("accepted", "").lower() == "false":
                    reason = row.get("reason", "unknown")
                    rejection_reasons[reason] = rejection_reasons.get(reason, 0) + 1

            # Публикация детальной истории
            await event_bus.publish(
                "LIVESTACK_HISTORY",
                {
                    "timestamp": datetime.now().isoformat(),
                    "total_frames": len(rows),
                    "recent_frames": len(recent_frames),
                    "recent_acceptance_rate": recent_acceptance_rate,
                    "rejection_reasons": rejection_reasons,
                    "last_frame": rows[-1] if rows else None,
                },
            )

            logger.debug(
                f"LiveStack history: {len(rows)} total frames, "
                f"recent acceptance: {recent_acceptance_rate:.2%}, "
                f"top rejection: {max(rejection_reasons.items(), key=lambda x: x[1], default=('none', 0))}"
            )

        except Exception as e:
            logger.error(f"Error parsing LiveStack history CSV: {e}")

    def _calculate_trend(self, history: List[float]) -> Optional[str]:
        """
        Расчет тренда на основе истории значений.

        Returns:
            "improving" - значения растут (для SNR) или падают (для rejection)
            "degrading" - значения падают (для SNR) или растут (для rejection)
            "stable" - значения стабильны
            None - недостаточно данных
        """
        if len(history) < 5:
            return None

        # Берем последние 10 значений (или все, если меньше)
        recent = history[-10:]
        if len(recent) < 5:
            return None

        # Линейная регрессия для определения тренда
        n = len(recent)
        x_mean = (n - 1) / 2.0
        y_mean = sum(recent) / n

        numerator = sum((i - x_mean) * (recent[i] - y_mean) for i in range(n))
        denominator = sum((i - x_mean) ** 2 for i in range(n))

        if denominator == 0:
            return "stable"

        slope = numerator / denominator

        # Порог для определения тренда (5% от среднего значения)
        threshold = abs(y_mean) * 0.05 if y_mean != 0 else 0.1

        if slope > threshold:
            return "improving"
        elif slope < -threshold:
            return "degrading"
        else:
            return "stable"

    def _generate_recommendations(
        self,
        snr: Optional[float],
        acceptance_rate: Optional[float],
        frames_stacked: int,
        frames_rejected: int,
        snr_trend: Optional[str],
        acceptance_trend: Optional[str],
    ) -> List[str]:
        """
        Генерация рекомендаций на основе метрик LiveStack.

        Returns:
            Список текстовых рекомендаций
        """
        recommendations = []

        # === SNR-based рекомендации ===
        if snr is not None:
            if snr < self.snr_target * 0.5:
                recommendations.append(
                    f"SNR очень низкий ({snr:.1f} vs target {self.snr_target:.1f}). "
                    f"Рассмотрите увеличение экспозиции или переход на более чувствительный фильтр."
                )
            elif snr < self.snr_target * 0.8:
                recommendations.append(
                    f"SNR ниже целевого ({snr:.1f} vs {self.snr_target:.1f}). "
                    f"Можно увеличить экспозицию на 20-30%."
                )

            if snr_trend == "degrading":
                recommendations.append(
                    "SNR деградирует. Проверьте условия наблюдения (облачность, световое загрязнение)."
                )

        # === Acceptance rate рекомендации ===
        if acceptance_rate is not None:
            total_frames = frames_stacked + frames_rejected

            if acceptance_rate < 0.5 and total_frames >= 10:
                recommendations.append(
                    f"Очень низкий acceptance rate ({acceptance_rate:.1%}). "
                    f"Проверьте качество гидирования, фокус и условия наблюдения."
                )
            elif acceptance_rate < self.acceptance_threshold and total_frames >= 10:
                recommendations.append(
                    f"Acceptance rate ниже целевого ({acceptance_rate:.1%} vs {self.acceptance_threshold:.1%}). "
                    f"Рассмотрите ужесточение критериев отбора кадров."
                )

            if acceptance_trend == "degrading":
                recommendations.append(
                    "Acceptance rate деградирует. Возможны проблемы с оборудованием или условиями."
                )

        # === Рекомендации на основе количества кадров ===
        if frames_stacked >= 50 and snr is not None and snr >= self.snr_target:
            recommendations.append(
                f"Достигнут целевой SNR ({snr:.1f}) после {frames_stacked} кадров. "
                f"Можно завершить съемку этой цели."
            )

        return recommendations

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику LiveStackWatcher."""
        return {
            "snr_history_size": len(self._snr_history),
            "acceptance_history_size": len(self._acceptance_history),
            "last_snr": self._snr_history[-1] if self._snr_history else None,
            "last_acceptance_rate": (
                self._acceptance_history[-1] if self._acceptance_history else None
            ),
            "snr_target": self.snr_target,
            "acceptance_threshold": self.acceptance_threshold,
        }
