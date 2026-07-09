"""
Metrics Source Monitor — автоматическое переключение между источниками метрик.
Реализация идеи 6: мониторинг качества источников и автовыбор лучшего.

Архитектура:
- Измеряет latency, error rate, completeness для каждого источника
- Scoring function выбирает лучший источник
- Автоматическое переключение с публикацией алерта
- Ручной override через API
- Feature flag: feature_flags.metrics.auto_source_selection

Использование:
    from app.core.metrics_source_monitor import metrics_source_monitor

    # Автоматический выбор
    best_source = await metrics_source_monitor.select_best_source()

    # Ручной override
    metrics_source_monitor.set_manual_override("prometheus")
"""

import logging
import asyncio
import time
from typing import Dict, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field

from app.core.config import settings
from app.core.events import event_bus
from app.agents.observatory_state import observatory_state

logger = logging.getLogger("MetricsSourceMonitor")


@dataclass
class SourceQuality:
    """Качество одного источника метрик."""

    name: str
    latency_ms: float = 0.0
    error_rate: float = 0.0
    completeness: float = 1.0
    last_check: Optional[str] = None
    consecutive_errors: int = 0
    total_checks: int = 0
    successful_checks: int = 0

    @property
    def is_healthy(self) -> bool:
        """Источник здоров если нет ошибок и latency приемлема."""
        return self.error_rate < 0.3 and self.latency_ms < 5000

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "latency_ms": round(self.latency_ms, 2),
            "error_rate": round(self.error_rate, 3),
            "completeness": round(self.completeness, 3),
            "last_check": self.last_check,
            "consecutive_errors": self.consecutive_errors,
            "total_checks": self.total_checks,
            "successful_checks": self.successful_checks,
            "is_healthy": self.is_healthy,
        }


class MetricsSourceMonitor:
    """
    Монитор качества источников метрик.

    Автоматически переключается между InfluxDB и Prometheus
    на основе качества (latency, error rate, completeness).

    При переключении:
    1. Публикуется METRICS_SOURCE_CHANGED событие
    2. Логируется в ObservatoryState
    3. Отправляется WARNING алерт
    """

    # Ожидаемое количество метрик в полном срезе
    EXPECTED_METRICS_COUNT = 25

    # История измерений (для расчёта error rate)
    HISTORY_SIZE = 20

    def __init__(self):
        self.sources: Dict[str, SourceQuality] = {
            "influxdb": SourceQuality(name="influxdb"),
            "prometheus": SourceQuality(name="prometheus"),
        }

        # Текущий активный источник
        self._active_source = self._load_primary_source()

        # Ручной override (если установлен — автопереключение отключено)
        self._manual_override: Optional[str] = None
        self._override_reason: Optional[str] = None

        # Feature flag
        self._auto_selection_enabled = self._load_auto_selection_flag()

        # История измерений для каждого источника
        self._history: Dict[str, list] = {
            "influxdb": [],
            "prometheus": [],
        }

        # Статистика переключений
        self._stats = {
            "total_switches": 0,
            "last_switch_time": None,
            "last_switch_reason": None,
        }

        # Время старта для grace period
        self._start_time = datetime.now()

        # Grace period: не переключаемся первые N секунд после старта
        # чтобы дать всем источникам время прислать первые данные
        self.GRACE_PERIOD_SECONDS = 30

        logger.info(
            f"📊 Metrics Source Monitor initialized "
            f"(auto: {self._auto_selection_enabled}, "
            f"active: {self._active_source})"
        )

    def _load_primary_source(self) -> str:
        """Загружает основной источник из settings."""
        try:
            ds = getattr(settings, "data_sources", None)
            if ds:
                return getattr(ds, "primary_metrics_source", "influxdb")
        except Exception:
            pass
        return "influxdb"

    def _load_auto_selection_flag(self) -> bool:
        """Загружает feature flag."""
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                metrics_ff = getattr(ff, "metrics", None)
                if metrics_ff:
                    return getattr(metrics_ff, "auto_source_selection", True)
        except Exception:
            pass
        return True

    async def check_and_switch(self) -> str:
        """
        Проверяет качество всех источников и переключается при необходимости.
        ИСПРАВЛЕНО (v4.2): Добавлен grace period после старта — первые 30 секунд
        автопереключение не выполняется, чтобы дать всем источникам время
        прислать первые данные.
        """
        if not self._auto_selection_enabled:
            return self._active_source

        if self._manual_override:
            return self._active_source

        # Grace period: первые 30 секунд после старта не переключаемся
        if self._start_time:
            elapsed = (datetime.now() - self._start_time).total_seconds()
            if elapsed < self.GRACE_PERIOD_SECONDS:
                logger.debug(
                    f"Metrics Source Monitor: grace period active "
                    f"({elapsed:.0f}/{self.GRACE_PERIOD_SECONDS}s), skipping check"
                )
                return self._active_source

        # Измеряем качество каждого источника
        for source_name in self.sources.keys():
            await self._measure_source(source_name)

        # Выбираем лучший
        best_source = self._select_best()

        # Переключаемся если нужно
        if best_source != self._active_source:
            await self._switch_to(best_source, reason="auto_quality_based")

        return self._active_source

    async def _measure_source(self, source_name: str) -> SourceQuality:
        """Измеряет качество одного источника."""
        source = self.sources[source_name]
        source.total_checks += 1

        start_time = time.perf_counter()

        try:
            # Проверяем доступность через observatory_state
            if source_name == "influxdb":
                is_available = self._check_influxdb()
                metrics_count = self._count_influxdb_metrics()
            else:  # prometheus
                is_available = self._check_prometheus()
                metrics_count = self._count_prometheus_metrics()

            latency = (time.perf_counter() - start_time) * 1000

            if is_available:
                source.successful_checks += 1
                source.consecutive_errors = 0
                source.completeness = min(
                    1.0, metrics_count / self.EXPECTED_METRICS_COUNT
                )
            else:
                source.consecutive_errors += 1
                source.completeness = 0.0

            source.latency_ms = latency
            source.last_check = datetime.now().isoformat()

            # Обновляем error rate (на основе истории)
            history = self._history[source_name]
            history.append(1 if is_available else 0)
            if len(history) > self.HISTORY_SIZE:
                history.pop(0)

            source.error_rate = 1.0 - (sum(history) / len(history)) if history else 0.0

        except Exception as e:
            logger.debug(f"Error measuring {source_name}: {e}")
            source.consecutive_errors += 1
            source.error_rate = 1.0
            source.completeness = 0.0

        return source

    def _check_influxdb(self) -> bool:
        """Проверяет доступность InfluxDB."""
        ds = observatory_state._influxdb_last_update
        if not ds:
            return False
        return (datetime.now() - ds).total_seconds() < observatory_state._source_timeout

    def _check_prometheus(self) -> bool:
        """Проверяет доступность Prometheus."""
        ds = observatory_state._prometheus_last_update
        if not ds:
            return False
        return (datetime.now() - ds).total_seconds() < observatory_state._source_timeout

    def _count_influxdb_metrics(self) -> int:
        """Подсчитывает количество метрик из InfluxDB."""
        return sum(
            1 for v in observatory_state.current_metrics.values() if v is not None
        )

    def _count_prometheus_metrics(self) -> int:
        """Подсчитывает количество метрик из Prometheus."""
        return self._count_influxdb_metrics()  # Метрики объединены

    def _select_best(self) -> str:
        """Выбирает лучший источник на основе scoring function."""
        scores = {}

        for name, source in self.sources.items():
            if not source.is_healthy:
                scores[name] = 0.0
                continue

            # Scoring function
            # Меньше latency = лучше (0-100)
            latency_score = max(0, 100 - source.latency_ms / 50)

            # Больше completeness = лучше (0-100)
            completeness_score = source.completeness * 100

            # Меньше error rate = лучше (0-100)
            error_score = (1 - source.error_rate) * 100

            # Weighted score
            score = latency_score * 0.3 + completeness_score * 0.5 + error_score * 0.2

            # Бонус для основного источника (hysteresis)
            if name == self._load_primary_source():
                score += 5.0

            scores[name] = score

        # Выбираем источник с максимальным score
        return max(scores.items(), key=lambda x: x[1])[0]

    async def _switch_to(self, new_source: str, reason: str):
        """Переключается на новый источник."""
        old_source = self._active_source
        self._active_source = new_source
        self._stats["total_switches"] += 1
        self._stats["last_switch_time"] = datetime.now().isoformat()
        self._stats["last_switch_reason"] = reason

        logger.warning(
            f"🔄 Metrics source switched: {old_source} → {new_source} "
            f"(reason: {reason})"
        )

        # Публикуем событие
        await event_bus.publish(
            "METRICS_SOURCE_CHANGED",
            {
                "old_source": old_source,
                "new_source": new_source,
                "reason": reason,
                "timestamp": datetime.now().isoformat(),
                "sources_quality": {
                    name: source.to_dict() for name, source in self.sources.items()
                },
            },
        )

        # ИСПРАВЛЕНО (v4.2): log_ai_action — это async метод, используем await
        await observatory_state.log_ai_action(
            agent="MetricsSourceMonitor",
            action=f"Switch source: {old_source} → {new_source}",
            reason=reason,
            result="Source switched successfully",
        )

        # Отправляем WARNING алерт
        await event_bus.publish(
            "ALERT",
            {
                "level": "WARNING",
                "message": (
                    f"Источник метрик переключён: {old_source} → {new_source} "
                    f"(причина: {reason})"
                ),
                "agent": "MetricsSourceMonitor",
                "timestamp": datetime.now().isoformat(),
            },
        )

    def set_manual_override(self, source: str, reason: str = "manual"):
        """
        Устанавливает ручной override.
        Отключает автоматическое переключение.
        """
        if source not in self.sources:
            logger.error(f"Unknown source: {source}")
            return

        self._manual_override = source
        self._override_reason = reason
        self._active_source = source

        logger.info(
            f"🔒 Manual override set: {source} (reason: {reason}). "
            f"Auto-switching disabled."
        )

    def clear_manual_override(self):
        """Снимает ручной override. Включает автопереключение."""
        if self._manual_override:
            logger.info(
                f"🔓 Manual override cleared "
                f"(was: {self._manual_override}). "
                f"Auto-switching enabled."
            )
            self._manual_override = None
            self._override_reason = None

    def get_active_source(self) -> str:
        """Возвращает текущий активный источник."""
        return self._active_source

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает полную статистику монитора."""
        return {
            "active_source": self._active_source,
            "auto_selection_enabled": self._auto_selection_enabled,
            "manual_override": self._manual_override,
            "override_reason": self._override_reason,
            "sources": {
                name: source.to_dict() for name, source in self.sources.items()
            },
            **self._stats,
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
metrics_source_monitor = MetricsSourceMonitor()
