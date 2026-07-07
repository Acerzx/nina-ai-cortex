"""
Cortex Metrics — Prometheus-совместимые метрики для мониторинга N.I.N.A. AI Cortex.

Устраняет проблему аудита 11.2: отсутствие метрик для самого Cortex.

Предоставляемые метрики:
- cortex_events_total: счётчик обработанных событий EventBus (по типам)
- cortex_event_processing_seconds: гистограмма времени обработки событий
- cortex_decisions_total: счётчик решений агентов (по агенту и типу)
- cortex_llm_requests_total: счётчик запросов к LLM (по модели и статусу)
- cortex_llm_request_duration_seconds: гистограмма времени ответа LLM
- cortex_eventbus_queue_size: текущий размер очереди EventBus
- cortex_eventbus_subscribers: количество активных подписчиков
- cortex_operation_mode: текущий режим работы (gauge)
- cortex_sequence_running: статус выполнения секвенсора (gauge)
- cortex_api_requests_total: счётчик HTTP-запросов к API
- cortex_api_request_duration_seconds: гистограмма времени ответа API
- cortex_active_ws_connections: количество активных WebSocket-подключений
- cortex_uptime_seconds: время работы Cortex

Использование:
    from app.core.metrics import cortex_metrics

    # Инкремент счётчика
    cortex_metrics.events_total.labels(event_type="NEW_FRAME").inc()

    # Запись времени
    with cortex_metrics.llm_duration.labels(model="gemma4:31b").time():
        await llm_provider.generate(...)

    # Установка gauge
    cortex_metrics.queue_size.set(42)

    # Экспорт в Prometheus формате
    output = cortex_metrics.expose()
"""

import time
import asyncio
import logging
from typing import Dict, Any, Optional, List
from collections import defaultdict
from dataclasses import dataclass, field

logger = logging.getLogger("CortexMetrics")


# ============================================================================
# DATA CLASSES
# ============================================================================


@dataclass
class CounterMetric:
    """Счётчик (только увеличивается)."""

    name: str
    help_text: str
    labels: List[str] = field(default_factory=list)
    _values: Dict[tuple, float] = field(default_factory=lambda: defaultdict(float))
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def inc(self, value: float = 1.0, **label_values):
        """Увеличивает счётчик."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        async with self._lock:
            self._values[key] += value

    def inc_sync(self, value: float = 1.0, **label_values):
        """Синхронное увеличение (без блокировки)."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        self._values[key] += value

    def get(self, **label_values) -> float:
        """Возвращает текущее значение."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        return self._values.get(key, 0.0)

    def expose(self) -> str:
        """Экспорт в Prometheus формате."""
        lines = [f"# HELP {self.name} {self.help_text}"]
        lines.append(f"# TYPE {self.name} counter")
        for key, value in self._values.items():
            if self.labels:
                labels_str = ",".join(f'{l}="{v}"' for l, v in zip(self.labels, key))
                lines.append(f"{self.name}{{{labels_str}}} {value}")
            else:
                lines.append(f"{self.name} {value}")
        return "\n".join(lines)


@dataclass
class GaugeMetric:
    """Gauge (может увеличиваться и уменьшаться)."""

    name: str
    help_text: str
    labels: List[str] = field(default_factory=list)
    _values: Dict[tuple, float] = field(default_factory=lambda: defaultdict(float))
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def set(self, value: float, **label_values):
        """Устанавливает значение."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        async with self._lock:
            self._values[key] = value

    def set_sync(self, value: float, **label_values):
        """Синхронная установка."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        self._values[key] = value

    async def inc(self, value: float = 1.0, **label_values):
        """Увеличивает значение."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        async with self._lock:
            self._values[key] += value

    async def dec(self, value: float = 1.0, **label_values):
        """Уменьшает значение."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        async with self._lock:
            self._values[key] -= value

    def get(self, **label_values) -> float:
        """Возвращает текущее значение."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        return self._values.get(key, 0.0)

    def expose(self) -> str:
        """Экспорт в Prometheus формате."""
        lines = [f"# HELP {self.name} {self.help_text}"]
        lines.append(f"# TYPE {self.name} gauge")
        for key, value in self._values.items():
            if self.labels:
                labels_str = ",".join(f'{l}="{v}"' for l, v in zip(self.labels, key))
                lines.append(f"{self.name}{{{labels_str}}} {value}")
            else:
                lines.append(f"{self.name} {value}")
        return "\n".join(lines)


@dataclass
class HistogramMetric:
    """Гистограмма (распределение значений по бакетам)."""

    name: str
    help_text: str
    labels: List[str] = field(default_factory=list)
    buckets: List[float] = field(
        default_factory=lambda: [
            0.005,
            0.01,
            0.025,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            2.5,
            5.0,
            10.0,
        ]
    )
    _bucket_counts: Dict[tuple, List[int]] = field(default_factory=dict)
    _sums: Dict[tuple, float] = field(default_factory=lambda: defaultdict(float))
    _counts: Dict[tuple, int] = field(default_factory=lambda: defaultdict(int))
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def observe(self, value: float, **label_values):
        """Добавляет наблюдение."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        async with self._lock:
            if key not in self._bucket_counts:
                self._bucket_counts[key] = [0] * len(self.buckets)
            for i, bucket in enumerate(self.buckets):
                if value <= bucket:
                    self._bucket_counts[key][i] += 1
            self._sums[key] += value
            self._counts[key] += 1

    def observe_sync(self, value: float, **label_values):
        """Синхронное наблюдение."""
        key = tuple(label_values.get(l, "") for l in self.labels)
        if key not in self._bucket_counts:
            self._bucket_counts[key] = [0] * len(self.buckets)
        for i, bucket in enumerate(self.buckets):
            if value <= bucket:
                self._bucket_counts[key][i] += 1
        self._sums[key] += value
        self._counts[key] += 1

    class _Timer:
        """Контекстный менеджер для измерения времени."""

        def __init__(self, histogram, label_values):
            self.histogram = histogram
            self.label_values = label_values
            self.start_time = None

        def __enter__(self):
            self.start_time = time.perf_counter()
            return self

        def __exit__(self, *args):
            duration = time.perf_counter() - self.start_time
            self.histogram.observe_sync(duration, **self.label_values)

        async def __aenter__(self):
            self.start_time = time.perf_counter()
            return self

        async def __aexit__(self, *args):
            duration = time.perf_counter() - self.start_time
            await self.histogram.observe(duration, **self.label_values)

    def time(self, **label_values):
        """Возвращает контекстный менеджер для измерения времени."""
        return self._Timer(self, label_values)

    def expose(self) -> str:
        """Экспорт в Prometheus формате."""
        lines = [f"# HELP {self.name} {self.help_text}"]
        lines.append(f"# TYPE {self.name} histogram")
        for key in set(list(self._bucket_counts.keys()) + list(self._counts.keys())):
            counts = self._bucket_counts.get(key, [0] * len(self.buckets))
            total = self._counts.get(key, 0)
            total_sum = self._sums.get(key, 0.0)

            if self.labels:
                labels_str = ",".join(f'{l}="{v}"' for l, v in zip(self.labels, key))
                base_labels = labels_str
            else:
                base_labels = ""

            # Buckets (кумулятивные)
            cumulative = 0
            for i, bucket in enumerate(self.buckets):
                cumulative += counts[i]
                if base_labels:
                    lines.append(
                        f'{self.name}_bucket{{{base_labels},le="{bucket}"}} {cumulative}'
                    )
                else:
                    lines.append(f'{self.name}_bucket{{le="{bucket}"}} {cumulative}')

            # +Inf bucket
            if base_labels:
                lines.append(f'{self.name}_bucket{{{base_labels},le="+Inf"}} {total}')
                lines.append(f"{self.name}_sum{{{base_labels}}} {total_sum}")
                lines.append(f"{self.name}_count{{{base_labels}}} {total}")
            else:
                lines.append(f'{self.name}_bucket{{le="+Inf"}} {total}')
                lines.append(f"{self.name}_sum {total_sum}")
                lines.append(f"{self.name}_count {total}")

        return "\n".join(lines)


# ============================================================================
# CORTEX METRICS REGISTRY
# ============================================================================


class CortexMetrics:
    """
    Реестр всех метрик Cortex для Prometheus-экспорта.

    Использование:
        from app.core.metrics import cortex_metrics

        # Счётчик событий
        cortex_metrics.events_total.labels(event_type="NEW_FRAME").inc()

        # Время обработки события
        with cortex_metrics.event_processing_time.labels(event_type="NEW_FRAME").time():
            await process_event(...)

        # Экспорт для Prometheus
        output = cortex_metrics.expose()
    """

    def __init__(self):
        self._start_time = time.time()

        # ====================================================================
        # EventBus метрики
        # ====================================================================

        self.events_total = CounterMetric(
            name="cortex_events_total",
            help_text="Total number of events processed by EventBus",
            labels=["event_type"],
        )

        self.event_processing_time = HistogramMetric(
            name="cortex_event_processing_seconds",
            help_text="Time spent processing events in EventBus",
            labels=["event_type"],
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        )

        self.eventbus_queue_size = GaugeMetric(
            name="cortex_eventbus_queue_size",
            help_text="Current number of events in EventBus queue",
        )

        self.eventbus_subscribers = GaugeMetric(
            name="cortex_eventbus_subscribers",
            help_text="Number of active EventBus subscribers",
            labels=["event_type"],
        )

        self.event_handler_errors = CounterMetric(
            name="cortex_event_handler_errors_total",
            help_text="Total number of errors in event handlers",
            labels=["event_type"],
        )

        # ====================================================================
        # AI Agents метрики
        # ====================================================================

        self.decisions_total = CounterMetric(
            name="cortex_decisions_total",
            help_text="Total number of AI agent decisions",
            labels=["agent", "decision_type", "outcome"],
        )

        self.decision_confidence = HistogramMetric(
            name="cortex_decision_confidence",
            help_text="Distribution of decision confidence scores",
            labels=["agent"],
            buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
        )

        self.agents_active = GaugeMetric(
            name="cortex_agents_active",
            help_text="Number of currently active AI agents",
        )

        # ====================================================================
        # LLM метрики
        # ====================================================================

        self.llm_requests_total = CounterMetric(
            name="cortex_llm_requests_total",
            help_text="Total number of LLM requests",
            labels=["model", "status", "fallback"],
        )

        self.llm_request_duration = HistogramMetric(
            name="cortex_llm_request_duration_seconds",
            help_text="Duration of LLM requests",
            labels=["model"],
            buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0],
        )

        self.llm_tokens_used = CounterMetric(
            name="cortex_llm_tokens_total",
            help_text="Total number of tokens used in LLM requests",
            labels=["model"],
        )

        self.llm_available = GaugeMetric(
            name="cortex_llm_available",
            help_text="LLM availability status (1=available, 0=unavailable)",
            labels=["model"],
        )

        # ====================================================================
        # API метрики
        # ====================================================================

        self.api_requests_total = CounterMetric(
            name="cortex_api_requests_total",
            help_text="Total number of API requests",
            labels=["method", "path", "status_code"],
        )

        self.api_request_duration = HistogramMetric(
            name="cortex_api_request_duration_seconds",
            help_text="Duration of API requests",
            labels=["method", "path"],
            buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        )

        # ====================================================================
        # System метрики
        # ====================================================================

        self.operation_mode = GaugeMetric(
            name="cortex_operation_mode",
            help_text="Current operation mode (0=manual, 1=safe, 2=full_ai, 3=simulation)",
        )

        self.sequence_running = GaugeMetric(
            name="cortex_sequence_running",
            help_text="Whether a sequence is currently running (1=yes, 0=no)",
        )

        self.flat_mode_active = GaugeMetric(
            name="cortex_flat_mode_active",
            help_text="Whether FLAT_MODE is currently active (1=yes, 0=no)",
        )

        self.safety_status = GaugeMetric(
            name="cortex_safety_status",
            help_text="Safety monitor status (0=SAFE, 1=UNSAFE, -1=UNKNOWN)",
        )

        self.active_ws_connections = GaugeMetric(
            name="cortex_active_ws_connections",
            help_text="Number of active WebSocket connections",
        )

        self.uptime_seconds = GaugeMetric(
            name="cortex_uptime_seconds",
            help_text="Time since Cortex startup in seconds",
        )

        # ====================================================================
        # Execution Layer метрики
        # ====================================================================

        self.triggers_fired = CounterMetric(
            name="cortex_triggers_fired_total",
            help_text="Total number of triggers fired",
            labels=["trigger_name", "status"],
        )

        self.trigger_duration = HistogramMetric(
            name="cortex_trigger_duration_seconds",
            help_text="Duration of trigger execution",
            labels=["trigger_name"],
            buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
        )

        self.variables_set = CounterMetric(
            name="cortex_variables_set_total",
            help_text="Total number of global variables set",
            labels=["status"],
        )

        # ====================================================================
        # RAG метрики
        # ====================================================================

        self.rag_searches_total = CounterMetric(
            name="cortex_rag_searches_total",
            help_text="Total number of RAG searches",
            labels=["status"],
        )

        self.rag_search_duration = HistogramMetric(
            name="cortex_rag_search_duration_seconds",
            help_text="Duration of RAG searches",
            buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
        )

        self.rag_documents_total = GaugeMetric(
            name="cortex_rag_documents_total",
            help_text="Total number of documents in RAG database",
        )

        # ====================================================================
        # Ingestion Layer метрики
        # ====================================================================

        self.files_processed = CounterMetric(
            name="cortex_files_processed_total",
            help_text="Total number of files processed by watchers",
            labels=["watcher", "status"],
        )

        self.watchers_active = GaugeMetric(
            name="cortex_watchers_active",
            help_text="Number of active file watchers",
        )

        logger.info("✅ CortexMetrics initialized")

    def expose(self) -> str:
        """
        Экспортирует все метрики в Prometheus exposition формате.

        Returns:
            Строка в формате Prometheus text exposition
        """
        # Обновляем uptime
        self.uptime_seconds.set_sync(time.time() - self._start_time)

        # Собираем все метрики
        all_metrics = [
            self.events_total,
            self.event_processing_time,
            self.eventbus_queue_size,
            self.eventbus_subscribers,
            self.event_handler_errors,
            self.decisions_total,
            self.decision_confidence,
            self.agents_active,
            self.llm_requests_total,
            self.llm_request_duration,
            self.llm_tokens_used,
            self.llm_available,
            self.api_requests_total,
            self.api_request_duration,
            self.operation_mode,
            self.sequence_running,
            self.flat_mode_active,
            self.safety_status,
            self.active_ws_connections,
            self.uptime_seconds,
            self.triggers_fired,
            self.trigger_duration,
            self.variables_set,
            self.rag_searches_total,
            self.rag_search_duration,
            self.rag_documents_total,
            self.files_processed,
            self.watchers_active,
        ]

        output_lines = [
            "# N.I.N.A. AI Cortex Prometheus Metrics",
            f"# Generated at {time.strftime('%Y-%m-%dT%H:%M:%S%z')}",
            "",
        ]

        for metric in all_metrics:
            output_lines.append(metric.expose())
            output_lines.append("")

        return "\n".join(output_lines)

    def get_summary(self) -> Dict[str, Any]:
        """
        Возвращает сводку метрик в JSON-формате (для API).

        Полезно для dashboard и health check endpoints.
        """
        return {
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "events_total": sum(self.events_total._values.values()),
            "decisions_total": sum(self.decisions_total._values.values()),
            "llm_requests_total": sum(self.llm_requests_total._values.values()),
            "api_requests_total": sum(self.api_requests_total._values.values()),
            "triggers_fired_total": sum(self.triggers_fired._values.values()),
            "rag_searches_total": sum(self.rag_searches_total._values.values()),
            "files_processed_total": sum(self.files_processed._values.values()),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================

cortex_metrics = CortexMetrics()
