"""
Cortex Metrics — Prometheus-совместимые метрики для мониторинга N.I.N.A. AI Cortex.
ЭТАП 1.4 (рефакторинг):
- Переход со своей реализации (~800 строк) на prometheus-client (~350 строк)
- Потокобезопасность из коробки (встроена в prometheus-client)
- Изолированный CollectorRegistry для Cortex метрик
- Стандартный Prometheus exposition format через generate_latest()
ИСПРАВЛЕНО (Спринт 5 Фаза 2):
- Добавлена поддержка exemplars для связи метрик с traces
- Exemplars позволяют кликнуть на метрику в Grafana и открыть trace в Jaeger
Предоставляемые метрики (28 штук):
EventBus:
- cortex_events_total
- cortex_event_processing_seconds
- cortex_eventbus_queue_size
- cortex_eventbus_subscribers
- cortex_event_handler_errors_total
AI Agents:
- cortex_decisions_total
- cortex_decision_confidence
- cortex_agents_active
LLM:
- cortex_llm_requests_total
- cortex_llm_request_duration_seconds
- cortex_llm_tokens_total
- cortex_llm_available
API:
- cortex_api_requests_total
- cortex_api_request_duration_seconds
System:
- cortex_operation_mode
- cortex_sequence_running
- cortex_flat_mode_active
- cortex_safety_status
- cortex_active_ws_connections
- cortex_uptime_seconds
Execution:
- cortex_triggers_fired_total
- cortex_trigger_duration_seconds
- cortex_variables_set_total
RAG:
- cortex_rag_searches_total
- cortex_rag_search_duration_seconds
- cortex_rag_documents_total
Ingestion:
- cortex_files_processed_total
- cortex_watchers_active
"""

import time
import logging
from typing import Dict, Any, Optional
from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    CollectorRegistry,
    generate_latest,
)

logger = logging.getLogger("CortexMetrics")


# ============================================================================
# EXEMPLAR HELPER — связь метрик с traces
# ============================================================================
def get_exemplar() -> Optional[Dict[str, str]]:
    """
    Возвращает exemplar с текущим trace_id для связи с Jaeger.
    Используется в Prometheus метриках для возможности клика
    на метрику в Grafana и открытия соответствующего trace.
    Returns:
        Dict с trace_id или None если tracing отключен
    """
    try:
        from app.core.tracing import tracing_manager

        trace_id = tracing_manager.get_trace_id()
        if trace_id and trace_id != "-":
            return {"trace_id": trace_id}
    except Exception:
        pass
    return None


# ============================================================================
# WRAPPER КЛАССЫ (для обратной совместимости API)
# ============================================================================
# prometheus-client уже потокобезопасен, поэтому inc_sync/observe_sync/set_sync
# — это просто алиасы для inc/observe/set.
# Это позволяет существующему коду (middleware, event handlers) работать
# без изменений.


class CounterWrapper:
    """
    Wrapper над prometheus_client.Counter.
    Добавляет inc_sync() алиас для обратной совместимости.
    ИСПРАВЛЕНО (Спринт 5 Фаза 2): поддержка exemplars.
    """

    def __init__(self, counter: Counter):
        self._counter = counter

    def inc(
        self,
        amount: float = 1.0,
        exemplar: Optional[Dict[str, str]] = None,
        **label_values,
    ):
        """
        Увеличить счётчик (async-safe, потокобезопасно).
        Args:
            amount: На сколько увеличить
            exemplar: Опциональный exemplar с trace_id
            **label_values: Label значения
        """
        if label_values:
            if exemplar:
                self._counter.labels(**label_values).inc(amount, exemplar=exemplar)
            else:
                self._counter.labels(**label_values).inc(amount)
        else:
            if exemplar:
                self._counter.inc(amount, exemplar=exemplar)
            else:
                self._counter.inc(amount)

    def inc_sync(
        self,
        amount: float = 1.0,
        exemplar: Optional[Dict[str, str]] = None,
        **label_values,
    ):
        """
        Синхронное увеличение (алиас для inc).
        prometheus-client уже потокобезопасен.
        """
        self.inc(amount, exemplar=exemplar, **label_values)

    def labels(self, **label_values) -> "CounterWrapper":
        """Возвращает wrapper для конкретных label значений."""
        return CounterWrapper(self._counter.labels(**label_values))


class GaugeWrapper:
    """
    Wrapper над prometheus_client.Gauge.
    Добавляет set_sync/inc_sync/dec_sync алиасы для обратной совместимости.
    """

    def __init__(self, gauge: Gauge):
        self._gauge = gauge

    def set(self, value: float, **label_values):
        """Установить значение gauge."""
        if label_values:
            self._gauge.labels(**label_values).set(value)
        else:
            self._gauge.set(value)

    def set_sync(self, value: float, **label_values):
        """Синхронная установка (алиас для set)."""
        self.set(value, **label_values)

    def inc(self, amount: float = 1.0, **label_values):
        """Увеличить значение."""
        if label_values:
            self._gauge.labels(**label_values).inc(amount)
        else:
            self._gauge.inc(amount)

    def inc_sync(self, amount: float = 1.0, **label_values):
        """Синхронное увеличение (алиас для inc)."""
        self.inc(amount, **label_values)

    def dec(self, amount: float = 1.0, **label_values):
        """Уменьшить значение."""
        if label_values:
            self._gauge.labels(**label_values).dec(amount)
        else:
            self._gauge.dec(amount)

    def dec_sync(self, amount: float = 1.0, **label_values):
        """Синхронное уменьшение (алиас для dec)."""
        self.dec(amount, **label_values)

    def labels(self, **label_values) -> "GaugeWrapper":
        """Возвращает wrapper для конкретных label значений."""
        return GaugeWrapper(self._gauge.labels(**label_values))


class HistogramWrapper:
    """
    Wrapper над prometheus_client.Histogram.
    Добавляет observe_sync() алиас для обратной совместимости.
    ИСПРАВЛЕНО (Спринт 5 Фаза 2): поддержка exemplars.
    """

    def __init__(self, histogram: Histogram):
        self._histogram = histogram

    def observe(
        self, value: float, exemplar: Optional[Dict[str, str]] = None, **label_values
    ):
        """
        Добавить наблюдение.
        Args:
            value: Значение наблюдения
            exemplar: Опциональный exemplar с trace_id
            **label_values: Label значения
        """
        if label_values:
            if exemplar:
                self._histogram.labels(**label_values).observe(value, exemplar=exemplar)
            else:
                self._histogram.labels(**label_values).observe(value)
        else:
            if exemplar:
                self._histogram.observe(value, exemplar=exemplar)
            else:
                self._histogram.observe(value)

    def observe_sync(
        self, value: float, exemplar: Optional[Dict[str, str]] = None, **label_values
    ):
        """Синхронное наблюдение (алиас для observe)."""
        self.observe(value, exemplar=exemplar, **label_values)

    def labels(self, **label_values) -> "HistogramWrapper":
        """Возвращает wrapper для конкретных label значений."""
        return HistogramWrapper(self._histogram.labels(**label_values))

    def time(self, **label_values):
        """Контекстный менеджер для измерения времени."""
        if label_values:
            return self._histogram.labels(**label_values).time()
        return self._histogram.time()


# ============================================================================
# CORTEX METRICS REGISTRY
# ============================================================================
class CortexMetrics:
    """
    Реестр всех метрик Cortex для Prometheus-экспорта.
    Использует изолированный CollectorRegistry, чтобы:
    - Не конфликтовать с другими библиотеками (fastapi, httpx и т.д.)
    - Контролировать, какие метрики экспортируются
    - Избежать засорения /metrics эндпоинта
    ИСПРАВЛЕНО (Спринт 5 Фаза 2): добавлена поддержка exemplars.
    Использование:
    from app.core.metrics import cortex_metrics
    # Счётчик событий с exemplar
    exemplar = get_exemplar()
    cortex_metrics.events_total.inc_sync(event_type="NEW_FRAME", exemplar=exemplar)
    # Время обработки события
    with cortex_metrics.event_processing_time.time(event_type="NEW_FRAME"):
        await process_event(...)
    # Установка gauge
    cortex_metrics.queue_size.set(42)
    # Экспорт в Prometheus формате
    output = cortex_metrics.expose()
    """

    def __init__(self):
        self._start_time = time.time()
        # Изолированный registry для Cortex метрик
        self._registry = CollectorRegistry()

        # ====================================================================
        # EventBus метрики
        # ====================================================================
        self.events_total = CounterWrapper(
            Counter(
                "cortex_events_total",
                "Total number of events processed by EventBus",
                labelnames=["event_type"],
                registry=self._registry,
            )
        )

        self.event_processing_time = HistogramWrapper(
            Histogram(
                "cortex_event_processing_seconds",
                "Time spent processing events in EventBus",
                labelnames=["event_type"],
                buckets=[
                    0.001,
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
                ],
                registry=self._registry,
            )
        )

        self.eventbus_queue_size = GaugeWrapper(
            Gauge(
                "cortex_eventbus_queue_size",
                "Current number of events in EventBus queue",
                registry=self._registry,
            )
        )

        self.eventbus_subscribers = GaugeWrapper(
            Gauge(
                "cortex_eventbus_subscribers",
                "Number of active EventBus subscribers",
                labelnames=["event_type"],
                registry=self._registry,
            )
        )

        self.event_handler_errors = CounterWrapper(
            Counter(
                "cortex_event_handler_errors_total",
                "Total number of errors in event handlers",
                labelnames=["event_type"],
                registry=self._registry,
            )
        )

        # ====================================================================
        # AI Agents метрики
        # ====================================================================
        self.decisions_total = CounterWrapper(
            Counter(
                "cortex_decisions_total",
                "Total number of AI agent decisions",
                labelnames=["agent", "decision_type", "outcome"],
                registry=self._registry,
            )
        )

        self.decision_confidence = HistogramWrapper(
            Histogram(
                "cortex_decision_confidence",
                "Distribution of decision confidence scores",
                labelnames=["agent"],
                buckets=[0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.95, 1.0],
                registry=self._registry,
            )
        )

        self.agents_active = GaugeWrapper(
            Gauge(
                "cortex_agents_active",
                "Number of currently active AI agents",
                registry=self._registry,
            )
        )

        # ====================================================================
        # LLM метрики
        # ====================================================================
        self.llm_requests_total = CounterWrapper(
            Counter(
                "cortex_llm_requests_total",
                "Total number of LLM requests",
                labelnames=["model", "status", "fallback"],
                registry=self._registry,
            )
        )

        self.llm_request_duration = HistogramWrapper(
            Histogram(
                "cortex_llm_request_duration_seconds",
                "Duration of LLM requests",
                labelnames=["model"],
                buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0],
                registry=self._registry,
            )
        )

        self.llm_tokens_used = CounterWrapper(
            Counter(
                "cortex_llm_tokens_total",
                "Total number of tokens used in LLM requests",
                labelnames=["model"],
                registry=self._registry,
            )
        )

        self.llm_available = GaugeWrapper(
            Gauge(
                "cortex_llm_available",
                "LLM availability status (1=available, 0=unavailable)",
                labelnames=["model"],
                registry=self._registry,
            )
        )

        # ====================================================================
        # API метрики
        # ====================================================================
        self.api_requests_total = CounterWrapper(
            Counter(
                "cortex_api_requests_total",
                "Total number of API requests",
                labelnames=["method", "path", "status_code"],
                registry=self._registry,
            )
        )

        self.api_request_duration = HistogramWrapper(
            Histogram(
                "cortex_api_request_duration_seconds",
                "Duration of API requests",
                labelnames=["method", "path"],
                buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
                registry=self._registry,
            )
        )

        # ====================================================================
        # System метрики
        # ====================================================================
        self.operation_mode = GaugeWrapper(
            Gauge(
                "cortex_operation_mode",
                "Current operation mode (0=manual, 1=safe, 2=full_ai, 3=simulation)",
                registry=self._registry,
            )
        )

        self.sequence_running = GaugeWrapper(
            Gauge(
                "cortex_sequence_running",
                "Whether a sequence is currently running (1=yes, 0=no)",
                registry=self._registry,
            )
        )

        self.flat_mode_active = GaugeWrapper(
            Gauge(
                "cortex_flat_mode_active",
                "Whether FLAT_MODE is currently active (1=yes, 0=no)",
                registry=self._registry,
            )
        )

        self.safety_status = GaugeWrapper(
            Gauge(
                "cortex_safety_status",
                "Safety monitor status (0=SAFE, 1=UNSAFE, -1=UNKNOWN)",
                registry=self._registry,
            )
        )

        self.active_ws_connections = GaugeWrapper(
            Gauge(
                "cortex_active_ws_connections",
                "Number of active WebSocket connections",
                registry=self._registry,
            )
        )

        self.uptime_seconds = GaugeWrapper(
            Gauge(
                "cortex_uptime_seconds",
                "Time since Cortex startup in seconds",
                registry=self._registry,
            )
        )

        # ====================================================================
        # Execution Layer метрики
        # ====================================================================
        self.triggers_fired = CounterWrapper(
            Counter(
                "cortex_triggers_fired_total",
                "Total number of triggers fired",
                labelnames=["trigger_name", "status"],
                registry=self._registry,
            )
        )

        self.trigger_duration = HistogramWrapper(
            Histogram(
                "cortex_trigger_duration_seconds",
                "Duration of trigger execution",
                labelnames=["trigger_name"],
                buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
                registry=self._registry,
            )
        )

        self.variables_set = CounterWrapper(
            Counter(
                "cortex_variables_set_total",
                "Total number of global variables set",
                labelnames=["status"],
                registry=self._registry,
            )
        )

        # ====================================================================
        # RAG метрики
        # ====================================================================
        self.rag_searches_total = CounterWrapper(
            Counter(
                "cortex_rag_searches_total",
                "Total number of RAG searches",
                labelnames=["status"],
                registry=self._registry,
            )
        )

        self.rag_search_duration = HistogramWrapper(
            Histogram(
                "cortex_rag_search_duration_seconds",
                "Duration of RAG searches",
                buckets=[0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
                registry=self._registry,
            )
        )

        self.rag_documents_total = GaugeWrapper(
            Gauge(
                "cortex_rag_documents_total",
                "Total number of documents in RAG database",
                registry=self._registry,
            )
        )

        # ====================================================================
        # Ingestion Layer метрики
        # ====================================================================
        self.files_processed = CounterWrapper(
            Counter(
                "cortex_files_processed_total",
                "Total number of files processed by watchers",
                labelnames=["watcher", "status"],
                registry=self._registry,
            )
        )

        self.watchers_active = GaugeWrapper(
            Gauge(
                "cortex_watchers_active",
                "Number of active file watchers",
                registry=self._registry,
            )
        )

        # ====================================================================
        # Background Tasks метрики (Спринт 5)
        # ====================================================================
        self.background_tasks_total = GaugeWrapper(
            Gauge(
                "cortex_background_tasks_total",
                "Total number of registered background tasks",
                registry=self._registry,
            )
        )

        self.background_tasks_enabled = GaugeWrapper(
            Gauge(
                "cortex_background_tasks_enabled",
                "Number of enabled background tasks",
                registry=self._registry,
            )
        )

        self.background_task_executions_total = CounterWrapper(
            Counter(
                "cortex_background_task_executions_total",
                "Total number of background task executions",
                labelnames=["task_name", "status"],
                registry=self._registry,
            )
        )

        self.background_task_errors_total = CounterWrapper(
            Counter(
                "cortex_background_task_errors_total",
                "Total number of background task errors",
                labelnames=["task_name", "error_type"],
                registry=self._registry,
            )
        )

        self.background_task_duration_seconds = HistogramWrapper(
            Histogram(
                "cortex_background_task_duration_seconds",
                "Duration of background task executions",
                labelnames=["task_name"],
                buckets=[0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 120.0],
                registry=self._registry,
            )
        )

        logger.info(
            "✅ CortexMetrics initialized (prometheus-client backend, "
            "isolated registry, 28 metrics, exemplars enabled)"
        )

    def expose(self) -> str:
        """
        Экспортирует все метрики в Prometheus exposition формате.
        Использует generate_latest() из prometheus-client —
        стандартный способ генерации text format для /metrics эндпоинта.
        Returns:
            Строка в формате Prometheus text exposition
        """
        # Обновляем uptime перед экспортом
        self.uptime_seconds.set(time.time() - self._start_time)

        # Генерируем стандартный Prometheus output
        output_bytes = generate_latest(self._registry)
        return output_bytes.decode("utf-8")

    def get_summary(self) -> Dict[str, Any]:
        """
        Возвращает сводку метрик в JSON-формате (для API).
        Полезно для:
        - dashboard и health check endpoints
        - Быстрого просмотра состояния системы
        - WebSocket broadcast на Frontend
        Читает значения напрямую из registry через get_sample_value().
        """

        def get_counter_value(name: str) -> float:
            """Получает значение counter из registry."""
            try:
                return self._registry.get_sample_value(f"{name}_total") or 0.0
            except Exception:
                return 0.0

        return {
            "uptime_seconds": round(time.time() - self._start_time, 2),
            "events_total": get_counter_value("cortex_events"),
            "decisions_total": get_counter_value("cortex_decisions"),
            "llm_requests_total": get_counter_value("cortex_llm_requests"),
            "api_requests_total": get_counter_value("cortex_api_requests"),
            "triggers_fired_total": get_counter_value("cortex_triggers_fired"),
            "rag_searches_total": get_counter_value("cortex_rag_searches"),
            "files_processed_total": get_counter_value("cortex_files_processed"),
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
cortex_metrics = CortexMetrics()
