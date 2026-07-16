"""
OpenTelemetry Distributed Tracing — observability для N.I.N.A. AI Cortex.

Архитектура:
- Автоматическая инструментация: FastAPI, httpx, SQLite, Qdrant
- Ручные spans: EventBus, агенты, RAG, LangGraph workflows
- OTLP exporter (gRPC) → Jaeger/Zipkin/Grafana Tempo
- Console exporter для отладки
- Graceful degradation: если OTLP endpoint недоступен — работаем без tracing
- Dynamic sampling: 100% для критических событий, sample_rate для остальных

Использование:
    from app.core.tracing import tracing_manager, trace_span, span_context

    # Декоратор для автоматического span
    @trace_span("my_operation")
    async def my_function():
        ...

    # Context manager для ручного span
    async with span_context("operation_name", {"key": "value"}) as span:
        span.set_attribute("extra", "data")
        ...

    # Получение текущего trace_id для логирования
    trace_id = tracing_manager.get_trace_id()
    logger.info(f"[{trace_id}] Processing request")

Конфигурация:
- tracing.enabled: включить/выключить tracing
- tracing.exporter: "otlp" | "console" | "none"
- tracing.otlp_endpoint: адрес OTLP collector (например, http://localhost:4317)
- tracing.service_name: имя сервиса в Jaeger
- tracing.sample_rate: 0.0-1.0 (доля трассируемых запросов)
- tracing.sentry_dsn: Sentry DSN (опционально)

ИСПРАВЛЕНО (Спринт 5 Фаза 2):
- Добавлены instrumentors для SQLite и Qdrant
- Добавлен CriticalEventSampler для динамического sampling
- Добавлена интеграция с Sentry (опционально)
- Добавлен метод get_trace_id() для correlation логов

ИСПРАВЛЕНО (финальное):
- Удалён TraceIdFormatter (дублирование)
- Оставлен только TraceIdFilter (ВСЕГДА добавляет record.trace_id)
"""

import logging
import functools
from typing import Optional, Callable, Any, Dict, Set
from contextlib import asynccontextmanager

logger = logging.getLogger("Tracing")

# ============================================================================
# GRACEFUL IMPORT — если OpenTelemetry не установлен, работаем без tracing
# ============================================================================
try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import (
        BatchSpanProcessor,
        ConsoleSpanExporter,
        SpanExporter,
    )
    from opentelemetry.sdk.resources import Resource, SERVICE_NAME, SERVICE_VERSION
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.trace.sampling import (
        TraceIdRatioBased,
        ParentBased,
        Sampler,
        SamplingResult,
        Decision,
    )
    from opentelemetry.context import Context
    from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    from opentelemetry.instrumentation.httpx import HTTPXClientInstrumentor

    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    logger.warning(
        "⚠️ OpenTelemetry packages not installed. "
        "Tracing disabled. Install with: "
        "pip install opentelemetry-api opentelemetry-sdk "
        "opentelemetry-exporter-otlp-proto-grpc "
        "opentelemetry-instrumentation-fastapi "
        "opentelemetry-instrumentation-httpx"
    )


# ============================================================================
# DYNAMIC SAMPLER — 100% для критических событий
# ============================================================================
if OPENTELEMETRY_AVAILABLE:

    class CriticalEventSampler(Sampler):
        """
        Динамический sampler: всегда трассирует критические события,
        для остальных — применяет sample_rate.

        Критические события (ALERT, SAFETY_UNSAFE, ERROR) должны
        быть видны всегда, даже если sample_rate низкий (например, 0.1).

        Args:
            sample_rate: Доля трассируемых обычных запросов (0.0-1.0)
        """

        CRITICAL_EVENT_TYPES: Set[str] = {
            "ALERT",
            "PREDICTIVE_ALERT",
            "SAFETY_UNSAFE",
            "ERROR",
            "CRITICAL",
            "EMERGENCY_PARK",
            "GUARDIAN_ACTION",
        }

        def __init__(self, sample_rate: float = 1.0):
            self._sample_rate = sample_rate
            self._ratio_sampler = TraceIdRatioBased(sample_rate)

        def should_sample(
            self,
            parent_context: Optional[Context],
            trace_id: int,
            name: str,
            kind=None,
            attributes=None,
            links=(),
            trace_state=None,
        ) -> "SamplingResult":
            """
            ИСПРАВЛЕНО: Сигнатура соответствует OpenTelemetry SDK.
            Старая сигнатура передавала attributes как 4-й позиционный,
            но в SDK 4-й параметр — kind, что вызывало конфликт.
            """
            if attributes:
                event_type = attributes.get("event.type", "")
                severity = attributes.get("event.severity", "")
                if event_type in self.CRITICAL_EVENT_TYPES or severity in (
                    "CRITICAL",
                    "HIGH",
                ):
                    return SamplingResult(Decision.RECORD_AND_SAMPLE)

            return self._ratio_sampler.should_sample(
                parent_context=parent_context,
                trace_id=trace_id,
                name=name,
                kind=kind,
                attributes=attributes,
                links=links,
                trace_state=trace_state,
            )

        def get_description(self) -> str:
            return (
                f"CriticalEventSampler(rate={self._sample_rate}, "
                f"critical_events={len(self.CRITICAL_EVENT_TYPES)})"
            )

else:
    CriticalEventSampler = None


# ============================================================================
# TRACING MANAGER
# ============================================================================
class TracingManager:
    """
    Менеджер OpenTelemetry tracing.

    Features:
    - Ленивая инициализация (при первом использовании)
    - Graceful degradation при отсутствии пакетов
    - Поддержка OTLP (gRPC) и Console exporters
    - Автоматическая инструментация FastAPI, httpx, SQLite, Qdrant
    - Dynamic sampling через CriticalEventSampler
    - Опциональная интеграция с Sentry
    - Получение trace_id для correlation логов
    """

    def __init__(self):
        self._initialized = False
        self._tracer: Optional[Any] = None
        self._provider: Optional[Any] = None
        self._enabled = False
        self._instrumented_apps: list = []
        self._sentry_enabled = False

        # Статистика
        self._stats = {
            "spans_created": 0,
            "spans_exported": 0,
            "export_errors": 0,
            "instrumentors_enabled": [],
        }

    def initialize(
        self,
        enabled: bool = True,
        exporter_type: str = "otlp",
        otlp_endpoint: str = "http://localhost:4317",
        service_name: str = "nina-ai-cortex",
        service_version: str = "5.0.0",
        sample_rate: float = 1.0,
        console_export: bool = False,
    ) -> bool:
        """
        Инициализирует OpenTelemetry tracing с динамическим sampling.

        Args:
            enabled: Включить tracing
            exporter_type: "otlp" | "console" | "none"
            otlp_endpoint: Адрес OTLP collector (gRPC)
            service_name: Имя сервиса в Jaeger
            service_version: Версия сервиса
            sample_rate: Доля трассируемых запросов (0.0-1.0)
            console_export: Дублировать spans в консоль (для отладки)

        Returns:
            True если инициализация успешна
        """
        if self._initialized:
            logger.warning("Tracing already initialized")
            return self._enabled

        if not enabled:
            logger.info("⏭️ Tracing disabled by configuration")
            self._initialized = True
            self._enabled = False
            return False

        if not OPENTELEMETRY_AVAILABLE:
            logger.warning("⚠️ OpenTelemetry not available — tracing disabled")
            self._initialized = True
            self._enabled = False
            return False

        try:
            # 1. Создаём Resource с метаданными сервиса
            resource = Resource.create(
                {
                    SERVICE_NAME: service_name,
                    SERVICE_VERSION: service_version,
                    "deployment.environment": "development",
                }
            )

            # 2. Создаём CriticalEventSampler (вместо обычного TraceIdRatioBased)
            sampler = ParentBased(root=CriticalEventSampler(sample_rate))

            # 3. Создаём TracerProvider
            self._provider = TracerProvider(
                resource=resource,
                sampler=sampler,
            )

            # 4. Настраиваем Exporter
            exporter: Optional[SpanExporter] = None

            if exporter_type == "otlp":
                try:
                    exporter = OTLPSpanExporter(
                        endpoint=otlp_endpoint,
                        insecure=True,
                    )
                    logger.info(f"📡 OTLP exporter configured: {otlp_endpoint}")
                except Exception as e:
                    logger.error(
                        f"❌ Failed to create OTLP exporter: {e}. "
                        f"Falling back to console."
                    )
                    exporter = ConsoleSpanExporter()
                    exporter_type = "console"

            elif exporter_type == "console":
                exporter = ConsoleSpanExporter()
                logger.info("📺 Console exporter configured")

            elif exporter_type == "none":
                logger.info("⏭️ No exporter configured — spans will be dropped")
                self._initialized = True
                self._enabled = False
                return False

            else:
                logger.warning(
                    f"Unknown exporter type: {exporter_type}. Using console."
                )
                exporter = ConsoleSpanExporter()

            # 5. Добавляем SpanProcessor (batch для производительности)
            if exporter:
                processor = BatchSpanProcessor(
                    exporter,
                    max_queue_size=2048,
                    schedule_delay_millis=5000,
                    max_export_batch_size=512,
                )
                self._provider.add_span_processor(processor)

            # 6. Console exporter для отладки (опционально)
            if console_export and exporter_type != "console":
                console_processor = BatchSpanProcessor(
                    ConsoleSpanExporter(),
                    max_queue_size=100,
                    schedule_delay_millis=1000,
                    max_export_batch_size=50,
                )
                self._provider.add_span_processor(console_processor)
                logger.info("📺 Console exporter added for debugging")

            # 7. Устанавливаем глобальный TracerProvider
            trace.set_tracer_provider(self._provider)
            self._tracer = trace.get_tracer(
                service_name,
                service_version,
            )

            self._initialized = True
            self._enabled = True

            logger.info(
                f"✅ OpenTelemetry initialized: "
                f"service={service_name}, "
                f"exporter={exporter_type}, "
                f"sampler=CriticalEventSampler(rate={sample_rate})"
            )
            return True

        except Exception as e:
            logger.error(f"❌ Failed to initialize tracing: {e}", exc_info=True)
            self._initialized = True
            self._enabled = False
            return False

    def instrument_fastapi(self, app) -> bool:
        """
        Автоматическая инструментация FastAPI приложения.
        Создаёт spans для каждого HTTP запроса.
        """
        if not self._enabled or not OPENTELEMETRY_AVAILABLE:
            return False

        try:
            FastAPIInstrumentor.instrument_app(app)
            self._instrumented_apps.append("fastapi")
            self._stats["instrumentors_enabled"].append("fastapi")
            logger.info("✅ FastAPI auto-instrumentation enabled")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to instrument FastAPI: {e}")
            return False

    def instrument_httpx(self) -> bool:
        """
        Автоматическая инструментация httpx клиентов.
        Создаёт spans для каждого HTTP запроса (NINA API, Ollama, etc.)
        """
        if not self._enabled or not OPENTELEMETRY_AVAILABLE:
            return False

        try:
            HTTPXClientInstrumentor().instrument()
            self._instrumented_apps.append("httpx")
            self._stats["instrumentors_enabled"].append("httpx")
            logger.info("✅ httpx auto-instrumentation enabled")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to instrument httpx: {e}")
            return False

    def instrument_sqlite(self) -> bool:
        """
        Автоматическая инструментация SQLite операций.
        Создаёт spans для каждого SQL запроса в Decision Audit,
        Sessions Metadata, Metrics History.
        """
        if not self._enabled or not OPENTELEMETRY_AVAILABLE:
            return False

        try:
            from opentelemetry.instrumentation.sqlite3 import SQLite3Instrumentor

            SQLite3Instrumentor().instrument()
            self._instrumented_apps.append("sqlite3")
            self._stats["instrumentors_enabled"].append("sqlite3")
            logger.info("✅ SQLite auto-instrumentation enabled")
            return True
        except ImportError:
            logger.warning(
                "⚠️ opentelemetry-instrumentation-sqlite3 not installed. "
                "Install with: pip install opentelemetry-instrumentation-sqlite3"
            )
            return False
        except Exception as e:
            logger.error(f"❌ Failed to instrument SQLite: {e}")
            return False

    def instrument_qdrant(self) -> bool:
        """
        Автоматическая инструментация Qdrant операций.
        Создаёт spans для каждого векторного поиска, upsert, delete.
        """
        if not self._enabled or not OPENTELEMETRY_AVAILABLE:
            return False

        try:
            try:
                from opentelemetry.instrumentation.qdrant import QdrantInstrumentor

                QdrantInstrumentor().instrument()
                self._instrumented_apps.append("qdrant")
                self._stats["instrumentors_enabled"].append("qdrant")
                logger.info("✅ Qdrant auto-instrumentation enabled")
                return True
            except ImportError:
                import os

                os.environ["QDRANT_CLIENT_TELEMETRY"] = "true"
                self._instrumented_apps.append("qdrant_builtin")
                self._stats["instrumentors_enabled"].append("qdrant_builtin")
                logger.info(
                    "✅ Qdrant built-in telemetry enabled "
                    "(install opentelemetry-instrumentation-qdrant for full spans)"
                )
                return True
        except Exception as e:
            logger.error(f"❌ Failed to instrument Qdrant: {e}")
            return False

    def instrument_sentry(self, dsn: Optional[str] = None) -> bool:
        """
        Интеграция с Sentry для production мониторинга.
        Автоматически собирает traces + exceptions.
        """
        if not dsn:
            logger.debug("Sentry DSN not provided — skipping Sentry integration")
            return False

        try:
            import sentry_sdk
            from sentry_sdk.integrations.fastapi import FastApiIntegration
            from sentry_sdk.integrations.logging import LoggingIntegration

            sentry_sdk.init(
                dsn=dsn,
                traces_sample_rate=0.1,
                environment="production",
                integrations=[
                    FastApiIntegration(),
                    LoggingIntegration(
                        level=logging.INFO,
                        event_level=logging.ERROR,
                    ),
                ],
            )

            self._sentry_enabled = True
            self._instrumented_apps.append("sentry")
            self._stats["instrumentors_enabled"].append("sentry")
            logger.info("✅ Sentry integration enabled")
            return True
        except ImportError:
            logger.warning(
                "⚠️ sentry-sdk not installed. "
                "Install with: pip install sentry-sdk[fastapi]"
            )
            return False
        except Exception as e:
            logger.error(f"❌ Failed to initialize Sentry: {e}")
            return False

    def get_trace_id(self) -> str:
        """
        Возвращает текущий trace_id для correlation логов.
        Используется в TraceIdFilter для добавления trace_id в логи.

        Returns:
            Trace ID (первые 16 hex символов) или "-" если нет активного span
        """
        if not self._enabled or not OPENTELEMETRY_AVAILABLE:
            return "-"

        try:
            span = trace.get_current_span()
            if span and span.is_recording():
                ctx = span.get_span_context()
                if ctx and ctx.trace_id:
                    return format(ctx.trace_id, "032x")[:16]
            return "-"
        except Exception:
            return "-"

    def get_span_id(self) -> str:
        """
        Возвращает текущий span_id для correlation логов.

        Returns:
            Span ID (первые 8 hex символов) или "-" если нет активного span
        """
        if not self._enabled or not OPENTELEMETRY_AVAILABLE:
            return "-"

        try:
            span = trace.get_current_span()
            if span and span.is_recording():
                ctx = span.get_span_context()
                if ctx and ctx.span_id:
                    return format(ctx.span_id, "016x")[:8]
            return "-"
        except Exception:
            return "-"

    @property
    def tracer(self):
        """Возвращает глобальный tracer."""
        if not self._enabled:
            return None
        return self._tracer

    @property
    def enabled(self) -> bool:
        """Проверяет, включён ли tracing."""
        return self._enabled

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику tracing."""
        return {
            **self._stats,
            "enabled": self._enabled,
            "initialized": self._initialized,
            "opentelemetry_available": OPENTELEMETRY_AVAILABLE,
            "instrumented": self._instrumented_apps,
            "sentry_enabled": self._sentry_enabled,
        }

    async def shutdown(self):
        """Корректно закрывает tracing (flush spans)."""
        if self._provider and self._enabled:
            try:
                self._provider.force_flush(timeout_millis=5000)
                self._provider.shutdown()
                logger.info("🛑 Tracing shutdown complete")
            except Exception as e:
                logger.error(f"Error shutting down tracing: {e}")

        if self._sentry_enabled:
            try:
                import sentry_sdk

                sentry_sdk.flush()
                logger.info("🛑 Sentry flush complete")
            except Exception as e:
                logger.debug(f"Error flushing Sentry: {e}")

        self._enabled = False
        self._initialized = False


# ============================================================================
# DECORATORS & CONTEXT MANAGERS
# ============================================================================
def trace_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
):
    """
    Декоратор для автоматического создания span.

    Использование:
        @trace_span("my_operation")
        async def my_function(arg1, arg2):
            ...
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            if not tracing_manager.enabled:
                return await func(*args, **kwargs)

            tracer = tracing_manager.tracer
            if not tracer:
                return await func(*args, **kwargs)

            with tracer.start_as_current_span(name) as span:
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)

                span.set_attribute("code.function", func.__name__)
                span.set_attribute("code.module", func.__module__)

                try:
                    result = await func(*args, **kwargs)
                    span.set_status(trace.StatusCode.OK)
                    tracing_manager._stats["spans_created"] += 1
                    return result
                except Exception as e:
                    span.set_status(
                        trace.StatusCode.ERROR,
                        str(e),
                    )
                    span.record_exception(e)
                    raise

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            if not tracing_manager.enabled:
                return func(*args, **kwargs)

            tracer = tracing_manager.tracer
            if not tracer:
                return func(*args, **kwargs)

            with tracer.start_as_current_span(name) as span:
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)
                span.set_attribute("code.function", func.__name__)

                try:
                    result = func(*args, **kwargs)
                    span.set_status(trace.StatusCode.OK)
                    tracing_manager._stats["spans_created"] += 1
                    return result
                except Exception as e:
                    span.set_status(trace.StatusCode.ERROR, str(e))
                    span.record_exception(e)
                    raise

        import asyncio

        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        return sync_wrapper

    return decorator


@asynccontextmanager
async def span_context(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
):
    """
    Context manager для ручного создания span.

    Использование:
        async with span_context("my_operation", {"key": "value"}) as span:
            span.set_attribute("extra", "data")
            ...
    """
    if not tracing_manager.enabled:
        yield None
        return

    tracer = tracing_manager.tracer
    if not tracer:
        yield None
        return

    with tracer.start_as_current_span(name) as span:
        if attributes:
            for key, value in attributes.items():
                span.set_attribute(key, value)

        try:
            yield span
            tracing_manager._stats["spans_created"] += 1
        except Exception as e:
            if OPENTELEMETRY_AVAILABLE:
                span.set_status(trace.StatusCode.ERROR, str(e))
                span.record_exception(e)
            raise


# ============================================================================
# TRACING FORMATTER (решает проблему multiprocessing)
# ============================================================================
class TracingFormatter(logging.Formatter):
    """
    Formatter, который автоматически добавляет trace_id в каждую запись лога.

    Решает проблему KeyError 'trace_id' при multiprocessing.spawn:
    - Formatter сериализуется через pickle и переносится в дочерние процессы
    - При каждом форматировании сам извлекает trace_id из OpenTelemetry
    - Если trace_id нет — подставляет "-"
    - Не зависит от Filter (который не переносится через multiprocessing)
    """

    def format(self, record):
        # Автоматически добавляем trace_id, если его нет в record
        if not hasattr(record, "trace_id"):
            try:
                from opentelemetry import trace

                span = trace.get_current_span()
                if span and span.is_recording():
                    ctx = span.get_span_context()
                    if ctx and ctx.trace_id and ctx.trace_id != 0:
                        record.trace_id = format(ctx.trace_id, "032x")[:16]
                    else:
                        record.trace_id = "-"
                else:
                    record.trace_id = "-"
            except ImportError:
                # OpenTelemetry не установлен
                record.trace_id = "-"
            except Exception:
                record.trace_id = "-"

        # Добавляем span_id, если его нет
        if not hasattr(record, "span_id"):
            try:
                from opentelemetry import trace

                span = trace.get_current_span()
                if span and span.is_recording():
                    ctx = span.get_span_context()
                    if ctx and ctx.span_id and ctx.span_id != 0:
                        record.span_id = format(ctx.span_id, "016x")[:8]
                    else:
                        record.span_id = "-"
                else:
                    record.span_id = "-"
            except ImportError:
                record.span_id = "-"
            except Exception:
                record.span_id = "-"

        # Вызываем родительский format
        return super().format(record)


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
tracing_manager = TracingManager()
