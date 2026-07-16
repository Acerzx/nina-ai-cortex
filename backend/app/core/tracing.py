"""
OpenTelemetry Distributed Tracing — observability для N.I.N.A. AI Cortex.
Архитектура:
- Автоматическая инструментация: FastAPI, httpx, SQLite
- Ручные spans: EventBus, агенты, RAG, LangGraph workflows
- OTLP exporter (gRPC) → Jaeger/Zipkin/Grafana Tempo
- Console exporter для отладки
- Graceful degradation: если OTLP endpoint недоступен — работаем без tracing
Использование:
from app.core.tracing import tracer, trace_span
# Декоратор для автоматического span
@trace_span("my_operation")
async def my_function():
    ...
# Context manager для ручного span
async with tracer.start_as_current_span("operation_name") as span:
    span.set_attribute("key", "value")
    ...
Конфигурация:
- tracing.enabled: включить/выключить tracing
- tracing.exporter: "otlp" | "console" | "none"
- tracing.otlp_endpoint: адрес OTLP collector (например, http://localhost:4317)
- tracing.service_name: имя сервиса в Jaeger
- tracing.sample_rate: 0.0-1.0 (доля трассируемых запросов)
"""

import logging
import functools
from typing import Optional, Callable, Any, Dict
from contextlib import asynccontextmanager

logger = logging.getLogger("Tracing")

# Graceful import — если OpenTelemetry не установлен, работаем без tracing
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
    from opentelemetry.sdk.trace.sampling import TraceIdRatioBased, ParentBased
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


class TracingManager:
    """
    Менеджер OpenTelemetry tracing.
    Features:
    - Ленивая инициализация (при первом использовании)
    - Graceful degradation при отсутствии пакетов
    - Поддержка OTLP (gRPC) и Console exporters
    - Автоматическая инструментация FastAPI и httpx
    """

    def __init__(self):
        self._initialized = False
        self._tracer: Optional[Any] = None
        self._provider: Optional[Any] = None
        self._enabled = False
        self._instrumented_apps: list = []

        # Статистика
        self._stats = {
            "spans_created": 0,
            "spans_exported": 0,
            "export_errors": 0,
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
        Инициализирует OpenTelemetry tracing.
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

            # 2. Создаём Sampler (доля трассируемых запросов)
            sampler = ParentBased(root=TraceIdRatioBased(sample_rate))

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
                        insecure=True,  # Для локальной разработки
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
                    max_queue_size=2048,  # Увеличено для соответствия batch_size
                    schedule_delay_millis=1000,
                    max_export_batch_size=512,  # Добавлено явно
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
                f"sample_rate={sample_rate}"
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
            logger.info("✅ httpx auto-instrumentation enabled")
            return True
        except Exception as e:
            logger.error(f"❌ Failed to instrument httpx: {e}")
            return False

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
                # Устанавливаем атрибуты
                if attributes:
                    for key, value in attributes.items():
                        span.set_attribute(key, value)

                # Добавляем имя функции
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
# SINGLETON INSTANCE
# ============================================================================
tracing_manager = TracingManager()
tracer = tracing_manager  # Алиас для удобства
