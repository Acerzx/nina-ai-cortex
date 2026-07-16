"""
Event Bus для публикации и подписки на события.
ИСПРАВЛЕНО (К-8):
- Ограниченная очередь (maxsize из settings.metrics.event_queue_maxsize)
- Graceful stop с таймаутом (ожидание завершения текущих обработчиков)
- Обработка переполнения очереди с логированием
ИСПРАВЛЕНО (R3):
- Retry-механизм для критических событий
- При ошибке подписчика событие повторно отправляется через 5 секунд
- До 3 попыток retry для критических событий
- Все ошибки подписчиков логируются с traceback
ИСПРАВЛЕНО (Спринт 5 — Фаза 1):
- OpenTelemetry distributed tracing для каждого publish() и subscriber
- Parent span eventbus.publish.{event_type} на publish
- Child span eventbus.subscribe.{event_type} на каждый callback
- W3C Trace Context propagation через data["_trace"]
- Graceful degradation если OpenTelemetry недоступен
"""

import asyncio
import logging
import time
import traceback
from datetime import datetime
from typing import Dict, Any, List, Callable, Awaitable, Set, Tuple

from app.core.config import settings

logger = logging.getLogger("EventBus")

# ============================================================================
# OPENTELEMETRY — graceful import
# ============================================================================
try:
    from opentelemetry import trace
    from opentelemetry.trace import StatusCode
    from opentelemetry.propagate import inject, extract
    from opentelemetry.context import attach, detach
    from opentelemetry.trace.propagation import set_span_in_context
    from app.core.tracing import tracing_manager, span_context

    OPENTELEMETRY_AVAILABLE = True
except ImportError:
    OPENTELEMETRY_AVAILABLE = False
    tracing_manager = None
    span_context = None

    class _FakeStatusCode:
        OK = "OK"
        ERROR = "ERROR"

    StatusCode = _FakeStatusCode()

# События, для которых включён retry при ошибке подписчика
CRITICAL_EVENTS: Set[str] = {
    "ALERT",
    "SEQUENCE_STOPPED",
    "SEQUENCE_STARTED",
    "SAFETY_UNSAFE",
    "PREDICTIVE_ALERT",
    "MERIDIAN_FLIP_STARTED",
    "MODE_CHANGED",
}

# Максимальное количество retry для критических событий
MAX_RETRY_ATTEMPTS: int = 3

# Задержка между retry (секунды)
RETRY_DELAY_SECONDS: float = 5.0


class EventBus:
    """
    Event Bus для публикации и подписки на события.

    ИСПРАВЛЕНО (К-8):
    - Ограниченная очередь с размером из конфига
    - Graceful stop с таймаутом

    ИСПРАВЛЕНО (R3):
    - Retry для критических событий при ошибке подписчика
    - Логирование всех ошибок с traceback

    ИСПРАВЛЕНО (Спринт 5):
    - OpenTelemetry distributed tracing
    - Parent span на publish, child span на каждый subscriber
    - Trace context propagation через data["_trace"]
    """

    def __init__(self):
        self._subscribers: Dict[
            str, List[Callable[[Dict[str, Any]], Awaitable[None]]]
        ] = {}

        # ИСПРАВЛЕНО (К-8): Ограниченная очередь с размером из конфига
        metrics_cfg = getattr(settings, "metrics", None)
        self._max_queue_size = getattr(metrics_cfg, "event_queue_maxsize", 10000)
        self._stop_timeout = getattr(metrics_cfg, "event_stop_timeout_seconds", 5.0)

        self._queue = asyncio.Queue(maxsize=self._max_queue_size)
        self._running = False
        self._dispatcher_task = None

        # ИСПРАВЛЕНО (Спринт 5): Статистика tracing
        self._stats = {
            "events_published": 0,
            "events_dropped_queue_full": 0,
            "subscriber_errors": 0,
            "retry_attempts": 0,
            "retry_successes": 0,
            "retry_failures": 0,
            "dispatched_subscribers": 0,
            "avg_dispatch_ms": 0.0,
            "_dispatch_time_samples": 0,
            "_dispatch_time_sum_ms": 0.0,
        }

        logger.info(
            f"📡 EventBus initialized "
            f"(max_queue_size={self._max_queue_size}, "
            f"tracing={'enabled' if OPENTELEMETRY_AVAILABLE else 'unavailable'})"
        )

    # ====================================================================
    # SUBSCRIBE / UNSUBSCRIBE
    # ====================================================================

    def subscribe(self, event_type: str, callback: Callable):
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable):
        if event_type in self._subscribers:
            self._subscribers[event_type] = [
                cb for cb in self._subscribers[event_type] if cb != callback
            ]

    # ====================================================================
    # PUBLISH
    # ====================================================================

    async def publish(self, event_type: str, data: Dict[str, Any]):
        """
        Публикует событие в очередь.

        ИСПРАВЛЕНО (К-8): Обработка переполнения очереди.
        ИСПРАВЛЕНО (Спринт 5): OpenTelemetry span + trace context injection.
        """
        # Snapshot data (защита от изменения в subscriber'ах)
        data = dict(data)

        # === Без tracing: простая логика ===
        if not (
            OPENTELEMETRY_AVAILABLE and tracing_manager and tracing_manager.enabled
        ):
            try:
                self._queue.put_nowait((event_type, data))
                self._stats["events_published"] += 1
            except asyncio.QueueFull:
                self._stats["events_dropped_queue_full"] += 1
                logger.warning(
                    f"⚠️ EventBus queue full ({self._max_queue_size} items). "
                    f"Dropping event: {event_type}"
                )
            return

        # === С tracing: span + inject context ===
        async with span_context(
            f"eventbus.publish.{event_type}",
            attributes={
                "event.type": event_type,
                "event.subscribers_count": len(self._subscribers.get(event_type, [])),
                "event.queue_size": self._queue.qsize(),
                "event.is_critical": event_type in CRITICAL_EVENTS,
            },
        ) as span:
            # Inject W3C Trace Context в data для propagation через очередь
            if span:
                carrier: Dict[str, str] = {}
                inject(carrier)
                ctx = span.get_span_context()
                data["_trace"] = {
                    "trace_id": format(ctx.trace_id, "032x"),
                    "span_id": format(ctx.span_id, "016x"),
                    "traceparent": carrier.get("traceparent"),
                    "timestamp": datetime.now().isoformat(),
                }
                span.set_attribute("event.trace_id", data["_trace"]["trace_id"])

            try:
                self._queue.put_nowait((event_type, data))
                self._stats["events_published"] += 1
            except asyncio.QueueFull:
                self._stats["events_dropped_queue_full"] += 1
                logger.warning(
                    f"⚠️ EventBus queue full ({self._max_queue_size} items). "
                    f"Dropping event: {event_type}"
                )
                if span:
                    span.set_status(StatusCode.ERROR, "Queue full — event dropped")

    # ====================================================================
    # LIFECYCLE
    # ====================================================================

    async def start(self):
        self._running = True
        self._dispatcher_task = asyncio.create_task(self._dispatcher())
        logger.info("🚀 EventBus started")

    async def stop(self):
        """
        Останавливает EventBus с graceful shutdown.
        """
        self._running = False

        if self._dispatcher_task:
            try:
                await asyncio.wait_for(
                    self._dispatcher_task, timeout=self._stop_timeout
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"⚠️ EventBus dispatcher timeout ({self._stop_timeout}s). "
                    f"Force stopping..."
                )
                self._dispatcher_task.cancel()
                try:
                    await self._dispatcher_task
                except asyncio.CancelledError:
                    pass

        # Очищаем оставшиеся события в очереди
        cleared_count = 0
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                self._queue.task_done()
                cleared_count += 1
            except asyncio.QueueEmpty:
                break

        if cleared_count > 0:
            logger.warning(f"⚠️ Cleared {cleared_count} unprocessed events from queue")

        logger.info("🛑 EventBus stopped gracefully")

    # ====================================================================
    # DISPATCHER
    # ====================================================================

    async def _dispatcher(self):
        while self._running:
            try:
                # Ждём событие с таймаутом для проверки флага _running
                try:
                    event_type, data = await asyncio.wait_for(
                        self._queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                # ИСПРАВЛЕНО (Спринт 5): dispatch с tracing
                await self._dispatch_event(event_type, data)

                self._queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Dispatcher error: {e}")

    async def _dispatch_event(self, event_type: str, data: Dict[str, Any]):
        """
        Dispatches событие всем подписчикам.

        ИСПРАВЛЕНО (R3): retry для критических событий.
        ИСПРАВЛЕНО (Спринт 5): child span для каждого subscriber.
        """
        subscribers = self._subscribers.get(event_type, [])
        if not subscribers:
            return

        is_critical = event_type in CRITICAL_EVENTS

        # === Извлекаем parent trace context из data ===
        # Если событие пришло через очередь с injected context — используем его
        trace_info = data.get("_trace", {})
        parent_context = None
        token = None

        if (
            OPENTELEMETRY_AVAILABLE
            and tracing_manager
            and tracing_manager.enabled
            and trace_info.get("traceparent")
        ):
            try:
                carrier = {"traceparent": trace_info["traceparent"]}
                parent_context = extract(carrier)
                token = attach(parent_context)
            except Exception as e:
                logger.debug(f"Failed to extract trace context: {e}")

        try:
            for callback in subscribers:
                callback_name = self._get_callback_name(callback)

                # === С tracing: child span для каждого subscriber ===
                if (
                    OPENTELEMETRY_AVAILABLE
                    and tracing_manager
                    and tracing_manager.enabled
                ):
                    await self._invoke_with_span(
                        event_type=event_type,
                        data=data,
                        callback=callback,
                        callback_name=callback_name,
                        is_critical=is_critical,
                    )
                else:
                    # === Без tracing: простая логика ===
                    await self._invoke_subscriber(
                        event_type=event_type,
                        data=data,
                        callback=callback,
                        callback_name=callback_name,
                        is_critical=is_critical,
                    )
        finally:
            # Detach parent context
            if token is not None:
                try:
                    detach(token)
                except Exception:
                    pass

    async def _invoke_with_span(
        self,
        event_type: str,
        data: Dict[str, Any],
        callback: Callable,
        callback_name: str,
        is_critical: bool,
    ):
        """
        Вызывает subscriber внутри child span.
        """
        async with span_context(
            f"eventbus.subscribe.{event_type}",
            attributes={
                "event.type": event_type,
                "subscriber.name": callback_name,
                "subscriber.module": getattr(callback, "__module__", "unknown"),
                "event.is_critical": is_critical,
            },
        ) as span:
            start_time = time.perf_counter()
            success = await self._invoke_subscriber(
                event_type=event_type,
                data=data,
                callback=callback,
                callback_name=callback_name,
                is_critical=is_critical,
            )
            elapsed_ms = (time.perf_counter() - start_time) * 1000

            if span:
                span.set_attribute("subscriber.duration_ms", round(elapsed_ms, 2))
                span.set_attribute("subscriber.success", success)

                if success:
                    span.set_status(StatusCode.OK)
                else:
                    span.set_status(StatusCode.ERROR, "Subscriber failed")

            # Обновляем статистику
            self._stats["_dispatch_time_samples"] += 1
            self._stats["_dispatch_time_sum_ms"] += elapsed_ms
            total = self._stats["_dispatch_time_samples"]
            if total > 0:
                self._stats["avg_dispatch_ms"] = round(
                    self._stats["_dispatch_time_sum_ms"] / total, 2
                )

    async def _invoke_subscriber(
        self,
        event_type: str,
        data: Dict[str, Any],
        callback: Callable,
        callback_name: str,
        is_critical: bool,
    ) -> bool:
        """
        Вызывает подписчика с опциональным retry.
        Returns:
            True если успешно, False если ошибка.
        """
        max_attempts = MAX_RETRY_ATTEMPTS if is_critical else 1

        for attempt in range(1, max_attempts + 1):
            try:
                await callback(data)
                self._stats["dispatched_subscribers"] += 1

                # Если это был retry и он успешен — логируем
                if attempt > 1:
                    self._stats["retry_successes"] += 1
                    logger.info(
                        f"✅ Retry succeeded for {event_type} → "
                        f"{callback_name} (attempt {attempt}/{max_attempts})"
                    )
                return True

            except Exception as e:
                self._stats["subscriber_errors"] += 1

                # Логируем ошибку с traceback
                tb_str = traceback.format_exc()
                logger.error(
                    f"❌ Error in subscriber {callback_name} "
                    f"for {event_type} "
                    f"(attempt {attempt}/{max_attempts}): "
                    f"{type(e).__name__}: {e}\n{tb_str}"
                )

                # Если есть ещё попытки — ждём и retry
                if attempt < max_attempts:
                    self._stats["retry_attempts"] += 1
                    logger.warning(
                        f"🔄 Retrying {event_type} → {callback_name} "
                        f"in {RETRY_DELAY_SECONDS}s "
                        f"(attempt {attempt + 1}/{max_attempts})"
                    )
                    await asyncio.sleep(RETRY_DELAY_SECONDS)
                else:
                    # Все попытки исчерпаны
                    if is_critical:
                        self._stats["retry_failures"] += 1
                        logger.critical(
                            f"🚨 CRITICAL: All {max_attempts} retry attempts "
                            f"failed for {event_type} → {callback_name}. "
                            f"Event may be lost!"
                        )
                    return False

        return False

    # ====================================================================
    # HELPERS
    # ====================================================================

    @staticmethod
    def _get_callback_name(callback: Callable) -> str:
        """Извлекает читаемое имя callback'а."""
        if hasattr(callback, "__qualname__"):
            return callback.__qualname__
        if hasattr(callback, "__name__"):
            return callback.__name__
        return str(callback)

    # ====================================================================
    # STATS
    # ====================================================================

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику EventBus."""
        return {
            **{k: v for k, v in self._stats.items() if not k.startswith("_")},
            "queue_size": self._queue.qsize(),
            "max_queue_size": self._max_queue_size,
            "running": self._running,
            "subscribers_count": {
                et: len(cbs) for et, cbs in self._subscribers.items()
            },
            "total_subscribers": sum(len(cbs) for cbs in self._subscribers.values()),
            "critical_events": sorted(CRITICAL_EVENTS),
            "max_retry_attempts": MAX_RETRY_ATTEMPTS,
            "retry_delay_seconds": RETRY_DELAY_SECONDS,
            "tracing": {
                "enabled": (
                    OPENTELEMETRY_AVAILABLE
                    and tracing_manager is not None
                    and tracing_manager.enabled
                ),
                "opentelemetry_available": OPENTELEMETRY_AVAILABLE,
            },
        }


event_bus = EventBus()
