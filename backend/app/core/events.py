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
"""

import asyncio
import logging
import traceback
from typing import Dict, Any, List, Callable, Awaitable, Set, Tuple
from app.core.config import settings

logger = logging.getLogger("EventBus")

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
    """

    def __init__(self):
        self._subscribers: Dict[
            str, List[Callable[[Dict[str, Any]], Awaitable[None]]]
        ] = {}

        # ИСПРАВЛЕНО (К-8): Ограниченная очередь с размером из конфига
        metrics_cfg = getattr(settings, "metrics", None)
        self._max_queue_size = getattr(metrics_cfg, "event_queue_maxsize", 10000)
        self._stop_timeout = getattr(metrics_cfg, "event_stop_timeout_seconds", 5.0)

        self._queue: asyncio.Queue = asyncio.Queue(maxsize=self._max_queue_size)
        self._running = False
        self._dispatcher_task: asyncio.Task = None

        # ИСПРАВЛЕНО (R3): Статистика retry
        self._stats = {
            "events_published": 0,
            "events_dropped_queue_full": 0,
            "subscriber_errors": 0,
            "retry_attempts": 0,
            "retry_successes": 0,
            "retry_failures": 0,
        }

        logger.info(
            f"📡 EventBus initialized "
            f"(max_queue_size={self._max_queue_size}, "
            f"critical_events={len(CRITICAL_EVENTS)}, "
            f"max_retry={MAX_RETRY_ATTEMPTS})"
        )

    def subscribe(self, event_type: str, callback: Callable):
        """Подписывает callback на событие."""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable):
        """Отписывает callback от события."""
        if event_type in self._subscribers:
            self._subscribers[event_type] = [
                cb for cb in self._subscribers[event_type] if cb != callback
            ]

    async def publish(self, event_type: str, data: Dict[str, Any]):
        """
        Публикует событие в очередь.

        ИСПРАВЛЕНО (К-8): Обработка переполнения очереди.
        ИСПРАВЛЕНО (R3): Счётчик опубликованных событий.
        """
        try:
            # Пытаемся добавить в очередь без блокировки
            self._queue.put_nowait((event_type, data))
            self._stats["events_published"] += 1
        except asyncio.QueueFull:
            # Очередь переполнена — логируем и пропускаем событие
            self._stats["events_dropped_queue_full"] += 1
            logger.warning(
                f"⚠️ EventBus queue full ({self._max_queue_size} items). "
                f"Dropping event: {event_type}"
            )

    async def start(self):
        """Запускает диспетчер событий."""
        self._running = True
        self._dispatcher_task = asyncio.create_task(self._dispatcher())
        logger.info("🚀 EventBus started")

    async def stop(self):
        """
        Останавливает EventBus с graceful shutdown.

        ИСПРАВЛЕНО (К-8):
        - Сигнализирует диспетчеру об остановке
        - Ожидает завершения текущих обработчиков с таймаутом
        - Очищает оставшиеся события в очереди
        """
        self._running = False

        # Ждём завершения текущего обработчика с таймаутом
        if self._dispatcher_task:
            try:
                await asyncio.wait_for(
                    self._dispatcher_task, timeout=self._stop_timeout
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"⚠️ EventBus dispatcher timeout "
                    f"({self._stop_timeout}s). Force stopping..."
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

    async def _dispatcher(self):
        """
        Диспетчер событий.

        ИСПРАВЛЕНО (R3):
        - Вызывает _dispatch_event для каждого события
        - _dispatch_event обрабатывает retry для критических событий
        """
        while self._running:
            try:
                # Ждём событие с таймаутом для проверки флага _running
                try:
                    event_type, data = await asyncio.wait_for(
                        self._queue.get(), timeout=1.0
                    )
                except asyncio.TimeoutError:
                    # Таймаут — проверяем флаг и продолжаем
                    continue

                # ИСПРАВЛЕНО (R3): Делегируем обработку в _dispatch_event
                await self._dispatch_event(event_type, data)

                self._queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Dispatcher error: {e}")

    async def _dispatch_event(self, event_type: str, data: Dict[str, Any]):
        """
        Dispatches событие всем подписчикам.

        ИСПРАВЛЕНО (R3):
        - Для критических событий: retry при ошибке подписчика
        - Для остальных событий: только логирование ошибки
        - Все ошибки логируются с traceback
        """
        subscribers = self._subscribers.get(event_type, [])
        if not subscribers:
            return

        is_critical = event_type in CRITICAL_EVENTS

        for callback in subscribers:
            callback_name = getattr(callback, "__qualname__", str(callback))

            await self._invoke_subscriber(
                event_type=event_type,
                data=data,
                callback=callback,
                callback_name=callback_name,
                is_critical=is_critical,
            )

    async def _invoke_subscriber(
        self,
        event_type: str,
        data: Dict[str, Any],
        callback: Callable,
        callback_name: str,
        is_critical: bool,
    ):
        """
        Вызывает подписчика с опциональным retry.

        ИСПРАВЛЕНО (R3):
        - Для критических событий: до MAX_RETRY_ATTEMPTS попыток
        - Задержка RETRY_DELAY_SECONDS между попытками
        - Логирование каждой ошибки с traceback
        """
        max_attempts = MAX_RETRY_ATTEMPTS if is_critical else 1

        for attempt in range(1, max_attempts + 1):
            try:
                await callback(data)

                # Если это был retry и он успешен — логируем
                if attempt > 1:
                    self._stats["retry_successes"] += 1
                    logger.info(
                        f"✅ Retry succeeded for {event_type} → "
                        f"{callback_name} (attempt {attempt}/{max_attempts})"
                    )
                return  # Успех — выходим

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

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику EventBus."""
        return {
            **self._stats,
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
        }


event_bus = EventBus()
