import asyncio
import logging
from typing import Dict, Any, List, Callable, Awaitable
from app.core.config import settings

logger = logging.getLogger("EventBus")


class EventBus:
    """
    Event Bus для публикации и подписки на события.

    ИСПРАВЛЕНО (К-8):
    - Ограниченная очередь (maxsize из settings.metrics.event_queue_maxsize)
    - Graceful stop с таймаутом (ожидание завершения текущих обработчиков)
    - Обработка переполнения очереди с логированием
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

        logger.info(f"📡 EventBus initialized (max_queue_size={self._max_queue_size})")

    def subscribe(self, event_type: str, callback: Callable):
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)

    def unsubscribe(self, event_type: str, callback: Callable):
        if event_type in self._subscribers:
            self._subscribers[event_type] = [
                cb for cb in self._subscribers[event_type] if cb != callback
            ]

    async def publish(self, event_type: str, data: Dict[str, Any]):
        """
        Публикует событие в очередь.

        ИСПРАВЛЕНО (К-8): Обработка переполнения очереди.
        """
        try:
            # Пытаемся добавить в очередь без блокировки
            self._queue.put_nowait((event_type, data))
        except asyncio.QueueFull:
            # Очередь переполнена — логируем и пропускаем событие
            logger.warning(
                f"⚠️ EventBus queue full ({self._max_queue_size} items). "
                f"Dropping event: {event_type}"
            )

    async def start(self):
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

    async def _dispatcher(self):
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

                # Обрабатываем событие
                for callback in self._subscribers.get(event_type, []):
                    try:
                        await callback(data)
                    except Exception as e:
                        logger.error(f"Error in subscriber for {event_type}: {e}")

                self._queue.task_done()

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Dispatcher error: {e}")


event_bus = EventBus()
