import asyncio
import logging
from typing import Dict, Any, List, Callable, Awaitable

logger = logging.getLogger("EventBus")


class EventBus:
    def __init__(self):
        self._subscribers: Dict[
            str, List[Callable[[Dict[str, Any]], Awaitable[None]]]
        ] = {}
        self._queue = asyncio.Queue()
        self._running = False

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
        await self._queue.put((event_type, data))

    async def start(self):
        self._running = True
        asyncio.create_task(self._dispatcher())

    async def stop(self):
        self._running = False

    async def _dispatcher(self):
        while self._running:
            try:
                event_type, data = await self._queue.get()
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
