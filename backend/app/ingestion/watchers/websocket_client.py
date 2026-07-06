import asyncio
import json
import logging
from typing import Optional, Callable, Awaitable
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException

from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("NinaWebSocketClient")


class NinaWebSocketClient:
    """
    Клиент для подключения к N.I.N.A. WebSocket API.
    Получает события в реальном времени: SequenceItemStarted, SequenceItemCompleted,
    MeridianFlipStarted, Error, EquipmentConnected/Disconnected.
    """

    def __init__(self, url: str, reconnect_delay: float = 5.0):
        self.url = url
        self.reconnect_delay = reconnect_delay
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws: Optional[websockets.WebSocketClientProtocol] = None

    async def start(self):
        self._running = True
        self._task = asyncio.create_task(self._connect_loop())
        logger.info(f"WebSocket Client started (url={self.url})")

    async def stop(self):
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._ws:
            await self._ws.close()

    async def _connect_loop(self):
        """Цикл подключения с экспоненциальной задержкой при обрыве."""
        while self._running:
            try:
                logger.info(f"Connecting to N.I.N.A. WebSocket: {self.url}")
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=10
                ) as ws:
                    self._ws = ws
                    logger.info("✅ WebSocket connected successfully")
                    await self._listen(ws)
            except ConnectionClosed as e:
                logger.warning(
                    f"WebSocket connection closed: {e.code} {e.reason}. Reconnecting in {self.reconnect_delay}s..."
                )
            except WebSocketException as e:
                logger.error(
                    f"WebSocket error: {e}. Reconnecting in {self.reconnect_delay}s..."
                )
            except Exception as e:
                logger.error(
                    f"Unexpected WebSocket error: {e}. Reconnecting in {self.reconnect_delay}s..."
                )

            if self._running:
                await asyncio.sleep(self.reconnect_delay)

    async def _listen(self, ws: websockets.WebSocketClientProtocol):
        """Слушает входящие сообщения и публикует их в EventBus."""
        async for message in ws:
            try:
                data = json.loads(message)
                event_type = data.get("Event", "UNKNOWN_EVENT")

                # Публикуем событие в EventBus
                await event_bus.publish(f"NINA_WS_{event_type.upper()}", data)

                # Специальная обработка для критических событий
                if event_type == "SequenceItemStarted":
                    await event_bus.publish("SEQUENCE_ITEM_STARTED", data)
                elif event_type == "SequenceItemCompleted":
                    await event_bus.publish("SEQUENCE_ITEM_COMPLETED", data)
                elif event_type == "SequenceStarted":
                    await event_bus.publish("SEQUENCE_STARTED", data)
                elif event_type == "SequenceStopped":
                    await event_bus.publish("SEQUENCE_STOPPED", data)
                elif event_type == "MeridianFlipStarted":
                    await event_bus.publish("MERIDIAN_FLIP_STARTED", data)
                elif event_type == "MeridianFlipCompleted":
                    await event_bus.publish("MERIDIAN_FLIP_COMPLETED", data)
                elif event_type == "Error":
                    await event_bus.publish("NINA_ERROR", data)

            except json.JSONDecodeError:
                logger.warning(f"Failed to parse WebSocket message: {message[:100]}")
            except Exception as e:
                logger.error(f"Error processing WebSocket message: {e}")
