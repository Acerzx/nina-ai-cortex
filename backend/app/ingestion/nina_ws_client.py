"""
N.I.N.A. WebSocket Client
Подключается к ninaAPI и слушает события в реальном времени.
"""

import asyncio
import json
import logging
from typing import Optional, Callable, Dict, Any
from websockets.client import connect, WebSocketClientProtocol
from websockets.exceptions import ConnectionClosed, ConnectionClosedError
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class NinaWebSocketClient:
    """
    WebSocket клиент для получения событий от N.I.N.A. в реальном времени.
    """

    def __init__(self, on_event: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.settings = get_settings()
        self.ws_url = self.settings.network.nina_ws_url
        self.on_event = on_event
        self.ws: Optional[WebSocketClientProtocol] = None
        self.running = False
        self.reconnect_delay = 5  # секунд между попытками переподключения
        self.max_reconnect_delay = 60

    async def start(self):
        """Запускает WebSocket клиент с автоматическим переподключением."""
        self.running = True
        current_delay = self.reconnect_delay

        while self.running:
            try:
                logger.info(f"🔌 Connecting to N.I.N.A. WebSocket: {self.ws_url}")
                async with connect(self.ws_url) as ws:
                    self.ws = ws
                    current_delay = self.reconnect_delay  # Сбрасываем задержку
                    logger.info("✅ WebSocket connected successfully")

                    await self._listen()

            except ConnectionClosed as e:
                logger.warning(f"⚠️ WebSocket connection closed: {e}")
            except ConnectionClosedError as e:
                logger.error(f"❌ WebSocket connection error: {e}")
            except Exception as e:
                logger.error(f"❌ WebSocket unexpected error: {e}")

            if self.running:
                logger.info(f"🔄 Reconnecting in {current_delay} seconds...")
                await asyncio.sleep(current_delay)
                current_delay = min(current_delay * 2, self.max_reconnect_delay)

    async def _listen(self):
        """Слушает входящие сообщения от WebSocket."""
        async for message in self.ws:
            try:
                event = json.loads(message)
                await self._process_event(event)
            except json.JSONDecodeError as e:
                logger.error(f"❌ Failed to parse WebSocket message: {e}")
            except Exception as e:
                logger.error(f"❌ Error processing WebSocket event: {e}")

    async def _process_event(self, event: Dict[str, Any]):
        """Обрабатывает полученное событие."""
        event_type = event.get("type", "Unknown")

        # Логируем только важные события, чтобы не спамить
        important_events = [
            "SequenceItemStarted",
            "SequenceItemCompleted",
            "SequenceStarted",
            "SequenceStopped",
            "MeridianFlipStarted",
            "MeridianFlipCompleted",
            "Error",
            "EquipmentConnected",
            "EquipmentDisconnected",
        ]

        if event_type in important_events:
            logger.info(f"📡 NINA Event: {event_type}")

        # Вызываем callback, если он установлен
        if self.on_event:
            await self.on_event(event)

    def stop(self):
        """Останавливает WebSocket клиент."""
        self.running = False
        if self.ws:
            asyncio.create_task(self.ws.close())
