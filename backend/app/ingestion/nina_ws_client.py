"""
N.I.N.A. WebSocket Client
Подключается к ninaAPI и слушает события в реальном времени.
"""

import asyncio
import json
import logging
from typing import Optional, Callable, Dict, Any
from websockets.client import connect, WebSocketClientProtocol
from websockets.exceptions import (
    ConnectionClosed,
    ConnectionClosedError,
    InvalidURI,
    InvalidHandshake,
)
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class NinaWebSocketClient:
    """
    WebSocket клиент для получения событий от N.I.N.A. в реальном времени.
    """

    def __init__(self, on_event: Optional[Callable[[Dict[str, Any]], None]] = None):
        self.settings = get_settings()
        self.ws_url = self.settings.network.nina_ws_url  # Берет из settings.yaml
        self.on_event = on_event
        self.ws: Optional[WebSocketClientProtocol] = None
        self.running = False
        self.connected = False
        self.reconnect_delay = 5
        self.max_reconnect_delay = 300
        self.connection_attempts = 0

    async def start(self):
        """Запускает WebSocket клиент с автоматическим переподключением."""
        self.running = True
        current_delay = self.reconnect_delay

        while self.running:
            try:
                if not self.connected:
                    logger.info(f"🔌 Connecting to N.I.N.A. WebSocket: {self.ws_url}")

                async with connect(self.ws_url) as ws:
                    self.ws = ws
                    self.connected = True
                    self.connection_attempts = 0
                    current_delay = self.reconnect_delay
                    logger.info("✅ WebSocket connected successfully")

                    await self._listen()

            except (ConnectionRefusedError, OSError) as e:
                if not self.connected:
                    self.connection_attempts += 1
                    if self.connection_attempts == 1:
                        logger.warning(
                            f"⚠️ N.I.N.A. API server is not running on {self.ws_url}"
                        )
                        logger.warning(
                            "   Please enable Advanced API in N.I.N.A. → Options → Plugins"
                        )
                    elif self.connection_attempts % 20 == 0:
                        logger.info(
                            f"⏳ Still waiting for N.I.N.A. API (attempt {self.connection_attempts})..."
                        )

            except (ConnectionClosed, ConnectionClosedError) as e:
                if self.connected:
                    logger.warning(f"⚠️ WebSocket connection closed: {e}")
                    self.connected = False

            except (InvalidURI, InvalidHandshake) as e:
                logger.error(f"❌ WebSocket configuration error: {e}")
                logger.error("   Check nina_ws_url in config/settings.yaml")
                await asyncio.sleep(60)
                continue

            except Exception as e:
                logger.error(f"❌ WebSocket unexpected error: {e}")

            if self.running:
                await asyncio.sleep(current_delay)
                current_delay = min(current_delay * 1.5, self.max_reconnect_delay)

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

        if self.on_event:
            await self.on_event(event)

    def stop(self):
        """Останавливает WebSocket клиент."""
        self.running = False
        if self.ws:
            asyncio.create_task(self.ws.close())
