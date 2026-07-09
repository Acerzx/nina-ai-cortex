"""
N.I.N.A. WebSocket Client
ИСПРАВЛЕНО (v4.0 — проблема #28):
- Добавлен exponential backoff с jitter для переподключений
- Максимальная задержка 60 секунд
- Случайный jitter для предотвращения thundering herd
"""

import asyncio
import json
import logging
import random
from typing import Optional
import websockets
from websockets.exceptions import ConnectionClosed, WebSocketException
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("NinaWebSocketClient")


class NinaWebSocketClient:
    """
    Клиент для подключения к N.I.N.A. WebSocket API.
    ИСПРАВЛЕНО (v4.0):
    - Exponential backoff с jitter
    - Защита от thundering herd
    """

    def __init__(self, url: str, initial_reconnect_delay: float = 5.0):
        self.url = url
        self.initial_reconnect_delay = initial_reconnect_delay
        self.max_reconnect_delay = 60.0  # Максимум 60 секунд
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._ws = None
        self._consecutive_failures = 0

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
            try:
                await self._ws.close()
                logger.info("✅ NINA WebSocket closed")
            except Exception as e:
                logger.debug(f"Error closing WebSocket: {e}")
            finally:
                self._ws = None

    def _calculate_backoff_delay(self) -> float:
        """
        Вычисляет задержку для exponential backoff с jitter.
        Формула: min(initial_delay * 2^failures + jitter, max_delay)
        """
        exponential_delay = self.initial_reconnect_delay * (
            2**self._consecutive_failures
        )
        # Jitter: случайное значение от 0 до initial_delay
        jitter = random.uniform(0, self.initial_reconnect_delay)
        return min(exponential_delay + jitter, self.max_reconnect_delay)

    async def _connect_loop(self):
        """Цикл подключения с exponential backoff при обрыве."""
        while self._running:
            try:
                logger.info(f"Connecting to N.I.N.A. WebSocket: {self.url}")
                async with websockets.connect(
                    self.url, ping_interval=20, ping_timeout=10
                ) as ws:
                    self._ws = ws
                    # Успешное подключение — сбрасываем счётчик
                    if self._consecutive_failures > 0:
                        logger.info(
                            f"✅ WebSocket connected after {self._consecutive_failures} failures"
                        )
                    self._consecutive_failures = 0

                    logger.info("✅ WebSocket connected successfully")
                    await self._listen(ws)

            except ConnectionClosed as e:
                self._consecutive_failures += 1
                delay = self._calculate_backoff_delay()
                logger.warning(
                    f"WebSocket connection closed: {e.code} {e.reason}. "
                    f"Reconnecting in {delay:.1f}s... "
                    f"(failures: {self._consecutive_failures})"
                )
            except WebSocketException as e:
                self._consecutive_failures += 1
                delay = self._calculate_backoff_delay()
                logger.error(
                    f"WebSocket error: {e}. "
                    f"Reconnecting in {delay:.1f}s... "
                    f"(failures: {self._consecutive_failures})"
                )
            except Exception as e:
                self._consecutive_failures += 1
                delay = self._calculate_backoff_delay()
                logger.error(
                    f"Unexpected WebSocket error: {e}. "
                    f"Reconnecting in {delay:.1f}s... "
                    f"(failures: {self._consecutive_failures})"
                )

            if self._running:
                await asyncio.sleep(delay)

    async def _listen(self, ws):
        """Слушает входящие сообщения и публикует их в EventBus."""
        async for message in ws:
            try:
                data = json.loads(message)
                event_type = data.get("Event", "UNKNOWN_EVENT")
                await event_bus.publish(f"NINA_WS_{event_type.upper()}", data)

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
