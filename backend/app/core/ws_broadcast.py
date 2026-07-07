"""
WebSocket Broadcast Manager
Пушит события Cortex на Frontend в реальном времени через WebSocket.
Устраняет Упрощение #30 (WebSocket Broadcasting).
"""
import asyncio
import json
import logging
from typing import Dict, Set, List, Optional, Any, Callable
from fastapi import WebSocket, WebSocketDisconnect
from datetime import datetime
from app.core.events import event_bus
from app.core.config import settings

logger = logging.getLogger("WSBroadcast")


class WebSocketConnection:
    """Представляет одно WebSocket подключение с метаданными."""

    def __init__(self, websocket: WebSocket, client_id: str):
        self.websocket = websocket
        self.client_id = client_id
        self.connected_at = datetime.now()
        self.subscribed_channels: Set[str] = {"all"}  # По умолчанию подписан на все каналы
        self._send_lock = asyncio.Lock()

    async def send_json(self, data: Dict[str, Any]) -> bool:
        """Безопасная отправка JSON с блокировкой."""
        async with self._send_lock:
            try:
                await self.websocket.send_json(data)
                return True
            except Exception as e:
                logger.debug(f"Failed to send to {self.client_id}: {e}")
                return False

    def is_subscribed_to(self, channel: str) -> bool:
        """Проверяет, подписан ли клиент на канал."""
        return "all" in self.subscribed_channels or channel in self.subscribed_channels


class WebSocketBroadcastManager:
    """
    Менеджер WebSocket подключений для broadcasting событий на Frontend.

    Архитектура каналов:
    - sequence: события секвенсора (ItemStarted, ItemCompleted, etc.)
    - metrics: метрики качества (HFR, FWHM, RMS, etc.)
    - weather: погодные данные
    - alerts: алерты от AI-агентов
    - plugins: статус плагинов
    - state: полное состояние обсерватории
    - all: подписка на все каналы (по умолчанию)
    """

    # Маппинг событий EventBus на каналы WebSocket
    EVENT_CHANNEL_MAP = {
        # Sequence events
        "SEQUENCE_ITEM_STARTED": "sequence",
        "SEQUENCE_ITEM_COMPLETED": "sequence",
        "SEQUENCE_STARTED": "sequence",
        "SEQUENCE_STOPPED": "sequence",
        "MERIDIAN_FLIP_STARTED": "sequence",
        "MERIDIAN_FLIP_COMPLETED": "sequence",
        "FLAT_MODE_START": "sequence",
        "FLAT_MODE_END": "sequence",
        "FLAT_MODE_CONFIRMED": "sequence",
        "FLAT_MODE_ENDED": "sequence",
        # Metrics events
        "NEW_FRAME": "metrics",
        "HOCUS_FOCUS_ANALYSIS": "metrics",
        "PROMETHEUS_UPDATE": "metrics",
        "AUTOFOCUS_REPORT": "metrics",
        "DITHER_STATS": "metrics",
        "GUIDING_ANALYSIS": "metrics",
        "FITS_HEADER_PARSED": "metrics",
        # Weather events
        "WEATHER_UPDATE": "weather",
        "AI_WEATHER_STATUS": "weather",
        # Alert events
        "ALERT": "alerts",
        "LOG_ERROR": "alerts",
        "NINA_ERROR": "alerts",
        # Plugin events
        "MASTERS_INDEXED": "plugins",
        "SESSION_DETAILS_UPDATE": "plugins",
        "NIGHT_SUMMARY": "plugins",
        # State events
        "LIVESTACK_STATUS": "state",
        "LIVESTACK_HISTORY": "state",
    }

    def __init__(self):
        self._connections: Dict[str, WebSocketConnection] = {}
        self._lock = asyncio.Lock()
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._running = False
        self._handlers_map: Dict[str, Callable] = {}  # Хранение ссылок на обработчики для корректной отписки

    async def start(self):
        """Запускает менеджер и подписывается на события EventBus."""
        if self._running:
            return
        self._running = True

        # ИСПРАВЛЕНО: Создаем замыкание для каждого event_type, чтобы зафиксировать event_type и channel
        for event_type, channel in self.EVENT_CHANNEL_MAP.items():
            async def event_handler(data: Dict[str, Any], et: str = event_type, ch: str = channel):
                """Обработчик с зафиксированными event_type и channel через closure."""
                await self.broadcast(et, data, channel=ch)

            event_bus.subscribe(event_type, event_handler)
            self._handlers_map[event_type] = event_handler

        # Запускаем heartbeat task
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        logger.info(
            f"🔌 WebSocket Broadcast Manager started ({len(self.EVENT_CHANNEL_MAP)} event types monitored)"
        )

    async def stop(self):
        """Останавливает менеджер и закрывает все подключения."""
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        # ИСПРАВЛЕНО: Корректная отписка с использованием сохраненных ссылок
        for event_type, handler in self._handlers_map.items():
            event_bus.unsubscribe(event_type, handler)
        self._handlers_map.clear()

        # Закрываем все подключения
        async with self._lock:
            for conn in self._connections.values():
                try:
                    await conn.websocket.close(code=1000, reason="Server shutdown")
                except Exception:
                    pass
            self._connections.clear()

        logger.info("WebSocket Broadcast Manager stopped")

    async def connect(self, websocket: WebSocket, client_id: str) -> WebSocketConnection:
        """Регистрирует новое WebSocket подключение."""
        await websocket.accept()
        conn = WebSocketConnection(websocket, client_id)

        async with self._lock:
            self._connections[client_id] = conn

        logger.info(
            f"✅ WebSocket client connected: {client_id} (total: {len(self._connections)})"
        )

        # Отправляем приветственное сообщение
        await conn.send_json({
            "type": "connection_established",
            "client_id": client_id,
            "timestamp": datetime.now().isoformat(),
            "available_channels": list(set(self.EVENT_CHANNEL_MAP.values())) + ["all"],
        })

        return conn

    async def disconnect(self, client_id: str):
        """Удаляет подключение из реестра."""
        async with self._lock:
            if client_id in self._connections:
                del self._connections[client_id]
        logger.info(
            f"❌ WebSocket client disconnected: {client_id} (total: {len(self._connections)})"
        )

    async def handle_client_message(self, client_id: str, message: Dict[str, Any]):
        """Обрабатывает входящие сообщения от клиента (например, подписка на каналы)."""
        conn = self._connections.get(client_id)
        if not conn:
            return

        msg_type = message.get("type")

        if msg_type == "subscribe":
            channels = message.get("channels", [])
            if isinstance(channels, list):
                conn.subscribed_channels = set(channels) if channels else {"all"}
            await conn.send_json({
                "type": "subscription_confirmed",
                "channels": list(conn.subscribed_channels),
            })
            logger.debug(f"Client {client_id} subscribed to: {conn.subscribed_channels}")

        elif msg_type == "ping":
            await conn.send_json({"type": "pong", "timestamp": datetime.now().isoformat()})

    async def broadcast(self, event_type: str, data: Dict[str, Any], channel: Optional[str] = None):
        """
        Рассылает событие всем подписанным клиентам.
        """
        if not self._connections:
            return

        # Определяем канал события
        if channel is None:
            channel = self.EVENT_CHANNEL_MAP.get(event_type, "state")

        # Формируем payload
        payload = {
            "type": "event",
            "event_type": event_type,
            "channel": channel,
            "data": data,
            "timestamp": datetime.now().isoformat(),
        }

        # Рассылаем всем подписанным клиентам
        disconnected = []
        async with self._lock:
            connections_snapshot = list(self._connections.values())

        for conn in connections_snapshot:
            if conn.is_subscribed_to(channel):
                success = await conn.send_json(payload)
                if not success:
                    disconnected.append(conn.client_id)

        # Удаляем "мертвые" подключения
        if disconnected:
            async with self._lock:
                for client_id in disconnected:
                    self._connections.pop(client_id, None)
            logger.debug(f"Cleaned up {len(disconnected)} dead connections")

    async def _heartbeat_loop(self):
        """Периодически отправляет heartbeat всем клиентам."""
        while self._running:
            try:
                await asyncio.sleep(30)  # Каждые 30 секунд

                if not self._connections:
                    continue

                heartbeat_msg = {
                    "type": "heartbeat",
                    "timestamp": datetime.now().isoformat(),
                    "active_connections": len(self._connections),
                }

                disconnected = []
                async with self._lock:
                    connections_snapshot = list(self._connections.values())

                for conn in connections_snapshot:
                    success = await conn.send_json(heartbeat_msg)
                    if not success:
                        disconnected.append(conn.client_id)

                # Удаляем "мертвые" подключения
                if disconnected:
                    async with self._lock:
                        for client_id in disconnected:
                            self._connections.pop(client_id, None)
                    logger.debug(f"Heartbeat: cleaned up {len(disconnected)} dead connections")

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику подключений."""
        return {
            "total_connections": len(self._connections),
            "connections": [
                {
                    "client_id": conn.client_id,
                    "connected_at": conn.connected_at.isoformat(),
                    "subscribed_channels": list(conn.subscribed_channels),
                }
                for conn in self._connections.values()
            ],
        }


# Singleton instance
ws_broadcast_manager = WebSocketBroadcastManager()