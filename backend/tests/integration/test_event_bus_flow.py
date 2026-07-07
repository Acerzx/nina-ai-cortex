"""
Integration tests for EventBus flow.
"""

import pytest
import asyncio
from app.core.events import EventBus


@pytest.mark.asyncio
async def test_event_bus_subscribe_and_publish():
    """Тест базовой подписки и публикации событий."""
    bus = EventBus()
    await bus.start()

    received = []

    async def handler(data):
        received.append(data)

    bus.subscribe("TEST_EVENT", handler)

    # Публикуем событие
    await bus.publish("TEST_EVENT", {"message": "hello"})
    await asyncio.sleep(0.1)  # Ждем обработки

    assert len(received) == 1
    assert received[0]["message"] == "hello"

    await bus.stop()


@pytest.mark.asyncio
async def test_event_bus_multiple_subscribers():
    """Тест множественных подписчиков на одно событие."""
    bus = EventBus()
    await bus.start()

    received1 = []
    received2 = []

    async def handler1(data):
        received1.append(data)

    async def handler2(data):
        received2.append(data)

    bus.subscribe("TEST_EVENT", handler1)
    bus.subscribe("TEST_EVENT", handler2)

    await bus.publish("TEST_EVENT", {"value": 42})
    await asyncio.sleep(0.1)

    assert len(received1) == 1
    assert len(received2) == 1
    assert received1[0]["value"] == 42
    assert received2[0]["value"] == 42

    await bus.stop()


@pytest.mark.asyncio
async def test_event_bus_unsubscribe():
    """Тест отписки от событий."""
    bus = EventBus()
    await bus.start()

    received = []

    async def handler(data):
        received.append(data)

    bus.subscribe("TEST_EVENT", handler)
    bus.unsubscribe("TEST_EVENT", handler)

    await bus.publish("TEST_EVENT", {"message": "should not receive"})
    await asyncio.sleep(0.1)

    assert len(received) == 0

    await bus.stop()


@pytest.mark.asyncio
async def test_event_bus_multiple_events():
    """Тест публикации множества событий."""
    bus = EventBus()
    await bus.start()

    received = []

    async def handler(data):
        received.append(data)

    bus.subscribe("TEST_EVENT", handler)

    # Публикуем 10 событий
    for i in range(10):
        await bus.publish("TEST_EVENT", {"id": i})

    await asyncio.sleep(0.5)

    assert len(received) == 10
    assert [r["id"] for r in received] == list(range(10))

    await bus.stop()


@pytest.mark.asyncio
async def test_event_bus_error_handling():
    """Тест обработки ошибок в обработчиках."""
    bus = EventBus()
    await bus.start()

    received = []

    async def failing_handler(data):
        raise ValueError("Test error")

    async def working_handler(data):
        received.append(data)

    bus.subscribe("TEST_EVENT", failing_handler)
    bus.subscribe("TEST_EVENT", working_handler)

    # Публикуем событие - failing_handler упадет, но working_handler должен сработать
    await bus.publish("TEST_EVENT", {"message": "test"})
    await asyncio.sleep(0.1)

    assert len(received) == 1
    assert received[0]["message"] == "test"

    await bus.stop()
