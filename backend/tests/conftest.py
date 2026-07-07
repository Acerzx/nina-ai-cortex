"""
Pytest fixtures для тестирования N.I.N.A. AI Cortex.
"""

import pytest
import asyncio
from pathlib import Path
from typing import AsyncGenerator
from app.core.events import EventBus
from app.agents.observatory_state import ObservatoryState
from app.simulation.fake_nina import FakeNinaAPI
from app.simulation.fake_phd2 import FakePhd2


@pytest.fixture(scope="session")
def event_loop():
    """Создает event loop для всей сессии тестов."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def event_bus() -> AsyncGenerator[EventBus, None]:
    """Создает изолированный EventBus для каждого теста."""
    bus = EventBus()
    await bus.start()
    yield bus
    await bus.stop()


@pytest.fixture
async def observatory_state() -> ObservatoryState:
    """Создает изолированный ObservatoryState для каждого теста."""
    state = ObservatoryState()
    await state.start()
    return state


@pytest.fixture
async def fake_nina(tmp_path: Path) -> AsyncGenerator[FakeNinaAPI, None]:
    """Создает FakeNinaAPI для тестирования."""
    session_dir = tmp_path / "fake_session"
    nina = FakeNinaAPI(session_dir=session_dir)
    await nina.start()
    yield nina
    await nina.stop()


@pytest.fixture
async def fake_phd2() -> AsyncGenerator[FakePhd2, None]:
    """Создает FakePhd2 для тестирования."""
    phd2 = FakePhd2()
    await phd2.start()
    yield phd2
    await phd2.stop()


@pytest.fixture
def fixture_path() -> Path:
    """Возвращает путь к тестовым фикстурам."""
    return Path(__file__).parent / "fixtures"
