"""
Unit tests for Watcher Agent.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock
from app.agents.watcher_agent import WatcherAgent, AnomalyReport
from app.agents.observatory_state import ObservatoryState


@pytest.mark.asyncio
async def test_watcher_detects_hfr_anomaly():
    """Тест детекции аномального роста HFR."""
    # Setup
    agent = WatcherAgent()

    # Mock ObservatoryState
    mock_state = MagicMock(spec=ObservatoryState)
    mock_state.history.hfr = [2.0, 2.1, 2.2, 2.3, 3.0, 3.2, 3.5]  # Рост

    # Inject mock state
    from app.agents import observatory_state as real_state

    real_state.history.hfr = [2.0, 2.1, 2.2, 2.3, 3.0, 3.2, 3.5]

    # Action
    anomaly = await agent._check_hfr_trend()

    # Assert
    assert anomaly is not None
    assert anomaly.metric == "HFR"
    assert anomaly.severity in ["MEDIUM", "HIGH"]
    assert anomaly.deviation_percent > 30.0


@pytest.mark.asyncio
async def test_watcher_no_anomaly_for_stable_metrics():
    """Тест отсутствия аномалии при стабильных метриках."""
    agent = WatcherAgent()

    from app.agents import observatory_state as real_state

    real_state.history.hfr = [2.0, 2.0, 2.1, 2.0, 2.1, 2.0, 2.1]  # Стабильно

    anomaly = await agent._check_hfr_trend()

    assert anomaly is None


@pytest.mark.asyncio
async def test_watcher_cooldown_prevents_spam():
    """Тест cooldown механизма для предотвращения спама алертов."""
    agent = WatcherAgent()
    agent._anomaly_cooldown_seconds = 1  # 1 секунда для теста

    # Первая аномалия должна пройти
    assert not agent._is_in_cooldown("test_anomaly")

    # Регистрируем аномалию
    from datetime import datetime

    agent._recent_anomalies["test_anomaly"] = datetime.now()

    # Сразу после - должна быть в cooldown
    assert agent._is_in_cooldown("test_anomaly")

    # Ждем окончания cooldown
    import asyncio

    await asyncio.sleep(1.1)

    # После ожидания - cooldown истек
    assert not agent._is_in_cooldown("test_anomaly")


@pytest.mark.asyncio
async def test_watcher_initialization():
    """Тест инициализации агента."""
    agent = WatcherAgent()

    assert agent.name == "Watcher"
    assert agent.role == "Monitor & Anomaly Detection"
    assert "hfr_increase_percent" in agent.thresholds
    assert "z_score_threshold" in agent.thresholds


@pytest.mark.asyncio
async def test_watcher_generates_alert():
    """Тест генерации алерта."""
    agent = WatcherAgent()

    anomaly = AnomalyReport(
        metric="HFR",
        current_value=3.5,
        baseline_value=2.0,
        deviation_percent=75.0,
        z_score=4.5,
        severity="HIGH",
    )

    # Mock event_bus
    from unittest.mock import patch

    with patch("app.agents.watcher_agent.event_bus") as mock_bus:
        mock_bus.publish = AsyncMock()

        await agent._handle_anomaly(anomaly)

        # Проверяем, что алерт был опубликован
        assert mock_bus.publish.called
        call_args = mock_bus.publish.call_args
        assert call_args[0][0] == "ALERT"
