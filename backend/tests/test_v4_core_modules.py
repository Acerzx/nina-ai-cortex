"""
Тесты для новых модулей v4.0.
Покрывает: BackgroundTaskManager, PredictiveHAL, MetricsSourceMonitor,
DecisionAnalyzer, SessionsMetadataStorage.
"""

import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime

from app.core.background_tasks import BackgroundTaskManager
from app.execution.predictive_hal import PredictiveHAL, PredictionSeverity, ActionType
from app.core.metrics_source_monitor import MetricsSourceMonitor
from app.analytics.decision_analyzer import DecisionAnalyzer
from app.storage.sessions_metadata import (
    SessionsMetadataStorage,
    SessionRecord,
    FrameRecord,
)


# ============================================================================
# BackgroundTaskManager
# ============================================================================
@pytest.mark.asyncio
async def test_background_task_manager_register_and_run():
    manager = BackgroundTaskManager()
    call_count = 0

    async def dummy_task():
        nonlocal call_count
        call_count += 1

    manager.register("test_task", dummy_task, interval_seconds=0.1, enabled=True)
    await manager.start()

    await asyncio.sleep(0.35)
    await manager.stop()

    assert call_count >= 2, "Task should have run at least twice"
    stats = manager.get_stats()
    assert stats["total_tasks"] == 1
    assert stats["enabled_tasks"] == 1


@pytest.mark.asyncio
async def test_background_task_manager_disable():
    manager = BackgroundTaskManager()
    call_count = 0

    async def dummy_task():
        nonlocal call_count
        call_count += 1

    manager.register("toggle_task", dummy_task, interval_seconds=0.1, enabled=True)
    await manager.start()
    await asyncio.sleep(0.15)

    manager.disable("toggle_task")
    await asyncio.sleep(0.2)
    await manager.stop()

    assert call_count == 1, "Task should stop after disable"


# ============================================================================
# PredictiveHAL
# ============================================================================
@pytest.mark.asyncio
async def test_predictive_hal_linear_regression():
    hal = PredictiveHAL()
    # y = 2x + 1 -> slope=2, intercept=1
    values = [1.0, 3.0, 5.0, 7.0, 9.0]
    slope, intercept = hal._linear_regression(values)
    assert abs(slope - 2.0) < 0.01
    assert abs(intercept - 1.0) < 0.01


@pytest.mark.asyncio
async def test_predictive_hal_pearson_correlation():
    hal = PredictiveHAL()
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [2.0, 4.0, 6.0, 8.0, 10.0]  # perfect positive correlation
    corr = hal._pearson_correlation(x, y)
    assert abs(corr - 1.0) < 0.01


# ============================================================================
# MetricsSourceMonitor
# ============================================================================
@pytest.mark.asyncio
async def test_metrics_source_monitor_manual_override():
    monitor = MetricsSourceMonitor()
    initial = monitor.get_active_source()

    monitor.set_manual_override("prometheus", reason="test")
    assert monitor.get_active_source() == "prometheus"
    assert monitor._manual_override == "prometheus"

    monitor.clear_manual_override()
    assert monitor._manual_override is None


# ============================================================================
# DecisionAnalyzer
# ============================================================================
@pytest.mark.asyncio
async def test_decision_analyzer_cache():
    analyzer = DecisionAnalyzer()
    # First call should compute
    with patch.object(analyzer, "_load_enabled_flag", return_value=True):
        perf1 = await analyzer.analyze_agent_performance("Watcher", days=1)
        assert perf1.agent == "Watcher"
        assert analyzer._is_cache_valid() is True


# ============================================================================
# SessionsMetadataStorage
# ============================================================================
@pytest.mark.asyncio
async def test_sessions_metadata_create_and_finalize(tmp_path):
    db_path = tmp_path / "test_sessions.db"
    storage = SessionsMetadataStorage(db_path)

    session = SessionRecord(
        session_id="test_session_001",
        target_name="M31",
        filter_name="Ha",
        exposure_time=300.0,
        start_time=datetime.now().isoformat(),
    )
    await storage.create_session(session)

    # Log frames
    for i in range(5):
        frame = FrameRecord(
            session_id="test_session_001",
            frame_index=i,
            hfr=2.0 + i * 0.1,
            fwhm=3.0,
            temperature=-15.0,
            image_type="LIGHT",
        )
        await storage.log_frame(frame)

    # Finalize
    result = await storage.finalize_session("test_session_001")
    assert result["frames_processed"] == 5
    assert result["quality_score"] is not None
    assert result["quality_score"] > 0

    stats = await storage.get_stats()
    assert stats["total_sessions"] == 1
    assert stats["total_frames"] == 5
