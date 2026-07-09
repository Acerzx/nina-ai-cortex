"""
End-to-End tests для полных сессий с детекцией аномалий.
Тестирует полный цикл: Event → Watcher → Diagnostician → Guardian → Execution.

ИСПРАВЛЕНО (audit 13.2):
- Заменены asyncio.sleep() на событийную синхронизацию
- Добавлен helper wait_for_condition для надёжного ожидания
- Устранены flaky-тесты
"""

import pytest
import asyncio
from pathlib import Path
from typing import Callable, Awaitable
from app.simulation.fake_nina import FakeNinaAPI
from app.ingestion.watchers.manager import WatcherManager
from app.agents.orchestrator import Orchestrator, OperationMode
from app.agents.watcher_agent import WatcherAgent
from app.agents.guardian_agent import GuardianAgent
from app.agents.diagnostician_agent import DiagnosticianAgent
from app.agents.strategist_agent import StrategistAgent
from app.agents.auditor_agent import AuditorAgent
from app.agents.calibrator_agent import CalibratorAgent
from app.agents.scheduler_agent import SchedulerAgent
from app.agents.copilot_agent import CopilotAgent

# from app.agents.memory_manager_agent import MemoryManagerAgent
from app.core.mode_manager import ModeManager
from app.core.events import event_bus


async def wait_for_condition(
    condition: Callable[[], bool],
    timeout: float = 10.0,
    interval: float = 0.1,
    description: str = "condition",
) -> bool:
    """
    ИСПРАВЛЕНО (audit 13.2): Ждёт выполнения условия с таймаутом.

    Вместо фиксированного asyncio.sleep() используем событийную синхронизацию.

    Args:
        condition: Функция без аргументов, возвращающая True когда условие выполнено
        timeout: Максимальное время ожидания (секунды)
        interval: Интервал между проверками (секунды)
        description: Описание условия (для отладки)

    Returns:
        True если условие выполнено, False если истёк таймаут
    """
    elapsed = 0.0
    while elapsed < timeout:
        if condition():
            return True
        await asyncio.sleep(interval)
        elapsed += interval

    return False


@pytest.mark.asyncio
async def test_full_session_with_hfr_anomaly(tmp_path: Path):
    """
    E2E тест: полная сессия с детекцией аномалии HFR и запуском автофокуса.

    ИСПРАВЛЕНО (audit 13.2): Использует wait_for_condition вместо sleep.
    """
    # Setup
    fake_nina = FakeNinaAPI(session_dir=tmp_path / "session1")
    watcher_manager = WatcherManager()
    orchestrator = Orchestrator()
    mode_manager = ModeManager()

    # Создаем агентов
    watcher = WatcherAgent()
    guardian = GuardianAgent()
    diagnostician = DiagnosticianAgent()
    strategist = StrategistAgent()
    auditor = AuditorAgent()
    calibrator = CalibratorAgent(masters_auditor=None)
    scheduler = SchedulerAgent()
    copilot = CopilotAgent()
    # memory_manager = MemoryManagerAgent()

    # Регистрируем агентов
    orchestrator.register_agent("Watcher", watcher)
    orchestrator.register_agent("Guardian", guardian)
    orchestrator.register_agent("Diagnostician", diagnostician)
    orchestrator.register_agent("Strategist", strategist)
    orchestrator.register_agent("Auditor", auditor)
    orchestrator.register_agent("Calibrator", calibrator)
    orchestrator.register_agent("Scheduler", scheduler)
    orchestrator.register_agent("Copilot", copilot)
    # orchestrator.register_agent("MemoryManager", memory_manager)

    # Запускаем компоненты
    await watcher_manager.start()
    await orchestrator.start()
    await mode_manager.start()
    await fake_nina.start()

    # Инициализируем агентов
    await watcher.initialize()
    await guardian.initialize()
    await diagnostician.initialize()

    # Запускаем секвенсор
    await fake_nina.start_sequence(target="M31", frames=10)

    # Ждём генерации 5 нормальных кадров
    condition_met = await wait_for_condition(
        lambda: fake_nina.frame_count >= 5,
        timeout=15.0,
        interval=0.2,
        description="5 frames generated",
    )
    assert condition_met, "Timeout waiting for 5 frames"

    # Инжектируем аномалию HFR
    await fake_nina.inject_anomaly("hfr_spike")

    # ИСПРАВЛЕНО (audit 13.2): Ждём детекции аномалии событийно
    anomaly_detected = await wait_for_condition(
        lambda: len(watcher._recent_anomalies) > 0,
        timeout=10.0,
        interval=0.2,
        description="HFR anomaly detected by Watcher",
    )
    assert anomaly_detected, "Watcher did not detect HFR anomaly within timeout"

    # Ждём анализа от Diagnostician
    diagnostic_done = await wait_for_condition(
        lambda: len(diagnostician._decision_log) > 0,
        timeout=10.0,
        interval=0.2,
        description="Diagnostician analyzed anomaly",
    )
    assert diagnostic_done, "Diagnostician did not analyze anomaly within timeout"

    # Ждём реакции от Guardian
    guardian_reacted = await wait_for_condition(
        lambda: fake_nina.autofocus_triggered or len(guardian._decision_log) > 0,
        timeout=10.0,
        interval=0.2,
        description="Guardian triggered autofocus",
    )
    assert guardian_reacted, "Guardian did not trigger autofocus within timeout"

    # Останавливаем
    await fake_nina.stop_sequence()
    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()
    await mode_manager.stop()


@pytest.mark.asyncio
async def test_full_session_with_meridian_flip(tmp_path: Path):
    """
    E2E тест: сессия с Meridian Flip.

    ИСПРАВЛЕНО (audit 13.2): Использует wait_for_condition.
    """
    fake_nina = FakeNinaAPI(session_dir=tmp_path / "session2")
    watcher_manager = WatcherManager()
    orchestrator = Orchestrator()

    await watcher_manager.start()
    await orchestrator.start()
    await fake_nina.start()

    await fake_nina.start_sequence(target="M42", frames=5)

    # Ждём начала секвенсора
    sequence_started = await wait_for_condition(
        lambda: fake_nina.sequence_running,
        timeout=5.0,
        description="sequence started",
    )
    assert sequence_started

    # Запускаем Meridian Flip
    await fake_nina.trigger_meridian_flip()

    # Ждём завершения Meridian Flip
    flip_completed = await wait_for_condition(
        lambda: not fake_nina.meridian_flip_triggered,
        timeout=35.0,  # Meridian flip занимает 30 секунд
        interval=0.5,
        description="meridian flip completed",
    )
    assert flip_completed, "Meridian flip did not complete within timeout"

    # Останавливаем
    await fake_nina.stop_sequence()
    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()


@pytest.mark.asyncio
async def test_full_session_with_safety_unsafe(tmp_path: Path):
    """
    E2E тест: сессия с переходом Safety Monitor в UNSAFE.

    ИСПРАВЛЕНО (audit 13.2): Использует wait_for_condition.
    """
    fake_nina = FakeNinaAPI(session_dir=tmp_path / "session3")
    watcher_manager = WatcherManager()
    orchestrator = Orchestrator()
    guardian = GuardianAgent()

    orchestrator.register_agent("Guardian", guardian)

    await watcher_manager.start()
    await orchestrator.start()
    await guardian.initialize()
    await fake_nina.start()

    await fake_nina.start_sequence(target="M31", frames=10)

    # Ждём запуска
    await wait_for_condition(
        lambda: fake_nina.sequence_running,
        timeout=5.0,
    )

    # Инжектируем UNSAFE
    await fake_nina.inject_anomaly("safety_unsafe")

    # Ждём реакции Guardian
    guardian_reacted = await wait_for_condition(
        lambda: len(guardian._decision_log) > 0,
        timeout=10.0,
        interval=0.2,
        description="Guardian reacted to UNSAFE",
    )
    assert guardian_reacted, "Guardian did not react to UNSAFE within timeout"

    # Останавливаем
    await fake_nina.stop_sequence()
    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()


@pytest.mark.asyncio
async def test_mode_switching_on_llm_failure():
    """
    E2E тест: переключение в SAFE_AUTONOMOUS при потере LLM API.

    ИСПРАВЛЕНО (audit 13.2): Убраны лишние sleep.
    """
    from unittest.mock import patch, AsyncMock

    mode_manager = ModeManager()
    await mode_manager.start()

    # Проверяем начальный режим
    assert mode_manager.current_mode == OperationMode.FULL_AI

    # Симулируем потерю LLM
    with patch("app.core.mode_manager.httpx.AsyncClient") as mock_client:
        mock_response = AsyncMock()
        mock_response.status_code = 503
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )

        await mode_manager._check_llm_health()

    # Проверяем переключение (без sleep — оно уже синхронное после await)
    assert mode_manager.current_mode == OperationMode.SAFE_AUTONOMOUS
    assert not mode_manager.llm_healthy

    await mode_manager.stop()


@pytest.mark.asyncio
async def test_session_digest_generation(tmp_path: Path):
    """
    E2E тест: генерация Session Digest после завершения сессии.

    ИСПРАВЛЕНО (audit 13.2): Использует wait_for_condition.
    """
    fake_nina = FakeNinaAPI(session_dir=tmp_path / "session4")
    watcher_manager = WatcherManager()
    orchestrator = Orchestrator()
    auditor = AuditorAgent()

    orchestrator.register_agent("Auditor", auditor)

    await watcher_manager.start()
    await orchestrator.start()
    await auditor.initialize()
    await fake_nina.start()

    # Запускаем и останавливаем секвенсор
    await fake_nina.start_sequence(target="M31", frames=5)

    # Ждём генерации кадров
    await wait_for_condition(
        lambda: fake_nina.frame_count >= 5,
        timeout=15.0,
        interval=0.2,
    )

    # Останавливаем
    await fake_nina.stop_sequence()

    # ИСПРАВЛЕНО (audit 13.2): Ждём генерации Session Digest
    digest_generated = await wait_for_condition(
        lambda: len(auditor._decision_log) > 0,
        timeout=10.0,
        interval=0.2,
        description="Session Digest generated",
    )
    assert digest_generated, "Auditor did not generate Session Digest"

    last_decision = auditor._decision_log[-1]
    assert last_decision.decision_type == "SESSION_DIGEST_GENERATED"
    assert "digest" in last_decision.outputs

    # Останавливаем
    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()
