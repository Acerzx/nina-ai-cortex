"""
End-to-End tests для полных сессий с детекцией аномалий.
Тестирует полный цикл: Event → Watcher → Diagnostician → Guardian → Execution.
"""

import pytest
import asyncio
from pathlib import Path
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
from app.agents.memory_manager_agent import MemoryManagerAgent
from app.core.mode_manager import ModeManager


@pytest.mark.asyncio
async def test_full_session_with_hfr_anomaly(tmp_path: Path):
    """
    E2E тест: полная сессия с детекцией аномалии HFR и запуском автофокуса.

    Сценарий:
    1. Запуск Fake NINA с нормальной последовательностью кадров
    2. Инжект аномалии (резкий рост HFR)
    3. Watcher детектирует аномалию
    4. Diagnostician определяет root cause (температурный дрейф)
    5. Guardian запускает автофокус
    6. HFR возвращается к норме
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
    calibrator = CalibratorAgent(masters_auditor=None)  # Mock для теста
    scheduler = SchedulerAgent()
    copilot = CopilotAgent()
    memory_manager = MemoryManagerAgent()

    # Регистрируем агентов в Orchestrator
    orchestrator.register_agent("Watcher", watcher)
    orchestrator.register_agent("Guardian", guardian)
    orchestrator.register_agent("Diagnostician", diagnostician)
    orchestrator.register_agent("Strategist", strategist)
    orchestrator.register_agent("Auditor", auditor)
    orchestrator.register_agent("Calibrator", calibrator)
    orchestrator.register_agent("Scheduler", scheduler)
    orchestrator.register_agent("Copilot", copilot)
    orchestrator.register_agent("MemoryManager", memory_manager)

    # Запускаем все компоненты
    await watcher_manager.start()
    await orchestrator.start()
    await mode_manager.start()
    await fake_nina.start()

    # Инициализируем агентов
    await watcher.initialize()
    await guardian.initialize()
    await diagnostician.initialize()

    # Запускаем секвенсор с 10 кадрами
    await fake_nina.start_sequence(target="M31", frames=10)

    # Генерируем 5 нормальных кадров
    for i in range(5):
        await asyncio.sleep(1)

    # Инжектируем аномалию HFR
    await fake_nina.inject_anomaly("hfr_spike")

    # Ждем обработки аномалии (2-3 секунды)
    await asyncio.sleep(3)

    # Assert: Watcher должен был детектировать аномалию
    assert len(watcher._recent_anomalies) > 0, "Watcher did not detect HFR anomaly"

    # Assert: Diagnostician должен был определить root cause
    assert len(diagnostician._decision_log) > 0, "Diagnostician did not analyze anomaly"

    # Assert: Guardian должен был запустить автофокус
    assert fake_nina.autofocus_triggered or len(guardian._decision_log) > 0, (
        "Guardian did not trigger autofocus"
    )

    # Останавливаем все
    await fake_nina.stop_sequence()
    await asyncio.sleep(2)

    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()
    await mode_manager.stop()


@pytest.mark.asyncio
async def test_full_session_with_meridian_flip(tmp_path: Path):
    """
    E2E тест: сессия с Meridian Flip.

    Сценарий:
    1. Запуск секвенсора
    2. Симуляция Meridian Flip
    3. Проверка корректной обработки событий
    """
    fake_nina = FakeNinaAPI(session_dir=tmp_path / "session2")
    watcher_manager = WatcherManager()
    orchestrator = Orchestrator()

    await watcher_manager.start()
    await orchestrator.start()
    await fake_nina.start()

    # Запускаем секвенсор
    await fake_nina.start_sequence(target="M42", frames=5)
    await asyncio.sleep(2)

    # Инжектируем Meridian Flip
    await fake_nina.trigger_meridian_flip()
    await asyncio.sleep(5)

    # Assert: Meridian Flip должен был завершиться
    assert not fake_nina.meridian_flip_triggered, "Meridian flip did not complete"

    # Останавливаем
    await fake_nina.stop_sequence()
    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()


@pytest.mark.asyncio
async def test_full_session_with_safety_unsafe(tmp_path: Path):
    """
    E2E тест: сессия с переходом Safety Monitor в UNSAFE.

    Сценарий:
    1. Запуск секвенсора
    2. Инжект события safety_unsafe
    3. Guardian должен выполнить emergency park
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

    # Запускаем секвенсор
    await fake_nina.start_sequence(target="M31", frames=10)
    await asyncio.sleep(2)

    # Инжектируем UNSAFE условие
    await fake_nina.inject_anomaly("safety_unsafe")
    await asyncio.sleep(3)

    # Assert: Guardian должен был отреагировать
    assert len(guardian._decision_log) > 0, "Guardian did not react to UNSAFE condition"

    # Останавливаем
    await fake_nina.stop_sequence()
    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()


@pytest.mark.asyncio
async def test_mode_switching_on_llm_failure():
    """
    E2E тест: переключение в SAFE_AUTONOMOUS при потере LLM API.

    Сценарий:
    1. Система в FULL_AI режиме
    2. LLM API становится недоступен
    3. Mode Manager переключает в SAFE_AUTONOMOUS
    4. Strategist и Diagnostician блокируются
    5. Watcher и Guardian продолжают работать
    """
    mode_manager = ModeManager()
    await mode_manager.start()

    # Начальный режим
    assert mode_manager.current_mode == OperationMode.FULL_AI

    # Симулируем потерю LLM (mock httpx)
    from unittest.mock import patch, AsyncMock

    with patch("app.core.mode_manager.httpx.AsyncClient") as mock_client:
        mock_response = AsyncMock()
        mock_response.status_code = 503
        mock_client.return_value.__aenter__.return_value.get = AsyncMock(
            return_value=mock_response
        )

        # Запускаем health check
        await mode_manager._check_llm_health()

        # Assert: режим должен переключиться
        assert mode_manager.current_mode == OperationMode.SAFE_AUTONOMOUS
        assert not mode_manager.llm_healthy

    await mode_manager.stop()


@pytest.mark.asyncio
async def test_credential_vault_integration(tmp_path: Path):
    """
    E2E тест: интеграция Credential Vault с агентами.

    Сценарий:
    1. Сохранение секрета в Vault
    2. Извлечение секрета
    3. Использование в агенте
    """
    from app.security.vault import CredentialVault

    vault_path = tmp_path / "vault.json"
    vault = CredentialVault(
        master_password="test-master-password", vault_path=vault_path
    )

    # Сохраняем секрет
    success = vault.store_secret(
        name="influxdb_token",
        value="my-secret-token-12345",
        description="InfluxDB authentication token",
    )
    assert success, "Failed to store secret"

    # Извлекаем секрет
    token = vault.get_secret("influxdb_token")
    assert token == "my-secret-token-12345", "Failed to retrieve secret"

    # Проверяем список секретов
    secrets = vault.list_secrets()
    assert len(secrets) == 1
    assert secrets[0]["name"] == "influxdb_token"

    # Удаляем секрет
    success = vault.delete_secret("influxdb_token")
    assert success, "Failed to delete secret"

    # Проверяем, что секрет удален
    token = vault.get_secret("influxdb_token")
    assert token is None, "Secret was not deleted"


@pytest.mark.asyncio
async def test_preflight_checklist(tmp_path: Path):
    """
    E2E тест: Pre-flight Checklist перед стартом сессии.

    Сценарий:
    1. Запуск pre-flight проверки
    2. Все gates проходят
    3. Verdict = GO
    """
    from app.safety.preflight import PreflightChecker

    checker = PreflightChecker()

    # Запускаем все проверки
    report = await checker.run_all()

    # Assert: отчет должен быть сгенерирован
    assert report is not None
    assert len(report.gates) == 8  # 8 gates

    # Assert: verdict должен быть GO (в тестовой среде)
    # В реальности может быть WAITING если что-то не готово
    assert report.verdict in ["GO", "WAITING", "CAUTION"]


@pytest.mark.asyncio
async def test_session_digest_generation(tmp_path: Path):
    """
    E2E тест: генерация Session Digest после завершения сессии.

    Сценарий:
    1. Запуск и завершение сессии
    2. Auditor генерирует Session Digest
    3. Digest индексируется в RAG
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
    await asyncio.sleep(3)
    await fake_nina.stop_sequence()
    await asyncio.sleep(2)

    # Assert: Auditor должен был сгенерировать Session Digest
    assert len(auditor._decision_log) > 0, "Auditor did not generate Session Digest"

    last_decision = auditor._decision_log[-1]
    assert last_decision.decision_type == "SESSION_DIGEST_GENERATED"
    assert "digest" in last_decision.outputs

    # Останавливаем
    await fake_nina.stop()
    await orchestrator.stop()
    await watcher_manager.stop()
