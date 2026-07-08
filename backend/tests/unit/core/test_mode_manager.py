"""
Unit tests for Mode Manager.
Тестирует переключение режимов и health check.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
import asyncio

from app.core.mode_manager import ModeManager, OperationMode


class TestModeManager:
    """Тесты Mode Manager."""

    @pytest.fixture
    async def mode_manager(self):
        """Создаёт тестовый ModeManager."""
        manager = ModeManager()
        await manager.start()
        yield manager
        await manager.stop()

    @pytest.mark.asyncio
    async def test_initial_mode_is_full_ai(self, mode_manager):
        """Тест что начальный режим FULL_AI."""
        assert mode_manager.current_mode == OperationMode.FULL_AI
        assert mode_manager.llm_healthy is True

    @pytest.mark.asyncio
    async def test_set_mode_to_safe(self, mode_manager):
        """Тест переключения в SAFE_AUTONOMOUS."""
        await mode_manager.set_mode(OperationMode.SAFE_AUTONOMOUS, reason="Test switch")

        assert mode_manager.current_mode == OperationMode.SAFE_AUTONOMOUS

    @pytest.mark.asyncio
    async def test_set_mode_to_manual(self, mode_manager):
        """Тест переключения в MANUAL."""
        await mode_manager.set_mode(OperationMode.MANUAL, reason="Test")

        assert mode_manager.current_mode == OperationMode.MANUAL

    @pytest.mark.asyncio
    async def test_set_mode_to_simulation(self, mode_manager):
        """Тест переключения в SIMULATION."""
        await mode_manager.set_mode(OperationMode.SIMULATION, reason="Test")

        assert mode_manager.current_mode == OperationMode.SIMULATION

    @pytest.mark.asyncio
    async def test_mode_change_publishes_event(self, mode_manager):
        """Тест что смена режима публикует событие."""
        received = []

        async def handler(data):
            received.append(data)

        # Подписываемся на событие
        from app.core.events import event_bus

        event_bus.subscribe("MODE_CHANGED", handler)

        # Меняем режим
        await mode_manager.set_mode(OperationMode.SAFE_AUTONOMOUS, reason="Test reason")

        # Ждём обработки
        await asyncio.sleep(0.1)

        # Проверяем
        assert len(received) == 1
        assert received[0]["old_mode"] == "full_ai"
        assert received[0]["new_mode"] == "safe"
        assert received[0]["reason"] == "Test reason"

        # Отписываемся
        event_bus.unsubscribe("MODE_CHANGED", handler)

    @pytest.mark.asyncio
    async def test_agent_permissions_full_ai(self, mode_manager):
        """Тест разрешений в FULL_AI режиме."""
        permissions = mode_manager.agent_permissions[OperationMode.FULL_AI]

        # В FULL_AI все агенты разрешены
        assert permissions["Watcher"] is True
        assert permissions["Guardian"] is True
        assert permissions["Diagnostician"] is True
        assert permissions["Strategist"] is True
        assert permissions["Scheduler"] is True
        assert permissions["Auditor"] is True
        assert permissions["Calibrator"] is True
        assert permissions["Copilot"] is True

    @pytest.mark.asyncio
    async def test_agent_permissions_safe_mode(self, mode_manager):
        """Тест разрешений в SAFE режиме."""
        permissions = mode_manager.agent_permissions[OperationMode.SAFE_AUTONOMOUS]

        # Watcher и Guardian разрешены
        assert permissions["Watcher"] is True
        assert permissions["Guardian"] is True

        # Strategist и Diagnostician запрещены
        assert permissions["Strategist"] is False
        assert permissions["Diagnostician"] is False

    @pytest.mark.asyncio
    async def test_is_agent_allowed(self, mode_manager):
        """Тест проверки разрешения агента."""
        # В FULL_AI все разрешены
        assert mode_manager.is_agent_allowed("Watcher") is True
        assert mode_manager.is_agent_allowed("Strategist") is True

        # Переключаем в SAFE
        await mode_manager.set_mode(OperationMode.SAFE_AUTONOMOUS, reason="Test")

        # Watcher разрешён, Strategist — нет
        assert mode_manager.is_agent_allowed("Watcher") is True
        assert mode_manager.is_agent_allowed("Strategist") is False

    @pytest.mark.asyncio
    async def test_health_check_success(self, mode_manager):
        """Тест успешного health check."""
        with patch("app.core.mode_manager.httpx.AsyncClient") as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )

            await mode_manager._check_llm_health()

            assert mode_manager.llm_healthy is True

    @pytest.mark.asyncio
    async def test_health_check_failure_switches_mode(self, mode_manager):
        """Тест что сбой health check переключает в SAFE."""
        with patch("app.core.mode_manager.httpx.AsyncClient") as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 503
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )

            # Начинаем в FULL_AI
            assert mode_manager.current_mode == OperationMode.FULL_AI

            await mode_manager._check_llm_health()

            # Должен переключиться в SAFE
            assert mode_manager.current_mode == OperationMode.SAFE_AUTONOMOUS
            assert mode_manager.llm_healthy is False

    @pytest.mark.asyncio
    async def test_health_check_recovery(self, mode_manager):
        """Тест восстановления после сбоя."""
        # Сначала переключаем в SAFE
        await mode_manager.set_mode(OperationMode.SAFE_AUTONOMOUS, reason="Test")
        mode_manager.llm_healthy = False

        # Симулируем восстановление
        with patch("app.core.mode_manager.httpx.AsyncClient") as mock_client:
            mock_response = AsyncMock()
            mock_response.status_code = 200
            mock_client.return_value.__aenter__.return_value.get = AsyncMock(
                return_value=mock_response
            )

            await mode_manager._check_llm_health()

            assert mode_manager.llm_healthy is True
            assert mode_manager.current_mode == OperationMode.FULL_AI

    @pytest.mark.asyncio
    async def test_get_stats(self, mode_manager):
        """Тест статистики Mode Manager."""
        stats = mode_manager.get_stats()

        assert "current_mode" in stats
        assert "llm_healthy" in stats
        assert "agent_permissions" in stats
        assert stats["current_mode"] == "full_ai"

    @pytest.mark.asyncio
    async def test_start_and_stop(self):
        """Тест запуска и остановки."""
        manager = ModeManager()

        await manager.start()
        assert manager._running is True
        assert manager._health_check_task is not None

        await manager.stop()
        assert manager._running is False

    @pytest.mark.asyncio
    async def test_double_start_is_safe(self, mode_manager):
        """Тест что двойной start не вызывает ошибок."""
        # Уже запущен в fixture
        await mode_manager.start()  # Не должен упасть

        assert mode_manager._running is True

    @pytest.mark.asyncio
    async def test_stop_without_start(self):
        """Тест что stop без start не вызывает ошибок."""
        manager = ModeManager()

        # Не вызываем start
        await manager.stop()  # Не должен упасть

    @pytest.mark.asyncio
    async def test_health_check_loop(self, mode_manager):
        """Тест работы health check loop."""
        # Проверяем, что loop запущен
        assert mode_manager._health_check_task is not None
        assert not mode_manager._health_check_task.done()
