"""
Тесты для TriggerEmulator.
Покрывает: валидацию параметров, защиту protected_params,
точный поиск эндпоинтов, обработку HTTP-результатов.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from app.execution.trigger_emulator import TriggerEmulator


@pytest.fixture
def emulator():
    emu = TriggerEmulator()
    # Минимальный реестр для тестов
    emu._registry = {
        "autofocus": {
            "method": "POST",
            "path": "/api/v1/autofocus/run",
            "params": {"wait": True},
            "parameter_ranges": {"wait": {"type": "boolean"}},
            "protected_params": {"wait"},
            "from_openapi": False,
            "risk_level": "MEDIUM",
            "category": "focuser",
        },
        "guider_start": {
            "method": "POST",
            "path": "/api/v1/guider/start",
            "params": {},
            "parameter_ranges": {},
            "protected_params": set(),
            "from_openapi": False,
            "risk_level": "LOW",
            "category": "guider",
        },
    }
    emu._openapi_client = None
    return emu


@pytest.mark.asyncio
async def test_merge_params_protected_rejection(emulator):
    """Защищённые параметры не должны перезаписываться."""
    trigger_config = emulator._registry["autofocus"]
    user_params = {"wait": False, "timeout": 30}

    merged, rejected = emulator._merge_parameters(trigger_config, user_params)

    assert "wait" in rejected
    assert merged["wait"] is True  # Original preserved
    assert merged["timeout"] == 30


@pytest.mark.asyncio
async def test_merge_params_type_validation(emulator):
    """Невалидные типы должны отклоняться."""
    trigger_config = emulator._registry["autofocus"]
    user_params = {"wait": "not_a_boolean"}

    merged, rejected = emulator._merge_parameters(trigger_config, user_params)

    assert "wait" in rejected
    assert merged["wait"] is True


@pytest.mark.asyncio
async def test_process_http_result_success(emulator):
    """Успешный ответ должен возвращать True и публиковать событие."""
    with patch("app.execution.trigger_emulator.event_bus") as mock_bus:
        mock_bus.publish = AsyncMock()

        result = emulator._process_http_result(
            result={"status": "success", "Response": "OK"},
            trigger_name="autofocus",
            actual_trigger="autofocus",
            params={"wait": True},
            rejected=[],
            reason="test",
        )

        assert result is True
        mock_bus.publish.assert_called_once()
        call_args = mock_bus.publish.call_args
        assert call_args[0][0] == "TRIGGER_FIRED"


@pytest.mark.asyncio
async def test_process_http_result_failure(emulator):
    """Ошибка API должна возвращать False."""
    with patch("app.execution.trigger_emulator.event_bus") as mock_bus:
        mock_bus.publish = AsyncMock()

        result = emulator._process_http_result(
            result={"status": "error", "code": 500, "message": "Internal error"},
            trigger_name="autofocus",
            actual_trigger="autofocus",
            params={},
            rejected=[],
            reason="test",
        )

        assert result is False
        assert emulator._stats["failed_triggers"] == 1
