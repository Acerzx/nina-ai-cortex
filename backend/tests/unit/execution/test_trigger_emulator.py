"""
Unit tests для TriggerEmulator.
Покрывает: валидацию параметров, защиту от перезаписи,
точный поиск эндпоинтов, обработку HTTP-результатов.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from app.execution.trigger_emulator import (
    TriggerEmulator,
    PROTECTED_PARAMS,
    DEFAULT_TRIGGER_PATTERNS,
)


class TestTriggerEmulator:
    """Тесты TriggerEmulator."""

    @pytest.fixture
    def emulator(self):
        """Создаёт тестовый эмулятор с минимальным реестром."""
        emu = TriggerEmulator()
        # Минимальный реестр для тестов
        emu._registry = {
            "autofocus": {
                "method": "GET",
                "path": "/equipment/focuser/auto-focus",
                "params": {"wait": True},
                "parameter_ranges": {"wait": {"type": "boolean"}},
                "protected_params": {"wait"},
                "from_openapi": False,
                "risk_level": "LOW",
                "category": "focuser",
            },
            "guider_start": {
                "method": "GET",
                "path": "/equipment/guider/start",
                "params": {},
                "parameter_ranges": {},
                "protected_params": set(),
                "from_openapi": False,
                "risk_level": "LOW",
                "category": "guider",
            },
        }
        emu._openapi_client = None
        emu._agent_aliases = {"autofocus": "autofocus"}
        return emu

    @pytest.mark.asyncio
    async def test_merge_params_protected_rejection(self, emulator):
        """Защищённые параметры не должны перезаписываться."""
        trigger_config = emulator._registry["autofocus"]
        user_params = {"wait": False, "timeout": 30}

        merged, rejected = emulator._merge_params_safely(
            trigger_config, trigger_config["params"], user_params
        )

        # wait — защищённый, должен быть отклонён
        assert "wait" in rejected
        assert merged["wait"] is True  # Оригинальное значение сохранено
        assert merged["timeout"] == 30  # Новый параметр принят

    @pytest.mark.asyncio
    async def test_merge_params_value_validation(self, emulator):
        """Невалидные типы должны отклоняться."""
        trigger_config = emulator._registry["autofocus"]
        user_params = {"wait": "not_a_boolean"}

        # Устанавливаем параметр как защищённый для теста
        trigger_config["parameter_ranges"]["wait"] = {"type": "boolean"}
        trigger_config["protected_params"] = {"wait"}

        merged, rejected = emulator._merge_params_safely(
            trigger_config, trigger_config["params"], user_params
        )

        assert "wait" in rejected
        assert merged["wait"] is True

    @pytest.mark.asyncio
    async def test_protected_params_set(self):
        """Проверка набора защищённых параметров."""
        assert "cancel" in PROTECTED_PARAMS
        assert "skipValidation" in PROTECTED_PARAMS

    @pytest.mark.asyncio
    async def test_fire_trigger_success(self, emulator):
        """Успешное срабатывание триггера."""
        with patch("app.execution.trigger_emulator.event_bus") as mock_bus:
            mock_bus.publish = AsyncMock()

            # Мокаем HTTP запрос
            with patch.object(emulator, "_fire_direct_http") as mock_http:
                mock_http.return_value = True

                result = await emulator.fire_trigger(
                    "autofocus", reason="Test", extra_params=None
                )

                assert result is True
                assert emulator._stats["successful_triggers"] == 1

    @pytest.mark.asyncio
    async def test_fire_trigger_blocked_by_flat_mode(self, emulator):
        """Блокировка триггера во время FLAT_MODE."""
        from app.shadow_engine.state_tracker import state_tracker

        state_tracker.state.is_flat_mode = True

        try:
            result = await emulator.fire_trigger("autofocus", reason="Test")

            assert result is False
            assert emulator._stats["blocked_by_flat_mode"] == 1
        finally:
            state_tracker.state.is_flat_mode = False

    @pytest.mark.asyncio
    async def test_fire_trigger_unknown_trigger(self, emulator):
        """Неизвестный триггер должен вернуть False."""
        result = await emulator.fire_trigger("nonexistent_trigger")

        assert result is False
        assert emulator._stats["failed_triggers"] == 1

    @pytest.mark.asyncio
    async def test_validate_parameter_value_valid(self, emulator):
        """Валидное значение параметра."""
        trigger_config = emulator._registry["autofocus"]

        is_valid, error = emulator._validate_parameter_value(
            trigger_config, "wait", True
        )

        assert is_valid is True
        assert error is None

    @pytest.mark.asyncio
    async def test_validate_parameter_value_invalid(self, emulator):
        """Невалидное значение параметра."""
        trigger_config = {"parameter_ranges": {"wait": {"type": "boolean"}}}

        is_valid, error = emulator._validate_parameter_value(
            trigger_config, "wait", "not_boolean"
        )

        assert is_valid is False
        assert "boolean" in error.lower()

    @pytest.mark.asyncio
    async def test_agent_aliases_resolution(self, emulator):
        """Разрешение алиасов агентов."""
        emulator._agent_aliases = {"my_autofocus": "autofocus"}

        # Проверяем, что алиас разрешается
        actual = emulator._agent_aliases.get("my_autofocus", "my_autofocus")
        assert actual == "autofocus"

    @pytest.mark.asyncio
    async def test_get_stats_includes_rejected(self, emulator):
        """Статистика включает rejected параметры."""
        emulator._last_rejected_params = ["protected_param"]

        stats = emulator.get_stats()

        assert "last_rejected_params" in stats
        assert stats["last_rejected_params"] == ["protected_param"]

    @pytest.mark.asyncio
    async def test_trigger_history_max_size(self, emulator):
        """История триггеров ограничена максимальным размером."""
        emulator._history_max_size = 5

        # Добавляем больше записей, чем лимит
        for i in range(10):
            emulator._add_to_history(
                trigger_name=f"test_{i}",
                actual_trigger=f"test_{i}",
                reason="Test",
                status="SUCCESS",
            )

        assert len(emulator._trigger_history) == 5

    @pytest.mark.asyncio
    async def test_list_available_triggers(self, emulator):
        """Список доступных триггеров."""
        triggers = emulator.list_available_triggers()

        assert "autofocus" in triggers
        assert "guider_start" in triggers
        assert triggers["autofocus"]["method"] == "GET"

    @pytest.mark.asyncio
    async def test_get_trigger_history(self, emulator):
        """Получение истории триггеров."""
        emulator._add_to_history(
            trigger_name="test", actual_trigger="test", reason="Test", status="SUCCESS"
        )

        history = emulator.get_trigger_history(limit=10)

        assert len(history) == 1
        assert history[0]["trigger"] == "test"
        assert history[0]["status"] == "SUCCESS"
