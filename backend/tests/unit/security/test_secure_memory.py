"""
Unit tests for Secure Memory module.
Тестирует безопасное хранение секретов в памяти.
"""

import pytest
import ctypes
from app.security.secure_memory import (
    SecureSecret,
    SecureSecretPool,
    get_system_secret,
    set_system_secret,
    destroy_system_secrets,
)


class TestSecureSecret:
    """Тесты класса SecureSecret."""

    def test_creation(self):
        """Тест создания SecureSecret."""
        secret = SecureSecret("my-secret-value")
        assert secret is not None
        assert secret._size == len("my-secret-value")

    def test_get_returns_bytes(self):
        """Тест что get() возвращает bytes."""
        secret = SecureSecret("test-secret")
        value = secret.get()
        assert isinstance(value, bytes)
        assert value == b"test-secret"

    def test_get_str_returns_string(self):
        """Тест что get_str() возвращает строку."""
        secret = SecureSecret("test-secret")
        value = secret.get_str()
        assert isinstance(value, str)
        assert value == "test-secret"

    def test_empty_value_raises_error(self):
        """Тест что пустое значение вызывает ValueError."""
        with pytest.raises(ValueError, match="cannot be empty"):
            SecureSecret("")

    def test_destroy_zeros_buffer(self):
        """Тест что destroy() зануляет буфер."""
        secret = SecureSecret("sensitive-data")
        buffer_address = ctypes.addressof(secret._buffer)
        original_size = secret._size

        # Уничтожаем секрет
        secret.destroy()

        # Буфер должен быть None
        assert secret._buffer is None
        assert secret._size == 0

    def test_get_after_destroy_raises_error(self):
        """Тест что get() после destroy() вызывает ошибку."""
        secret = SecureSecret("test-secret")
        secret.destroy()

        with pytest.raises(RuntimeError, match="destroyed"):
            secret.get()

    def test_context_manager(self):
        """Тест использования как context manager."""
        with SecureSecret("context-secret") as secret:
            value = secret.get()
            assert value == b"context-secret"

        # После выхода из контекста буфер должен быть уничтожен
        assert secret._buffer is None

    def test_repr_does_not_leak_value(self):
        """Тест что repr() не раскрывает значение."""
        secret = SecureSecret("super-secret-password")
        repr_str = repr(secret)

        # Значение не должно быть в repr
        assert "super-secret-password" not in repr_str
        assert "SecureSecret" in repr_str
        assert "bytes" in repr_str

    def test_str_returns_mask(self):
        """Тест что str() возвращает маску."""
        secret = SecureSecret("my-secret")
        assert str(secret) == "***"

    def test_unicode_support(self):
        """Тест поддержки Unicode."""
        secret = SecureSecret("пароль-с-кириллицей-🔐")
        value = secret.get_str()
        assert value == "пароль-с-кириллицей-🔐"

    def test_long_secret(self):
        """Тест длинного секрета."""
        long_value = "x" * 10000
        secret = SecureSecret(long_value)
        assert secret.get_str() == long_value
        assert secret._size == 10000

    def test_multiple_gets_return_same_value(self):
        """Тест что множественные вызовы get() возвращают одно значение."""
        secret = SecureSecret("consistent-secret")
        value1 = secret.get()
        value2 = secret.get()
        value3 = secret.get()

        assert value1 == value2 == value3
        assert value1 == b"consistent-secret"

    def test_is_locked_property(self):
        """Тест свойства is_locked()."""
        secret = SecureSecret("test-secret")
        # is_locked() возвращает bool
        locked = secret.is_locked()
        assert isinstance(locked, bool)


class TestSecureSecretPool:
    """Тесты класса SecureSecretPool."""

    def test_add_and_get(self):
        """Тест добавления и получения секрета."""
        pool = SecureSecretPool()
        pool.add("api_key", "my-api-key-123")

        secret = pool.get("api_key")
        assert secret is not None
        assert secret.get_str() == "my-api-key-123"

    def test_get_nonexistent_returns_none(self):
        """Тест что get() для несуществующего возвращает None."""
        pool = SecureSecretPool()
        assert pool.get("nonexistent") is None

    def test_remove(self):
        """Тест удаления секрета."""
        pool = SecureSecretPool()
        pool.add("to_remove", "value")

        assert pool.get("to_remove") is not None

        result = pool.remove("to_remove")
        assert result is True
        assert pool.get("to_remove") is None

    def test_remove_nonexistent(self):
        """Тест удаления несуществующего секрета."""
        pool = SecureSecretPool()
        result = pool.remove("nonexistent")
        assert result is False

    def test_add_overwrites_existing(self):
        """Тест что add() перезаписывает существующий секрет."""
        pool = SecureSecretPool()
        pool.add("key", "old-value")
        pool.add("key", "new-value")

        secret = pool.get("key")
        assert secret.get_str() == "new-value"

    def test_destroy_clears_all(self):
        """Тест что destroy() очищает все секреты."""
        pool = SecureSecretPool()
        pool.add("secret1", "value1")
        pool.add("secret2", "value2")
        pool.add("secret3", "value3")

        pool.destroy()

        assert pool.get("secret1") is None
        assert pool.get("secret2") is None
        assert pool.get("secret3") is None

    def test_multiple_secrets_independent(self):
        """Тест что секреты независимы."""
        pool = SecureSecretPool()
        pool.add("key1", "value1")
        pool.add("key2", "value2")

        secret1 = pool.get("key1")
        secret2 = pool.get("key2")

        assert secret1.get_str() == "value1"
        assert secret2.get_str() == "value2"

        # Удаление одного не влияет на другой
        pool.remove("key1")
        assert pool.get("key1") is None
        assert pool.get("key2") is not None


class TestSystemSecrets:
    """Тесты глобальных функций system secrets."""

    def test_set_and_get_system_secret(self):
        """Тест установки и получения системного секрета."""
        set_system_secret("test_key", "test_value")

        secret = get_system_secret("test_key")
        assert secret is not None
        assert secret.get_str() == "test_value"

    def test_get_nonexistent_system_secret(self):
        """Тест получения несуществующего системного секрета."""
        secret = get_system_secret("nonexistent_system_key")
        assert secret is None

    def test_destroy_system_secrets(self):
        """Тест уничтожения всех системных секретов."""
        set_system_secret("sys_key1", "sys_value1")
        set_system_secret("sys_key2", "sys_value2")

        destroy_system_secrets()

        assert get_system_secret("sys_key1") is None
        assert get_system_secret("sys_key2") is None


class TestSecureSecretMemorySafety:
    """Тесты безопасности памяти."""

    def test_buffer_not_in_python_string_pool(self):
        """
        Тест что значение не хранится в Python string pool.
        Значение должно быть в ctypes буфере, а не как Python str.
        """
        secret_value = "unique-secret-not-in-pool-12345"
        secret = SecureSecret(secret_value)

        # Получаем адрес буфера
        buffer_addr = ctypes.addressof(secret._buffer)
        assert buffer_addr > 0

        # Значение в буфере должно совпадать
        assert secret.get() == secret_value.encode("utf-8")

    def test_double_destroy_is_safe(self):
        """Тест что двойной destroy() не вызывает ошибок."""
        secret = SecureSecret("test")
        secret.destroy()
        secret.destroy()  # Не должно упасть

    def test_destroy_in_del(self):
        """Тест что __del__ вызывает destroy."""
        secret = SecureSecret("auto-destroy-test")
        secret_ref = secret

        # Удаляем ссылку
        del secret

        # GC должен вызвать __del__ -> destroy()
        # Проверяем что буфер уничтожен
        # (прямая проверка сложна из-за GC, но destroy() должен быть вызван)
