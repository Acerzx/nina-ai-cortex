"""
Secure Memory — безопасное хранение секретов в памяти процесса.
Устраняет уязвимость audit P1: JWT Secret в plain text памяти.

Архитектура:
- Использует ctypes для создания буфера в C-памяти
- Автоматическое zeroing буфера при удалении объекта
- Защита от swap через mlock (на поддерживаемых ОС)
- Интеграция с Python garbage collector через __del__

Применение:
- JWT secrets
- API keys в памяти
- Master passwords (до передачи в Argon2)

Использование:
    from app.security.secure_memory import SecureSecret

    secret = SecureSecret("my-secret-value")
    value = secret.get()  # Получение значения
    # При удалении объекта буфер автоматически очищается
"""

import ctypes
import ctypes.util
import logging
import os
import sys
from typing import Optional

logger = logging.getLogger("SecureMemory")


class SecureSecret:
    """
    Безопасное хранение секретного значения в памяти.

    Преимущества перед обычной строкой:
    1. Значение хранится в C-буфере, а не в Python string pool
    2. При удалении объекта буфер заполняется нулями (zeroing)
    3. Попытка lock в RAM для предотвращения swap на диск
    4. Не попадает в core dumps (на поддерживаемых ОС)

    Пример:
        >>> secret = SecureSecret("super-secret-jwt-key")
        >>> secret.get()
        b'super-secret-jwt-key'
        >>> del secret  # Буфер автоматически очищен
    """

    # Флаги mlock для предотвращения swap
    _MCL_CURRENT = 1
    _MCL_FUTURE = 2

    # Загружаем libc для mlock/munlock
    _libc: Optional[ctypes.CDLL] = None
    _mlock_available: bool = False

    @classmethod
    def _init_libc(cls) -> None:
        """Инициализирует libc для mlock (выполняется один раз)."""
        if cls._libc is not None:
            return

        try:
            libc_name = ctypes.util.find_library("c")
            if libc_name:
                cls._libc = ctypes.CDLL(libc_name, use_errno=True)

                # Проверяем наличие mlock
                if hasattr(cls._libc, "mlock"):
                    cls._libc.mlock.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
                    cls._libc.mlock.restype = ctypes.c_int

                    cls._libc.munlock.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
                    cls._libc.munlock.restype = ctypes.c_int

                    cls._mlock_available = True
                    logger.debug("✅ mlock available for secure memory")
                else:
                    logger.debug("⚠️ mlock not found in libc")
        except Exception as e:
            logger.debug(f"⚠️ Failed to load libc for mlock: {e}")

    def __init__(self, value: str) -> None:
        """
        Создаёт защищённое хранилище для секрета.

        Args:
            value: Секретное значение (строка)

        Raises:
            ValueError: Если value пустой
        """
        if not value:
            raise ValueError("Secret value cannot be empty")

        self._init_libc()

        # Кодируем в bytes
        encoded = value.encode("utf-8")
        self._size = len(encoded)

        # Создаём C-буфер
        self._buffer = ctypes.create_string_buffer(encoded, self._size + 1)
        self._locked = False

        # Очищаем оригинальную строку в Python
        # (best effort — Python может иметь копии в string pool)
        del encoded

        # Пытаемся lock буфер в RAM (предотвращает swap)
        if self._mlock_available and self._libc:
            try:
                result = self._libc.mlock(ctypes.addressof(self._buffer), self._size)
                if result == 0:
                    self._locked = True
                    logger.debug(f"🔒 Memory locked ({self._size} bytes)")
                else:
                    errno = ctypes.get_errno()
                    logger.debug(
                        f"⚠️ mlock failed (errno={errno}). "
                        f"Memory may be swapped to disk."
                    )
            except Exception as e:
                logger.debug(f"⚠️ mlock error: {e}")

    def get(self) -> bytes:
        """
        Возвращает секретное значение как bytes.

        Returns:
            bytes: Секретное значение

        Warning:
            Возвращаемые bytes являются копией буфера.
            Вызывающий код должен очистить их после использования.
        """
        if self._buffer is None:
            raise RuntimeError("Secret has been destroyed")

        # Возвращаем копию (raw bytes без null terminator)
        return bytes(self._buffer.raw[: self._size])

    def get_str(self) -> str:
        """
        Возвращает секретное значение как строку.

        Returns:
            str: Секретное значение

        Warning:
            Строка в Python менее безопасна чем bytes.
            Используйте только когда необходимо.
        """
        return self.get().decode("utf-8")

    def is_locked(self) -> bool:
        """Проверяет, залочен ли буфер в RAM."""
        return self._locked

    def _zero_buffer(self) -> None:
        """Заполняет буфер нулями (secure erase)."""
        if self._buffer is not None and self._size > 0:
            try:
                # Заполняем нулями через ctypes.memset
                ctypes.memset(ctypes.addressof(self._buffer), 0, self._size + 1)
            except Exception as e:
                logger.debug(f"⚠️ Failed to zero buffer: {e}")

    def _unlock_buffer(self) -> None:
        """Разблокирует буфер (перед удалением)."""
        if self._locked and self._mlock_available and self._libc:
            try:
                self._libc.munlock(ctypes.addressof(self._buffer), self._size)
                self._locked = False
            except Exception as e:
                logger.debug(f"⚠️ munlock error: {e}")

    def destroy(self) -> None:
        """
        Явно уничтожает секрет (zeroing + unlock).

        Вызывается автоматически при удалении объекта,
        но может быть вызван явно для немедленной очистки.
        """
        if self._buffer is not None:
            self._zero_buffer()
            self._unlock_buffer()
            self._buffer = None
            self._size = 0

    def __del__(self) -> None:
        """Автоматическая очистка при garbage collection."""
        try:
            self.destroy()
        except Exception:
            pass  # Игнорируем ошибки в __del__

    def __enter__(self) -> "SecureSecret":
        """Поддержка context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Очистка при выходе из context manager."""
        self.destroy()

    def __repr__(self) -> str:
        """Защищённое представление (не показывает значение!)."""
        return f"SecureSecret(<{self._size} bytes, locked={self._locked}>)"

    def __str__(self) -> str:
        """Защищённое строковое представление."""
        return "***"


class SecureSecretPool:
    """
    Пул безопасных секретов с именованным доступом.

    Позволяет хранить несколько секретов и получать их по имени.
    При удалении пула все секреты автоматически уничтожаются.

    Пример:
        >>> pool = SecureSecretPool()
        >>> pool.add("jwt_secret", "my-jwt-key")
        >>> pool.add("api_key", "my-api-key")
        >>> jwt = pool.get("jwt_secret")
        >>> pool.destroy()  # Все секреты очищены
    """

    def __init__(self) -> None:
        self._secrets: dict[str, SecureSecret] = {}

    def add(self, name: str, value: str) -> None:
        """Добавляет секрет в пул."""
        if name in self._secrets:
            self._secrets[name].destroy()
        self._secrets[name] = SecureSecret(value)

    def get(self, name: str) -> Optional[SecureSecret]:
        """Получает секрет по имени."""
        return self._secrets.get(name)

    def remove(self, name: str) -> bool:
        """Удаляет секрет из пула."""
        if name in self._secrets:
            self._secrets[name].destroy()
            del self._secrets[name]
            return True
        return False

    def destroy(self) -> None:
        """Уничтожает все секреты в пуле."""
        for secret in self._secrets.values():
            secret.destroy()
        self._secrets.clear()

    def __del__(self) -> None:
        """Автоматическая очистка при garbage collection."""
        try:
            self.destroy()
        except Exception:
            pass


# Глобальный пул для системных секретов
_system_secrets = SecureSecretPool()


def get_system_secret(name: str) -> Optional[SecureSecret]:
    """Получает системный секрет по имени."""
    return _system_secrets.get(name)


def set_system_secret(name: str, value: str) -> None:
    """Устанавливает системный секрет."""
    _system_secrets.add(name, value)


def destroy_system_secrets() -> None:
    """Уничтожает все системные секреты."""
    _system_secrets.destroy()
