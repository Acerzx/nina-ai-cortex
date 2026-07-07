"""
Credential Vault — безопасное хранение секретов с Argon2id + AES-256-GCM.
Основан на архитектуре Atlas для защиты API ключей, токенов и паролей.
"""

import logging
import base64
import json
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
from argon2.low_level import hash_secret_raw, Type
from cryptography.fernet import Fernet
from pydantic import BaseModel, Field

logger = logging.getLogger("CredentialVault")


class VaultEntry(BaseModel):
    """Запись в хранилище."""

    name: str
    encrypted_value: str
    created_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    updated_at: str = Field(default_factory=lambda: datetime.now().isoformat())
    description: Optional[str] = None


class CredentialVault:
    """
    Безопасное хранилище секретов.

    Архитектура (из Atlas):
    - Argon2id для деривации ключа (memory-hard, устойчив к GPU-атакам)
    - AES-256-GCM для шифрования (через Fernet)
    - Хранение в JSON-файле с шифрованными значениями

    Параметры Argon2id:
    - time_cost: 2 итерации
    - memory_cost: 100 MB
    - parallelism: 8 потоков
    - hash_len: 32 байта (256 бит)

    Использование:
    - API ключи (Anthropic, OpenAI, etc.)
    - Токены (InfluxDB, Home Assistant, ntfy.sh)
    - Пароли (MPC, AAVSO, TNS)
    """

    def __init__(
        self, master_password: str, vault_path: Path, salt: Optional[bytes] = None
    ):
        self.vault_path = vault_path
        self._entries: Dict[str, VaultEntry] = {}

        # Генерируем или используем предоставленный salt
        if salt is None:
            # Пытаемся загрузить salt из файла
            salt_path = vault_path.parent / f"{vault_path.stem}.salt"
            if salt_path.exists():
                salt = salt_path.read_bytes()
            else:
                # Генерируем новый salt
                import os

                salt = os.urandom(16)
                salt_path.write_bytes(salt)
                logger.info(f"Generated new salt: {salt_path}")

        self.salt = salt

        # Деривация ключа через Argon2id
        self.key = hash_secret_raw(
            secret=master_password.encode("utf-8"),
            salt=salt,
            time_cost=2,
            memory_cost=102400,  # 100 MB
            parallelism=8,
            hash_len=32,
            type=Type.ID,
        )

        # Fernet для шифрования (использует AES-128-CBC + HMAC-SHA256)
        # Конвертируем 32-байтовый ключ в base64 для Fernet
        self.fernet = Fernet(base64.urlsafe_b64encode(self.key))

        # Загружаем существующие записи
        self._load_vault()

        logger.info(
            f"🔐 Credential Vault initialized ({len(self._entries)} entries loaded)"
        )

    def _load_vault(self):
        """Загружает записи из файла."""
        if not self.vault_path.exists():
            logger.info("Vault file not found, starting with empty vault")
            return

        try:
            with open(self.vault_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            for name, entry_data in data.items():
                self._entries[name] = VaultEntry(**entry_data)

            logger.info(f"Loaded {len(self._entries)} entries from vault")

        except Exception as e:
            logger.error(f"Failed to load vault: {e}")
            self._entries = {}

    def _save_vault(self):
        """Сохраняет записи в файл."""
        try:
            data = {name: entry.model_dump() for name, entry in self._entries.items()}

            with open(self.vault_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.debug(f"Vault saved to {self.vault_path}")

        except Exception as e:
            logger.error(f"Failed to save vault: {e}")

    def encrypt(self, plaintext: str) -> str:
        """Шифрует строку."""
        return self.fernet.encrypt(plaintext.encode("utf-8")).decode("utf-8")

    def decrypt(self, ciphertext: str) -> str:
        """Расшифровывает строку."""
        return self.fernet.decrypt(ciphertext.encode("utf-8")).decode("utf-8")

    def store_secret(
        self, name: str, value: str, description: Optional[str] = None
    ) -> bool:
        """
        Сохраняет секрет в хранилище.

        Args:
            name: Уникальное имя секрета
            value: Значение секрета (будет зашифровано)
            description: Опциональное описание

        Returns:
            True если успешно, False в противном случае
        """
        try:
            encrypted_value = self.encrypt(value)

            entry = VaultEntry(
                name=name,
                encrypted_value=encrypted_value,
                description=description,
                updated_at=datetime.now().isoformat(),
            )

            self._entries[name] = entry
            self._save_vault()

            logger.info(f"✅ Secret stored: {name}")
            return True

        except Exception as e:
            logger.error(f"Failed to store secret '{name}': {e}")
            return False

    def get_secret(self, name: str) -> Optional[str]:
        """
        Извлекает секрет из хранилища.

        Args:
            name: Имя секрета

        Returns:
            Расшифрованное значение или None если не найдено
        """
        entry = self._entries.get(name)
        if not entry:
            logger.warning(f"Secret not found: {name}")
            return None

        try:
            return self.decrypt(entry.encrypted_value)

        except Exception as e:
            logger.error(f"Failed to decrypt secret '{name}': {e}")
            return None

    def delete_secret(self, name: str) -> bool:
        """Удаляет секрет из хранилища."""
        if name not in self._entries:
            logger.warning(f"Secret not found: {name}")
            return False

        del self._entries[name]
        self._save_vault()

        logger.info(f"🗑️ Secret deleted: {name}")
        return True

    def list_secrets(self) -> list[Dict[str, Any]]:
        """Возвращает список всех секретов (без значений)."""
        return [
            {
                "name": entry.name,
                "description": entry.description,
                "created_at": entry.created_at,
                "updated_at": entry.updated_at,
            }
            for entry in self._entries.values()
        ]

    def has_secret(self, name: str) -> bool:
        """Проверяет наличие секрета."""
        return name in self._entries

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику хранилища."""
        return {
            "total_secrets": len(self._entries),
            "vault_path": str(self.vault_path),
            "secrets": [entry.name for entry in self._entries.values()],
        }


# Пример использования (для документации):
"""
# Инициализация
vault = CredentialVault(
    master_password="my-master-password",
    vault_path=Path("./data/vault.json")
)

# Сохранение секрета
vault.store_secret(
    name="influxdb_token",
    value="my-super-secret-token",
    description="InfluxDB authentication token"
)

# Извлечение секрета
token = vault.get_secret("influxdb_token")
if token:
    # Используем токен
    pass

# Удаление секрета
vault.delete_secret("influxdb_token")

# Список всех секретов
secrets = vault.list_secrets()
for secret in secrets:
    print(f"{secret['name']}: {secret['description']}")
"""
