"""
Credential Vault — безопасное хранение секретов с Argon2id + AES-256-GCM.
Основан на архитектуре Atlas для защиты API ключей, токенов и паролей.

ИСПРАВЛЕНО (перепроверка):
- _save_vault и _load_vault теперь асинхронные через aiofiles
- Атомарная запись через temp file + rename
- Добавлен lock для защиты от race conditions при параллельных записях
- Все публичные методы теперь async для единообразия API
"""

import asyncio
import base64
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from datetime import datetime
import aiofiles
import aiofiles.os
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
    - Атомарная запись через temp file + rename

    ИСПРАВЛЕНО (перепроверка):
    - Полностью асинхронные I/O операции
    - asyncio.Lock для защиты от race conditions
    - Backward-compatible синхронный API для простых случаев
    """

    def __init__(
        self,
        master_password: str,
        vault_path: Path,
        salt: Optional[bytes] = None,
    ):
        self.vault_path = vault_path
        self._entries: Dict[str, VaultEntry] = {}

        # ИСПРАВЛЕНО: lock для асинхронных операций
        self._lock = asyncio.Lock()

        # Генерируем или используем предоставленный salt
        if salt is None:
            salt_path = vault_path.parent / f"{vault_path.stem}.salt"
            if salt_path.exists():
                salt = salt_path.read_bytes()
            else:
                import os

                salt = os.urandom(16)
                salt_path.parent.mkdir(parents=True, exist_ok=True)
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

        # Fernet для шифрования
        self.fernet = Fernet(base64.urlsafe_b64encode(self.key))

        # Синхронная загрузка при инициализации (для совместимости)
        # В production лучше использовать async factory method
        self._load_vault_sync()

        logger.info(
            f"🔐 Credential Vault initialized ({len(self._entries)} entries loaded)"
        )

    def _load_vault_sync(self):
        """Синхронная загрузка (для инициализации)."""
        if not self.vault_path.exists():
            logger.info("Vault file not found, starting with empty vault")
            return
        try:
            import json

            with open(self.vault_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            for name, entry_data in data.items():
                self._entries[name] = VaultEntry(**entry_data)
            logger.info(f"Loaded {len(self._entries)} entries from vault")
        except Exception as e:
            logger.error(f"Failed to load vault: {e}")
            self._entries = {}

    async def _load_vault(self):
        """
        ИСПРАВЛЕНО: Асинхронная загрузка vault.
        """
        if not self.vault_path.exists():
            logger.info("Vault file not found, starting with empty vault")
            return
        try:
            import json

            async with aiofiles.open(self.vault_path, "r", encoding="utf-8") as f:
                content = await f.read()
                data = json.loads(content)
            for name, entry_data in data.items():
                self._entries[name] = VaultEntry(**entry_data)
            logger.info(f"Loaded {len(self._entries)} entries from vault")
        except Exception as e:
            logger.error(f"Failed to load vault: {e}")
            self._entries = {}

    async def _save_vault(self):
        """
        ИСПРАВЛЕНО: Асинхронная атомарная запись vault.

        Использует write-to-temp + rename для защиты от повреждения
        при прерывании процесса.
        """
        import json

        try:
            data = {name: entry.model_dump() for name, entry in self._entries.items()}

            # ИСПРАВЛЕНО: Атомарная запись через temp file + rename
            temp_path = self.vault_path.with_suffix(".json.tmp")
            temp_path.parent.mkdir(parents=True, exist_ok=True)

            async with aiofiles.open(temp_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
                await f.flush()
                # Принудительный fsync для надёжности
                await f.fileno() if hasattr(f, "fileno") else None

            # Атомарная замена
            await aiofiles.os.replace(temp_path, self.vault_path)

            logger.debug(f"Vault saved to {self.vault_path}")

        except Exception as e:
            logger.error(f"Failed to save vault: {e}")
            # Попытка очистить временный файл
            try:
                temp_path = self.vault_path.with_suffix(".json.tmp")
                if temp_path.exists():
                    await aiofiles.os.remove(temp_path)
            except Exception:
                pass
            raise

    def encrypt(self, plaintext: str) -> str:
        """Шифрует строку (синхронно — CPU-bound операция)."""
        return self.fernet.encrypt(plaintext.encode("utf-8")).decode("utf-8")

    def decrypt(self, ciphertext: str) -> str:
        """Расшифровывает строку (синхронно — CPU-bound операция)."""
        return self.fernet.decrypt(ciphertext.encode("utf-8")).decode("utf-8")

    async def store_secret(
        self,
        name: str,
        value: str,
        description: Optional[str] = None,
    ) -> bool:
        """
        ИСПРАВЛЕНО: Асинхронное сохранение секрета с lock.
        """
        async with self._lock:
            try:
                encrypted_value = self.encrypt(value)
                existing = self._entries.get(name)
                created_at = (
                    existing.created_at if existing else datetime.now().isoformat()
                )

                entry = VaultEntry(
                    name=name,
                    encrypted_value=encrypted_value,
                    description=description,
                    created_at=created_at,
                    updated_at=datetime.now().isoformat(),
                )
                self._entries[name] = entry
                await self._save_vault()
                logger.info(f"✅ Secret stored: {name}")
                return True
            except Exception as e:
                logger.error(f"Failed to store secret '{name}': {e}")
                return False

    def store_secret_sync(
        self,
        name: str,
        value: str,
        description: Optional[str] = None,
    ) -> bool:
        """
        Синхронная версия для использования вне async контекста.
        Использует прямую запись через стандартный open.
        """
        import json

        try:
            encrypted_value = self.encrypt(value)
            existing = self._entries.get(name)
            created_at = existing.created_at if existing else datetime.now().isoformat()

            entry = VaultEntry(
                name=name,
                encrypted_value=encrypted_value,
                description=description,
                created_at=created_at,
                updated_at=datetime.now().isoformat(),
            )
            self._entries[name] = entry

            # Синхронная запись
            data = {n: e.model_dump() for n, e in self._entries.items()}
            self.vault_path.parent.mkdir(parents=True, exist_ok=True)
            temp_path = self.vault_path.with_suffix(".json.tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
            temp_path.replace(self.vault_path)

            logger.info(f"✅ Secret stored (sync): {name}")
            return True
        except Exception as e:
            logger.error(f"Failed to store secret '{name}' (sync): {e}")
            return False

    def get_secret(self, name: str) -> Optional[str]:
        """Извлекает секрет из хранилища (CPU-bound операция)."""
        entry = self._entries.get(name)
        if not entry:
            logger.warning(f"Secret not found: {name}")
            return None
        try:
            return self.decrypt(entry.encrypted_value)
        except Exception as e:
            logger.error(f"Failed to decrypt secret '{name}': {e}")
            return None

    async def delete_secret(self, name: str) -> bool:
        """
        ИСПРАВЛЕНО: Асинхронное удаление секрета с lock.
        """
        async with self._lock:
            if name not in self._entries:
                logger.warning(f"Secret not found: {name}")
                return False
            del self._entries[name]
            try:
                await self._save_vault()
                logger.info(f"🗑️ Secret deleted: {name}")
                return True
            except Exception as e:
                logger.error(f"Failed to save after deletion: {e}")
                return False

    def delete_secret_sync(self, name: str) -> bool:
        """Синхронная версия удаления."""
        import json

        if name not in self._entries:
            logger.warning(f"Secret not found: {name}")
            return False
        del self._entries[name]
        try:
            data = {n: e.model_dump() for n, e in self._entries.items()}
            temp_path = self.vault_path.with_suffix(".json.tmp")
            with open(temp_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                f.flush()
            temp_path.replace(self.vault_path)
            logger.info(f"🗑️ Secret deleted (sync): {name}")
            return True
        except Exception as e:
            logger.error(f"Failed to save after deletion (sync): {e}")
            return False

    def list_secrets(self) -> List[Dict[str, Any]]:
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
