"""
Unit tests for Credential Vault.
Тестирует шифрование, хранение и извлечение секретов.
"""

import pytest
from pathlib import Path
import json
from app.security.vault import CredentialVault, VaultEntry


class TestCredentialVault:
    """Тесты для CredentialVault."""

    @pytest.fixture
    def vault_dir(self, tmp_path: Path):
        """Создаёт временную директорию для vault."""
        return tmp_path / "vault"

    @pytest.fixture
    def vault(self, vault_dir: Path):
        """Создаёт тестовый vault."""
        vault_dir.mkdir(parents=True, exist_ok=True)
        return CredentialVault(
            master_password="test-master-password-strong-123",
            vault_path=vault_dir / "vault.json",
        )

    @pytest.mark.asyncio
    async def test_store_and_retrieve_secret(self, vault: CredentialVault):
        """Тест сохранения и извлечения секрета."""
        # Сохраняем
        success = vault.store_secret(
            name="test_api_key",
            value="super-secret-key-12345",
            description="Test API key",
        )
        assert success is True

        # Извлекаем
        retrieved = vault.get_secret("test_api_key")
        assert retrieved == "super-secret-key-12345"

    @pytest.mark.asyncio
    async def test_secret_is_encrypted_on_disk(
        self, vault: CredentialVault, vault_dir: Path
    ):
        """Тест что секрет зашифрован на диске."""
        vault.store_secret(
            name="secret_value",
            value="plaintext-should-be-encrypted",
        )

        # Читаем raw файл
        vault_file = vault_dir / "vault.json"
        assert vault_file.exists()

        with open(vault_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Проверяем, что plaintext не хранится в файле
        file_content = json.dumps(data)
        assert "plaintext-should-be-encrypted" not in file_content
        assert "secret_value" in data  # Имя хранится
        assert "encrypted_value" in data["secret_value"]

    @pytest.mark.asyncio
    async def test_delete_secret(self, vault: CredentialVault):
        """Тест удаления секрета."""
        vault.store_secret(name="to_delete", value="delete-me")
        assert vault.has_secret("to_delete") is True

        success = vault.delete_secret("to_delete")
        assert success is True
        assert vault.has_secret("to_delete") is False
        assert vault.get_secret("to_delete") is None

    @pytest.mark.asyncio
    async def test_delete_nonexistent_secret(self, vault: CredentialVault):
        """Тест удаления несуществующего секрета."""
        success = vault.delete_secret("nonexistent")
        assert success is False

    @pytest.mark.asyncio
    async def test_list_secrets(self, vault: CredentialVault):
        """Тест списка секретов."""
        vault.store_secret(name="secret1", value="value1", description="First")
        vault.store_secret(name="secret2", value="value2", description="Second")

        secrets = vault.list_secrets()
        assert len(secrets) == 2
        names = {s["name"] for s in secrets}
        assert names == {"secret1", "secret2"}

        # Значения не должны быть в списке
        for s in secrets:
            assert "value" not in s
            assert "encrypted_value" not in s

    @pytest.mark.asyncio
    async def test_get_nonexistent_secret(self, vault: CredentialVault):
        """Тест получения несуществующего секрета."""
        result = vault.get_secret("does_not_exist")
        assert result is None

    @pytest.mark.asyncio
    async def test_has_secret(self, vault: CredentialVault):
        """Тест проверки наличия секрета."""
        assert vault.has_secret("missing") is False
        vault.store_secret(name="present", value="here")
        assert vault.has_secret("present") is True

    @pytest.mark.asyncio
    async def test_update_secret(self, vault: CredentialVault):
        """Тест обновления существующего секрета."""
        vault.store_secret(name="updatable", value="original")
        assert vault.get_secret("updatable") == "original"

        vault.store_secret(name="updatable", value="updated")
        assert vault.get_secret("updatable") == "updated"

    @pytest.mark.asyncio
    async def test_get_stats(self, vault: CredentialVault):
        """Тест статистики vault."""
        stats = vault.get_stats()
        assert stats["total_secrets"] == 0

        vault.store_secret(name="s1", value="v1")
        vault.store_secret(name="s2", value="v2")

        stats = vault.get_stats()
        assert stats["total_secrets"] == 2
        assert "vault_path" in stats

    @pytest.mark.asyncio
    async def test_vault_persistence(self, vault_dir: Path):
        """Тест что vault переживает перезагрузку."""
        vault_path = vault_dir / "persistent.json"

        # Создаём vault и сохраняем секрет
        vault1 = CredentialVault(
            master_password="persistent-password-123456",
            vault_path=vault_path,
        )
        vault1.store_secret(name="persistent_secret", value="i-survive-reboot")

        # Создаём новый vault с тем же паролем
        vault2 = CredentialVault(
            master_password="persistent-password-123456",
            vault_path=vault_path,
        )

        # Секрет должен быть доступен
        retrieved = vault2.get_secret("persistent_secret")
        assert retrieved == "i-survive-reboot"

    @pytest.mark.asyncio
    async def test_vault_wrong_password(self, vault_dir: Path):
        """Тест что неправильный пароль не может расшифровать vault."""
        vault_path = vault_dir / "protected.json"

        # Создаём vault
        vault1 = CredentialVault(
            master_password="correct-password-12345678",
            vault_path=vault_path,
        )
        vault1.store_secret(name="protected", value="secret-value")

        # Пытаемся открыть с неправильным паролем
        vault2 = CredentialVault(
            master_password="wrong-password-87654321",
            vault_path=vault_path,
        )

        # Должна быть ошибка при расшифровке
        result = vault2.get_secret("protected")
        # Результат должен быть None из-за ошибки расшифровки
        assert result is None

    @pytest.mark.asyncio
    async def test_vault_unicode_secrets(self, vault: CredentialVault):
        """Тест хранения секретов с Unicode."""
        vault.store_secret(name="unicode_test", value="Пароль с кириллицей 🔐")
        retrieved = vault.get_secret("unicode_test")
        assert retrieved == "Пароль с кириллицей 🔐"

    @pytest.mark.asyncio
    async def test_vault_long_values(self, vault: CredentialVault):
        """Тест хранения длинных значений."""
        long_value = "x" * 10000
        vault.store_secret(name="long_secret", value=long_value)
        retrieved = vault.get_secret("long_secret")
        assert retrieved == long_value
