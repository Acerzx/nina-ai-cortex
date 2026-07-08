"""
Unit tests for Credential Vault.
Тестирует шифрование, хранение и извлечение секретов.
"""

import pytest
from pathlib import Path
import json
import tempfile

from app.security.vault import CredentialVault, VaultEntry


class TestCredentialVault:
    """Тесты Credential Vault."""

    @pytest.fixture
    def vault_dir(self):
        """Создаёт временную директорию для vault."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @pytest.fixture
    def vault(self, vault_dir):
        """Создаёт тестовый vault."""
        vault_path = vault_dir / "vault.json"
        return CredentialVault(
            master_password="test-master-password-123456", vault_path=vault_path
        )

    def test_vault_initialization(self, vault):
        """Тест инициализации vault."""
        assert vault is not None
        assert vault._entries == {}

    def test_store_secret(self, vault):
        """Тест сохранения секрета."""
        success = vault.store_secret(
            name="test_api_key", value="super-secret-value", description="Test API key"
        )

        assert success is True
        assert "test_api_key" in vault._entries

        # Проверяем, что значение зашифровано
        entry = vault._entries["test_api_key"]
        assert entry.encrypted_value != "super-secret-value"
        assert entry.description == "Test API key"

    def test_get_secret(self, vault):
        """Тест извлечения секрета."""
        # Сохраняем секрет
        vault.store_secret(name="test_token", value="my-secret-token")

        # Извлекаем
        value = vault.get_secret("test_token")

        assert value == "my-secret-token"

    def test_get_nonexistent_secret(self, vault):
        """Тест извлечения несуществующего секрета."""
        value = vault.get_secret("nonexistent")

        assert value is None

    def test_delete_secret(self, vault):
        """Тест удаления секрета."""
        # Сохраняем секрет
        vault.store_secret(name="to_delete", value="delete-me")
        assert vault.has_secret("to_delete") is True

        # Удаляем
        success = vault.delete_secret("to_delete")

        assert success is True
        assert vault.has_secret("to_delete") is False
        assert vault.get_secret("to_delete") is None

    def test_delete_nonexistent_secret(self, vault):
        """Тест удаления несуществующего секрета."""
        success = vault.delete_secret("nonexistent")

        assert success is False

    def test_list_secrets(self, vault):
        """Тест списка секретов."""
        vault.store_secret(name="secret1", value="value1", description="First")
        vault.store_secret(name="secret2", value="value2", description="Second")

        secrets = vault.list_secrets()

        assert len(secrets) == 2
        names = {s["name"] for s in secrets}
        assert names == {"secret1", "secret2"}

        # Проверяем, что значения не включены
        for secret in secrets:
            assert "value" not in secret
            assert "encrypted_value" not in secret
            assert "description" in secret

    def test_has_secret(self, vault):
        """Тест проверки наличия секрета."""
        assert vault.has_secret("missing") is False

        vault.store_secret(name="present", value="here")

        assert vault.has_secret("present") is True

    def test_vault_persistence(self, vault_dir):
        """Тест персистентности vault между сессиями."""
        vault_path = vault_dir / "persistent.json"

        # Создаём vault и сохраняем секрет
        vault1 = CredentialVault(
            master_password="persistent-password-123", vault_path=vault_path
        )
        vault1.store_secret(name="persistent_secret", value="i-survive-reboot")

        # Создаём новый vault с тем же паролем
        vault2 = CredentialVault(
            master_password="persistent-password-123", vault_path=vault_path
        )

        # Секрет должен быть доступен
        value = vault2.get_secret("persistent_secret")
        assert value == "i-survive-reboot"

    def test_vault_wrong_password(self, vault_dir):
        """Тест что неправильный пароль не может расшифровать vault."""
        vault_path = vault_dir / "protected.json"

        # Создаём vault с правильным паролем
        vault1 = CredentialVault(
            master_password="correct-password-123456", vault_path=vault_path
        )
        vault1.store_secret(name="protected", value="secret-value")

        # Пытаемся открыть с неправильным паролем
        vault2 = CredentialVault(
            master_password="wrong-password-876543", vault_path=vault_path
        )

        # Должна быть ошибка при расшифровке
        value = vault2.get_secret("protected")
        assert value is None  # Ошибка расшифровки возвращает None

    def test_encrypt_decrypt(self, vault):
        """Тест шифрования и расшифровки."""
        plaintext = "sensitive-data-to-encrypt"

        encrypted = vault.encrypt(plaintext)
        assert encrypted != plaintext

        decrypted = vault.decrypt(encrypted)
        assert decrypted == plaintext

    def test_vault_unicode_secrets(self, vault):
        """Тест хранения секретов с Unicode."""
        vault.store_secret(name="unicode_test", value="Пароль с кириллицей 🔐")

        value = vault.get_secret("unicode_test")
        assert value == "Пароль с кириллицей 🔐"

    def test_vault_long_values(self, vault):
        """Тест хранения длинных значений."""
        long_value = "x" * 10000

        vault.store_secret(name="long_secret", value=long_value)

        value = vault.get_secret("long_secret")
        assert value == long_value

    def test_vault_update_secret(self, vault):
        """Тест обновления существующего секрета."""
        vault.store_secret(name="updatable", value="original")
        assert vault.get_secret("updatable") == "original"

        vault.store_secret(name="updatable", value="updated")

        assert vault.get_secret("updatable") == "updated"

    def test_get_stats(self, vault):
        """Тест статистики vault."""
        stats = vault.get_stats()

        assert stats["total_secrets"] == 0

        vault.store_secret(name="s1", value="v1")
        vault.store_secret(name="s2", value="v2")

        stats = vault.get_stats()
        assert stats["total_secrets"] == 2
        assert "vault_path" in stats

    def test_vault_file_encrypted_on_disk(self, vault, vault_dir):
        """Тест что секрет зашифрован на диске."""
        vault.store_secret(name="disk_test", value="plaintext-secret")

        # Читаем raw файл
        vault_path = vault_dir / "vault.json"
        with open(vault_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Проверяем, что plaintext не хранится в файле
        file_content = json.dumps(data)
        assert "plaintext-secret" not in file_content
        assert "disk_test" in data  # Имя хранится
        assert "encrypted_value" in data["disk_test"]

    def test_salt_generation(self, vault_dir):
        """Тест генерации salt."""
        vault_path = vault_dir / "salt_test.json"

        # Создаём vault без salt
        vault1 = CredentialVault(master_password="test-password", vault_path=vault_path)

        # Salt должен быть создан
        salt_path = vault_path.parent / "salt_test.salt"
        assert salt_path.exists()

        # Читаем salt
        salt1 = salt_path.read_bytes()
        assert len(salt1) == 16

        # Создаём второй vault с тем же путём
        vault2 = CredentialVault(master_password="test-password", vault_path=vault_path)

        # Salt должен быть тем же
        salt2 = salt_path.read_bytes()
        assert salt1 == salt2
