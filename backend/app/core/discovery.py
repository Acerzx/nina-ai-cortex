import xmltodict
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from app.core.config import settings

logger = logging.getLogger("ProfileParser")


class ProfileParser:
    """
    Парсит XML-профили N.I.N.A. и извлекает pluginStorage.
    Устраняет Упрощение #32: динамическое обнаружение настроек плагинов.
    """

    def __init__(self, profiles_dir: Path):
        self.profiles_dir = profiles_dir

    def parse_active_profile(self) -> Dict[str, Dict[str, Any]]:
        """Находит и парсит активный (последний использованный) профиль."""
        profiles = list(self.profiles_dir.glob("*.profile"))
        if not profiles:
            logger.warning(f"No profiles found in {self.profiles_dir}")
            return {}

        # N.I.N.A. обновляет <LastUsed> в XML. Берем последний измененный файл.
        active_profile_path = max(profiles, key=lambda p: p.stat().st_mtime)
        logger.info(f"Parsing active profile: {active_profile_path.name}")

        try:
            with open(active_profile_path, "r", encoding="utf-8") as f:
                doc = xmltodict.parse(f.read())
        except Exception as e:
            logger.error(f"Failed to parse XML profile: {e}")
            return {}

        return self._extract_plugin_storage(doc)

    def _extract_plugin_storage(self, doc: Dict) -> Dict[str, Dict[str, Any]]:
        """Извлекает и нормализует pluginStorage из XML."""
        registry = {}
        try:
            plugin_storage = self._find_key(doc, "pluginStorage")
            if not plugin_storage:
                logger.warning("pluginStorage not found in profile")
                return registry

            items = plugin_storage.get(
                "a:KeyValueOfguidArrayOfKeyValueOfstringanyTypeox8ieOcg", []
            )
            if not isinstance(items, list):
                items = [items]

            for item in items:
                guid = item.get("a:Key")
                if not guid:
                    continue

                settings_list = item.get("a:Value", {}).get(
                    "a:KeyValueOfstringanyType", []
                )
                if not isinstance(settings_list, list):
                    settings_list = [settings_list]

                plugin_settings = {}
                for setting in settings_list:
                    key = setting.get("a:Key")
                    value_node = setting.get("a:Value")
                    value = self._parse_value(value_node)
                    if key:
                        plugin_settings[key] = value

                registry[guid] = plugin_settings

            logger.info(f"Extracted settings for {len(registry)} plugins from profile")
            return registry
        except Exception as e:
            logger.error(f"Error extracting plugin storage: {e}")
            return registry

    def _find_key(self, obj: Any, key: str) -> Optional[Any]:
        """Рекурсивный поиск ключа в словаре."""
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            for v in obj.values():
                result = self._find_key(v, key)
                if result is not None:
                    return result
        elif isinstance(obj, list):
            for item in obj:
                result = self._find_key(item, key)
                if result is not None:
                    return result
        return None

    def _parse_value(self, node: Any) -> Any:
        """Парсит значение из XML-узла, обрабатывая JSON-строки (BiasLibrary, DarkLibrary)."""
        if node is None:
            return None

        if isinstance(node, dict):
            text = node.get("#text")
            if text is None:
                return node

            # Парсинг JSON-массивов и объектов
            if isinstance(text, str) and (
                (text.startswith("[") and text.endswith("]"))
                or (text.startswith("{") and text.endswith("}"))
            ):
                try:
                    return json.loads(text)
                except json.JSONDecodeError:
                    pass

            # Обработка булевых и числовых значений
            if text.lower() == "true":
                return True
            if text.lower() == "false":
                return False
            try:
                if "." in text:
                    return float(text)
                return int(text)
            except ValueError:
                return text

        return str(node)
