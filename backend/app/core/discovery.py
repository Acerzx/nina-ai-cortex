import xmltodict
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from app.core.config import get_settings

logger = logging.getLogger(__name__)

# Маппинг известных GUID плагинов к их именам (из вашего профиля)
KNOWN_PLUGINS = {
    "0f1d10b6-d306-4168-b751-d454cbac9670": "Hocus Focus",
    "10bc1716-54af-425e-b307-c0ca1ce10600": "LiveStack",
    "dcb1d37b-f121-4966-99ec-d11410c562b6": "Session Metadata",
    "97021132-0c25-4443-b947-fe5efbe0a3d6": "AutoFocus Analysis",
    "2b8caa03-b7c2-47e2-aa54-49190f7a0ea8": "Night Summary",
    "00eec1ff-31fd-47b4-bbff-1a71b63b0330": "Advanced API",
    "9d4f7ba2-10f2-4373-bfcb-b4b3dcbe21db": "SolveEveryLight",
    "00aa6286-a2f7-490e-bc08-7844af7175f5": "Flexure Compensator",
    "0e9e3e58-42fc-4553-8e6e-aba061af4f54": "Two Point Polar Alignment",
    "76db8780-e24a-4166-bd5f-5786ab793856": "Target Planning",
}


class PluginDiscovery:
    def __init__(self):
        self.settings = get_settings()
        self.profiles_dir = Path(self.settings.nina_environment.profiles_dir)
        self.discovered_plugins: Dict[str, Dict[str, Any]] = {}
        self.active_profile: Optional[Path] = None

    def find_active_profile(self) -> Optional[Path]:
        """Находит последний использованный профиль."""
        logger.info(f"🔍 Searching for profiles in: {self.profiles_dir}")

        if not self.profiles_dir.exists():
            logger.error(f"❌ Profiles directory does not exist: {self.profiles_dir}")
            return None

        if not self.profiles_dir.is_dir():
            logger.error(f"❌ Profiles path is not a directory: {self.profiles_dir}")
            return None

        # Ищем все возможные файлы профилей
        profiles = []
        for pattern in ["*.profile.txt", "*.xml", "*.profile"]:
            found = list(self.profiles_dir.glob(pattern))
            logger.info(f"  Found {len(found)} files matching pattern '{pattern}'")
            profiles.extend(found)

        # Также ищем все файлы в директории для диагностики
        all_files = list(self.profiles_dir.iterdir())
        logger.info(f"  Total files in directory: {len(all_files)}")
        if all_files:
            logger.info(f"  First 5 files: {[f.name for f in all_files[:5]]}")

        if not profiles:
            logger.error(
                "❌ No profile files found with extensions .profile.txt, .xml, or .profile"
            )
            logger.error(f"   Please check that profiles exist in: {self.profiles_dir}")
            return None

        # Сортируем по времени изменения, берем самый свежий
        active = sorted(profiles, key=lambda p: p.stat().st_mtime, reverse=True)[0]
        self.active_profile = active
        logger.info(f"✅ Found active profile: {active.name}")
        logger.info(f"   Last modified: {active.stat().st_mtime}")
        return active

    def parse_profile(self, profile_path: Path) -> Dict[str, Any]:
        """Парсит XML профиль и извлекает pluginStorage."""
        try:
            logger.info(f"📖 Parsing profile: {profile_path.name}")

            with open(profile_path, "r", encoding="utf-8") as f:
                xml_content = f.read()

            logger.info(f"   File size: {len(xml_content)} bytes")

            doc = xmltodict.parse(xml_content)
            profile = doc.get("Profile", {})
            plugin_settings = profile.get("PluginSettings", {})
            plugin_storage = plugin_settings.get("pluginStorage", {})

            if not plugin_storage:
                logger.warning("⚠️ No pluginStorage found in profile")
                return {}

            # .NET XML сериализация создает специфичные ключи
            kv_array = plugin_storage.get(
                "a:KeyValueOfguidArrayOfKeyValueOfstringanyTypeox8ieOcg", []
            )
            if not isinstance(kv_array, list):
                kv_array = [kv_array]

            logger.info(f"   Found {len(kv_array)} plugin entries")

            for item in kv_array:
                guid = item.get("a:Key", "")
                plugin_name = KNOWN_PLUGINS.get(guid, f"Unknown ({guid[:8]}...)")

                values = item.get("a:Value", {})
                kv_values = values.get("a:KeyValueOfstringanyType", [])
                if not isinstance(kv_values, list):
                    kv_values = [kv_values]

                plugin_config = {}
                for kv in kv_values:
                    key = kv.get("a:Key", "")
                    value_node = kv.get("a:Value", "")
                    # Извлекаем текст из узла, если он обернут в тип (b:string, b:int)
                    if isinstance(value_node, dict):
                        value = value_node.get("#text", str(value_node))
                    else:
                        value = value_node

                    plugin_config[key] = value

                self.discovered_plugins[plugin_name] = plugin_config

            return self.discovered_plugins

        except Exception as e:
            logger.error(f"❌ Failed to parse profile {profile_path}: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return {}

    def run(self):
        """Запускает процесс обнаружения."""
        profile = self.find_active_profile()
        if profile:
            self.parse_profile(profile)

        logger.info(f"\n{'=' * 60}")
        logger.info(f"✅ Discovered {len(self.discovered_plugins)} known plugins:")
        logger.info(f"{'=' * 60}")
        for name, config in self.discovered_plugins.items():
            logger.info(f"  ├─ {name}")
            if "SavePath" in config:
                logger.info(f"  │  └─ SavePath: {config['SavePath']}")
            if "WorkingDirectory" in config:
                logger.info(f"  │  └─ WorkingDir: {config['WorkingDirectory']}")
            if len(config) > 2:
                logger.info(f"  │  └─ {len(config) - 2} more settings")
