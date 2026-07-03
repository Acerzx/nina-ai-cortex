import xmltodict
import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional
from app.core.config import get_settings

logger = logging.getLogger(__name__)

# ПОЛНЫЙ РЕЕСТР ПЛАГИНОВ (GUID, Имена папок, Названия)
PLUGIN_REGISTRY = {
    "0f1d10b6-d306-4168-b751-d454cbac9670": {
        "name": "Hocus Focus",
        "folder": "Hocus Focus",
    },
    "10bc1716-54af-425e-b307-c0ca1ce10600": {
        "name": "LiveStack",
        "folder": "LiveStack",
    },
    "dcb1d37b-f121-4966-99ec-d11410c562b6": {
        "name": "Session Metadata",
        "folder": "Session Metadata",
    },
    "97021132-0c25-4443-b947-fe5efbe0a3d6": {
        "name": "AutoFocus Analysis",
        "folder": "AutoFocus Analysis",
    },
    "2b8caa03-b7c2-47e2-aa54-49190f7a0ea8": {
        "name": "Night Summary",
        "folder": "NightSummary",
    },
    "00eec1ff-31fd-47b4-bbff-1a71b63b0330": {
        "name": "Advanced API",
        "folder": "Advanced API",
    },
    "9d4f7ba2-10f2-4373-bfcb-b4b3dcbe21db": {
        "name": "SolveEveryLight",
        "folder": "SolveEveryLight",
    },
    "00aa6286-a2f7-490e-bc08-7844af7175f5": {
        "name": "Flexure Compensator",
        "folder": "FlexureCompensator",
    },
    "0e9e3e58-42fc-4553-8e6e-aba061af4f54": {
        "name": "Two Point Polar Alignment",
        "folder": "TwoPointPolarAlignment",
    },
    "76db8780-e24a-4166-bd5f-5786ab793856": {
        "name": "Target Planning",
        "folder": "TargetPlanning",
    },
    "81b04674-ea65-4fe8-b79b-a77c1d209183": {"name": "NINA.Web", "folder": "NINA.Web"},
    "52c17ee7-6d6c-4ee1-8fa0-85bcf6677bef": {
        "name": "AutoConnect",
        "folder": "AutoConnect",
    },
    "afa13a89-8ae3-4975-a953-683c6b6e2bbe": {"name": "EQMOD Quirks", "folder": "EQMOD"},
    "2b4b2fd6-46ce-4f34-b184-4a8b3058dc86": {
        "name": "SharpCap Integration",
        "folder": "SharpCap",
    },
    "b4541ba9-7b07-4d71-b8e1-6c73d4933ea0": {"name": "Tree View", "folder": "TreeView"},
    "b7e3f1a2-9c4d-4e8b-a6f5-1d2c3b4a5e6f": {"name": "SkyWave", "folder": "SkyWave"},
    "25ac9c96-885e-4733-a437-a5d4863a1c7e": {
        "name": "Temperature Control",
        "folder": "TempControl",
    },
    "6bd8bce9-c199-401a-aaf8-47ea8ee5ae32": {
        "name": "Alpaca/ASCOM",
        "folder": "Alpaca",
    },
    "ef99cb7e-3c22-491c-b26a-54315222bf9b": {
        "name": "Visual Polar Alignment",
        "folder": "VisualPolar",
    },
    # Плагины, которые могут не иметь записей в профиле, но лежат в папке
    "PROMETHEUS": {"name": "Prometheus Exporter", "folder": "Prometheus Exporter"},
    "INFLUXDB": {"name": "InfluxDB Exporter", "folder": "InfluxDB Exporter"},
    "INJECT_AF": {"name": "Inject Autofocus", "folder": "InjectAutofocus"},
    "SEQUENCER_PLUS": {"name": "Sequencer+", "folder": "Sequencer+"},
    "DYNAMIC_SEQ": {"name": "Dynamic Sequencer", "folder": "DynamicSequencer"},
    "TARGET_SCHED": {"name": "Target Scheduler", "folder": "TargetScheduler"},
    "PHD2_TOOLS": {"name": "PHD2 Tools", "folder": "PHD2Tools"},
    "FASTER_FLATS": {"name": "Faster Flats", "folder": "FasterFlats"},
    "DEVICE_CMD": {"name": "Device Commands", "folder": "DeviceCommands"},
    "SCOPE_CTRL": {"name": "Scope Control", "folder": "ScopeControl"},
    "SHUTDOWN_PC": {"name": "Shutdown PC", "folder": "Shutdown PC"},
    "AI_WEATHER": {"name": "AI Weather", "folder": "AIWeather"},
    "HOME_ASSIST": {"name": "Home Assistant", "folder": "HomeAssistant"},
    "MOON_ANGLE": {"name": "Moon Angle", "folder": "MoonAngle"},
    "PYTHON_PLUG": {"name": "Python Plugin", "folder": "PythonPlugin"},
    "EXT_SCRIPTS": {"name": "External Scripts", "folder": "ExternalScripts"},
    "ORBITALS": {"name": "Orbitals", "folder": "Orbitals"},
    "BENCHMARK": {"name": "Benchmark", "folder": "Benchmark"},
    "DITHER_STATS": {"name": "Dither Statistics", "folder": "DitherStatistics"},
    "GUIDING_ANAL": {"name": "Guiding Analyzer", "folder": "GuidingAnalyzer"},
}


class PluginDiscovery:
    def __init__(self):
        self.settings = get_settings()
        self.profiles_dir = Path(self.settings.nina_environment.profiles_dir)
        self.plugins_dir = Path(self.settings.nina_environment.plugins_dir)

        self.discovered_plugins: Dict[str, Dict[str, Any]] = {}
        self.active_profile: Optional[Path] = None

    def find_active_profile(self) -> Optional[Path]:
        if not self.profiles_dir.exists():
            return None
        profiles = list(self.profiles_dir.glob("*.profile")) + list(
            self.profiles_dir.glob("*.profile.txt")
        )
        if not profiles:
            return None
        active = sorted(profiles, key=lambda p: p.stat().st_mtime, reverse=True)[0]
        self.active_profile = active
        return active

    def parse_profile(self, profile_path: Path):
        """Извлекает настройки плагинов из XML профиля"""
        try:
            with open(profile_path, "r", encoding="utf-8") as f:
                doc = xmltodict.parse(f.read())

            plugin_storage = (
                doc.get("Profile", {})
                .get("PluginSettings", {})
                .get("pluginStorage", {})
            )
            kv_array = plugin_storage.get(
                "a:KeyValueOfguidArrayOfKeyValueOfstringanyTypeox8ieOcg", []
            )
            if not isinstance(kv_array, list):
                kv_array = [kv_array]

            for item in kv_array:
                guid = item.get("a:Key", "")
                registry_entry = PLUGIN_REGISTRY.get(guid)
                plugin_name = (
                    registry_entry["name"]
                    if registry_entry
                    else f"Unknown ({guid[:8]}...)"
                )

                values = item.get("a:Value", {}).get("a:KeyValueOfstringanyType", [])
                if not isinstance(values, list):
                    values = [values]

                config = {}
                for kv in values:
                    key = kv.get("a:Key", "")
                    val = kv.get("a:Value", "")
                    config[key] = (
                        val.get("#text", str(val)) if isinstance(val, dict) else val
                    )

                self.discovered_plugins[plugin_name] = {
                    "status": "Configured",
                    "settings": config,
                }
        except Exception as e:
            logger.error(f"❌ Profile parse error: {e}")

    def scan_plugins_folder(self):
        """Сканирует папку Plugins/3.0.0/ для поиска ВСЕХ установленных плагинов"""
        if not self.plugins_dir.exists():
            logger.warning(f"⚠️ Plugins directory not found: {self.plugins_dir}")
            return

        logger.info(f"📂 Scanning plugins folder: {self.plugins_dir}")

        # Проходим по всем папкам в Plugins/3.0.0/
        for plugin_folder in self.plugins_dir.iterdir():
            if plugin_folder.is_dir():
                folder_name = plugin_folder.name

                # Ищем совпадение в реестре по имени папки
                matched_name = None
                for key, meta in PLUGIN_REGISTRY.items():
                    if meta["folder"].lower() == folder_name.lower():
                        matched_name = meta["name"]
                        break

                if matched_name:
                    # Если плагин уже найден в профиле, оставляем статус "Configured"
                    if matched_name not in self.discovered_plugins:
                        self.discovered_plugins[matched_name] = {
                            "status": "Installed (Default Settings)",
                            "settings": {},
                            "folder": str(plugin_folder),
                        }
                else:
                    # Неизвестный плагин, но он установлен
                    self.discovered_plugins[f"Custom/Unknown ({folder_name})"] = {
                        "status": "Installed (Unknown)",
                        "settings": {},
                        "folder": str(plugin_folder),
                    }

    def run(self):
        logger.info("🔍 Starting comprehensive Plugin Discovery...")

        # Шаг 1: Читаем настройки из профиля
        profile = self.find_active_profile()
        if profile:
            logger.info(f"📖 Reading settings from: {profile.name}")
            self.parse_profile(profile)

        # Шаг 2: Сканируем папку плагинов для полного покрытия
        self.scan_plugins_folder()

        # Отчет
        configured = sum(
            1 for v in self.discovered_plugins.values() if v["status"] == "Configured"
        )
        installed = sum(
            1 for v in self.discovered_plugins.values() if "Installed" in v["status"]
        )

        logger.info(f"\n{'=' * 60}")
        logger.info(
            f"✅ Discovery Complete: {len(self.discovered_plugins)} Total Plugins Found"
        )
        logger.info(f"   ├─ Configured (in Profile): {configured}")
        logger.info(f"   └─ Installed (Folder Scan): {installed}")
        logger.info(f"{'=' * 60}")

        # Краткий вывод в консоль без спама
        for name, data in sorted(self.discovered_plugins.items()):
            status_icon = "⚙️" if data["status"] == "Configured" else "📦"
            logger.info(f"  {status_icon} {name} [{data['status']}]")
