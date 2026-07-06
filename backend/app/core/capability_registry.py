import logging
import xmltodict
import json
from pathlib import Path
from typing import Dict, Any, Optional

logger = logging.getLogger("CapabilityRegistry")


class CapabilityRegistry:
    """Реестр возможностей. Передается через DI в Watchers и Execution."""

    def __init__(self, profiles_dir: Path):
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._load_registry(profiles_dir)

    def _load_registry(self, profiles_dir: Path):
        profiles = list(profiles_dir.glob("*.profile"))
        if not profiles:
            return
        active_profile = max(profiles, key=lambda p: p.stat().st_mtime)
        try:
            with open(active_profile, "r", encoding="utf-8") as f:
                doc = xmltodict.parse(f.read())
            storage = self._find_key(doc, "pluginStorage")
            if not storage:
                return

            items = storage.get(
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
                    plugin_settings[key] = self._parse_value(value_node)
                self._registry[guid] = plugin_settings
            logger.info(f"Registry loaded: {len(self._registry)} plugins")
        except Exception as e:
            logger.error(f"Failed to load registry: {e}")

    def _find_key(self, obj: Any, key: str) -> Optional[Any]:
        if isinstance(obj, dict):
            if key in obj:
                return obj[key]
            for v in obj.values():
                res = self._find_key(v, key)
                if res:
                    return res
        elif isinstance(obj, list):
            for i in obj:
                res = self._find_key(i, key)
                if res:
                    return res
        return None

    def _parse_value(self, node: Any) -> Any:
        if not isinstance(node, dict):
            return str(node) if node else None
        text = node.get("#text")
        if not text:
            return node
        if isinstance(text, str) and (
            (text.startswith("[") and text.endswith("]"))
            or (text.startswith("{") and text.endswith("}"))
        ):
            try:
                return json.loads(text)
            except:
                pass
        if text.lower() in ("true", "false"):
            return text.lower() == "true"
        try:
            return float(text) if "." in text else int(text)
        except:
            return text

    def get_plugin_path(self, guid: str, key: str) -> Optional[Path]:
        val = self._registry.get(guid, {}).get(key)
        return Path(val) if val and isinstance(val, str) else None

    def get_plugin_setting(self, guid: str, key: str, default=None):
        return self._registry.get(guid, {}).get(key, default)
