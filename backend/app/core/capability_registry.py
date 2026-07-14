"""
Capability Registry — реестр возможностей N.I.N.A. плагинов.

ЭТАП 3.1 (полный рефакторинг):
- Новая модель PluginCapability (Pydantic) с полным описание плагина
- Автодетекция установленных плагинов через сканирование plugins_dir
- Маппинг GUID → watcher класс
- Приоритеты данных для каждого плагина
- Методы для проверки установки и получения списка watcher'ов

Архитектура приоритетов данных:
┌─────────────────────────────────────────────────────────────┐
│  PRIMARY: InfluxDB Exporter                                  │
│  → Все time-series метрики, история                          │
│  → Обновление каждые 2-3 секунды                             │
│                                                              │
│  FALLBACK: Prometheus Exporter                               │
│  → Моментальные срезы, уникальные метрики (AF r², status)   │
│  → Активен когда InfluxDB недоступен                         │
│                                                              │
│  ENRICHMENT: File Watchers (per-image детализация)           │
│  → Hocus Focus, Session Metadata, FITS Headers               │
│  → LiveStack (SNR), Dither Statistics, AutoFocus Analysis    │
│                                                              │
│  POST_MORTEM: Анализ после сессии                            │
│  → Night Summary, Guiding Analyzer                           │
│                                                              │
│  INTERNAL: In-memory данные (без файлов)                     │
│  → Flexure Compensator                                       │
└─────────────────────────────────────────────────────────────┘

Использование:
    from app.core.capability_registry import CapabilityRegistry

    registry = CapabilityRegistry(settings.nina_environment.profiles_dir)

    # Проверка установки плагина
    if registry.is_plugin_installed("hocus-focus"):
        print("Hocus Focus установлен!")

    # Получение списка watcher'ов для установленных плагинов
    watchers = registry.get_watchers_for_installed_plugins()

    # Приоритеты источников для метрики
    sources = registry.get_data_source_priority("hfr")
"""

import logging
import json
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
from dataclasses import dataclass, field
from datetime import datetime
import xmltodict

logger = logging.getLogger("CapabilityRegistry")


# ============================================================================
# МОДЕЛИ ДАННЫХ
# ============================================================================


@dataclass
class PluginCapability:
    """
    Полное описание возможности плагина.

    Attributes:
        guid: Уникальный идентификатор плагина (из plugin.json)
        name: Человекочитаемое имя плагина
        installed: Установлен ли плагин (определяется автоматически)
        version: Версия плагина (из plugin.json)
        data_path: Путь к данным плагина (относительно appdata_root)
        output_format: Формат вывода (json, csv, fits_header, influxdb, prometheus)
        provides_metrics: Список метрик, которые предоставляет плагин
        watcher_class: Имя класса watcher'а для обработки данных
        priority: Приоритет данных (PRIMARY, FALLBACK, ENRICHMENT, POST_MORTEM, INTERNAL)
        repository: Ссылка на GitHub репозиторий
        description: Краткое описание плагина
    """

    guid: str
    name: str
    installed: bool = False
    version: Optional[str] = None
    data_path: Optional[str] = None
    output_format: Optional[str] = None
    provides_metrics: List[str] = field(default_factory=list)
    watcher_class: Optional[str] = None
    priority: str = "ENRICHMENT"
    repository: Optional[str] = None
    description: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Сериализация для API."""
        return {
            "guid": self.guid,
            "name": self.name,
            "installed": self.installed,
            "version": self.version,
            "data_path": self.data_path,
            "output_format": self.output_format,
            "provides_metrics": self.provides_metrics,
            "watcher_class": self.watcher_class,
            "priority": self.priority,
            "repository": self.repository,
            "description": self.description,
        }


# ============================================================================
# РЕЕСТР ИЗВЕСТНЫХ ПЛАГИНОВ
# ============================================================================

# Полный реестр всех известных плагинов N.I.N.A.
# GUID'ы взяты из plugin.json каждого плагина
KNOWN_PLUGINS: Dict[str, PluginCapability] = {
    # === PRIMARY: InfluxDB Exporter ===
    "daleghent.influxdb-exporter": PluginCapability(
        guid="daleghent.influxdb-exporter",
        name="InfluxDB Exporter",
        data_path=None,  # Push model (NINA → InfluxDB)
        output_format="influxdb",
        provides_metrics=[
            "camera_temp",
            "camera_cooler_power",
            "focuser_position",
            "focuser_temp",
            "guider_rms_ra",
            "guider_rms_dec",
            "guider_rms_total",
            "mount_altitude",
            "mount_azimuth",
            "rotator_angle",
            "wx_temperature",
            "wx_humidity",
            "wx_dewpoint",
            "wx_cloud_cover",
            "wx_wind_speed",
            "wx_wind_gust",
            "wx_wind_direction",
            "wx_pressure",
            "image_hfr",
            "image_fwhm",
            "image_stars",
            "image_median",
        ],
        watcher_class="InfluxDBMetricsProvider",
        priority="PRIMARY",
        repository="https://github.com/daleghent/nina-influxdb-exporter",
        description="Экспорт time-series метрик в InfluxDB 2.x",
    ),
    # === FALLBACK: Prometheus Exporter ===
    "jewzaam.prometheus-exporter": PluginCapability(
        guid="jewzaam.prometheus-exporter",
        name="Prometheus Exporter",
        data_path=None,  # Pull model (Cortex → scrape)
        output_format="prometheus",
        provides_metrics=[
            "autofocus_rsquares",
            "sequence_status",
            "autofocus_running",
            "equipment_camera",
            "equipment_mount",
            "equipment_focuser",
            "equipment_filterwheel",
            "equipment_guider",
            "equipment_dome",
        ],
        watcher_class="PrometheusScraper",
        priority="FALLBACK",
        repository="https://github.com/jewzaam/nina-prometheus-exporter",
        description="Экспорт метрик N.I.N.A. в Prometheus (порт 9876)",
    ),
    # === ENRICHMENT: Session Metadata ===
    "tcpalmer.sessionmetadata": PluginCapability(
        guid="tcpalmer.sessionmetadata",
        name="Session Metadata",
        data_path="sessions_root/{session}/",
        output_format="json",
        provides_metrics=[
            "frame_metadata",
            "acquisition_details",
            "weather",
            "hfr",
            "fwhm",
            "stars",
            "rms_total",
            "exposure_time",
            "gain",
            "filter",
        ],
        watcher_class="SessionWatcher",
        priority="ENRICHMENT",
        repository="https://github.com/tcpalmer/nina.plugin.sessionmetadata",
        description="Метаданные сессии и каждого кадра (JSON/CSV)",
    ),
    # === ENRICHMENT: Hocus Focus ===
    "0f1d10b6-d306-4168-b751-d454cbac9670": PluginCapability(
        guid="0f1d10b6-d306-4168-b751-d454cbac9670",
        name="Hocus Focus",
        data_path="HocusFocusIntermediate/*.csv",
        output_format="csv",
        provides_metrics=[
            "per_star_fwhm",
            "per_star_hfr",
            "per_star_eccentricity",
            "per_star_coma",
            "per_star_astigmatism",
        ],
        watcher_class="HocusFocusWatcher",
        priority="ENRICHMENT",
        repository="https://github.com/ghilios/hocus-focus",
        description="Детальный анализ звезд (FWHM, эксцентриситет, кома, астигматизм)",
    ),
    # === ENRICHMENT: LiveStack ===
    "10bc1716-54af-425e-b307-c0ca1ce10600": PluginCapability(
        guid="10bc1716-54af-425e-b307-c0ca1ce10600",
        name="LiveStack",
        data_path="sessions_root/Live/",
        output_format="json+csv",
        provides_metrics=[
            "snr",
            "acceptance_rate",
            "frames_stacked",
            "frames_rejected",
        ],
        watcher_class="LiveStackWatcher",
        priority="ENRICHMENT",
        repository="https://github.com/isbeorn/nina.plugin.livestack",
        description="Real-time стекинг и калибровка (единственный источник SNR!)",
    ),
    # === ENRICHMENT: Dither Statistics ===
    "thierrytsch.dither-statistics": PluginCapability(
        guid="thierrytsch.dither-statistics",
        name="Dither Statistics",
        data_path="Documents/NINA/DitherStatistics/",
        output_format="csv+json",
        provides_metrics=[
            "cd_discrepancy",
            "voronoi_cv",
            "gfm",
            "nni",
            "dither_quality_score",
        ],
        watcher_class="DitherStatisticsWatcher",
        priority="ENRICHMENT",
        repository="https://github.com/Thierrytsch/NINA-DitherStatistics",
        description="Анализ качества дизеринга (CD, GFM, Voronoi CV)",
    ),
    # === ENRICHMENT: SolveEveryLight ===
    "astroalex80.solve-every-light": PluginCapability(
        guid="astroalex80.solve-every-light",
        name="SolveEveryLight",
        data_path=None,  # Writes to FITS headers
        output_format="fits_header",
        provides_metrics=[
            "wcs_coords",
            "plate_solve_success",
            "moon_angl",
            "sun_angle",
        ],
        watcher_class="FITSHeaderScanner",
        priority="ENRICHMENT",
        repository="https://github.com/astroalex80/NINA.Plugin.SolveEveryLight",
        description="Plate solve каждого LIGHT-кадра, запись WCS в FITS-хедер",
    ),
    # === ENRICHMENT: AutoFocus Analysis ===
    "97021132-0c25-4443-b947-fe5efbe0a3d6": PluginCapability(
        guid="97021132-0c25-4443-b947-fe5efbe0a3d6",
        name="AutoFocus Analysis",
        data_path="AutoFocus/*.json",
        output_format="json",
        provides_metrics=["af_curve", "rsquares", "backlash", "af_quality"],
        watcher_class="AutoFocusAnalysisWatcher",
        priority="ENRICHMENT",
        repository="https://github.com/isbeorn/nina.plugin.autofocusanalysis",
        description="Анализ кривых автофокуса",
    ),
    # === POST_MORTEM: Night Summary ===
    "isbeorn.night-summary": PluginCapability(
        guid="isbeorn.night-summary",
        name="Night Summary",
        data_path="sessions_root/{session}/NightSummary.json",
        output_format="json",
        provides_metrics=["night_summary"],
        watcher_class="NightSummaryWatcher",
        priority="POST_MORTEM",
        repository="https://github.com/isbeorn/nina.plugin.nightsummary",
        description="Итоговые отчеты за ночь",
    ),
    # === POST_MORTEM: Guiding Analyzer ===
    "jphf007.guiding-analyzer": PluginCapability(
        guid="jphf007.guiding-analyzer",
        name="Guiding Analyzer",
        data_path="Documents/NINA/GuidingAnalyzer/",
        output_format="csv+json+pdf",
        provides_metrics=["fft_spectrum", "periodic_error", "backlash", "polar_error"],
        watcher_class="GuidingAnalyzerWatcher",
        priority="POST_MORTEM",
        repository="https://github.com/jphf007/GuidingAnalyzer",
        description="FFT анализ PHD2 логов, детекция PE, backlash, полярной ошибки",
    ),
    # === INTERNAL: Flexure Compensator ===
    "michelegz.flexure-compensator": PluginCapability(
        guid="michelegz.flexure-compensator",
        name="Flexure Compensator",
        data_path=None,  # In-memory corrections
        output_format="internal",
        provides_metrics=["flexure_drift_vector"],
        watcher_class=None,  # No file output
        priority="INTERNAL",
        repository="https://github.com/michelegz/nina.plugin.flexurecompensator",
        description="Компенсация прогиба OAG через plate solve",
    ),
    # === УДАЛЁННЫЕ (не используются в Cortex): ===
    # AI Weather (часто неактивен из-за неполадок подключения)
    # Основная метрика окружающей среды — датчик температуры на фокусере
    # Косвенные показатели: роса/иней на оптике
    "ai-weather": PluginCapability(
        guid="ai-weather",
        name="AI Weather",
        data_path="AIWeather/status.json",
        output_format="json",
        provides_metrics=["weather_status"],
        watcher_class=None,  # УДАЛЁН
        priority="ENRICHMENT",
        repository=None,
        description="AI Weather плагин (часто неактивен, удалён из Cortex)",
    ),
    # Dynamic Sequencer Watcher (редактор сам знает о изменениях)
    "dynamic-sequencer": PluginCapability(
        guid="dynamic-sequencer",
        name="Dynamic Sequencer",
        data_path="Documents/DynamicSequencer/Projects/",
        output_format="json",
        provides_metrics=["project_updates"],
        watcher_class=None,  # УДАЛЁН
        priority="ENRICHMENT",
        repository=None,
        description="Dynamic Sequencer (watcher удалён, используется DynamicEditor)",
    ),
}


# ============================================================================
# CAPABILITY REGISTRY
# ============================================================================


class CapabilityRegistry:
    """
    Реестр возможностей N.I.N.A. плагинов.

    Обеспечивает:
    1. Парсинг XML-профилей N.I.N.A. для извлечения путей плагинов
    2. Автодетекцию установленных плагинов через сканирование plugins_dir
    3. Маппинг GUID → watcher класс
    4. Приоритеты данных для каждого плагина

    Использование:
        registry = CapabilityRegistry(settings.nina_environment.profiles_dir)

        # Проверка установки
        if registry.is_plugin_installed("hocus-focus"):
            print("Hocus Focus установлен!")

        # Список watcher'ов
        watchers = registry.get_watchers_for_installed_plugins()
    """

    def __init__(self, profiles_dir: Path, plugins_dir: Optional[Path] = None):
        """
        Инициализирует Capability Registry.

        Args:
            profiles_dir: Путь к директории профилей N.I.N.A.
            plugins_dir: Путь к директории плагинов N.I.N.A. (опционально)
        """
        self._registry: Dict[str, Dict[str, Any]] = {}
        self._installed_plugins: Set[str] = set()
        self._plugin_capabilities: Dict[str, PluginCapability] = {}

        # 1. Загружаем XML-профили (для путей плагинов)
        self._load_registry(profiles_dir)

        # 2. Детектим установленные плагины
        if plugins_dir:
            self._detect_installed_plugins(plugins_dir)

        # 3. Инициализируем capabilities
        self._initialize_capabilities()

        logger.info(
            f"✅ Capability Registry initialized: "
            f"{len(self._registry)} plugins in profile, "
            f"{len(self._installed_plugins)} installed, "
            f"{len(self._plugin_capabilities)} capabilities registered"
        )

    def _load_registry(self, profiles_dir: Path):
        """Загружает XML-профили N.I.N.A. для извлечения путей плагинов."""
        profiles = list(profiles_dir.glob("*.profile"))
        if not profiles:
            logger.warning(f"No profiles found in {profiles_dir}")
            return

        # Берём самый свежий профиль
        active_profile = max(profiles, key=lambda p: p.stat().st_mtime)

        try:
            with open(active_profile, "r", encoding="utf-8") as f:
                doc = xmltodict.parse(f.read())

            storage = self._find_key(doc, "pluginStorage")
            if not storage:
                logger.warning("pluginStorage not found in profile")
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

            logger.info(f"Registry loaded: {len(self._registry)} plugins from profile")

        except Exception as e:
            logger.error(f"Failed to load registry: {e}")

    def _detect_installed_plugins(self, plugins_dir: Path):
        """
        Детектит установленные плагины через сканирование plugins_dir.

        Ищет plugin.json в каждой поддиректории и извлекает GUID.
        """
        if not plugins_dir.exists():
            logger.warning(f"Plugins directory does not exist: {plugins_dir}")
            return

        for plugin_dir in plugins_dir.iterdir():
            if not plugin_dir.is_dir():
                continue

            # Ищем plugin.json
            manifest = plugin_dir / "plugin.json"
            if not manifest.exists():
                continue

            try:
                with open(manifest, "r", encoding="utf-8") as f:
                    data = json.load(f)

                guid = data.get("Id") or data.get("GUID") or data.get("guid")
                version = data.get("Version") or data.get("version")

                if guid:
                    self._installed_plugins.add(guid)

                    # Обновляем capability если есть
                    if guid in self._plugin_capabilities:
                        self._plugin_capabilities[guid].installed = True
                        self._plugin_capabilities[guid].version = version

                    logger.debug(f"Detected installed plugin: {guid} (v{version})")

            except Exception as e:
                logger.debug(f"Failed to read plugin manifest {manifest}: {e}")

        logger.info(f"Detected {len(self._installed_plugins)} installed plugins")

    def _initialize_capabilities(self):
        """Инициализирует capabilities для всех известных плагинов."""
        for guid, capability in KNOWN_PLUGINS.items():
            # Копируем capability и обновляем installed статус
            cap = PluginCapability(
                guid=capability.guid,
                name=capability.name,
                installed=guid in self._installed_plugins,
                version=capability.version,
                data_path=capability.data_path,
                output_format=capability.output_format,
                provides_metrics=capability.provides_metrics.copy(),
                watcher_class=capability.watcher_class,
                priority=capability.priority,
                repository=capability.repository,
                description=capability.description,
            )
            self._plugin_capabilities[guid] = cap

    def _find_key(self, obj: Any, key: str) -> Optional[Any]:
        """Рекурсивно ищет ключ в XML-документе."""
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
        """Парсит значение из XML-узла."""
        if not isinstance(node, dict):
            return str(node) if node else None

        text = node.get("#text")
        if not text:
            return node

        # JSON-подобные строки
        if isinstance(text, str) and (
            (text.startswith("[") and text.endswith("]"))
            or (text.startswith("{") and text.endswith("}"))
        ):
            try:
                return json.loads(text)
            except:
                pass

        # Boolean
        if text.lower() in ("true", "false"):
            return text.lower() == "true"

        # Numeric
        try:
            return float(text) if "." in text else int(text)
        except:
            return text

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def is_plugin_installed(self, name_or_guid: str) -> bool:
        """
        Проверяет, установлен ли плагин.

        Args:
            name_or_guid: Имя плагина (например, "hocus-focus") или GUID

        Returns:
            True если плагин установлен
        """
        # Поиск по GUID
        if name_or_guid in self._installed_plugins:
            return True

        # Поиск по имени
        for guid, cap in self._plugin_capabilities.items():
            if cap.name.lower().replace(" ", "-") == name_or_guid.lower():
                return guid in self._installed_plugins

        return False

    def get_plugin_capability(self, name_or_guid: str) -> Optional[PluginCapability]:
        """
        Возвращает PluginCapability для плагина.

        Args:
            name_or_guid: Имя плагина или GUID

        Returns:
            PluginCapability или None
        """
        # Поиск по GUID
        if name_or_guid in self._plugin_capabilities:
            return self._plugin_capabilities[name_or_guid]

        # Поиск по имени
        for guid, cap in self._plugin_capabilities.items():
            if cap.name.lower().replace(" ", "-") == name_or_guid.lower():
                return cap

        return None

    def get_watchers_for_installed_plugins(self) -> List[str]:
        """
        Возвращает список watcher-классов для установленных плагинов.

        Returns:
            Список имён watcher-классов (например, ["HocusFocusWatcher", "LiveStackWatcher"])
        """
        watchers = []
        for guid, cap in self._plugin_capabilities.items():
            if cap.installed and cap.watcher_class:
                watchers.append(cap.watcher_class)
        return sorted(set(watchers))  # Уникальные, отсортированные

    def get_data_source_priority(self, metric_type: str) -> List[Dict[str, Any]]:
        """
        Возвращает источники данных для метрики, отсортированные по приоритету.

        Args:
            metric_type: Тип метрики (например, "hfr", "rms_ra")

        Returns:
            Список словарей с информацией об источниках
        """
        sources = []
        for guid, cap in self._plugin_capabilities.items():
            if metric_type in cap.provides_metrics and cap.installed:
                sources.append(
                    {
                        "plugin": cap.name,
                        "guid": cap.guid,
                        "priority": cap.priority,
                        "watcher_class": cap.watcher_class,
                        "output_format": cap.output_format,
                    }
                )

        # Сортировка по приоритету
        priority_order = {
            "PRIMARY": 0,
            "FALLBACK": 1,
            "ENRICHMENT": 2,
            "POST_MORTEM": 3,
            "INTERNAL": 4,
        }
        sources.sort(key=lambda x: priority_order.get(x["priority"], 99))

        return sources

    def get_plugin_path(self, guid: str, key: str) -> Optional[Path]:
        """
        Возвращает путь из XML-профиля плагина.

        Args:
            guid: GUID плагина
            key: Ключ настройки (например, "SavePath")

        Returns:
            Path или None
        """
        val = self._registry.get(guid, {}).get(key)
        return Path(val) if val and isinstance(val, str) else None

    def get_plugin_setting(self, guid: str, key: str, default=None):
        """
        Возвращает настройку плагина из XML-профиля.

        Args:
            guid: GUID плагина
            key: Ключ настройки
            default: Значение по умолчанию

        Returns:
            Значение настройки или default
        """
        return self._registry.get(guid, {}).get(key, default)

    def get_all_capabilities(self) -> Dict[str, PluginCapability]:
        """Возвращает все capabilities."""
        return dict(self._plugin_capabilities)

    def get_installed_capabilities(self) -> Dict[str, PluginCapability]:
        """Возвращает capabilities только для установленных плагинов."""
        return {
            guid: cap
            for guid, cap in self._plugin_capabilities.items()
            if cap.installed
        }

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику Capability Registry."""
        installed_count = sum(
            1 for cap in self._plugin_capabilities.values() if cap.installed
        )

        by_priority = {}
        for cap in self._plugin_capabilities.values():
            if cap.installed:
                by_priority[cap.priority] = by_priority.get(cap.priority, 0) + 1

        return {
            "total_known_plugins": len(self._plugin_capabilities),
            "installed_plugins": installed_count,
            "plugins_in_profile": len(self._registry),
            "by_priority": by_priority,
            "watchers_for_installed": self.get_watchers_for_installed_plugins(),
        }
