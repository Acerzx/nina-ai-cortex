"""
Watcher Manager — централизованный хаб для управления всеми watchers, pollers и subscribers.
Устраняет проблему дублирования инициализации и обеспечивает единый lifecycle management.
"""

import asyncio
import logging
from pathlib import Path
from app.core.config import settings
from app.core.events import event_bus
from app.core.capability_registry import CapabilityRegistry

# Ingestion Watchers
from app.ingestion.watchers.session_watcher import SessionWatcher
from app.ingestion.watchers.hocus_focus_watcher import HocusFocusWatcher
from app.ingestion.watchers.fits_header_scanner import FITSHeaderScanner
from app.ingestion.watchers.prometheus_scraper import PrometheusScraper
from app.ingestion.watchers.log_tailer import LogTailer
from app.ingestion.watchers.livestack_watcher import LiveStackWatcher
from app.ingestion.watchers.masters_auditor import MastersLibraryAuditor
from app.ingestion.watchers.dither_guiding_watchers import (
    DitherStatisticsWatcher,
    GuidingAnalyzerWatcher,
)
from app.ingestion.watchers.autofocus_analysis_watcher import AutoFocusAnalysisWatcher
from app.ingestion.watchers.night_summary_watcher import NightSummaryWatcher
from app.ingestion.watchers.ai_weather_watcher import AIWeatherWatcher
from app.ingestion.watchers.dynamic_sequencer_watcher import DynamicSequencerWatcher
from app.ingestion.watchers.websocket_client import NinaWebSocketClient
from app.ingestion.subscribers.influxdb_subscriber import InfluxDBSubscriber

# Shadow Engine & Execution
from app.shadow_engine.state_tracker import state_tracker
from app.shadow_engine.sequence_parser import SequenceParser
from app.execution.safety_interceptor import safety_interceptor
from app.execution.hal import hal

# Multi-Agent Swarm Foundation
from app.agents.observatory_state import observatory_state

logger = logging.getLogger("WatcherManager")


class WatcherManager:
    """
    Централизованный менеджер для всех watchers, pollers и subscribers.
    Обеспечивает единый lifecycle management и Dependency Injection.
    """

    def __init__(self):
        self.watchers = []
        self.pollers = []
        self.ws_client = None
        self.influx = None
        self.registry = None
        self.masters_auditor = None  # ИСПРАВЛЕНО: сохраняем ссылку на аудитор

    async def start(self):
        """
        Запускает все компоненты системы в правильном порядке:
        1. EventBus
        2. Capability Registry (DI)
        3. Foundation (ObservatoryState, HAL)
        4. Shadow Engine (Sequence Parser)
        5. File Watchers
        6. Pollers (Prometheus, LogTailer)
        7. Subscribers (InfluxDB)
        8. Masters Library Audit (background)
        9. WebSocket Client (to N.I.N.A.)
        10. Safety Interceptor
        """
        logger.info("🚀 Initializing N.I.N.A. AI Cortex...")

        # 1. EventBus
        await event_bus.start()

        # 2. DI: Инициализация Capability Registry
        logger.info("📋 Loading Capability Registry from XML profile...")
        self.registry = CapabilityRegistry(settings.nina_environment.profiles_dir)

        # 3. Foundation (ObservatoryState, HAL)
        logger.info("🧠 Initializing ObservatoryState and HAL...")
        await observatory_state.start()
        await hal.start()

        # 4. Shadow Engine (парсинг Sequence.json)
        logger.info("📖 Parsing Sequence.json for Shadow Engine...")
        parser = SequenceParser()
        graph = parser.parse()
        state_tracker.set_shadow_graph(graph)

        # 5. File Watchers (Передаем registry через DI)
        logger.info("📂 Starting File Watchers...")
        self.watchers.extend(
            [
                SessionWatcher(self.registry),
                HocusFocusWatcher(self.registry),
                FITSHeaderScanner(self.registry),
                LiveStackWatcher(self.registry),
                DitherStatisticsWatcher(self.registry),
                GuidingAnalyzerWatcher(self.registry),
                AutoFocusAnalysisWatcher(self.registry),
                NightSummaryWatcher(self.registry),
                AIWeatherWatcher(self.registry),
                DynamicSequencerWatcher(self.registry),  # ИСПРАВЛЕНО: добавлен
            ]
        )
        for watcher in self.watchers:
            watcher.start()
        logger.info(f"   ✅ {len(self.watchers)} File Watchers started")

        # 6. Pollers (Prometheus, LogTailer, InfluxDB)
        logger.info("🔄 Starting Pollers...")

        # === ОСНОВНОЙ ИСТОЧНИК: InfluxDB ===
        from app.ingestion.providers.influxdb_metrics import influxdb_metrics_provider

        await influxdb_metrics_provider.start()

        # === РЕЗЕРВНЫЙ ИСТОЧНИК: Prometheus ===
        prometheus = PrometheusScraper()
        await prometheus.start()
        self.pollers.append(prometheus)

        log_tailer = LogTailer()
        await log_tailer.start()
        self.pollers.append(log_tailer)

        logger.info(f"   ✅ {len(self.pollers)} Pollers started")

        # 7. Subscribers (InfluxDB)
        logger.info("📊 Starting InfluxDB Subscriber...")
        self.influx = InfluxDBSubscriber()
        await self.influx.start()

        # 8. Masters Library Audit (background task)
        logger.info("📚 Starting Masters Library Audit in background...")
        self.masters_auditor = MastersLibraryAuditor()  # ИСПРАВЛЕНО: сохраняем ссылку
        asyncio.create_task(self.masters_auditor.scan_library())

        # 9. WebSocket Client (к N.I.N.A.)
        logger.info("📡 Starting WebSocket Client to N.I.N.A....")
        self.ws_client = NinaWebSocketClient(url=settings.network.nina_ws_url)
        await self.ws_client.start()

        # 10. Safety Interceptor
        logger.info("🛡️ Starting Safety Interceptor...")
        await safety_interceptor.start()

        logger.info("=" * 70)
        logger.info("✅ Cortex fully initialized with Dependency Injection.")
        logger.info("=" * 70)

    async def stop(self):
        """
        Корректно останавливает все компоненты в обратном порядке.
        """
        logger.info("🛑 Stopping all Cortex components...")

        # Останавливаем watchers
        for watcher in self.watchers:
            try:
                watcher.stop()
            except Exception as e:
                logger.error(f"Error stopping watcher: {e}")

        # Останавливаем pollers
        for poller in self.pollers:
            try:
                await poller.stop()
            except Exception as e:
                logger.error(f"Error stopping poller: {e}")

        # ИСПРАВЛЕНО: Останавливаем InfluxDB Metrics Provider
        try:
            from app.ingestion.providers.influxdb_metrics import (
                influxdb_metrics_provider,
            )

            await influxdb_metrics_provider.stop()
        except Exception as e:
            logger.debug(f"Error stopping InfluxDB Metrics Provider: {e}")

        # Останавливаем WebSocket client
        if self.ws_client:
            try:
                await self.ws_client.stop()
            except Exception as e:
                logger.error(f"Error stopping WebSocket client: {e}")

        # Останавливаем InfluxDB subscriber
        if self.influx:
            try:
                await self.influx.stop()
            except Exception as e:
                logger.error(f"Error stopping InfluxDB subscriber: {e}")

        # Останавливаем Safety Interceptor
        try:
            await safety_interceptor.stop()
        except Exception as e:
            logger.error(f"Error stopping Safety Interceptor: {e}")

        # Останавливаем EventBus
        await event_bus.stop()

        logger.info("✅ All Cortex components stopped gracefully.")
