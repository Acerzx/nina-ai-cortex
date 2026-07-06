import asyncio, logging
from app.core.events import event_bus
from app.core.config import settings
from app.core.capability_registry import CapabilityRegistry

# Watchers
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
from app.ingestion.watchers.websocket_client import NinaWebSocketClient
from app.ingestion.subscribers.influxdb_subscriber import InfluxDBSubscriber

# Shadow & Execution
from app.shadow_engine.state_tracker import state_tracker
from app.shadow_engine.sequence_parser import SequenceParser
from app.execution.safety_interceptor import safety_interceptor
from app.execution.hal import hal
from app.agents.observatory_state import observatory_state

logger = logging.getLogger("WatcherManager")


class WatcherManager:
    def __init__(self):
        self.watchers = []
        self.pollers = []
        self.ws_client = None
        self.influx = None
        self.registry = None

    async def start(self):
        logger.info("🚀 Initializing N.I.N.A. AI Cortex...")
        await event_bus.start()

        # 1. DI: Инициализация Registry
        self.registry = CapabilityRegistry(settings.nina_environment.profiles_dir)

        # 2. Foundation
        await observatory_state.start()
        await hal.start()

        # 3. Shadow Engine
        parser = SequenceParser()
        graph = parser.parse()
        state_tracker.set_shadow_graph(graph)

        # 4. File Watchers (Передаем registry через DI)
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
            ]
        )
        for w in self.watchers:
            w.start()

        # 5. Pollers & Subscribers
        prometheus = PrometheusScraper()
        await prometheus.start()
        self.pollers.append(prometheus)

        log_tailer = LogTailer()
        await log_tailer.start()
        self.pollers.append(log_tailer)

        self.influx = InfluxDBSubscriber()
        await self.influx.start()

        # 6. Masters Audit
        asyncio.create_task(MastersLibraryAuditor(self.registry).scan_library())

        # 7. WebSocket & Safety
        self.ws_client = NinaWebSocketClient(url=settings.network.nina_ws_url)
        await self.ws_client.start()
        await safety_interceptor.start()

        logger.info("✅ Cortex fully initialized with Dependency Injection.")

    async def stop(self):
        for w in self.watchers:
            w.stop()
        for p in self.pollers:
            await p.stop()
        if self.ws_client:
            await self.ws_client.stop()
        if self.influx:
            await self.influx.stop()
        await safety_interceptor.stop()
        await event_bus.stop()
