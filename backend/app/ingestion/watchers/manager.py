"""
Watcher Manager — централизованный хаб для управления всеми watchers и pollers.

ЭТАП 8 (финальная синхронизация):
- Удалены импорты и использование мёртвых модулей:
  * InfluxDBSubscriber (Этап 1)
  * AIWeatherWatcher (Этап 3)
  * DynamicSequencerWatcher (Этап 3)
  * SafetyInterceptor + PythonBridge (Этап 3)
- Оставлены 8 watcher'ов + 2 poller'а + WebSocket client

ИСПРАВЛЕНО (audit 5.1): Сохранение ссылок на фоновые задачи для предотвращения
их отмены сборщиком мусора в Python 3.12+
"""

import asyncio
import logging
from pathlib import Path
from typing import Set

from app.core.config import settings
from app.core.events import event_bus
from app.core.capability_registry import CapabilityRegistry

# === Ingestion Watchers (8 активных) ===
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
from app.ingestion.watchers.autofocus_analysis_watcher import (
    AutoFocusAnalysisWatcher,
)
from app.ingestion.watchers.night_summary_watcher import NightSummaryWatcher
from app.ingestion.watchers.websocket_client import NinaWebSocketClient

# === УДАЛЕНО (Этап 3): AIWeatherWatcher, DynamicSequencerWatcher ===
# === УДАЛЕНО (Этап 1): InfluxDBSubscriber ===
# === УДАЛЕНО (Этап 3): SafetyInterceptor ===

# Shadow Engine & Execution
from app.shadow_engine.state_tracker import state_tracker
from app.shadow_engine.sequence_parser import SequenceParser
from app.execution.hal import hal

# Multi-Agent Swarm Foundation
from app.agents.observatory_state import observatory_state

logger = logging.getLogger("WatcherManager")


class WatcherManager:
    """
    Централизованный менеджер для всех watchers и pollers.

    ЭТАП 8: Убраны все мёртвые модули.
    Оставлены 8 watcher'ов + 2 poller'а + WebSocket client.
    """

    def __init__(self):
        self.watchers = []
        self.pollers = []
        self.ws_client = None
        self.registry = None
        self.masters_auditor = None

        # ИСПРАВЛЕНО (audit 5.1): Хранение ссылок на фоновые задачи
        self._background_tasks: Set[asyncio.Task] = set()

    async def start(self):
        """
        Запускает все компоненты системы в правильном порядке:
        1. EventBus
        2. Capability Registry (DI)
        3. Foundation (ObservatoryState, HAL)
        4. Shadow Engine (Sequence Parser)
        5. File Watchers (8 штук)
        6. Pollers (InfluxDB Provider, Prometheus, LogTailer)
        7. Masters Library Audit (background)
        8. WebSocket Client (to N.I.N.A.)
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
        graph = await parser.parse()
        state_tracker.set_shadow_graph(graph)

        # 5. File Watchers (8 активных, без удалённых)
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
                # УДАЛЕНО (Этап 3): AIWeatherWatcher
                # УДАЛЕНО (Этап 3): DynamicSequencerWatcher
            ]
        )
        for watcher in self.watchers:
            await watcher.start()
        logger.info(f"   ✅ {len(self.watchers)} File Watchers started")

        # 6. Pollers
        logger.info("🔄 Starting Pollers...")

        # === ОСНОВНОЙ ИСТОЧНИК: InfluxDB Metrics Provider ===
        from app.ingestion.providers.influxdb_metrics import (
            influxdb_metrics_provider,
        )

        await influxdb_metrics_provider.start()

        # === РЕЗЕРВНЫЙ ИСТОЧНИК: Prometheus Scraper ===
        prometheus = PrometheusScraper()
        await prometheus.start()
        self.pollers.append(prometheus)

        # === LogTailer ===
        log_tailer = LogTailer()
        await log_tailer.start()
        self.pollers.append(log_tailer)

        logger.info(f"   ✅ {len(self.pollers)} Pollers started")

        # УДАЛЕНО (Этап 1): InfluxDB Subscriber
        # (on-demand Flux queries не используются,
        #  InfluxDB Metrics Provider покрывает все потребности)

        # 7. Masters Library Audit (background task)
        logger.info("📚 Starting Masters Library Audit in background...")
        self.masters_auditor = MastersLibraryAuditor()

        # ИСПРАВЛЕНО (audit 5.1): Сохраняем ссылку на задачу
        scan_task = asyncio.create_task(self.masters_auditor.scan_library())
        self._background_tasks.add(scan_task)
        scan_task.add_done_callback(self._background_tasks.discard)
        logger.info("   ✅ Masters Library Audit task scheduled")

        # 8. WebSocket Client (к N.I.N.A.)
        logger.info("📡 Starting WebSocket Client to N.I.N.A....")
        self.ws_client = NinaWebSocketClient(url=settings.network.nina_ws_url)
        await self.ws_client.start()

        # УДАЛЕНО (Этап 3): Safety Interceptor
        # (PythonBridge удалён, Safety Interceptor не нужен)

        logger.info("=" * 70)
        logger.info(
            "✅ Cortex fully initialized "
            f"({len(self.watchers)} watchers, {len(self.pollers)} pollers)"
        )
        logger.info("=" * 70)

    async def stop(self):
        """
        Корректно останавливает все компоненты в обратном порядке.
        """
        logger.info("🛑 Stopping all Cortex components...")

        # ИСПРАВЛЕНО (audit 5.1): Отменяем все фоновые задачи
        if self._background_tasks:
            logger.info(
                f"   ⏹️ Cancelling {len(self._background_tasks)} background tasks..."
            )
            for task in list(self._background_tasks):
                if not task.done():
                    task.cancel()
            if self._background_tasks:
                await asyncio.gather(*self._background_tasks, return_exceptions=True)
            self._background_tasks.clear()
            logger.info("   ✅ All background tasks cancelled")

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

        # Останавливаем InfluxDB Metrics Provider
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

        # УДАЛЕНО (Этап 1): InfluxDB Subscriber stop
        # УДАЛЕНО (Этап 3): Safety Interceptor stop

        # Останавливаем EventBus
        await event_bus.stop()

        logger.info("✅ All Cortex components stopped gracefully.")
