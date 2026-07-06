"""
N.I.N.A. AI Cortex - Main Application
Точка входа FastAPI приложения с управлением жизненным циклом всех сервисов.
"""

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import logging
import asyncio
from typing import Dict, Any, List
import time

from app.core.config import get_settings
from app.core.discovery import PluginDiscovery
from app.shadow_engine.sequence_parser import SequenceParser
from app.ingestion.log_tailer import NinaLogTailer
from app.ingestion.session_watcher import SessionWatcher
from app.ingestion.nina_ws_client import NinaWebSocketClient
from app.ingestion.prometheus_scraper import PrometheusScraper

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ============================================================================
# APPLICATION STATE
# ============================================================================


class AppState:
    """Контейнер для глобального состояния приложения."""

    def __init__(self):
        self.discovery: PluginDiscovery = None
        self.sequence_parser: SequenceParser = None
        self.sequence_shadow: Dict[str, Any] = {}
        self.log_tailer: NinaLogTailer = None
        self.session_watcher: SessionWatcher = None
        self.ws_client: NinaWebSocketClient = None
        self.prom_scraper: PrometheusScraper = None
        self.background_tasks: List[asyncio.Task] = []
        self.start_time: float = time.time()


app_state = AppState()

# ============================================================================
# LIFESPAN MANAGER
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения.
    Инициализирует все сервисы при старте и корректно завершает при shutdown.
    """
    logger.info("🚀 Starting N.I.N.A. AI Cortex...")

    try:
        # 1. Загрузка конфигурации
        settings = get_settings()
        logger.info(f"⚙️  Configuration loaded:")
        logger.info(f"   ├─ N.I.N.A. AppData: {settings.nina_environment.appdata_root}")
        logger.info(f"   ├─ Sessions Root: {settings.nina_environment.sessions_root}")
        logger.info(f"   └─ API Host: {settings.network.nina_api_host}")

        # 2. Plugin Discovery
        logger.info("🔍 Running Plugin Discovery...")
        app_state.discovery = PluginDiscovery()
        app_state.discovery.run()
        logger.info(
            f"✅ Discovered {len(app_state.discovery.discovered_plugins)} plugins"
        )

        # 3. Sequence Parser
        logger.info("🧬 Parsing Advanced Sequence...")
        app_state.sequence_parser = SequenceParser()
        app_state.sequence_shadow = app_state.sequence_parser.parse()

        if app_state.sequence_shadow:
            stats = app_state.sequence_shadow.get("stats", {})
            logger.info(f"✅ Sequence parsed successfully:")
            logger.info(f"   ├─ Containers: {stats.get('total_containers', 0)}")
            logger.info(f"   ├─ Instructions: {stats.get('total_instructions', 0)}")
            logger.info(f"   ├─ Triggers: {stats.get('total_triggers', 0)}")
            logger.info(f"   ├─ Conditions: {stats.get('total_conditions', 0)}")
            logger.info(f"   ├─ MessageBoxes: {stats.get('total_message_boxes', 0)}")
            logger.info(
                f"   └─ Global Variables: {len(app_state.sequence_shadow.get('global_variables', {}))}"
            )
        else:
            logger.warning("⚠️  Sequence parsing returned empty result")

        # 4. Инициализация Ingestion сервисов
        logger.info("📡 Initializing Ingestion Services...")
        loop = asyncio.get_running_loop()

        # Log Tailer
        app_state.log_tailer = NinaLogTailer()
        task1 = asyncio.create_task(app_state.log_tailer.start())
        app_state.background_tasks.append(task1)
        logger.info("   ✅ Log Tailer started")

        # Session Watcher
        app_state.session_watcher = SessionWatcher(loop)
        task2 = asyncio.create_task(app_state.session_watcher.start())
        app_state.background_tasks.append(task2)
        logger.info("   ✅ Session Watcher started")

        # WebSocket Client
        app_state.ws_client = NinaWebSocketClient()
        task3 = asyncio.create_task(app_state.ws_client.start())
        app_state.background_tasks.append(task3)
        logger.info("   ✅ WebSocket Client started")

        # Prometheus Scraper
        app_state.prom_scraper = PrometheusScraper()
        task4 = asyncio.create_task(app_state.prom_scraper.start())
        app_state.background_tasks.append(task4)
        logger.info("   ✅ Prometheus Scraper started")

        logger.info("=" * 70)
        logger.info("✅ N.I.N.A. AI Cortex is fully operational")
        logger.info("=" * 70)

    except Exception as e:
        logger.error(f"❌ Failed to initialize application: {e}", exc_info=True)
        raise

    # Приложение запущено, ждем shutdown
    yield

    # ========================================================================
    # SHUTDOWN
    # ========================================================================
    logger.info("=" * 70)
    logger.info("👋 Shutting down N.I.N.A. AI Cortex...")
    logger.info("=" * 70)

    # Остановка всех фоновых задач
    if app_state.background_tasks:
        logger.info(
            f"⏹️  Stopping {len(app_state.background_tasks)} background tasks..."
        )
        for task in app_state.background_tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    logger.error(f"Error cancelling task: {e}")

    # Остановка конкретных сервисов
    if app_state.ws_client:
        app_state.ws_client.stop()
        logger.info("   ✅ WebSocket Client stopped")

    if app_state.log_tailer:
        app_state.log_tailer.stop()
        logger.info("   ✅ Log Tailer stopped")

    if app_state.prom_scraper:
        app_state.prom_scraper.stop()
        logger.info("   ✅ Prometheus Scraper stopped")

    uptime = time.time() - app_state.start_time
    logger.info(f"✅ Shutdown complete. Uptime: {uptime:.2f} seconds")


# ============================================================================
# FASTAPI APPLICATION
# ============================================================================

app = FastAPI(
    title="N.I.N.A. AI Cortex",
    description="Cognitive Overlay for N.I.N.A. Astrophotography Software",
    version="0.3.0",
    lifespan=lifespan,
)

# CORS Middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # В production заменить на конкретные домены
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================================
# API ENDPOINTS
# ============================================================================


@app.get("/", tags=["Status"])
async def root():
    """
    Корневой эндпоинт - статус системы.
    """
    uptime = time.time() - app_state.start_time

    return {
        "status": "online",
        "message": "N.I.N.A. AI Cortex is listening...",
        "uptime_seconds": round(uptime, 2),
        "plugins_found": len(app_state.discovery.discovered_plugins)
        if app_state.discovery
        else 0,
        "sequence_loaded": bool(app_state.sequence_shadow),
        "websocket_connected": app_state.ws_client.connected
        if app_state.ws_client
        else False,
    }


@app.get("/health", tags=["Status"])
async def health_check():
    """
    Health check эндпоинт для мониторинга.
    """
    checks = {
        "plugins_discovery": app_state.discovery is not None,
        "sequence_parsed": bool(app_state.sequence_shadow),
        "websocket_connected": app_state.ws_client.connected
        if app_state.ws_client
        else False,
        "log_tailer_running": app_state.log_tailer.running
        if app_state.log_tailer
        else False,
        "session_watcher_running": app_state.session_watcher is not None,
        "prometheus_scraper_running": app_state.prom_scraper.running
        if app_state.prom_scraper
        else False,
    }

    all_healthy = all(checks.values())

    return {
        "status": "healthy" if all_healthy else "degraded",
        "checks": checks,
        "uptime_seconds": round(time.time() - app_state.start_time, 2),
    }


@app.get("/api/v1/plugins", tags=["Plugins"])
async def get_plugins():
    """
    Возвращает список всех обнаруженных плагинов.
    """
    if not app_state.discovery:
        return {"error": "Plugin discovery not initialized"}

    return app_state.discovery.discovered_plugins


@app.get("/api/v1/sequence/shadow", tags=["Sequence"])
async def get_sequence_shadow():
    """
    Возвращает полный теневой граф Advanced Sequence.
    """
    if not app_state.sequence_shadow:
        return {"error": "Sequence not parsed"}

    return app_state.sequence_shadow


@app.get("/api/v1/sequence/state", tags=["Sequence"])
async def get_sequence_state():
    """
    Возвращает текущее состояние выполнения секвенсора.
    """
    if not app_state.ws_client:
        return {"error": "WebSocket client not initialized"}

    return {
        "connected": app_state.ws_client.connected,
        "last_event": getattr(app_state.ws_client, "last_event", None),
        "events_received": getattr(app_state.ws_client, "events_received", 0),
    }


@app.get("/api/v1/metrics", tags=["Metrics"])
async def get_metrics():
    """
    Возвращает текущие метрики из Prometheus scraper.
    """
    if not app_state.prom_scraper:
        return {"error": "Prometheus scraper not initialized"}

    return app_state.prom_scraper.metrics


@app.get("/api/v1/metrics/stats", tags=["Metrics"])
async def get_metrics_stats():
    """
    Возвращает статистику работы Prometheus scraper.
    """
    if not app_state.prom_scraper:
        return {"error": "Prometheus scraper not initialized"}

    return {
        "successful_scrapes": app_state.prom_scraper.successful_scrapes,
        "failed_scrapes": app_state.prom_scraper.failed_scrapes,
        "metrics_count": len(app_state.prom_scraper.metrics),
        "last_error": app_state.prom_scraper.last_error,
        "url": app_state.prom_scraper.url,
    }


# ============================================================================
# ERROR HANDLERS
# ============================================================================


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Глобальный обработчик ошибок.
    """
    logger.error(f"Unhandled exception: {exc}", exc_info=True)
    return {
        "error": "Internal server error",
        "detail": str(exc),
    }


# ============================================================================
# STARTUP MESSAGE
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    logger.info("=" * 70)
    logger.info("N.I.N.A. AI Cortex - Cognitive Overlay")
    logger.info("=" * 70)

    uvicorn.run(
        "app.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
