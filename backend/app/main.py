from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import asyncio
from app.core.config import get_settings
from app.core.discovery import PluginDiscovery
from app.ingestion.log_tailer import NinaLogTailer
from app.ingestion.session_watcher import SessionWatcher
from app.ingestion.nina_ws_client import NinaWebSocketClient  # ИСПОЛЬЗУЕМ НОВЫЙ КЛИЕНТ
from app.shadow_engine.sequence_parser import SequenceParser
from app.shadow_engine.state_tracker import SequenceStateTracker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

background_tasks = []


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("🚀 Starting N.I.N.A. AI Cortex...")

    # 1. Discovery
    discovery = PluginDiscovery()
    discovery.run()
    app.state.discovery = discovery

    # 2. Shadow Sequence Parser
    seq_parser = SequenceParser()
    shadow_graph = seq_parser.parse()
    app.state.sequence_shadow = shadow_graph

    # 3. State Tracker
    state_tracker = SequenceStateTracker(shadow_graph)
    app.state.state_tracker = state_tracker

    # 4. Background Ingestion
    loop = asyncio.get_running_loop()
    log_tailer = NinaLogTailer()
    session_watcher = SessionWatcher(loop)

    # WebSocket клиент с callback для State Tracker
    ws_client = NinaWebSocketClient(on_event=state_tracker.process_event)

    task1 = asyncio.create_task(log_tailer.start())
    task2 = asyncio.create_task(session_watcher.start())
    task3 = asyncio.create_task(ws_client.start())

    background_tasks.extend([task1, task2, task3])
    logger.info(
        "✅ Background Ingestion Services started (LogTailer, SessionWatcher, WebSocket)"
    )

    yield

    logger.info("👋 Shutting down N.I.N.A. AI Cortex...")
    ws_client.stop()
    for task in background_tasks:
        task.cancel()


app = FastAPI(
    title="N.I.N.A. AI Cortex",
    description="Cognitive Overlay for N.I.N.A.",
    version="0.4.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "N.I.N.A. AI Cortex is listening...",
        "plugins_found": len(app.state.discovery.discovered_plugins),
        "mode": "WebSocket + REST API",
    }


@app.get("/api/v1/plugins")
async def get_plugins():
    return app.state.discovery.discovered_plugins


@app.get("/api/v1/sequence/shadow")
async def get_sequence_shadow():
    return app.state.sequence_shadow


@app.get("/api/v1/sequence/state")
async def get_sequence_state():
    """Возвращает текущее состояние выполнения секвенсора."""
    return app.state.state_tracker.get_current_state()
