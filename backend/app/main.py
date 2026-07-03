from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import asyncio
import sys

# ... (импорты config и discovery)
from app.ingestion.log_tailer import NinaLogTailer
from app.ingestion.session_watcher import SessionWatcher

# Настройка логирования
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
    from app.core.discovery import PluginDiscovery

    discovery = PluginDiscovery()
    discovery.run()
    app.state.discovery = discovery

    # 2. Запуск фоновых сервисов Ingestion
    loop = asyncio.get_running_loop()

    log_tailer = NinaLogTailer()
    session_watcher = SessionWatcher(loop)

    task1 = asyncio.create_task(log_tailer.start())
    task2 = asyncio.create_task(session_watcher.start())

    background_tasks.extend([task1, task2])
    logger.info("✅ Background Ingestion Services started (LogTailer, SessionWatcher)")

    yield

    # Shutdown
    logger.info("👋 Shutting down N.I.N.A. AI Cortex...")
    for task in background_tasks:
        task.cancel()


app = FastAPI(
    title="N.I.N.A. AI Cortex",
    description="Cognitive Overlay for N.I.N.A. Astrophotography Software",
    version="0.1.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {"status": "online", "message": "N.I.N.A. AI Cortex is listening..."}


@app.get("/api/v1/plugins")
async def get_plugins():
    return app.state.discovery.discovered_plugins
