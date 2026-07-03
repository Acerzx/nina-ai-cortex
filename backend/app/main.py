from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import asyncio
import sys

from app.core.config import get_settings
from app.core.discovery import PluginDiscovery
from app.ingestion.log_tailer import NinaLogTailer
from app.ingestion.session_watcher import SessionWatcher
from app.shadow_engine.sequence_parser import SequenceParser  # НОВЫЙ ИМПОРТ

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

    # 2. Shadow Sequence Parser (НОВОЕ)
    seq_parser = SequenceParser()
    app.state.sequence_shadow = seq_parser.parse()

    # 3. Background Ingestion
    loop = asyncio.get_running_loop()
    log_tailer = NinaLogTailer()
    session_watcher = SessionWatcher(loop)

    task1 = asyncio.create_task(log_tailer.start())
    task2 = asyncio.create_task(session_watcher.start())
    background_tasks.extend([task1, task2])

    logger.info("✅ Background Ingestion Services started")
    yield

    logger.info("👋 Shutting down N.I.N.A. AI Cortex...")
    for task in background_tasks:
        task.cancel()


app = FastAPI(
    title="N.I.N.A. AI Cortex",
    description="Cognitive Overlay for N.I.N.A.",
    version="0.2.0",
    lifespan=lifespan,
)


@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "N.I.N.A. AI Cortex is listening...",
        "plugins_found": len(app.state.discovery.discovered_plugins),
        "sequence_variables": app.state.sequence_shadow.global_variables,
    }


@app.get("/api/v1/plugins")
async def get_plugins():
    return app.state.discovery.discovered_plugins


# НОВЫЙ ЭНДПОИНТ: Состояние секвенсора
@app.get("/api/v1/sequence/shadow")
async def get_sequence_shadow():
    """Возвращает полный Context-Aware Shadow Graph"""
    return app.state.sequence_shadow
