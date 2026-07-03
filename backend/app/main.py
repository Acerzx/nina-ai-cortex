from fastapi import FastAPI
from contextlib import asynccontextmanager
import logging
import sys
from pathlib import Path

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Импорт внутренних модулей
from app.core.config import get_settings
from app.core.discovery import PluginDiscovery

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- STARTUP ---
    logger.info("🚀 Starting N.I.N.A. AI Cortex...")
    settings = get_settings()
    logger.info(f"⚙️ Config loaded. Sessions root: {settings.nina_environment.sessions_root}")
    
    # Запуск обнаружения плагинов
    discovery = PluginDiscovery()
    discovery.run()
    
    # Сохраняем discovery в state приложения
    app.state.discovery = discovery
    
    yield
    
    # --- SHUTDOWN ---
    logger.info("👋 Shutting down N.I.N.A. AI Cortex...")

app = FastAPI(
    title="N.I.N.A. AI Cortex",
    description="Cognitive Overlay for N.I.N.A. Astrophotography Software",
    version="0.1.0",
    lifespan=lifespan
)

@app.get("/")
async def root():
    return {
        "status": "online",
        "message": "N.I.N.A. AI Cortex is running",
        "discovered_plugins": list(app.state.discovery.discovered_plugins.keys())
    }

@app.get("/api/v1/plugins")
async def get_plugins():
    """Возвращает полную карту обнаруженных плагинов и их настроек."""
    return app.state.discovery.discovered_plugins