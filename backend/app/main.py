"""
N.I.N.A. AI Cortex - Main Application Entry Point
FastAPI сервер, управляющий жизненным циклом всех компонентов Cortex.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional

from app.core.config import settings
from app.core.events import event_bus
from app.ingestion.watchers.manager import WatcherManager
from app.shadow_engine.state_tracker import state_tracker
from app.shadow_engine.sequence_parser import SequenceParser
from app.agents.observatory_state import observatory_state
from app.execution.trigger_emulator import trigger_emulator
from app.execution.global_var_injector import global_var_injector

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=getattr(logging, settings.logging.level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("CortexMain")

# Глобальный менеджер вотчеров (DI Hub)
watcher_manager = WatcherManager()


# ============================================================================
# LIFESPAN (Startup / Shutdown)
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения.
    Запускает все фоновые задачи, вотчеры, WebSocket клиенты и AI-агентов.
    """
    logger.info("=" * 70)
    logger.info("🚀 N.I.N.A. AI Cortex v2.0 Starting Up...")
    logger.info("=" * 70)

    try:
        # 1. Запуск всех компонентов Ingestion, Shadow Engine и Execution
        await watcher_manager.start()

        # 2. Принудительный парсинг Sequence.json (если еще не спарсен в manager)
        # Это нужно, чтобы API сразу мог отдать теневой граф
        if not state_tracker._shadow_graph:
            logger.info("Parsing Sequence.json for Shadow Engine...")
            parser = SequenceParser()
            graph_data = parser.parse()
            state_tracker.set_shadow_graph(graph_data)

        logger.info("✅ Cortex is fully operational and ready to accept connections.")
        logger.info(f"🌐 API Docs available at: http://localhost:8000/docs")

    except Exception as e:
        logger.critical(f"❌ FATAL: Failed to start Cortex: {e}", exc_info=True)
        raise

    yield  # <-- Приложение работает здесь (обрабатывает HTTP/WS запросы)

    # Shutdown
    logger.info("=" * 70)
    logger.info("🛑 N.I.N.A. AI Cortex Shutting Down...")
    logger.info("=" * 70)

    try:
        await watcher_manager.stop()
        logger.info("✅ Cortex stopped gracefully.")
    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}", exc_info=True)


# ============================================================================
# FASTAPI APP INITIALIZATION
# ============================================================================
app = FastAPI(
    title="N.I.N.A. AI Cortex API",
    description="Когнитивная надстройка над N.I.N.A. с Multi-Agent AI архитектурой",
    version="2.0.0",
    lifespan=lifespan,
)

# CORS (для Frontend на Vue 3 / Nuxt)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "*"
    ],  # В production ограничить доменом фронтенда (например, "http://localhost:3000")
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# API ENDPOINTS
# ============================================================================


@app.get("/health", tags=["System"])
async def health_check():
    """
    Health Check эндпоинт (Устранение Упрощения #46).
    Проверяет статус всех критических компонентов ядра.
    """
    return {
        "status": "healthy",
        "version": "2.0.0",
        "components": {
            "event_bus": event_bus._running,
            "sequence_running": state_tracker.state.is_running,
            "flat_mode": state_tracker.state.is_flat_mode,
            "safety_status": observatory_state.safety_status,
        },
    }


@app.get("/api/v1/sequence/shadow", tags=["Shadow Engine"])
async def get_sequence_shadow():
    """Возвращает полный теневой граф секвенсора (DAG)."""
    if not state_tracker._shadow_graph:
        raise HTTPException(status_code=404, detail="Sequence shadow graph not loaded")
    return state_tracker._shadow_graph


@app.get("/api/v1/sequence/state", tags=["Shadow Engine"])
async def get_sequence_state():
    """Возвращает текущее состояние выполнения секвенсора."""
    return state_tracker.get_state()


@app.get("/api/v1/observatory/state", tags=["AI Agents"])
async def get_observatory_full_state():
    """
    Возвращает единое состояние обсерватории (ObservatoryState).
    Используется Frontend'ом (Pinia stores) и LangGraph агентами.
    """
    return observatory_state.get_full_state()


class TriggerRequest(BaseModel):
    trigger_name: str
    reason: str = "Manual API Call"


@app.post("/api/v1/execution/trigger", tags=["Execution Layer"])
async def fire_trigger(request: TriggerRequest):
    """
    Ручной вызов триггера через API (для тестов и UI).
    Проходит через Trigger Emulator и HAL.
    """
    logger.info(f"API Request: Fire trigger '{request.trigger_name}'")
    success = await trigger_emulator.fire_trigger(request.trigger_name, request.reason)

    if success:
        observatory_state.log_ai_action(
            "API", f"Fire Trigger: {request.trigger_name}", request.reason, "Success"
        )
        return {"status": "success", "message": f"Trigger {request.trigger_name} fired"}
    else:
        raise HTTPException(
            status_code=400, detail="Trigger blocked by HAL, FLAT_MODE or not available"
        )


class VariableRequest(BaseModel):
    name: str
    value: Any
    reason: str = "Manual API Call"


@app.post("/api/v1/execution/variable", tags=["Execution Layer"])
async def set_variable(request: VariableRequest):
    """Изменение глобальной переменной Sequencer+ через API."""
    logger.info(f"API Request: Set variable '{request.name}' = {request.value}")
    success = await global_var_injector.set_variable(
        request.name, request.value, request.reason
    )

    if success:
        observatory_state.log_ai_action(
            "API", f"Set Var: {request.name}={request.value}", request.reason, "Success"
        )
        return {"status": "success", "message": f"Variable {request.name} updated"}
    else:
        raise HTTPException(
            status_code=400, detail="Variable change blocked by HAL or critical phase"
        )


@app.get("/api/v1/plugins", tags=["Discovery"])
async def get_discovered_plugins():
    """Возвращает список всех обнаруженных плагинов из Capability Registry."""
    # Локальный импорт для использования DI инстанса из WatcherManager
    registry = watcher_manager.registry
    if not registry:
        raise HTTPException(
            status_code=503, detail="Capability Registry not initialized"
        )

    # Возвращаем сводку по плагинам, не раскрывая все чувствительные пути
    plugins_summary = {}
    for guid, plugin_settings in registry._registry.items():
        plugins_summary[guid] = {
            "settings_count": len(plugin_settings),
            "has_paths": any(
                isinstance(v, str) and ("\\" in str(v) or "/" in str(v))
                for v in plugin_settings.values()
            ),
        }

    return {"total_plugins": len(plugins_summary), "plugins": plugins_summary}
