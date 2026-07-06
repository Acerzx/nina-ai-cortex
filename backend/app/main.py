"""
N.I.N.A. AI Cortex - Main Application Entry Point
FastAPI сервер, управляющий жизненным циклом всех компонентов Cortex.
"""

import logging
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, Optional, List

from app.core.config import settings
from app.core.events import event_bus
from app.core.rag_engine import rag_engine
from app.core.ws_broadcast import ws_broadcast_manager
from app.ingestion.watchers.manager import WatcherManager
from app.shadow_engine.state_tracker import state_tracker
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
        # Включает: парсинг Sequence.json, запуск всех watchers, pollers, WebSocket client
        await watcher_manager.start()

        # 2. Инициализация RAG Engine (Qdrant + Ollama embeddings)
        logger.info("Initializing RAG Engine...")
        await rag_engine.initialize()

        # 3. Запуск WebSocket Broadcast Manager для Frontend
        logger.info("Starting WebSocket Broadcast Manager...")
        await ws_broadcast_manager.start()

        logger.info("✅ Cortex is fully operational and ready to accept connections.")
        logger.info(f"🌐 API Docs available at: http://localhost:8000/docs")
        logger.info(
            f"🔌 WebSocket endpoint: ws://localhost:8000{settings.ws_broadcast.path}"
        )

    except Exception as e:
        logger.critical(f"❌ FATAL: Failed to start Cortex: {e}", exc_info=True)
        raise

    yield  # <-- Приложение работает здесь (обрабатывает HTTP/WS запросы)

    # Shutdown
    logger.info("=" * 70)
    logger.info("🛑 N.I.N.A. AI Cortex Shutting Down...")
    logger.info("=" * 70)

    try:
        # Остановка в обратном порядке
        await ws_broadcast_manager.stop()
        await rag_engine.close()
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
    allow_origins=["*"],  # В production ограничить доменом фронтенда
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ============================================================================
# SYSTEM ENDPOINTS
# ============================================================================


@app.get("/health", tags=["System"])
async def health_check():
    """
    Health Check эндпоинт (Устранение Упрощения #46).
    Проверяет статус всех критических компонентов ядра.
    """
    rag_stats = await rag_engine.get_stats()
    ws_stats = ws_broadcast_manager.get_stats()

    return {
        "status": "healthy",
        "version": "2.0.0",
        "components": {
            "event_bus": event_bus._running,
            "sequence_running": state_tracker.state.is_running,
            "flat_mode": state_tracker.state.is_flat_mode,
            "safety_status": observatory_state.safety_status,
            "rag_engine": rag_stats.get("status", "unknown"),
            "ws_connections": ws_stats.get("total_connections", 0),
        },
    }


# ============================================================================
# WEBSOCKET BROADCAST ENDPOINT
# ============================================================================


@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket, client_id: Optional[str] = Query(None)
):
    """
    WebSocket endpoint для real-time broadcasting событий на Frontend.
    Устраняет Упрощение #30.

    Клиент может:
    - Подключиться и получать все события (по умолчанию)
    - Подписаться на конкретные каналы (sequence, metrics, alerts, etc.)
    - Отправлять команды (subscribe, ping)
    """
    # Генерируем уникальный client_id если не предоставлен
    if not client_id:
        client_id = str(uuid.uuid4())[:8]

    # Регистрируем подключение
    conn = await ws_broadcast_manager.connect(websocket, client_id)

    try:
        # Основной цикл обработки сообщений от клиента
        while True:
            try:
                # Ждем сообщения от клиента
                message = await websocket.receive_json()
                await ws_broadcast_manager.handle_client_message(client_id, message)
            except WebSocketDisconnect:
                break
            except Exception as e:
                logger.debug(f"Error processing client message: {e}")
                break

    except WebSocketDisconnect:
        pass
    finally:
        # Удаляем подключение при разрыве
        await ws_broadcast_manager.disconnect(client_id)


# ============================================================================
# SHADOW ENGINE ENDPOINTS
# ============================================================================


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


# ============================================================================
# AI AGENTS ENDPOINTS
# ============================================================================


@app.get("/api/v1/observatory/state", tags=["AI Agents"])
async def get_observatory_full_state():
    """
    Возвращает единое состояние обсерватории (ObservatoryState).
    Используется Frontend'ом (Pinia stores) и LangGraph агентами.
    """
    return observatory_state.get_full_state()


# ============================================================================
# EXECUTION LAYER ENDPOINTS
# ============================================================================


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


# ============================================================================
# RAG ENGINE ENDPOINTS
# ============================================================================


class RAGSearchRequest(BaseModel):
    query: str
    top_k: int = 5
    filters: Optional[Dict[str, Any]] = None


@app.post("/api/v1/rag/search", tags=["RAG Engine"])
async def rag_search(request: RAGSearchRequest):
    """
    Семантический поиск по базе знаний RAG.
    Используется для тестирования и отладки RAG-системы.
    """
    logger.info(f"RAG Search: '{request.query}' (top_k={request.top_k})")

    results = await rag_engine.search(
        query=request.query, top_k=request.top_k, filters=request.filters
    )

    return {"query": request.query, "results_count": len(results), "results": results}


@app.get("/api/v1/rag/context", tags=["RAG Engine"])
async def rag_get_context(
    query: str = Query(..., description="Поисковый запрос"),
    max_tokens: int = Query(
        2000, description="Максимальное количество токенов в контексте"
    ),
):
    """
    Получает контекст для LLM на основе запроса.
    Используется AI-агентами для принятия решений.
    """
    context = await rag_engine.get_context(query=query, max_tokens=max_tokens)
    return {
        "query": query,
        "context": context,
        "tokens_approx": len(context) // 4,  # Примерная оценка токенов
    }


@app.get("/api/v1/rag/stats", tags=["RAG Engine"])
async def rag_stats():
    """Возвращает статистику RAG-базы знаний."""
    stats = await rag_engine.get_stats()
    return stats


# ============================================================================
# DISCOVERY ENDPOINTS
# ============================================================================


@app.get("/api/v1/plugins", tags=["Discovery"])
async def get_discovered_plugins():
    """Возвращает список всех обнаруженных плагинов из Capability Registry."""
    registry = watcher_manager.registry
    if not registry:
        raise HTTPException(
            status_code=503, detail="Capability Registry not initialized"
        )

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


# ============================================================================
# WEBSOCKET BROADCAST STATS
# ============================================================================


@app.get("/api/v1/ws/stats", tags=["WebSocket"])
async def ws_stats():
    """Возвращает статистику WebSocket подключений."""
    return ws_broadcast_manager.get_stats()
