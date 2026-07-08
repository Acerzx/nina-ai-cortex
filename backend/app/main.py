"""
N.I.N.A. AI Cortex - Main Application Entry Point
FastAPI сервер, управляющий жизненным циклом всех компонентов Cortex.

УПРОЩЕНО (рефакторинг v3):
- Удалена избыточная безопасность (JWT, API keys, rate limiting)
- Удален Credential Vault (локальное использование не требует шифрования)
- Удален MemoryManagerAgent (не используется)
- Добавлена интеграция с Hybrid LangGraph Orchestrator
- Все endpoints теперь открытые (локальная сеть)
"""

import asyncio
import logging
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from fastapi import (
    FastAPI,
    HTTPException,
    WebSocket,
    WebSocketDisconnect,
    Query,
    Request,
    Response,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

from app.core.config import settings
from app.core.events import event_bus
from app.core.rag_engine import rag_engine
from app.core.ws_broadcast import ws_broadcast_manager
from app.core.metrics import cortex_metrics
from app.ingestion.watchers.manager import WatcherManager
from app.shadow_engine.state_tracker import state_tracker
from app.agents.observatory_state import observatory_state
from app.execution.trigger_emulator import trigger_emulator
from app.execution.global_var_injector import global_var_injector

# AI Agents
from app.agents.orchestrator import orchestrator, OperationMode
from app.agents.watcher_agent import WatcherAgent
from app.agents.guardian_agent import GuardianAgent
from app.agents.diagnostician_agent import DiagnosticianAgent
from app.agents.strategist_agent import StrategistAgent
from app.agents.auditor_agent import AuditorAgent
from app.agents.calibrator_agent import CalibratorAgent
from app.agents.scheduler_agent import SchedulerAgent
from app.agents.copilot_agent import CopilotAgent
from app.agents.hybrid_langgraph_orchestrator import hybrid_orchestrator
from app.core.mode_manager import mode_manager
from app.safety.preflight import preflight_checker
from app.agents.llm_client import llm_client

# Storage
from app.storage.disk_monitor import disk_monitor
from app.storage.decision_audit import decision_audit

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=getattr(logging, settings.logging.level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logging.getLogger("WatcherAgent").setLevel(logging.DEBUG)
logger = logging.getLogger("CortexMain")

# ============================================================================
# GLOBAL INSTANCES
# ============================================================================
watcher_manager = WatcherManager()

# 9 AI-агентов (убран MemoryManager)
watcher_agent = WatcherAgent()
guardian_agent = GuardianAgent()
diagnostician_agent = DiagnosticianAgent()
strategist_agent = StrategistAgent()
auditor_agent = AuditorAgent()
calibrator_agent: Optional[CalibratorAgent] = None
scheduler_agent = SchedulerAgent()
copilot_agent = CopilotAgent()


# ============================================================================
# EVENT BUS METRICS SUBSCRIBERS
# ============================================================================
async def _on_event_bus_event(event_type: str, data: Dict[str, Any]):
    """
    Автоматический сбор метрик из EventBus.
    ИСПРАВЛЕНО: метрики вызываются через inc(**labels), а не .labels().inc()
    """
    await cortex_metrics.events_total.inc(event_type=event_type)


async def _on_decision_made(data: Dict[str, Any]):
    """Сбор метрик о решениях агентов."""
    agent = data.get("agent", "unknown")
    decision_type = data.get("decision_type", "unknown")
    confidence = data.get("confidence", 0.5)
    await cortex_metrics.decisions_total.inc(
        agent=agent, decision_type=decision_type, outcome="pending"
    )
    await cortex_metrics.decision_confidence.observe(confidence, agent=agent)


async def _on_trigger_fired(data: Dict[str, Any]):
    """Сбор метрик о срабатывании триггеров."""
    trigger_name = data.get("trigger", "unknown")
    status = data.get("status", "unknown")
    duration = data.get("duration_seconds", 0.0)
    await cortex_metrics.triggers_fired.inc(trigger_name=trigger_name, status=status)
    if duration > 0:
        await cortex_metrics.trigger_duration.observe(
            duration, trigger_name=trigger_name
        )


async def _on_llm_response(data: Dict[str, Any]):
    """Сбор метрик о LLM запросах."""
    model = data.get("model", "unknown")
    status = data.get("status", "success")
    fallback = "true" if data.get("from_fallback", False) else "false"
    duration = data.get("duration_seconds", 0.0)
    tokens = data.get("tokens_used", 0)
    await cortex_metrics.llm_requests_total.inc(
        model=model, status=status, fallback=fallback
    )
    await cortex_metrics.llm_request_duration.observe(duration, model=model)
    if tokens > 0:
        await cortex_metrics.llm_tokens_used.inc(tokens, model=model)


# ============================================================================
# LIFESPAN (Startup / Shutdown)
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    global calibrator_agent
    logger.info("=" * 70)
    logger.info("🚀 N.I.N.A. AI Cortex v3.0 Starting Up...")
    logger.info("=" * 70)

    try:
        # 1. Запуск Ingestion, Shadow Engine и Execution
        await watcher_manager.start()

        # 2. RAG Engine
        logger.info("📚 Initializing RAG Engine...")
        await rag_engine.initialize()

        # 3. WebSocket Broadcast Manager
        logger.info("🔌 Starting WebSocket Broadcast Manager...")
        await ws_broadcast_manager.start()

        # 4. Mode Manager
        logger.info("🎛️ Starting Mode Manager...")
        await mode_manager.start()

        # 5. LLM Client
        logger.info("🤖 Initializing LLM Client (Ollama connection)...")
        await llm_client.initialize()

        # 6. Calibrator Agent
        calibrator_agent = CalibratorAgent(
            masters_auditor=watcher_manager.masters_auditor
        )

        # 7. Инициализация агентов
        logger.info("🤖 Initializing 9 AI Agents...")
        await watcher_agent.initialize()
        await guardian_agent.initialize()
        await diagnostician_agent.initialize()
        await strategist_agent.initialize()
        await auditor_agent.initialize()
        await calibrator_agent.initialize()
        await scheduler_agent.initialize()
        await copilot_agent.initialize()

        # 8. Регистрация в Orchestrator
        orchestrator.register_agent("Watcher", watcher_agent)
        orchestrator.register_agent("Guardian", guardian_agent)
        orchestrator.register_agent("Diagnostician", diagnostician_agent)
        orchestrator.register_agent("Strategist", strategist_agent)
        orchestrator.register_agent("Auditor", auditor_agent)
        orchestrator.register_agent("Calibrator", calibrator_agent)
        orchestrator.register_agent("Scheduler", scheduler_agent)
        orchestrator.register_agent("Copilot", copilot_agent)

        # 9. Запуск Orchestrator
        await orchestrator.start()

        # 10. Подписка на события для сбора метрик
        logger.info("📊 Subscribing to EventBus for metrics collection...")
        for event_type in [
            "SEQUENCE_STARTED",
            "SEQUENCE_STOPPED",
            "SEQUENCE_ITEM_STARTED",
            "SEQUENCE_ITEM_COMPLETED",
            "NEW_FRAME",
            "ALERT",
            "LOG_EVENT",
            "PROMETHEUS_UPDATE",
            "INFLUXDB_UPDATE",
            "WEATHER_UPDATE",
            "MODE_CHANGED",
            "TRIGGER_FIRED",
            "DECISION_MADE",
            "LLM_RESPONSE",
            "RAG_SEARCH",
            "MASTERS_INDEXED",
        ]:
            event_bus.subscribe(
                event_type, lambda data, et=event_type: _on_event_bus_event(et, data)
            )

        event_bus.subscribe("DECISION_MADE", _on_decision_made)
        event_bus.subscribe("TRIGGER_FIRED", _on_trigger_fired)
        event_bus.subscribe("LLM_RESPONSE", _on_llm_response)

        logger.info("=" * 70)
        logger.info("✅ All AI Agents initialized and registered")
        logger.info("✅ Cortex is fully operational and ready to accept connections.")
        logger.info(f"🌐 API Docs available at: http://localhost:8000/docs")
        logger.info(f"📊 Metrics endpoint: http://localhost:8000/metrics")
        logger.info(
            f"🔌 WebSocket endpoint: ws://localhost:8000{settings.ws_broadcast.path}"
        )
        logger.info("=" * 70)

    except Exception as e:
        logger.critical(f"❌ FATAL: Failed to start Cortex: {e}", exc_info=True)
        raise

    yield  # <-- Приложение работает

    # ========================================================================
    # SHUTDOWN (в обратном порядке)
    # ========================================================================
    logger.info("=" * 70)
    logger.info("🛑 N.I.N.A. AI Cortex Shutting Down...")
    logger.info("=" * 70)

    try:
        # 1. Отписка от событий метрик
        for event_type in [
            "SEQUENCE_STARTED",
            "SEQUENCE_STOPPED",
            "SEQUENCE_ITEM_STARTED",
            "SEQUENCE_ITEM_COMPLETED",
            "NEW_FRAME",
            "ALERT",
            "LOG_EVENT",
            "PROMETHEUS_UPDATE",
            "INFLUXDB_UPDATE",
            "WEATHER_UPDATE",
            "MODE_CHANGED",
            "TRIGGER_FIRED",
            "DECISION_MADE",
            "LLM_RESPONSE",
            "RAG_SEARCH",
            "MASTERS_INDEXED",
        ]:
            try:
                event_bus.unsubscribe(
                    event_type,
                    lambda data, et=event_type: _on_event_bus_event(et, data),
                )
            except Exception:
                pass

        try:
            event_bus.unsubscribe("DECISION_MADE", _on_decision_made)
            event_bus.unsubscribe("TRIGGER_FIRED", _on_trigger_fired)
            event_bus.unsubscribe("LLM_RESPONSE", _on_llm_response)
        except Exception:
            pass

        # 2. Останавливаем агентов
        await orchestrator.stop()
        await copilot_agent.shutdown()
        await scheduler_agent.shutdown()
        if calibrator_agent:
            await calibrator_agent.shutdown()
        await auditor_agent.shutdown()
        await strategist_agent.shutdown()
        await diagnostician_agent.shutdown()
        await guardian_agent.shutdown()
        await watcher_agent.shutdown()

        # 3. Останавливаем менеджеры
        await mode_manager.stop()

        # 4. Закрываем broadcast и RAG
        await ws_broadcast_manager.stop()
        await rag_engine.close()

        # 5. Останавливаем watchers
        await watcher_manager.stop()

        # 6. Закрываем LLM клиент
        await llm_client.close()

        # 7. Даем время на закрытие всех соединений
        await asyncio.sleep(0.5)

        logger.info("✅ Cortex stopped gracefully.")

    except Exception as e:
        logger.error(f"❌ Error during shutdown: {e}", exc_info=True)


# ============================================================================
# FASTAPI APP INITIALIZATION
# ============================================================================
app = FastAPI(
    title="N.I.N.A. AI Cortex API",
    description=(
        "Когнитивная надстройка над N.I.N.A. с Multi-Agent AI архитектурой. "
        "9 AI-агентов, LangGraph координация, RAG-система, Pre-flight Checklist. "
        "Локальное использование без аутентификации."
    ),
    version="3.0.0",
    lifespan=lifespan,
)

# ============================================================================
# CORS MIDDLEWARE
# ============================================================================
if settings.cors.enabled:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors.allowed_origins,
        allow_credentials=settings.cors.allow_credentials,
        allow_methods=settings.cors.allowed_methods,
        allow_headers=settings.cors.allowed_headers,
        max_age=settings.cors.max_age,
    )
    logger.info(
        f"🌐 CORS configured with {len(settings.cors.allowed_origins)} allowed origins"
    )
else:
    logger.warning("⚠️ CORS is disabled in configuration")


# ============================================================================
# METRICS MIDDLEWARE
# ============================================================================
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """Middleware для сбора метрик API запросов."""
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    method = request.method
    path = request.url.path
    status_code = response.status_code

    cortex_metrics.api_requests_total.labels(
        method=method, path=path, status_code=str(status_code)
    ).inc()
    cortex_metrics.api_request_duration.labels(method=method, path=path).observe(
        duration
    )

    return response


# ============================================================================
# PYDANTIC MODELS для запросов
# ============================================================================
class TriggerRequest(BaseModel):
    trigger_name: str
    reason: str = "Manual API Call"


class VariableRequest(BaseModel):
    name: str
    value: Any
    reason: str = "Manual API Call"


class RAGSearchRequest(BaseModel):
    query: str
    top_k: int = 5
    filters: Optional[Dict[str, Any]] = None


# ============================================================================
# SYSTEM ENDPOINTS (public)
# ============================================================================
@app.get("/health", tags=["System"])
async def health_check():
    """Health Check эндпоинт."""
    rag_stats = await rag_engine.get_stats()
    ws_stats = ws_broadcast_manager.get_stats()
    metrics_summary = cortex_metrics.get_summary()
    return {
        "status": "healthy",
        "version": "3.0.0",
        "timestamp": datetime.now().isoformat(),
        "components": {
            "event_bus": event_bus._running,
            "sequence_running": state_tracker.state.is_running,
            "flat_mode": state_tracker.state.is_flat_mode,
            "safety_status": observatory_state.safety_status,
            "rag_engine": rag_stats.get("status", "unknown"),
            "ws_connections": ws_stats.get("total_connections", 0),
            "llm_available": llm_client.is_available(),
            "operation_mode": mode_manager.current_mode.value,
        },
        "metrics": metrics_summary,
        "uptime_seconds": time.time() - cortex_metrics._start_time,
    }


@app.get("/", tags=["System"], include_in_schema=False)
async def root():
    """Корневой эндпоинт — редирект на документацию."""
    return RedirectResponse(url="/docs")


@app.get("/api", tags=["System"], include_in_schema=False)
@app.get("/api/v1", tags=["System"], include_in_schema=False)
async def api_root():
    """Корневой API эндпоинт."""
    return {
        "name": "N.I.N.A. AI Cortex API",
        "version": "3.0.0",
        "documentation": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }


# ============================================================================
# PROMETHEUS /metrics ENDPOINT
# ============================================================================
@app.get("/metrics", tags=["Observability"], include_in_schema=False)
async def prometheus_metrics(request: Request):
    """Prometheus exposition format endpoint."""
    rag_stats = await rag_engine.get_stats()
    ws_stats = ws_broadcast_manager.get_stats()

    # ИСПРАВЛЕНО: set_sync() вместо set() для синхронного контекста
    cortex_metrics.active_ws_connections.set_sync(ws_stats.get("total_connections", 0))
    cortex_metrics.sequence_running.set_sync(1 if state_tracker.state.is_running else 0)
    cortex_metrics.flat_mode_active.set_sync(
        1 if state_tracker.state.is_flat_mode else 0
    )
    cortex_metrics.llm_available.set_sync(
        1 if llm_client.is_available() else 0, model="primary"
    )

    if "points_count" in rag_stats:
        cortex_metrics.rag_documents_total.set_sync(rag_stats["points_count"])

    mode_value = {"manual": 0, "safe": 1, "full_ai": 2, "simulation": 3}.get(
        mode_manager.current_mode.value, -1
    )
    cortex_metrics.operation_mode.set_sync(mode_value)

    safety_value = {"SAFE": 0, "UNSAFE": 1, "UNKNOWN": -1}.get(
        observatory_state.safety_status, -1
    )
    cortex_metrics.safety_status.set_sync(safety_value)

    cortex_metrics.watchers_active.set_sync(len(watcher_manager.watchers))
    cortex_metrics.agents_active.set_sync(len(orchestrator.agents))

    output = cortex_metrics.expose()
    return Response(content=output, media_type="text/plain; version=0.0.4")


# ============================================================================
# WEBSOCKET BROADCAST ENDPOINT
# ============================================================================
@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket, client_id: Optional[str] = Query(None)
):
    """WebSocket endpoint для real-time broadcasting."""
    if not client_id:
        client_id = str(uuid.uuid4())[:8]
    conn = await ws_broadcast_manager.connect(websocket, client_id)
    try:
        while True:
            try:
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
        await ws_broadcast_manager.disconnect(client_id)


@app.get("/api/v1/ws/stats", tags=["WebSocket"])
async def ws_stats():
    """Возвращает статистику WebSocket подключений."""
    return ws_broadcast_manager.get_stats()


# ============================================================================
# SHADOW ENGINE ENDPOINTS
# ============================================================================
@app.get("/api/v1/sequence/shadow", tags=["Shadow Engine"])
async def get_sequence_shadow():
    """Возвращает полный теневой граф секвенсора."""
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
    """Возвращает единое состояние обсерватории."""
    return observatory_state.get_full_state()


@app.get("/api/v1/observatory/session-summary", tags=["AI Agents"])
async def get_session_summary():
    """Возвращает краткую сводку текущей сессии."""
    return observatory_state.get_session_summary()


@app.get("/api/v1/agents/status", tags=["AI Agents"])
async def get_agents_status():
    """Возвращает статус всех AI-агентов."""
    return {
        "orchestrator": orchestrator.get_stats(),
        "mode_manager": mode_manager.get_stats(),
        "agents": {
            "Watcher": watcher_agent.get_stats(),
            "Guardian": guardian_agent.get_stats(),
            "Diagnostician": diagnostician_agent.get_stats(),
            "Strategist": strategist_agent.get_stats(),
            "Auditor": auditor_agent.get_stats(),
            "Calibrator": calibrator_agent.get_stats() if calibrator_agent else {},
            "Scheduler": scheduler_agent.get_stats(),
            "Copilot": copilot_agent.get_stats(),
        },
    }


@app.post("/api/v1/agents/mode", tags=["AI Agents"])
async def set_operation_mode(mode: str):
    """Устанавливает режим работы системы."""
    try:
        operation_mode = OperationMode(mode)
        await mode_manager.set_mode(operation_mode, reason="Manual API call")
        await orchestrator.set_mode(operation_mode)
        return {"status": "success", "mode": mode}
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid mode: {mode}. Valid modes: {[m.value for m in OperationMode]}",
        )


@app.get("/api/v1/agents/decisions", tags=["AI Agents"])
async def get_recent_decisions(limit: int = Query(20, ge=1, le=100)):
    """Возвращает последние решения агентов."""
    return {
        "decisions": orchestrator.get_recent_decisions(limit=limit),
        "total": len(orchestrator._decisions_log),
    }


@app.get("/api/v1/agents/llm-status", tags=["AI Agents"])
async def get_llm_status():
    """Проверяет доступность локального LLM."""
    return {
        "available": llm_client.is_available(),
        "model": settings.ai_settings.primary_model,
        "host": settings.ai_settings.ollama_host,
    }


@app.post("/api/v1/agents/test-llm", tags=["AI Agents"])
async def test_llm_generation(
    prompt: str = Query(..., description="Тестовый промпт"),
    agent: str = Query("Copilot", description="Имя агента"),
):
    """Тестовый эндпоинт для проверки генерации LLM."""
    if not llm_client.is_available():
        raise HTTPException(status_code=503, detail="LLM (Ollama) is not available")
    response = await llm_client.generate(
        agent_name=agent, prompt=prompt, max_tokens=500
    )
    return {"prompt": prompt, "agent": agent, "response": response}


# ============================================================================
# METRICS ENDPOINTS
# ============================================================================
@app.get("/api/v1/metrics", tags=["Metrics"])
async def get_metrics():
    """Возвращает текущие метрики обсерватории."""
    metrics = observatory_state.current_metrics
    weather = observatory_state.weather
    astronomy = observatory_state.astronomy
    trends = {}
    for metric_name in ["hfr", "fwhm", "rms_ra", "rms_dec", "temperature"]:
        trend = observatory_state.get_trend(metric_name, window=10)
        if trend is not None:
            trends[metric_name] = trend
    history_stats = {}
    for metric_name in ["hfr", "fwhm", "rms_ra", "rms_dec"]:
        history_list = getattr(observatory_state.history, metric_name, [])
        if history_list:
            history_stats[metric_name] = {
                "count": len(history_list),
                "min": min(history_list),
                "max": max(history_list),
                "avg": sum(history_list) / len(history_list),
            }
    return {
        "timestamp": datetime.now().isoformat(),
        "metrics": metrics,
        "weather": weather,
        "astronomy": astronomy,
        "trends": trends,
        "history_stats": history_stats,
        "safety_status": observatory_state.safety_status,
        "modes": {
            "flat_mode": observatory_state.is_flat_mode,
            "guiding_active": observatory_state.is_guiding_active,
            "autofocus_running": observatory_state.is_autofocus_running,
        },
    }


@app.get("/api/v1/metrics/history", tags=["Metrics"])
async def get_metrics_history(
    metric: str = Query(..., description="Имя метрики"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Возвращает историю конкретной метрики."""
    history_list = getattr(observatory_state.history, metric, None)
    if history_list is None:
        raise HTTPException(
            status_code=404,
            detail=f"Metric '{metric}' not found.",
        )
    limited_history = (
        history_list[-limit:] if len(history_list) > limit else history_list
    )
    timestamps = []
    now = datetime.now()
    from datetime import timedelta

    for i in range(len(limited_history)):
        timestamp = now - timedelta(seconds=(len(limited_history) - i) * 3)
        timestamps.append(timestamp.isoformat())
    return {
        "metric": metric,
        "count": len(limited_history),
        "timestamps": timestamps,
        "values": limited_history,
        "stats": {
            "min": min(limited_history) if limited_history else None,
            "max": max(limited_history) if limited_history else None,
            "avg": sum(limited_history) / len(limited_history)
            if limited_history
            else None,
            "trend": observatory_state.get_trend(metric, window=10),
        },
    }


# ============================================================================
# EXECUTION LAYER ENDPOINTS
# ============================================================================
@app.post("/api/v1/execution/trigger", tags=["Execution Layer"])
async def fire_trigger(body: TriggerRequest):
    """Ручной вызов триггера через API."""
    logger.info(f"API Request: Fire trigger '{body.trigger_name}'")
    success = await trigger_emulator.fire_trigger(body.trigger_name, body.reason)
    if success:
        observatory_state.log_ai_action(
            "API",
            f"Fire Trigger: {body.trigger_name}",
            body.reason,
            "Success",
        )
        return {"status": "success", "message": f"Trigger {body.trigger_name} fired"}
    else:
        raise HTTPException(
            status_code=400,
            detail="Trigger blocked by HAL, FLAT_MODE or not available",
        )


@app.post("/api/v1/execution/variable", tags=["Execution Layer"])
async def set_variable(body: VariableRequest):
    """Изменение глобальной переменной Sequencer+."""
    logger.info(f"API Request: Set variable '{body.name}' = {body.value}")
    success = await global_var_injector.set_variable(body.name, body.value, body.reason)
    if success:
        observatory_state.log_ai_action(
            "API",
            f"Set Var: {body.name}={body.value}",
            body.reason,
            "Success",
        )
        return {"status": "success", "message": f"Variable {body.name} updated"}
    else:
        raise HTTPException(
            status_code=400,
            detail="Variable change blocked by HAL or critical phase",
        )


# ============================================================================
# RAG ENGINE ENDPOINTS
# ============================================================================
@app.post("/api/v1/rag/search", tags=["RAG Engine"])
async def rag_search(body: RAGSearchRequest):
    """Семантический поиск по базе знаний RAG."""
    logger.info(f"RAG Search: '{body.query}' (top_k={body.top_k})")
    results = await rag_engine.search(
        query=body.query, top_k=body.top_k, filters=body.filters
    )
    return {
        "query": body.query,
        "results_count": len(results),
        "results": results,
    }


@app.get("/api/v1/rag/context", tags=["RAG Engine"])
async def rag_get_context(
    query: str = Query(..., description="Поисковый запрос"),
    max_tokens: int = Query(2000, description="Максимальное количество токенов"),
):
    """Получает контекст для LLM на основе запроса."""
    context = await rag_engine.get_context(query=query, max_tokens=max_tokens)
    return {
        "query": query,
        "context": context,
        "tokens_approx": len(context) // 4,
    }


@app.get("/api/v1/rag/stats", tags=["RAG Engine"])
async def rag_stats():
    """Возвращает статистику RAG-базы знаний."""
    return await rag_engine.get_stats()


# ============================================================================
# DISCOVERY & MASTERS LIBRARY ENDPOINTS
# ============================================================================
@app.get("/api/v1/plugins", tags=["Discovery"])
async def get_discovered_plugins():
    """Возвращает список всех обнаруженных плагинов."""
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


@app.get("/api/v1/masters/catalog", tags=["Masters Library"])
async def get_masters_catalog():
    """Возвращает каталог доступных мастер-кадров."""
    if not watcher_manager.masters_auditor:
        raise HTTPException(
            status_code=503, detail="Masters Auditor not initialized yet"
        )
    return {
        "summary": watcher_manager.masters_auditor.get_summary_by_category(),
        "stats": watcher_manager.masters_auditor.get_stats(),
    }


@app.get("/api/v1/masters/find", tags=["Masters Library"])
async def find_matching_master(
    image_type: str = Query(..., description="Тип кадра: BIAS, DARK, FLAT"),
    temperature: float = Query(..., description="Температура сенсора"),
    exposure: Optional[float] = Query(None),
    gain: Optional[int] = Query(None),
    offset: Optional[int] = Query(None),
    filter_name: Optional[str] = Query(None),
    temp_tolerance: float = Query(2.0),
):
    """Ищет наиболее подходящий мастер-кадр."""
    if not watcher_manager.masters_auditor:
        raise HTTPException(
            status_code=503, detail="Masters Auditor not initialized yet"
        )
    master = watcher_manager.masters_auditor.find_matching_master(
        image_type=image_type,
        temperature=temperature,
        exposure=exposure,
        gain=gain,
        offset=offset,
        filter_name=filter_name,
        temp_tolerance=temp_tolerance,
    )
    if not master:
        raise HTTPException(
            status_code=404,
            detail=f"No matching {image_type} master found for the given parameters",
        )
    return master


# ============================================================================
# SAFETY ENDPOINTS
# ============================================================================
@app.post("/api/v1/safety/preflight", tags=["Safety"])
async def run_preflight_check():
    """Запускает pre-flight проверку перед стартом сессии."""
    report = await preflight_checker.run_all()
    return report


# ============================================================================
# STORAGE ENDPOINTS
# ============================================================================
@app.get("/api/v1/storage/disk-usage", tags=["Storage"])
async def get_disk_usage():
    """Возвращает информацию об использовании дискового пространства."""
    return await disk_monitor.get_stats()


@app.post("/api/v1/storage/cleanup", tags=["Storage"])
async def apply_retention_policy(
    policy_name: str = Query(..., description="Имя политики"),
):
    """Применяет политику удаления старых данных."""
    result = await disk_monitor.apply_retention_policy(policy_name)
    if result:
        return result
    else:
        raise HTTPException(
            status_code=400,
            detail=f"Unknown policy: {policy_name}. "
            f"Available: {list(disk_monitor.policies.keys())}",
        )


# ============================================================================
# DECISION AUDIT TRAIL ENDPOINTS
# ============================================================================
@app.get("/api/v1/audit/decisions", tags=["Decision Audit"])
async def get_audit_decisions(
    agent: Optional[str] = Query(None),
    decision_type: Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """Возвращает историю решений из Decision Audit Trail."""
    records = await decision_audit.get_decisions(
        agent=agent,
        decision_type=decision_type,
        session_id=session_id,
        limit=limit,
        offset=offset,
    )
    return {
        "records": [r.model_dump() for r in records],
        "count": len(records),
    }


@app.get("/api/v1/audit/stats", tags=["Decision Audit"])
async def get_audit_stats():
    """Возвращает статистику Decision Audit Trail."""
    return await decision_audit.get_stats()


# ============================================================================
# SIMULATION MODE ENDPOINTS
# ============================================================================
@app.post("/api/v1/simulation/start", tags=["Simulation"])
async def start_simulation(
    target: str = Query("M31", description="Имя цели"),
    frames: int = Query(10, description="Количество кадров"),
):
    """Запускает симуляцию сессии (Fake NINA)."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.start()
    await fake_nina.start_sequence(target=target, frames=frames)
    await mode_manager.set_mode(
        OperationMode.SIMULATION, reason="Simulation started via API"
    )
    return {
        "status": "success",
        "message": f"Simulation started: {target} ({frames} frames)",
    }


@app.post("/api/v1/simulation/stop", tags=["Simulation"])
async def stop_simulation():
    """Останавливает симуляцию."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.stop_sequence()
    await fake_nina.stop()
    await mode_manager.set_mode(
        OperationMode.FULL_AI, reason="Simulation stopped via API"
    )
    return {"status": "success", "message": "Simulation stopped"}


@app.post("/api/v1/simulation/inject-anomaly", tags=["Simulation"])
async def inject_anomaly(
    anomaly_type: str = Query(..., description="Тип аномалии"),
):
    """Инжектирует аномалию для тестирования."""
    from app.simulation.fake_nina import fake_nina

    valid_types = [
        "hfr_spike",
        "rms_spike",
        "temp_drift",
        "guiding_lost",
        "safety_unsafe",
    ]
    if anomaly_type not in valid_types:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid anomaly type. Valid types: {valid_types}",
        )
    await fake_nina.inject_anomaly(anomaly_type)
    return {"status": "success", "message": f"Anomaly '{anomaly_type}' injected"}


@app.post("/api/v1/simulation/trigger-autofocus", tags=["Simulation"])
async def trigger_autofocus_simulation():
    """Симулирует запуск автофокуса."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.trigger_autofocus()
    return {"status": "success", "message": "Autofocus triggered"}


@app.post("/api/v1/simulation/trigger-meridian-flip", tags=["Simulation"])
async def trigger_meridian_flip_simulation():
    """Симулирует Meridian Flip."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.trigger_meridian_flip()
    return {"status": "success", "message": "Meridian flip triggered"}


@app.post("/api/v1/simulation/reset-cooldowns", tags=["Simulation"])
async def reset_agent_cooldowns():
    """Сбрасывает cooldown всех агентов (для тестирования)."""
    watcher_agent._recent_anomalies.clear()
    if calibrator_agent:
        calibrator_agent._recent_alerts.clear()
    return {
        "status": "success",
        "message": "All agent cooldowns reset",
        "watcher_anomalies_cleared": True,
        "calibrator_alerts_cleared": True,
    }


# ============================================================================
# TRIGGERS ENDPOINTS
# ============================================================================
@app.get("/api/v1/triggers", tags=["Execution Layer"])
async def list_available_triggers():
    """Возвращает список всех доступных триггеров."""
    return trigger_emulator.list_available_triggers()


@app.get("/api/v1/triggers/{trigger_name}", tags=["Execution Layer"])
async def get_trigger_info(trigger_name: str):
    """Возвращает информацию о конкретном триггере."""
    triggers = trigger_emulator.list_available_triggers()
    if trigger_name not in triggers:
        raise HTTPException(
            status_code=404,
            detail=f"Trigger '{trigger_name}' not found. "
            f"Available: {', '.join(sorted(triggers.keys()))}",
        )
    return triggers[trigger_name]


# ============================================================================
# LANGGRAPH ORCHESTRATOR ENDPOINTS
# ============================================================================
@app.get("/api/v1/langgraph/workflows", tags=["LangGraph"])
async def list_langgraph_workflows():
    """Возвращает список активных LangGraph workflows."""
    active = hybrid_orchestrator.list_active_workflows()
    workflows = []
    for wf_id in active:
        state = hybrid_orchestrator.get_workflow_status(wf_id)
        if state:
            workflows.append(
                {
                    "workflow_id": wf_id,
                    "type": state["workflow_type"],
                    "status": state["status"],
                    "created_at": state["created_at"],
                }
            )
    return {
        "active_workflows": len(active),
        "workflows": workflows,
    }


@app.get("/api/v1/langgraph/workflow/{workflow_id}", tags=["LangGraph"])
async def get_langgraph_workflow(workflow_id: str):
    """Возвращает статус конкретного LangGraph workflow."""
    state = hybrid_orchestrator.get_workflow_status(workflow_id)
    if not state:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow '{workflow_id}' not found",
        )
    return state


@app.post("/api/v1/langgraph/start", tags=["LangGraph"])
async def start_langgraph_workflow(
    workflow_type: str = Query(
        ..., description="Тип workflow: diagnostic, post_mortem, adaptive"
    ),
    trigger_event: str = Query("manual", description="Событие-триггер"),
):
    """Запускает новый LangGraph workflow."""
    from app.agents.hybrid_langgraph_orchestrator import WorkflowType

    try:
        wf_type = WorkflowType(workflow_type)
    except ValueError:
        valid_types = [t.value for t in WorkflowType]
        raise HTTPException(
            status_code=400,
            detail=f"Invalid workflow type: {workflow_type}. Valid types: {valid_types}",
        )

    workflow_id = await hybrid_orchestrator.start_workflow(
        workflow_type=wf_type,
        trigger_event={"type": trigger_event, "source": "api"},
        context={"source": "manual_api_call"},
    )

    return {
        "status": "success",
        "workflow_id": workflow_id,
        "workflow_type": workflow_type,
        "message": f"Workflow {workflow_id} started",
    }
