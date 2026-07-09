"""
N.I.N.A. AI Cortex - Main Application Entry Point
FastAPI сервер, управляющий жизненным циклом всех компонентов Cortex.

ИСПРАВЛЕНО (рефакторинг v3):
- Удалены: JWT-аутентификация, rate limiting (slowapi), избыточная безопасность
- Удален Credential Vault (локальное использование не требует шифрования)
- Удален MemoryManagerAgent (не используется)
- Добавлена интеграция с Hybrid LangGraph Orchestrator
- Все endpoints открытые (локальная сеть)
- Метрики используют inc_sync/observe_sync вместо .labels().inc()
"""

import asyncio
import logging
import os
import time
import uuid
from app.core.background_tasks import background_tasks
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
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
from app.core.mode_manager import mode_manager
from app.safety.preflight import preflight_checker
from app.agents.llm_client import llm_client

# НОВОЕ: Hybrid LangGraph Orchestrator
from app.agents.hybrid_langgraph_orchestrator import (
    hybrid_orchestrator,
    WorkflowType,
    WorkflowStatus,
)

# Storage
from app.storage.disk_monitor import disk_monitor
from app.storage.decision_audit import decision_audit

# НОВОЕ (v4.0): RAG Updater для автообновления базы знаний
from app.core.rag_updater import rag_updater

# НОВОЕ (v4.0): Decision Analyzer для статистического анализа решений
from app.analytics.decision_analyzer import decision_analyzer

# НОВОЕ (v4.0): Sessions Metadata для хранения всех данных сессий
from app.storage.sessions_metadata import sessions_metadata

# НОВОЕ (v4.0): Predictive HAL для предсказательной безопасности
from app.execution.predictive_hal import predictive_hal

# НОВОЕ (v4.0): Shadow Visualizer для Mermaid экспорта
from app.shadow_engine.shadow_visualizer import shadow_visualizer

# НОВОЕ (v4.0): Metrics Source Monitor
from app.core.metrics_source_monitor import metrics_source_monitor

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
# EVENT BUS METRICS SUBSCRIBERS (audit 11.2)
# ============================================================================
async def _on_event_bus_event(event_type: str, data: Dict[str, Any]):
    """
    Автоматический сбор метрик из EventBus.
    ИСПРАВЛЕНО: метрики вызываются через inc_sync/observe_sync вместо .labels().inc()
    """
    cortex_metrics.events_total.inc_sync(event_type=event_type)


async def _on_decision_made(data: Dict[str, Any]):
    """Сбор метрик о решениях агентов."""
    agent = data.get("agent", "unknown")
    decision_type = data.get("decision_type", "unknown")
    confidence = data.get("confidence", 0.5)
    cortex_metrics.decisions_total.inc_sync(
        agent=agent, decision_type=decision_type, outcome="pending"
    )
    cortex_metrics.decision_confidence.observe_sync(confidence, agent=agent)


async def _on_trigger_fired(data: Dict[str, Any]):
    """Сбор метрик о срабатывании триггеров."""
    trigger_name = data.get("trigger", "unknown")
    status = data.get("status", "unknown")
    duration = data.get("duration_seconds", 0.0)
    cortex_metrics.triggers_fired.inc_sync(trigger_name=trigger_name, status=status)
    if duration > 0:
        cortex_metrics.trigger_duration.observe_sync(
            duration, trigger_name=trigger_name
        )


async def _on_llm_response(data: Dict[str, Any]):
    """Сбор метрик о LLM запросах."""
    model = data.get("model", "unknown")
    status = data.get("status", "success")
    fallback = "true" if data.get("from_fallback", False) else "false"
    duration = data.get("duration_seconds", 0.0)
    tokens = data.get("tokens_used", 0)
    cortex_metrics.llm_requests_total.inc_sync(
        model=model, status=status, fallback=fallback
    )
    cortex_metrics.llm_request_duration.observe_sync(duration, model=model)
    if tokens > 0:
        cortex_metrics.llm_tokens_used.inc_sync(tokens, model=model)


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

        # 10. ИСПРАВЛЕНО (audit 11.2): Подписка на события для сбора метрик

        logger.info("📊 Subscribing to EventBus for metrics collection...")

        # ИСПРАВЛЕНО: Храним ссылки на все обработчики
        _event_handlers: Dict[str, Callable] = {}

        # Подписываемся на все события для общего счётчика
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
            # ИСПРАВЛЕНО: Сохраняем ссылку на обработчик
            async def _handler(data: Dict[str, Any], et: str = event_type) -> None:
                await _on_event_bus_event(et, data)

            _event_handlers[event_type] = _handler
            event_bus.subscribe(event_type, _handler)

        # Специальные подписки для детальных метрик
        _event_handlers["DECISION_MADE"] = _on_decision_made
        _event_handlers["TRIGGER_FIRED"] = _on_trigger_fired
        _event_handlers["LLM_RESPONSE"] = _on_llm_response

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
        # 8. Останавливаем Background Task Manager (v4.0)
        await background_tasks.stop()

        # 11. Background Task Manager (v4.0)
        logger.info("⏰ Initializing Background Task Manager...")
        await background_tasks.start()

        # Регистрируем фоновые задачи
        # 11.1. Автоочистка Decision Audit Trail
        if settings.decision_audit.auto_cleanup_enabled:
            async def decision_audit_cleanup_task():
                """Периодическая очистка старых решений."""
                result = await decision_audit.cleanup_old_decisions()
                if result["deleted_by_age"] > 0 or result["deleted_by_count"] > 0:
                    logger.info(f"🗑️ Auto-cleanup: {result}")

            background_tasks.register(
                name="decision_audit_cleanup",
                coro=decision_audit_cleanup_task,
                interval_seconds=settings.decision_audit.auto_cleanup_interval_hours * 3600,
                enabled=True,
                description="Auto-cleanup old decisions based on retention policy",
            )

        # 11.2. Автоочистка Disk Monitor (retention policies)
        async def disk_retention_task():
            """Периодическое применение retention policy."""
            from app.storage.disk_monitor import disk_monitor
            result = await disk_monitor.apply_retention_policy("keep_last_30_days")
            if result and result.sessions_deleted > 0:
                logger.info(f"🗑️ Auto-retention: {result.sessions_deleted} sessions deleted")

        # Читаем интервал из storage settings (или дефолт 24 часа)
        storage_cfg = settings.thresholds.storage
        retention_interval_hours = getattr(storage_cfg, "retention_cleanup_interval_hours", 24)

        background_tasks.register(
            name="disk_retention",
            coro=disk_retention_task,
            interval_seconds=retention_interval_hours * 3600,
            enabled=True,
            description="Auto-apply retention policies for old sessions",
        )
    

        # 11.4. RAG автообновление (v4.0 — идея 1.2)
        rag_auto_enabled = False
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                rag_ff = getattr(ff, "rag", None)
                if rag_ff:
                    rag_auto_enabled = getattr(rag_ff, "auto_update_enabled", False)
        except Exception:
            pass
        
        if rag_auto_enabled:
            background_tasks.register(
                name="rag_auto_update",
                coro=rag_updater.update,
                interval_seconds=rag_updater._check_interval_hours * 3600,
                enabled=True,
                description="Auto-update RAG with new sessions and documentation",
            )
            logger.info(
                f"   ✅ RAG auto-update registered "
                f"(every {rag_updater._check_interval_hours}h)"
            )
        else:
            logger.info("   ⏭️ RAG auto-update disabled (feature flag off)")



        # 11.3. RAG автообновление (feature flag)
        if getattr(getattr(settings, "feature_flags", None), "rag", None) and \
        getattr(settings.feature_flags.rag, "auto_update_enabled", False):
            async def rag_auto_update_task():
                """Периодическое обновление RAG базы."""
                # TODO: Реализация в следующем этапе
                logger.debug("🔄 RAG auto-update check (stub)")

            background_tasks.register(
                name="rag_auto_update",
                coro=rag_auto_update_task,
                interval_seconds=6 * 3600,  # каждые 6 часов
                enabled=True,
                description="Auto-update RAG with new documentation and sessions",
            )

        logger.info(f"   ✅ {len(background_tasks._tasks)} background tasks registered")

        # 11.5. Predictive HAL (v4.0 — идея 3)
        predictive_enabled = False
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                hal_ff = getattr(ff, "hal", None)
                if hal_ff:
                    predictive_enabled = getattr(hal_ff, "predictive_enabled", False)
        except Exception:
            pass
        
        if predictive_enabled:
            async def predictive_hal_check_task():
                """Периодическая проверка Predictive HAL."""
                await predictive_hal.check_all()
            
            background_tasks.register(
                name="predictive_hal_check",
                coro=predictive_hal_check_task,
                interval_seconds=30.0,  # каждые 30 секунд
                enabled=True,
                description="Predictive HAL: analyze trends for proactive safety",
            )
            logger.info("   ✅ Predictive HAL registered (every 30s)")
        else:
            logger.info("   ⏭️ Predictive HAL disabled (feature flag off)")


        # 11.6. Metrics Source Monitor (v4.0 — идея 6)
        metrics_auto_enabled = False
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                metrics_ff = getattr(ff, "metrics", None)
                if metrics_ff:
                    metrics_auto_enabled = getattr(metrics_ff, "auto_source_selection", True)
        except Exception:
            pass
        
        if metrics_auto_enabled:
            async def metrics_source_check_task():
                """Периодическая проверка качества источников."""
                await metrics_source_monitor.check_and_switch()
            
            background_tasks.register(
                name="metrics_source_monitor",
                coro=metrics_source_check_task,
                interval_seconds=60.0,  # каждую минуту
                enabled=True,
                description="Monitor metrics sources quality and auto-switch",
            )
            logger.info("   ✅ Metrics Source Monitor registered (every 60s)")
        else:
            logger.info("   ⏭️ Metrics Source Monitor disabled (feature flag off)")

    # ========================================================================
    # SHUTDOWN (в обратном порядке)
    # ========================================================================
    logger.info("=" * 70)
    logger.info("🛑 N.I.N.A. AI Cortex Shutting Down...")
    logger.info("=" * 70)

    try:



        # 1. ИСПРАВЛЕНО (проблема #1): Корректная отписка с использованием сохранённых ссылок
        for event_type, handler in _event_handlers.items():
            try:
                event_bus.unsubscribe(event_type, handler)
            except Exception as e:
                logger.debug(f"Failed to unsubscribe from {event_type}: {e}")
"""





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
        "9 AI-агентов, LangGraph координация, RAG-система, Pre-flight Checklist, "
        "Simulation Mode. Локальное использование без аутентификации."
    ),
    version="3.0.0",
    lifespan=lifespan,
)

# ============================================================================
# CORS MIDDLEWARE — ИСПРАВЛЕНО (audit F2)
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
# METRICS MIDDLEWARE (audit 11.2)
# ============================================================================
@app.middleware("http")
async def metrics_middleware(request: Request, call_next):
    """
    Middleware для сбора метрик API запросов.
    ИСПРАВЛЕНО: используем inc_sync/observe_sync вместо .labels().inc()
    """
    start_time = time.time()

    # Выполняем запрос
    response = await call_next(request)

    # Считаем метрики
    duration = time.time() - start_time
    method = request.method
    path = request.url.path
    status_code = response.status_code

    # Обновляем метрики
    cortex_metrics.api_requests_total.inc_sync(
        method=method, path=path, status_code=str(status_code)
    )
    cortex_metrics.api_request_duration.observe_sync(duration, method=method, path=path)

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
async def health_check(request: Request):
    """
    Health Check эндпоинт (public).
    Проверяет статус всех критических компонентов ядра.
    ИСПРАВЛЕНО (audit 11.2): Добавлены метрики компонентов.
    """
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
    """Корневой API эндпоинт — список всех доступных endpoints."""
    return {
        "name": "N.I.N.A. AI Cortex API",
        "version": "3.0.0",
        "documentation": "/docs",
        "health": "/health",
        "metrics": "/metrics",
    }


# ============================================================================
# PROMETHEUS /metrics ENDPOINT — ИСПРАВЛЕНО (audit 11.2)
# ============================================================================
@app.get("/metrics", tags=["Observability"], include_in_schema=False)
async def prometheus_metrics(request: Request):
    """
    Prometheus exposition format endpoint.
    Экспортирует метрики самого Cortex для мониторинга.
    ИСПРАВЛЕНО (audit 11.2): Использует cortex_metrics.expose() для
    генерации полного набора метрик в Prometheus формате.
    """
    # Обновляем динамические метрики перед экспортом
    rag_stats = await rag_engine.get_stats()
    ws_stats = ws_broadcast_manager.get_stats()

    # Обновляем gauge метрики
    cortex_metrics.active_ws_connections.set_sync(ws_stats.get("total_connections", 0))
    cortex_metrics.sequence_running.set_sync(1 if state_tracker.state.is_running else 0)
    cortex_metrics.flat_mode_active.set_sync(
        1 if state_tracker.state.is_flat_mode else 0
    )
    cortex_metrics.llm_available.set_sync(
        1 if llm_client.is_available() else 0, model="primary"
    )

    # Обновляем RAG метрики
    if "points_count" in rag_stats:
        cortex_metrics.rag_documents_total.set_sync(rag_stats["points_count"])

    # Обновляем operation_mode
    mode_value = {"manual": 0, "safe": 1, "full_ai": 2, "simulation": 3}.get(
        mode_manager.current_mode.value, -1
    )
    cortex_metrics.operation_mode.set_sync(mode_value)

    # Обновляем safety_status
    safety_value = {"SAFE": 0, "UNSAFE": 1, "UNKNOWN": -1}.get(
        observatory_state.safety_status, -1
    )
    cortex_metrics.safety_status.set_sync(safety_value)

    # Обновляем количество активных watchers
    cortex_metrics.watchers_active.set_sync(len(watcher_manager.watchers))

    # Обновляем количество активных агентов
    cortex_metrics.agents_active.set_sync(len(orchestrator.agents))

    # Генерируем Prometheus exposition
    output = cortex_metrics.expose()
    return Response(content=output, media_type="text/plain; version=0.0.4")


# ============================================================================
# WEBSOCKET BROADCAST ENDPOINT (public, auth handled inside)
# ============================================================================
@app.websocket("/ws")
async def websocket_endpoint(
    websocket: WebSocket, client_id: Optional[str] = Query(None)
):
    """WebSocket endpoint для real-time broadcasting событий на Frontend."""
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
async def ws_stats(request: Request):
    """Возвращает статистику WebSocket подключений."""
    return ws_broadcast_manager.get_stats()


# ============================================================================
# SHADOW ENGINE ENDPOINTS
# ============================================================================
@app.get("/api/v1/sequence/shadow", tags=["Shadow Engine"])
async def get_sequence_shadow(request: Request):
    """Возвращает полный теневой граф секвенсора (DAG)."""
    if not state_tracker._shadow_graph:
        raise HTTPException(status_code=404, detail="Sequence shadow graph not loaded")
    return state_tracker._shadow_graph


@app.get("/api/v1/sequence/state", tags=["Shadow Engine"])
async def get_sequence_state(request: Request):
    """Возвращает текущее состояние выполнения секвенсора."""
    return state_tracker.get_state()


# ============================================================================
# AI AGENTS ENDPOINTS
# ============================================================================
@app.get("/api/v1/observatory/state", tags=["AI Agents"])
async def get_observatory_full_state(request: Request):
    """Возвращает единое состояние обсерватории (ObservatoryState)."""
    return observatory_state.get_full_state()


@app.get("/api/v1/observatory/session-summary", tags=["AI Agents"])
async def get_session_summary(request: Request):
    \"\"\"Возвращает краткую сводку текущей сессии для LLM контекста.\"\"\"
    # ИСПРАВЛЕНО (проблема #4): get_session_summary удалён, используем get_full_state
    full_state = observatory_state.get_full_state()
    
    # Формируем краткую сводку из полного состояния
    summary = {
        "metrics": full_state.get("metrics", {}),
        "weather": full_state.get("weather", {}),
        "astronomy": full_state.get("astronomy", {}),
        "safety": full_state.get("safety"),
        "sequence": full_state.get("sequence", {}),
        "active_alerts_count": len(full_state.get("active_alerts", [])),
        "targets_count": len(full_state.get("targets", [])),
    }
    
    return summary


@app.get("/api/v1/agents/status", tags=["AI Agents"])
async def get_agents_status(request: Request):
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
async def set_operation_mode(
    request: Request,
    mode: str,
):
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
async def get_recent_decisions(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
):
    """Возвращает последние решения агентов из Decision Audit Trail."""
    return {
        "decisions": orchestrator.get_recent_decisions(limit=limit),
        "total": len(orchestrator._decisions_log),
    }


@app.get("/api/v1/agents/llm-status", tags=["AI Agents"])
async def get_llm_status(request: Request):
    """Проверяет доступность локального LLM (Ollama)."""
    return {
        "available": llm_client.is_available(),
        "model": settings.ai_settings.primary_model,
        "host": settings.ai_settings.ollama_host,
    }


@app.post("/api/v1/agents/test-llm", tags=["AI Agents"])
async def test_llm_generation(
    request: Request,
    prompt: str = Query(..., description="Тестовый промпт"),
    agent: str = Query("Copilot", description="Имя агента для системного промпта"),
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
async def get_metrics(request: Request):
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
    request: Request,
    metric: str = Query(..., description="Имя метрики"),
    limit: int = Query(100, ge=1, le=1000),
):
    """Возвращает историю конкретной метрики."""
    history_list = getattr(observatory_state.history, metric, None)
    if history_list is None:
        raise HTTPException(
            status_code=404,
            detail=f"Metric '{metric}' not found. Available: hfr, fwhm, rms_ra, rms_dec, temperature, wind_speed, humidity",
        )

    limited_history = (
        history_list[-limit:] if len(history_list) > limit else history_list
    )

    timestamps = []
    now = datetime.now()
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
async def fire_trigger(
    request: Request,
    body: TriggerRequest,
):
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
async def set_variable(
    request: Request,
    body: VariableRequest,
):
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
async def rag_search(
    request: Request,
    body: RAGSearchRequest,
):
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
    request: Request,
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
async def rag_stats(request: Request):
    """Возвращает статистику RAG-базы знаний."""
    return await rag_engine.get_stats()


# ============================================================================
# DISCOVERY & MASTERS LIBRARY ENDPOINTS
# ============================================================================
@app.get("/api/v1/plugins", tags=["Discovery"])
async def get_discovered_plugins(request: Request):
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


@app.get("/api/v1/masters/catalog", tags=["Masters Library"])
async def get_masters_catalog(request: Request):
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
    request: Request,
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
async def run_preflight_check(request: Request):
    """Запускает pre-flight проверку перед стартом сессии."""
    report = await preflight_checker.run_all()
    return report


# ============================================================================
# STORAGE ENDPOINTS
# ============================================================================
@app.get("/api/v1/storage/disk-usage", tags=["Storage"])
async def get_disk_usage(request: Request):
    """Возвращает информацию об использовании дискового пространства."""
    return await disk_monitor.get_stats()


@app.post("/api/v1/storage/cleanup", tags=["Storage"])
async def apply_retention_policy(
    request: Request,
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
    request: Request,
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
async def get_audit_stats(request: Request):
    """Возвращает статистику Decision Audit Trail."""
    return await decision_audit.get_stats()


# ============================================================================
# SIMULATION MODE ENDPOINTS
# ============================================================================
@app.post("/api/v1/simulation/start", tags=["Simulation"])
async def start_simulation(
    request: Request,
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
async def stop_simulation(request: Request):
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
    request: Request,
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
async def trigger_autofocus_simulation(request: Request):
    """Симулирует запуск автофокуса."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.trigger_autofocus()
    return {"status": "success", "message": "Autofocus triggered"}


@app.post("/api/v1/simulation/trigger-meridian-flip", tags=["Simulation"])
async def trigger_meridian_flip_simulation(request: Request):
    """Симулирует Meridian Flip."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.trigger_meridian_flip()
    return {"status": "success", "message": "Meridian flip triggered"}


@app.post("/api/v1/simulation/reset-cooldowns", tags=["Simulation"])
async def reset_agent_cooldowns(request: Request):
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
async def list_available_triggers(request: Request):
    """Возвращает список всех доступных триггеров."""
    return trigger_emulator.list_available_triggers()


@app.get("/api/v1/triggers/{trigger_name}", tags=["Execution Layer"])
async def get_trigger_info(
    request: Request,
    trigger_name: str,
):
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
# Гибридный LangGraph оркестратор для сложных многошаговых workflows:
# - DIAGNOSTIC: поиск root cause через RAG + корреляции
# - POST_MORTEM: анализ завершённой сессии
# - ADAPTIVE: адаптация к изменяющимся условиям (погода, оборудование)
# ============================================================================


@app.get("/api/v1/langgraph/types", tags=["LangGraph"])
async def list_workflow_types(request: Request):
    """
    Возвращает список всех доступных типов LangGraph workflows.
    Каждый тип описывает сценарий использования.
    """
    return {
        "workflow_types": [
            {
                "type": WorkflowType.DIAGNOSTIC.value,
                "description": "Complex diagnostic workflow: root cause analysis через RAG + корреляции метрик",
                "use_case": "Когда Watcher детектирует сложную аномалию (HFR деградация, RMS spike)",
                "expected_duration_seconds": "30-120",
            },
            {
                "type": WorkflowType.POST_MORTEM.value,
                "description": "Post-mortem analysis завершённой сессии",
                "use_case": "После SEQUENCE_STOPPED для генерации Session Digest",
                "expected_duration_seconds": "10-60",
            },
            {
                "type": WorkflowType.ADAPTIVE.value,
                "description": "Adaptive response на изменение условий",
                "use_case": "При резком изменении погоды или сбое оборудования",
                "expected_duration_seconds": "5-30",
            },
        ],
        "total": len(WorkflowType),
    }


@app.get("/api/v1/langgraph/workflows", tags=["LangGraph"])
async def list_active_workflows(request: Request):
    """
    Возвращает список всех активных LangGraph workflows.
    Включает ID, тип, статус, время создания.
    """
    active_ids = hybrid_orchestrator.list_active_workflows()
    workflows = []

    for wf_id in active_ids:
        state = hybrid_orchestrator.get_workflow_status(wf_id)
        if state:
            workflows.append(
                {
                    "workflow_id": wf_id,
                    "type": state.get("workflow_type"),
                    "status": state.get("status"),
                    "created_at": state.get("created_at"),
                    "updated_at": state.get("updated_at"),
                    "retry_count": state.get("retry_count", 0),
                    "max_retries": state.get("max_retries", 3),
                    "errors_count": len(state.get("errors", [])),
                }
            )

    return {
        "active_count": len(active_ids),
        "workflows": workflows,
    }


@app.get("/api/v1/langgraph/workflow/{workflow_id}", tags=["LangGraph"])
async def get_workflow_status(request: Request, workflow_id: str):
    """
    Возвращает детальный статус конкретного LangGraph workflow.
    Включает все поля состояния: рекомендации, выполненные действия, ошибки.
    """
    state = hybrid_orchestrator.get_workflow_status(workflow_id)

    if not state:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow '{workflow_id}' not found. "
            f"Use GET /api/v1/langgraph/workflows to see active workflows.",
        )

    # Возвращаем полный state без внутренних полей
    return {
        "workflow_id": state.get("workflow_id"),
        "type": state.get("workflow_type"),
        "status": state.get("status"),
        "created_at": state.get("created_at"),
        "updated_at": state.get("updated_at"),
        "trigger_event": state.get("trigger_event"),
        "context": state.get("context"),
        "recommendations": state.get("recommendations", []),
        "executed_actions": state.get("executed_actions", []),
        "final_outcome": state.get("final_outcome"),
        "retry_count": state.get("retry_count", 0),
        "max_retries": state.get("max_retries", 3),
        "errors": state.get("errors", []),
        # Типо-специфичные поля
        "diagnostic_fields": {
            "symptoms": state.get("symptoms", []),
            "root_causes": state.get("root_causes", []),
            "confidence": state.get("diagnostic_confidence"),
        }
        if state.get("workflow_type") == WorkflowType.DIAGNOSTIC.value
        else None,
        "post_mortem_fields": {
            "session_id": state.get("session_id"),
            "lessons_learned": state.get("lessons_learned", []),
        }
        if state.get("workflow_type") == WorkflowType.POST_MORTEM.value
        else None,
        "adaptive_fields": {
            "current_conditions": state.get("current_conditions", {}),
            "adaptation_actions": state.get("adaptation_actions", []),
        }
        if state.get("workflow_type") == WorkflowType.ADAPTIVE.value
        else None,
    }


@app.post("/api/v1/langgraph/start", tags=["LangGraph"])
async def start_langgraph_workflow(
    request: Request,
    workflow_type: str = Query(
        ..., description="Тип workflow: diagnostic, post_mortem, adaptive"
    ),
    trigger_event: str = Query(
        "manual",
        description="Событие-триггер (например: 'HFR_degradation', 'session_ended')",
    ),
    max_retries: int = Query(
        3, ge=0, le=10, description="Максимальное количество попыток retry"
    ),
):
    """
    Запускает новый LangGraph workflow.

    Workflow работает асинхронно в фоне. Статус можно отслеживать через
    GET /api/v1/langgraph/workflow/{workflow_id}.

    Доступные типы workflow:
    - **diagnostic**: поиск root cause через RAG + корреляции метрик
    - **post_mortem**: анализ завершённой сессии
    - **adaptive**: адаптация к изменяющимся условиям

    Returns:
        workflow_id — ID запущенного workflow для отслеживания
    """
    # Валидация типа workflow
    try:
        wf_type = WorkflowType(workflow_type)
    except ValueError:
        valid_types = [t.value for t in WorkflowType]
        raise HTTPException(
            status_code=400,
            detail=f"Invalid workflow type: '{workflow_type}'. "
            f"Valid types: {valid_types}. "
            f"Use GET /api/v1/langgraph/types to see all available types.",
        )

    # Запуск workflow
    try:
        workflow_id = await hybrid_orchestrator.start_workflow(
            workflow_type=wf_type,
            trigger_event={
                "type": trigger_event,
                "source": "api_manual",
                "timestamp": datetime.now().isoformat(),
            },
            context={
                "source": "manual_api_call",
                "initiated_by": "user",
            },
            max_retries=max_retries,
        )

        # Логируем в ObservatoryState для объяснимости
        observatory_state.log_ai_action(
            agent="LangGraphOrchestrator",
            action=f"Start Workflow: {workflow_type}",
            reason=f"Manual trigger: {trigger_event}",
            result=f"Workflow {workflow_id} started",
        )

        return {
            "status": "started",
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "max_retries": max_retries,
            "status_url": f"/api/v1/langgraph/workflow/{workflow_id}",
            "message": f"Workflow '{workflow_id}' started successfully. "
            f"Use the status_url to track progress.",
        }

    except Exception as e:
        logger.error(f"Failed to start workflow: {e}", exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to start workflow: {str(e)}",
        )


@app.post("/api/v1/langgraph/cancel/{workflow_id}", tags=["LangGraph"])
async def cancel_langgraph_workflow(request: Request, workflow_id: str):
    """
    Отменяет активный LangGraph workflow.

    Workflow будет помечен как CANCELLED и больше не будет выполнять действия.
    Уже выполненные действия не откатываются.
    """
    state = hybrid_orchestrator.get_workflow_status(workflow_id)

    if not state:
        raise HTTPException(
            status_code=404,
            detail=f"Workflow '{workflow_id}' not found.",
        )

    # Проверяем, что workflow ещё активен
    current_status = state.get("status")
    if current_status in (WorkflowStatus.COMPLETED.value, WorkflowStatus.FAILED.value):
        raise HTTPException(
            status_code=400,
            detail=f"Workflow '{workflow_id}' already finished with status '{current_status}'. "
            f"Cannot cancel completed workflows.",
        )

    # Помечаем как CANCELLED
    state["status"] = "cancelled"
    state["updated_at"] = datetime.now().isoformat()
    state["final_outcome"] = "cancelled_by_user"

    # Логируем отмену
    observatory_state.log_ai_action(
        agent="LangGraphOrchestrator",
        action=f"Cancel Workflow: {workflow_id}",
        reason="Manual cancellation via API",
        result="Workflow cancelled",
    )

    return {
        "status": "cancelled",
        "workflow_id": workflow_id,
        "message": f"Workflow '{workflow_id}' has been cancelled.",
    }


@app.get("/api/v1/langgraph/stats", tags=["LangGraph"])
async def get_langgraph_stats(request: Request):
    """
    Возвращает общую статистику LangGraph оркестратора.
    """
    active_ids = hybrid_orchestrator.list_active_workflows()

    # Подсчёт по статусам
    status_counts = {}
    type_counts = {}

    for wf_id, state in hybrid_orchestrator.active_workflows.items():
        status = state.get("status", "unknown")
        wf_type = state.get("workflow_type", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1
        type_counts[wf_type] = type_counts.get(wf_type, 0) + 1

    return {
        "total_workflows": len(hybrid_orchestrator.active_workflows),
        "active_workflows": len(active_ids),
        "by_status": status_counts,
        "by_type": type_counts,
        "available_types": [t.value for t in WorkflowType],
        "orchestrator_initialized": hybrid_orchestrator.graph is not None,
    }
# ============================================================================
# BACKGROUND TASKS ENDPOINTS (v4.0)
# ============================================================================
@app.get("/api/v1/system/background-tasks", tags=["System"])
async def get_background_tasks_stats(request: Request):
    """Возвращает статистику всех фоновых задач системы."""
    return background_tasks.get_stats()


@app.post("/api/v1/system/background-tasks/{task_name}/toggle", tags=["System"])
async def toggle_background_task(
    request: Request,
    task_name: str,
    enabled: bool = Query(..., description="Включить/выключить задачу"),
):
    """Включает или выключает конкретную фоновую задачу."""
    if enabled:
        success = background_tasks.enable(task_name)
    else:
        success = background_tasks.disable(task_name)

    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"Task '{task_name}' not found. "
                   f"Available: {list(background_tasks._tasks.keys())}",
        )

    return {
        "status": "success",
        "task": task_name,
        "enabled": enabled,
    }

# ============================================================================
# RAG UPDATER ENDPOINTS (v4.0 — идея 1.2)
# ============================================================================
@app.get("/api/v1/rag/updater/stats", tags=["RAG Engine"])
async def rag_updater_stats(request: Request):
    """Возвращает статистику RAG Updater (автообновление базы знаний)."""
    return rag_updater.get_stats()


@app.post("/api/v1/rag/updater/force", tags=["RAG Engine"])
async def force_rag_update(request: Request):
    """
    Принудительно запускает обновление RAG.
    Индексирует новые Session Digest и изменённую документацию.
    """
    logger.info("API Request: Force RAG update")
    result = await rag_updater.force_update()
    
    observatory_state.log_ai_action(
        agent="API",
        action="Force RAG Update",
        reason="Manual API call",
        result=f"Sessions: {result.get('sessions_indexed', 0)}, "
               f"Docs: {result.get('docs_indexed', 0)}",
    )
    
    return result


# ============================================================================
# DECISION ANALYZER ENDPOINTS (v4.0 — идея 2, RL foundation)
# ============================================================================
@app.get("/api/v1/analytics/agent/{agent_name}", tags=["Analytics"])
async def get_agent_performance(
    request: Request,
    agent_name: str,
    days: int = Query(30, ge=1, le=365, description="Период анализа (дни)"),
):
    """
    Возвращает статистику производительности конкретного AI-агента.
    Включает: success rate, количество решений по типам, среднюю уверенность.
    """
    perf = await decision_analyzer.analyze_agent_performance(
        agent=agent_name,
        days=days,
    )
    return perf.to_dict()


@app.get("/api/v1/analytics/agents", tags=["Analytics"])
async def get_all_agents_performance(
    request: Request,
    days: int = Query(30, ge=1, le=365, description="Период анализа (дни)"),
):
    """Возвращает производительность всех AI-агентов."""
    all_perf = await decision_analyzer.analyze_all_agents(days=days)
    return {
        agent: perf.to_dict()
        for agent, perf in all_perf.items()
        if perf.total_decisions > 0
    }


@app.get("/api/v1/analytics/recommendations", tags=["Analytics"])
async def get_recommendations(
    request: Request,
    agent: Optional[str] = Query(None, description="Фильтр по агенту"),
    days: int = Query(30, ge=1, le=365),
):
    """
    Возвращает рекомендации по улучшению производительности агентов.
    Генерируются на основе анализа success rate и паттернов ошибок.
    """
    recs = await decision_analyzer.generate_recommendations(
        agent=agent,
        days=days,
    )
    return {
        "recommendations": [r.to_dict() for r in recs],
        "count": len(recs),
    }


@app.get("/api/v1/analytics/report", tags=["Analytics"])
async def get_analytics_report(
    request: Request,
    days: int = Query(30, ge=1, le=365),
):
    """
    Генерирует полный аналитический отчёт по всем агентам.
    Включает: общую статистику, лучшего/худшего агента, рекомендации.
    """
    report = await decision_analyzer.generate_full_report(days=days)
    return report


@app.get("/api/v1/analytics/stats", tags=["Analytics"])
async def get_analyzer_stats(request: Request):
    """Возвращает статистику самого Decision Analyzer."""
    return decision_analyzer.get_stats()


# ============================================================================
# SESSIONS METADATA ENDPOINTS (v4.0 — идея 1.3)
# ============================================================================
@app.get("/api/v1/sessions", tags=["Sessions Metadata"])
async def list_sessions(
    request: Request,
    target: Optional[str] = Query(None, description="Фильтр по имени цели"),
    date_from: Optional[str] = Query(None, description="Начальная дата (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Конечная дата (YYYY-MM-DD)"),
    min_quality: Optional[float] = Query(None, ge=0, le=10, description="Минимальный quality_score"),
    limit: int = Query(50, ge=1, le=500),
):
    """
    Возвращает список сессий с фильтрацией.
    Можно фильтровать по цели, датам, минимальному quality_score.
    """
    sessions = await sessions_metadata.get_sessions(
        target_name=target,
        date_from=date_from,
        date_to=date_to,
        min_quality=min_quality,
        limit=limit,
    )
    return {
        "sessions": [s.model_dump() for s in sessions],
        "count": len(sessions),
    }


@app.get("/api/v1/sessions/{session_id}", tags=["Sessions Metadata"])
async def get_session_details(
    request: Request,
    session_id: str,
):
    """
    Возвращает детальную информацию о сессии.
    Включает: общую информацию, кадры, проблемы, статистику.
    """
    stats = await sessions_metadata.get_session_stats(session_id)
    if "error" in stats:
        raise HTTPException(status_code=404, detail=stats["error"])
    return stats


@app.get("/api/v1/sessions/{session_id}/frames", tags=["Sessions Metadata"])
async def get_session_frames(
    request: Request,
    session_id: str,
    image_type: Optional[str] = Query(None, description="Фильтр по типу кадра"),
    limit: int = Query(1000, ge=1, le=10000),
):
    """
    Возвращает все кадры сессии.
    Можно фильтровать по типу кадра (LIGHT, FLAT, DARK, BIAS).
    """
    frames = await sessions_metadata.get_frames(
        session_id=session_id,
        image_type=image_type,
        limit=limit,
    )
    return {
        "session_id": session_id,
        "frames": [f.model_dump() for f in frames],
        "count": len(frames),
    }


@app.get("/api/v1/sessions/{session_id}/export", tags=["Sessions Metadata"])
async def export_session(
    request: Request,
    session_id: str,
):
    """
    Экспортирует все кадры сессии в CSV файл.
    Возвращает путь к созданному файлу.
    """
    output_path = Path(f"./data/exports/{session_id}_frames.csv")
    success = await sessions_metadata.export_session_csv(session_id, output_path)
    
    if not success:
        raise HTTPException(
            status_code=404,
            detail=f"No frames found for session {session_id}"
        )
    
    return {
        "status": "success",
        "session_id": session_id,
        "export_path": str(output_path),
        "message": f"Session exported to {output_path}",
    }


@app.get("/api/v1/sessions/stats", tags=["Sessions Metadata"])
async def get_sessions_metadata_stats(request: Request):
    """Возвращает общую статистику хранилища сессий."""
    return await sessions_metadata.get_stats()

# ============================================================================
# PREDICTIVE HAL ENDPOINTS (v4.0 — идея 3)
# ============================================================================
@app.get("/api/v1/safety/predictive", tags=["Safety"])
async def get_predictive_hal_stats(request: Request):
    """Возвращает статистику Predictive HAL."""
    return predictive_hal.get_stats()


@app.post("/api/v1/safety/predictive/check", tags=["Safety"])
async def force_predictive_check(request: Request):
    """Принудительно запускает проверку всех предсказаний."""
    predictions = await predictive_hal.force_check()
    return {
        "predictions_count": len(predictions),
        "predictions": predictions,
    }


@app.get("/api/v1/sequence/shadow/mermaid", tags=["Shadow Engine"])
async def get_shadow_mermaid(
    request: Request,
    include_details: bool = Query(True, description="Включать детали узлов"),
    max_depth: int = Query(10, ge=1, le=50, description="Максимальная глубина"),
    show_triggers: bool = Query(True, description="Показывать триггеры"),
    show_conditions: bool = Query(True, description="Показывать условия"),
):
    """
    Возвращает теневой граф в формате Mermaid для документации.
    
    Mermaid можно вставить в:
    - GitHub README (авто-рендеринг)
    - Markdown документы
    - Notion/Obsidian
    """
    mermaid_code = shadow_visualizer.generate_mermaid(
        include_details=include_details,
        max_depth=max_depth,
        show_triggers=show_triggers,
        show_conditions=show_conditions,
    )
    
    return {
        "format": "mermaid",
        "code": mermaid_code,
        "markdown": f"```mermaid\n{mermaid_code}\n```",
        "stats": state_tracker.get_stats(),
    }


@app.get("/api/v1/sequence/shadow/html", tags=["Shadow Engine"])
async def get_shadow_html_report(request: Request):
    """
    Возвращает полный HTML-отчёт с интерактивной диаграммой теневого графа.
    Откройте в браузере для просмотра.
    """
    html = shadow_visualizer.generate_full_html_report()
    return Response(content=html, media_type="text/html")


@app.get("/api/v1/sequence/shadow/visualizer/stats", tags=["Shadow Engine"])
async def get_shadow_visualizer_stats(request: Request):
    """Возвращает статистику Shadow Visualizer."""
    return shadow_visualizer.get_stats()

# ============================================================================
# METRICS SOURCE MONITOR ENDPOINTS (v4.0 — идея 6)
# ============================================================================
@app.get("/api/v1/metrics/sources", tags=["Metrics"])
async def get_metrics_sources_status(request: Request):
    """Возвращает статус всех источников метрик и текущий активный."""
    return metrics_source_monitor.get_stats()


@app.post("/api/v1/metrics/sources/override", tags=["Metrics"])
async def set_metrics_source_override(
    request: Request,
    source: str = Query(..., description="Имя источника: influxdb или prometheus"),
    reason: str = Query("manual", description="Причина override"),
):
    """
    Устанавливает ручной override источника метрик.
    Отключает автоматическое переключение до снятия override.
    """
    if source not in ["influxdb", "prometheus"]:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid source: {source}. Valid: influxdb, prometheus"
        )
    
    metrics_source_monitor.set_manual_override(source, reason)
    return {
        "status": "success",
        "active_source": source,
        "manual_override": True,
        "reason": reason,
    }


@app.delete("/api/v1/metrics/sources/override", tags=["Metrics"])
async def clear_metrics_source_override(request: Request):
    """Снимает ручной override и включает автопереключение."""
    metrics_source_monitor.clear_manual_override()
    return {
        "status": "success",
        "active_source": metrics_source_monitor.get_active_source(),
        "manual_override": False,
    }