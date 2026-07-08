"""
N.I.N.A. AI Cortex - Main Application Entry Point
FastAPI сервер, управляющий жизненным циклом всех компонентов Cortex.
ИСПРАВЛЕНО (audit v2):
- 11.2: Добавлены Prometheus метрики через middleware и EventBus подписки
- 11.1: Маскирование чувствительных данных в логах (global_var_injector)
- C4: JWT-аутентификация и авторизация по ролям
- C5: Обязательный мастер-пароль Vault в production
- F2: CORS whitelist из settings.yaml
- F3: Rate limiting через slowapi
- P0: Устранён hardcoded master password — генерация случайного в dev
- P1: Добавлена очистка секретов при shutdown через destroy_jwt_secret()
"""

import asyncio
import logging
import os
import secrets
import time
import uuid
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
    Depends,
    Request,
    Response,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from app.core.config import settings
from app.core.events import event_bus
from app.core.rag_engine import rag_engine
from app.core.ws_broadcast import ws_broadcast_manager
from app.core.metrics import cortex_metrics  # ← НОВОЕ (audit 11.2)
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
from app.agents.memory_manager_agent import MemoryManagerAgent
from app.core.mode_manager import mode_manager
from app.safety.preflight import preflight_checker
from app.agents.llm_client import llm_client

# Storage & Security
from app.storage.disk_monitor import disk_monitor
from app.storage.decision_audit import decision_audit
from app.security.vault import CredentialVault

# ИСПРАВЛЕНО (audit C4): импорты модуля аутентификации
from app.security.auth import (
    UserRole,
    TokenData,
    TokenResponse,
    create_access_token,
    get_current_user,
    require_role,
    authorize_request,
    list_api_keys,
    register_api_key,
    revoke_api_key,
    destroy_jwt_secret,  # ← НОВОЕ (audit P1): очистка JWT секрета
)

# ИСПРАВЛЕНО (audit P0/P1): импорты для безопасного управления секретами
from app.security.secure_memory import destroy_system_secrets


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

# 10 AI-агентов
watcher_agent = WatcherAgent()
guardian_agent = GuardianAgent()
diagnostician_agent = DiagnosticianAgent()
strategist_agent = StrategistAgent()
auditor_agent = AuditorAgent()
calibrator_agent: Optional[CalibratorAgent] = None
scheduler_agent = SchedulerAgent()
copilot_agent = CopilotAgent()
memory_manager_agent = MemoryManagerAgent()

# Credential Vault (инициализируется в lifespan)
credential_vault: Optional[CredentialVault] = None


# ============================================================================
# EVENT BUS METRICS SUBSCRIBERS (audit 11.2)
# ============================================================================
async def _on_event_bus_event(event_type: str, data: Dict[str, Any]):
    """
    Автоматический сбор метрик из EventBus.
    Вызывается для каждого опубликованного события.
    """
    cortex_metrics.events_total.labels(event_type=event_type).inc()


async def _on_decision_made(data: Dict[str, Any]):
    """Сбор метрик о решениях агентов."""
    agent = data.get("agent", "unknown")
    decision_type = data.get("decision_type", "unknown")
    confidence = data.get("confidence", 0.5)
    cortex_metrics.decisions_total.labels(
        agent=agent, decision_type=decision_type, outcome="pending"
    ).inc()
    cortex_metrics.decision_confidence.labels(agent=agent).observe(confidence)


async def _on_trigger_fired(data: Dict[str, Any]):
    """Сбор метрик о срабатывании триггеров."""
    trigger_name = data.get("trigger", "unknown")
    status = data.get("status", "unknown")
    duration = data.get("duration_seconds", 0.0)
    cortex_metrics.triggers_fired.labels(trigger_name=trigger_name, status=status).inc()
    if duration > 0:
        cortex_metrics.trigger_duration.labels(trigger_name=trigger_name).observe(
            duration
        )


async def _on_llm_response(data: Dict[str, Any]):
    """Сбор метрик о LLM запросах."""
    model = data.get("model", "unknown")
    status = data.get("status", "success")
    fallback = "true" if data.get("from_fallback", False) else "false"
    duration = data.get("duration_seconds", 0.0)
    tokens = data.get("tokens_used", 0)
    cortex_metrics.llm_requests_total.labels(
        model=model, status=status, fallback=fallback
    ).inc()
    cortex_metrics.llm_request_duration.labels(model=model).observe(duration)
    if tokens > 0:
        cortex_metrics.llm_tokens_used.labels(model=model).inc(tokens)


# ============================================================================
# LIFESPAN (Startup / Shutdown)
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление жизненным циклом приложения."""
    global calibrator_agent, credential_vault

    logger.info("=" * 70)
    logger.info("🚀 N.I.N.A. AI Cortex v2.0 Starting Up...")
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

        # 6. Credential Vault — ИСПРАВЛЕНО (audit C5, P0)
        logger.info("🔐 Initializing Credential Vault...")
        vault_path = Path("./data/vault.json")
        vault_path.parent.mkdir(parents=True, exist_ok=True)
        master_password = os.getenv("VAULT_MASTER_PASSWORD")
        is_production = os.getenv("ENVIRONMENT", "development") == "production"

        if not master_password:
            if is_production:
                # В production — фатальная ошибка, нет fallback
                raise RuntimeError(
                    "❌ FATAL: VAULT_MASTER_PASSWORD environment variable "
                    "MUST be set in production. "
                    "Generate with: python -c 'import secrets; print(secrets.token_urlsafe(32))'"
                )
            else:
                # ИСПРАВЛЕНО (audit P0): В dev генерируем СЛУЧАЙНЫЙ пароль
                # вместо фиксированного "dev-master-password-DO-NOT-USE-IN-PROD"
                master_password = secrets.token_urlsafe(32)
                logger.warning(
                    "⚠️ VAULT_MASTER_PASSWORD not set — generated random ephemeral "
                    "password for development. Vault data will NOT persist across "
                    "restarts (different password each time)."
                )
        else:
            # Пароль задан в окружении — проверяем длину
            if len(master_password) < 16:
                logger.warning(
                    "⚠️ VAULT_MASTER_PASSWORD is shorter than 16 chars — "
                    "consider using a stronger password in production."
                )
            # Проверяем на известные дефолтные значения
            known_defaults = [
                "dev-master-password-change-me",
                "dev-master-password-DO-NOT-USE-IN-PROD",
                "123456",
                "password",
                "admin",
            ]
            if master_password in known_defaults:
                if is_production:
                    raise RuntimeError(
                        f"❌ FATAL: VAULT_MASTER_PASSWORD is set to a known default "
                        f"value '{master_password}'. This is a critical security risk "
                        f"in production. Please generate a strong random password."
                    )
                else:
                    logger.warning(
                        f"⚠️ VAULT_MASTER_PASSWORD is set to known default "
                        f"'{master_password}'. This is insecure even for development. "
                        f"Consider generating a random password."
                    )
            else:
                logger.info(
                    f"✅ Vault master password loaded from environment "
                    f"(length: {len(master_password)})"
                )

        credential_vault = CredentialVault(
            master_password=master_password, vault_path=vault_path
        )

        # 7. Calibrator Agent
        calibrator_agent = CalibratorAgent(
            masters_auditor=watcher_manager.masters_auditor
        )

        # 8. Инициализация агентов
        logger.info("🤖 Initializing 10 AI Agents...")
        await watcher_agent.initialize()
        await guardian_agent.initialize()
        await diagnostician_agent.initialize()
        await strategist_agent.initialize()
        await auditor_agent.initialize()
        await calibrator_agent.initialize()
        await scheduler_agent.initialize()
        await copilot_agent.initialize()
        await memory_manager_agent.initialize()

        # 9. Регистрация в Orchestrator
        orchestrator.register_agent("Watcher", watcher_agent)
        orchestrator.register_agent("Guardian", guardian_agent)
        orchestrator.register_agent("Diagnostician", diagnostician_agent)
        orchestrator.register_agent("Strategist", strategist_agent)
        orchestrator.register_agent("Auditor", auditor_agent)
        orchestrator.register_agent("Calibrator", calibrator_agent)
        orchestrator.register_agent("Scheduler", scheduler_agent)
        orchestrator.register_agent("Copilot", copilot_agent)
        orchestrator.register_agent("MemoryManager", memory_manager_agent)

        # 10. Запуск Orchestrator
        await orchestrator.start()

        # 11. ИСПРАВЛЕНО (audit 11.2): Подписка на события для сбора метрик
        logger.info("📊 Subscribing to EventBus for metrics collection...")
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
            event_bus.subscribe(
                event_type, lambda data, et=event_type: _on_event_bus_event(et, data)
            )

        # Специальные подписки для детальных метрик
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
        # 1. Отписка от событий метрик (audit 11.2)
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
        await memory_manager_agent.shutdown()
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

        # ====================================================================
        # ИСПРАВЛЕНО (audit P1): Очистка секретов из памяти
        # ====================================================================
        logger.info("🔐 Cleaning up secrets from memory...")
        try:
            # Уничтожаем JWT секрет из защищённой памяти
            destroy_jwt_secret()
            logger.debug("   ✅ JWT secret destroyed")
        except Exception as e:
            logger.debug(f"   ⚠️ Error destroying JWT secret: {e}")

        try:
            # Уничтожаем все системные секреты (из SecureSecretPool)
            destroy_system_secrets()
            logger.debug("   ✅ System secrets destroyed")
        except Exception as e:
            logger.debug(f"   ⚠️ Error destroying system secrets: {e}")

        logger.info("✅ All secrets cleaned from memory")

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
        "10 AI-агентов, LangGraph координация, RAG-система, Pre-flight Checklist, "
        "Credential Vault, Simulation Mode. "
        "🔒 Требуется JWT Bearer token или X-API-Key для большинства endpoints."
    ),
    version="2.0.0",
    lifespan=lifespan,
)

# Rate limiter
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

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
    Считает количество запросов, время ответа, статусы.
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
    cortex_metrics.api_requests_total.labels(
        method=method, path=path, status_code=str(status_code)
    ).inc()
    cortex_metrics.api_request_duration.labels(method=method, path=path).observe(
        duration
    )
    return response


# ============================================================================
# AUTH DEPENDENCY (audit C4)
# ============================================================================
AuthDep = Depends(authorize_request)


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


class SecretRequest(BaseModel):
    name: str
    value: str
    description: Optional[str] = None


class TokenRequest(BaseModel):
    subject: str
    role: str = "readonly"
    expires_minutes: int = 60 * 24


class APIKeyRequest(BaseModel):
    name: str
    role: str = "readonly"
    scopes: List[str] = []


# ============================================================================
# SYSTEM ENDPOINTS (public)
# ============================================================================
@app.get("/health", tags=["System"])
@limiter.limit("60/minute")
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
        "version": "2.0.0",
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
            "auth_enabled": settings.auth.enabled,
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
        "version": "2.0.0",
        "documentation": "/docs",
        "health": "/health",
        "metrics": "/metrics",
        "authentication": {
            "methods": ["Bearer JWT", "X-API-Key"],
            "token_endpoint": "/api/v1/auth/token (admin)",
            "api_key_endpoint": "/api/v1/auth/api-key (admin)",
        },
    }


# ============================================================================
# PROMETHEUS /metrics ENDPOINT — ИСПРАВЛЕНО (audit 11.2)
# ============================================================================
@app.get("/metrics", tags=["Observability"], include_in_schema=False)
@limiter.limit("30/minute")
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
    cortex_metrics.active_ws_connections.set(ws_stats.get("total_connections", 0))
    cortex_metrics.sequence_running.set(1 if state_tracker.state.is_running else 0)
    cortex_metrics.flat_mode_active.set(1 if state_tracker.state.is_flat_mode else 0)
    cortex_metrics.llm_available.labels(model="primary").set(
        1 if llm_client.is_available() else 0
    )

    # Обновляем RAG метрики
    if "points_count" in rag_stats:
        cortex_metrics.rag_documents_total.set(rag_stats["points_count"])

    # Обновляем operation_mode
    mode_value = {"manual": 0, "safe": 1, "full_ai": 2, "simulation": 3}.get(
        mode_manager.current_mode.value, -1
    )
    cortex_metrics.operation_mode.set(mode_value)

    # Обновляем safety_status
    safety_value = {"SAFE": 0, "UNSAFE": 1, "UNKNOWN": -1}.get(
        observatory_state.safety_status, -1
    )
    cortex_metrics.safety_status.set(safety_value)

    # Обновляем количество активных watchers
    cortex_metrics.watchers_active.set(len(watcher_manager.watchers))

    # Обновляем количество активных агентов
    cortex_metrics.agents_active.set(len(orchestrator.agents))

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
@limiter.limit("30/minute")
async def ws_stats(request: Request, _user: TokenData = AuthDep):
    """Возвращает статистику WebSocket подключений."""
    return ws_broadcast_manager.get_stats()


# ============================================================================
# AUTH ENDPOINTS — НОВЫЕ (audit C4)
# ============================================================================
@app.post("/api/v1/auth/token", tags=["Authentication"], response_model=TokenResponse)
@limiter.limit("10/minute")
async def issue_token(
    request: Request,
    body: TokenRequest,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Выпускает JWT access token."""
    try:
        role = UserRole(body.role)
    except ValueError:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid role: {body.role}. Valid: {[r.value for r in UserRole]}",
        )
    token = create_access_token(
        subject=body.subject,
        role=role,
        expires_delta=timedelta(minutes=body.expires_minutes),
    )
    return TokenResponse(
        access_token=token,
        expires_in=body.expires_minutes * 60,
        role=role,
    )


@app.post("/api/v1/auth/api-key", tags=["Authentication"])
@limiter.limit("5/minute")
async def create_api_key(
    request: Request,
    body: APIKeyRequest,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Создаёт API-ключ для machine-to-machine."""
    try:
        role = UserRole(body.role)
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Invalid role: {body.role}")
    raw_key = register_api_key(name=body.name, role=role, scopes=body.scopes)
    return {
        "name": body.name,
        "api_key": raw_key,
        "role": role.value,
        "warning": "⚠️ Save this key now — it will NOT be shown again.",
    }


@app.get("/api/v1/auth/api-keys", tags=["Authentication"])
@limiter.limit("30/minute")
async def get_api_keys(
    request: Request,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Список API-ключей (без секретов)."""
    return {"keys": list_api_keys()}


@app.delete("/api/v1/auth/api-key/{name}", tags=["Authentication"])
@limiter.limit("10/minute")
async def delete_api_key(
    request: Request,
    name: str,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Отзыв API-ключа."""
    if revoke_api_key(name):
        return {"status": "revoked", "name": name}
    raise HTTPException(status_code=404, detail=f"API key '{name}' not found")


@app.get("/api/v1/auth/whoami", tags=["Authentication"])
@limiter.limit("60/minute")
async def whoami(request: Request, user: TokenData = AuthDep):
    """Показывает информацию о текущем аутентифицированном пользователе."""
    return {
        "subject": user.sub,
        "role": user.role.value,
        "scopes": user.scopes,
    }


# ============================================================================
# SHADOW ENGINE ENDPOINTS
# ============================================================================
@app.get("/api/v1/sequence/shadow", tags=["Shadow Engine"])
@limiter.limit("60/minute")
async def get_sequence_shadow(request: Request, _user: TokenData = AuthDep):
    """Возвращает полный теневой граф секвенсора (DAG)."""
    if not state_tracker._shadow_graph:
        raise HTTPException(status_code=404, detail="Sequence shadow graph not loaded")
    return state_tracker._shadow_graph


@app.get("/api/v1/sequence/state", tags=["Shadow Engine"])
@limiter.limit("60/minute")
async def get_sequence_state(request: Request, _user: TokenData = AuthDep):
    """Возвращает текущее состояние выполнения секвенсора."""
    return state_tracker.get_state()


# ============================================================================
# AI AGENTS ENDPOINTS
# ============================================================================
@app.get("/api/v1/observatory/state", tags=["AI Agents"])
@limiter.limit("120/minute")
async def get_observatory_full_state(request: Request, _user: TokenData = AuthDep):
    """Возвращает единое состояние обсерватории (ObservatoryState)."""
    return observatory_state.get_full_state()


@app.get("/api/v1/observatory/session-summary", tags=["AI Agents"])
@limiter.limit("60/minute")
async def get_session_summary(request: Request, _user: TokenData = AuthDep):
    """Возвращает краткую сводку текущей сессии для LLM контекста."""
    return observatory_state.get_session_summary()


@app.get("/api/v1/agents/status", tags=["AI Agents"])
@limiter.limit("60/minute")
async def get_agents_status(request: Request, _user: TokenData = AuthDep):
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
            "MemoryManager": memory_manager_agent.get_stats(),
        },
    }


@app.post("/api/v1/agents/mode", tags=["AI Agents"])
@limiter.limit("10/minute")
async def set_operation_mode(
    request: Request,
    mode: str,
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Устанавливает режим работы системы."""
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
@limiter.limit("60/minute")
async def get_recent_decisions(
    request: Request,
    limit: int = Query(20, ge=1, le=100),
    _user: TokenData = AuthDep,
):
    """Возвращает последние решения агентов из Decision Audit Trail."""
    return {
        "decisions": orchestrator.get_recent_decisions(limit=limit),
        "total": len(orchestrator._decisions_log),
    }


@app.get("/api/v1/agents/llm-status", tags=["AI Agents"])
@limiter.limit("30/minute")
async def get_llm_status(request: Request, _user: TokenData = AuthDep):
    """Проверяет доступность локального LLM (Ollama)."""
    return {
        "available": llm_client.is_available(),
        "model": settings.ai_settings.model_name,
        "host": settings.ai_settings.ollama_host,
    }


@app.post("/api/v1/agents/test-llm", tags=["AI Agents"])
@limiter.limit(f"{settings.auth.llm_rate_limit_per_minute}/minute")
async def test_llm_generation(
    request: Request,
    prompt: str = Query(..., description="Тестовый промпт"),
    agent: str = Query("Copilot", description="Имя агента для системного промпта"),
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Тестовый эндпоинт для проверки генерации LLM."""
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
@limiter.limit("120/minute")
async def get_metrics(request: Request, _user: TokenData = AuthDep):
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
@limiter.limit("60/minute")
async def get_metrics_history(
    request: Request,
    metric: str = Query(..., description="Имя метрики"),
    limit: int = Query(100, ge=1, le=1000),
    _user: TokenData = AuthDep,
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
# EXECUTION LAYER ENDPOINTS — 🔒 OPERATOR+ (audit C4)
# ============================================================================
@app.post("/api/v1/execution/trigger", tags=["Execution Layer"])
@limiter.limit(f"{settings.auth.trigger_rate_limit_per_minute}/minute")
async def fire_trigger(
    request: Request,
    body: TriggerRequest,
    user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Ручной вызов триггера через API."""
    logger.info(
        f"API Request: Fire trigger '{body.trigger_name}' "
        f"(user={user.sub}, role={user.role.value})"
    )
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
@limiter.limit(f"{settings.auth.trigger_rate_limit_per_minute}/minute")
async def set_variable(
    request: Request,
    body: VariableRequest,
    user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Изменение глобальной переменной Sequencer+."""
    logger.info(
        f"API Request: Set variable '{body.name}' = {body.value} (user={user.sub})"
    )
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
@limiter.limit("30/minute")
async def rag_search(
    request: Request,
    body: RAGSearchRequest,
    _user: TokenData = AuthDep,
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
@limiter.limit("30/minute")
async def rag_get_context(
    request: Request,
    query: str = Query(..., description="Поисковый запрос"),
    max_tokens: int = Query(2000, description="Максимальное количество токенов"),
    _user: TokenData = AuthDep,
):
    """Получает контекст для LLM на основе запроса."""
    context = await rag_engine.get_context(query=query, max_tokens=max_tokens)
    return {
        "query": query,
        "context": context,
        "tokens_approx": len(context) // 4,
    }


@app.get("/api/v1/rag/stats", tags=["RAG Engine"])
@limiter.limit("30/minute")
async def rag_stats(request: Request, _user: TokenData = AuthDep):
    """Возвращает статистику RAG-базы знаний."""
    return await rag_engine.get_stats()


# ============================================================================
# DISCOVERY & MASTERS LIBRARY ENDPOINTS
# ============================================================================
@app.get("/api/v1/plugins", tags=["Discovery"])
@limiter.limit("30/minute")
async def get_discovered_plugins(request: Request, _user: TokenData = AuthDep):
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
@limiter.limit("30/minute")
async def get_masters_catalog(request: Request, _user: TokenData = AuthDep):
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
@limiter.limit("30/minute")
async def find_matching_master(
    request: Request,
    image_type: str = Query(..., description="Тип кадра: BIAS, DARK, FLAT"),
    temperature: float = Query(..., description="Температура сенсора"),
    exposure: Optional[float] = Query(None),
    gain: Optional[int] = Query(None),
    offset: Optional[int] = Query(None),
    filter_name: Optional[str] = Query(None),
    temp_tolerance: float = Query(2.0),
    _user: TokenData = AuthDep,
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
@limiter.limit("10/minute")
async def run_preflight_check(
    request: Request,
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Запускает pre-flight проверку перед стартом сессии."""
    report = await preflight_checker.run_all()
    return report


# ============================================================================
# SECURITY ENDPOINTS (Credential Vault) — 🔒 ADMIN ONLY (audit C4)
# ============================================================================
@app.get("/api/v1/security/vault", tags=["Security"])
@limiter.limit("30/minute")
async def list_vault_secrets(
    request: Request,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Список секретов в Vault (без значений)."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")
    return {
        "secrets": credential_vault.list_secrets(),
        "stats": credential_vault.get_stats(),
    }


@app.post("/api/v1/security/vault/store", tags=["Security"])
@limiter.limit("10/minute")
async def store_secret(
    request: Request,
    body: SecretRequest,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Сохраняет секрет в Vault."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")
    success = credential_vault.store_secret(
        name=body.name,
        value=body.value,
        description=body.description,
    )
    if success:
        return {"status": "success", "message": f"Secret '{body.name}' stored"}
    else:
        raise HTTPException(status_code=500, detail="Failed to store secret")


@app.get("/api/v1/security/vault/{name}", tags=["Security"])
@limiter.limit("30/minute")
async def get_secret(
    request: Request,
    name: str,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Извлекает секрет из Vault."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")
    value = credential_vault.get_secret(name)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Secret '{name}' not found")
    return {"name": name, "value": value}


@app.delete("/api/v1/security/vault/{name}", tags=["Security"])
@limiter.limit("10/minute")
async def delete_secret(
    request: Request,
    name: str,
    _admin: TokenData = Depends(require_role(UserRole.ADMIN)),
):
    """🔒 ADMIN ONLY: Удаляет секрет из Vault."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")
    success = credential_vault.delete_secret(name)
    if success:
        return {"status": "success", "message": f"Secret '{name}' deleted"}
    else:
        raise HTTPException(status_code=404, detail=f"Secret '{name}' not found")


# ============================================================================
# STORAGE ENDPOINTS
# ============================================================================
@app.get("/api/v1/storage/disk-usage", tags=["Storage"])
@limiter.limit("30/minute")
async def get_disk_usage(request: Request, _user: TokenData = AuthDep):
    """Возвращает информацию об использовании дискового пространства."""
    return await disk_monitor.get_stats()


@app.post("/api/v1/storage/cleanup", tags=["Storage"])
@limiter.limit("5/minute")
async def apply_retention_policy(
    request: Request,
    policy_name: str = Query(..., description="Имя политики"),
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Применяет политику удаления старых данных."""
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
@limiter.limit("30/minute")
async def get_audit_decisions(
    request: Request,
    agent: Optional[str] = Query(None),
    decision_type: Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    _user: TokenData = AuthDep,
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
@limiter.limit("30/minute")
async def get_audit_stats(request: Request, _user: TokenData = AuthDep):
    """Возвращает статистику Decision Audit Trail."""
    return await decision_audit.get_stats()


# ============================================================================
# SIMULATION MODE ENDPOINTS — 🔒 OPERATOR+ (audit C4)
# ============================================================================
@app.post("/api/v1/simulation/start", tags=["Simulation"])
@limiter.limit("10/minute")
async def start_simulation(
    request: Request,
    target: str = Query("M31", description="Имя цели"),
    frames: int = Query(10, description="Количество кадров"),
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Запускает симуляцию сессии (Fake NINA)."""
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
@limiter.limit("10/minute")
async def stop_simulation(
    request: Request,
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Останавливает симуляцию."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.stop_sequence()
    await fake_nina.stop()
    await mode_manager.set_mode(
        OperationMode.FULL_AI, reason="Simulation stopped via API"
    )
    return {"status": "success", "message": "Simulation stopped"}


@app.post("/api/v1/simulation/inject-anomaly", tags=["Simulation"])
@limiter.limit("20/minute")
async def inject_anomaly(
    request: Request,
    anomaly_type: str = Query(..., description="Тип аномалии"),
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Инжектирует аномалию для тестирования."""
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
@limiter.limit("10/minute")
async def trigger_autofocus_simulation(
    request: Request,
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Симулирует запуск автофокуса."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.trigger_autofocus()
    return {"status": "success", "message": "Autofocus triggered"}


@app.post("/api/v1/simulation/trigger-meridian-flip", tags=["Simulation"])
@limiter.limit("10/minute")
async def trigger_meridian_flip_simulation(
    request: Request,
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Симулирует Meridian Flip."""
    from app.simulation.fake_nina import fake_nina

    await fake_nina.trigger_meridian_flip()
    return {"status": "success", "message": "Meridian flip triggered"}


@app.post("/api/v1/simulation/reset-cooldowns", tags=["Simulation"])
@limiter.limit("10/minute")
async def reset_agent_cooldowns(
    request: Request,
    _user: TokenData = Depends(require_role(UserRole.OPERATOR)),
):
    """🔒 OPERATOR+: Сбрасывает cooldown всех агентов (для тестирования)."""
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
@limiter.limit("30/minute")
async def list_available_triggers(request: Request, _user: TokenData = AuthDep):
    """Возвращает список всех доступных триггеров."""
    return trigger_emulator.list_available_triggers()


@app.get("/api/v1/triggers/{trigger_name}", tags=["Execution Layer"])
@limiter.limit("30/minute")
async def get_trigger_info(
    request: Request,
    trigger_name: str,
    _user: TokenData = AuthDep,
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
