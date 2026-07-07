"""
N.I.N.A. AI Cortex - Main Application Entry Point
FastAPI сервер, управляющий жизненным циклом всех компонентов Cortex.

Объединяет:
- Ingestion Layer (11 watchers + parsers)
- Shadow Engine (Sequence.json parsing)
- Execution Layer (Trigger Emulator, HAL, Safety Interceptor)
- 10 AI-агентов с LangGraph координацией
- RAG-систему (Qdrant + Ollama)
- Credential Vault (Argon2id + AES-256-GCM)
- Pre-flight Checklist (8 gates)
- Disk Monitor + Retention Engine
- Simulation Mode (Fake NINA/PHD2)
- WebSocket Broadcasting для Frontend
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
from pathlib import Path

# ============================================================================
# LOGGING SETUP
# ============================================================================
logging.basicConfig(
    level=getattr(logging, settings.logging.level.upper(), logging.INFO),
    format="%(asctime)s | %(levelname)-8s | %(name)-25s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("CortexMain")

# ============================================================================
# GLOBAL INSTANCES
# ============================================================================
# DI Hub для всех watchers, pollers, subscribers
watcher_manager = WatcherManager()

# 10 AI-агентов
watcher_agent = WatcherAgent()
guardian_agent = GuardianAgent()
diagnostician_agent = DiagnosticianAgent()
strategist_agent = StrategistAgent()
auditor_agent = AuditorAgent()
# Calibrator получит ссылку на masters_auditor после запуска watcher_manager
calibrator_agent: Optional[CalibratorAgent] = None
scheduler_agent = SchedulerAgent()
copilot_agent = CopilotAgent()
memory_manager_agent = MemoryManagerAgent()

# Credential Vault (инициализируется в lifespan после получения master password)
credential_vault: Optional[CredentialVault] = None


# ============================================================================
# LIFESPAN (Startup / Shutdown)
# ============================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Управление жизненным циклом приложения.
    Запускает все фоновые задачи, вотчеры, WebSocket клиенты и AI-агентов.
    """
    global calibrator_agent, credential_vault

    logger.info("=" * 70)
    logger.info("🚀 N.I.N.A. AI Cortex v2.0 Starting Up...")
    logger.info("=" * 70)

    try:
        # 1. Запуск всех компонентов Ingestion, Shadow Engine и Execution
        #    Включает: Capability Registry, ObservatoryState, HAL, Sequence Parser,
        #    File Watchers, Pollers, InfluxDB, Masters Auditor, WebSocket Client, Safety Interceptor
        await watcher_manager.start()

        # 2. Инициализация RAG Engine (Qdrant + Ollama embeddings)
        logger.info("📚 Initializing RAG Engine...")
        await rag_engine.initialize()

        # 3. Запуск WebSocket Broadcast Manager для Frontend
        logger.info("🔌 Starting WebSocket Broadcast Manager...")
        await ws_broadcast_manager.start()

        # 4. Инициализация Mode Manager (управление режимами работы)
        logger.info("🎛️ Starting Mode Manager...")
        await mode_manager.start()

        # 5. Инициализация LLM Client (Ollama)
        logger.info("🤖 Initializing LLM Client (Ollama connection)...")
        await llm_client.initialize()

        # 6. Инициализация Credential Vault
        logger.info("🔐 Initializing Credential Vault...")
        vault_path = Path("./data/vault.json")
        vault_path.parent.mkdir(parents=True, exist_ok=True)
        # В production master_password должен запрашиваться безопасно
        # Здесь используем значение из .env или дефолтное для разработки
        import os

        master_password = os.getenv(
            "VAULT_MASTER_PASSWORD", "dev-master-password-change-me"
        )
        credential_vault = CredentialVault(
            master_password=master_password, vault_path=vault_path
        )

        # 7. Инициализация Calibrator Agent (требует masters_auditor)
        calibrator_agent = CalibratorAgent(
            masters_auditor=watcher_manager.masters_auditor
        )

        # 8. Инициализация всех AI-агентов
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

        # 9. Регистрация агентов в Orchestrator
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

        logger.info("=" * 70)
        logger.info("✅ All AI Agents initialized and registered")
        logger.info("✅ Cortex is fully operational and ready to accept connections.")
        logger.info(f"🌐 API Docs available at: http://localhost:8000/docs")
        logger.info(
            f"🔌 WebSocket endpoint: ws://localhost:8000{settings.ws_broadcast.path}"
        )
        logger.info("=" * 70)

    except Exception as e:
        logger.critical(f"❌ FATAL: Failed to start Cortex: {e}", exc_info=True)
        raise

    yield  # <-- Приложение работает здесь (обрабатывает HTTP/WS запросы)

    # ========================================================================
    # SHUTDOWN (в обратном порядке)
    # ========================================================================
    logger.info("=" * 70)
    logger.info("🛑 N.I.N.A. AI Cortex Shutting Down...")
    logger.info("=" * 70)

    try:
        await llm_client.close()
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
        await mode_manager.stop()
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
    description=(
        "Когнитивная надстройка над N.I.N.A. с Multi-Agent AI архитектурой. "
        "10 AI-агентов, LangGraph координация, RAG-система, Pre-flight Checklist, "
        "Credential Vault, Simulation Mode."
    ),
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
    Health Check эндпоинт.
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
            "llm_available": llm_client.is_available(),
            "operation_mode": mode_manager.current_mode.value,
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

    Клиент может:
    - Подключиться и получать все события (по умолчанию)
    - Подписаться на конкретные каналы (sequence, metrics, alerts, etc.)
    - Отправлять команды (subscribe, ping)
    """
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


@app.get("/api/v1/observatory/session-summary", tags=["AI Agents"])
async def get_session_summary():
    """Возвращает краткую сводку текущей сессии для LLM контекста."""
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
            "MemoryManager": memory_manager_agent.get_stats(),
        },
    }


@app.post("/api/v1/agents/mode", tags=["AI Agents"])
async def set_operation_mode(mode: str):
    """
    Устанавливает режим работы системы.

    Режимы:
    - full_ai: Все агенты активны, LLM работает
    - safe: Только Watcher + Guardian, без Strategist
    - manual: Только мониторинг, без автодействий
    - simulation: Режим симуляции (Fake NINA/PHD2)
    """
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
    """Возвращает последние решения агентов из Decision Audit Trail."""
    return {
        "decisions": orchestrator.get_recent_decisions(limit=limit),
        "total": len(orchestrator._decisions_log),
    }


@app.get("/api/v1/agents/llm-status", tags=["AI Agents"])
async def get_llm_status():
    """Проверяет доступность локального LLM (Ollama)."""
    return {
        "available": llm_client.is_available(),
        "model": settings.ai_settings.model_name,
        "host": settings.ai_settings.ollama_host,
    }


@app.post("/api/v1/agents/test-llm", tags=["AI Agents"])
async def test_llm_generation(
    prompt: str = Query(..., description="Тестовый промпт"),
    agent: str = Query("Copilot", description="Имя агента для системного промпта"),
):
    """Тестовый эндпоинт для проверки генерации LLM."""
    if not llm_client.is_available():
        raise HTTPException(status_code=503, detail="LLM (Ollama) is not available")

    response = await llm_client.generate(
        agent_name=agent,
        prompt=prompt,
        max_tokens=500,
    )

    return {"prompt": prompt, "agent": agent, "response": response}


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
            "API",
            f"Fire Trigger: {request.trigger_name}",
            request.reason,
            "Success",
        )
        return {"status": "success", "message": f"Trigger {request.trigger_name} fired"}
    else:
        raise HTTPException(
            status_code=400,
            detail="Trigger blocked by HAL, FLAT_MODE or not available",
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
            "API",
            f"Set Var: {request.name}={request.value}",
            request.reason,
            "Success",
        )
        return {"status": "success", "message": f"Variable {request.name} updated"}
    else:
        raise HTTPException(
            status_code=400,
            detail="Variable change blocked by HAL or critical phase",
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
    return {
        "query": request.query,
        "results_count": len(results),
        "results": results,
    }


@app.get("/api/v1/rag/context", tags=["RAG Engine"])
async def rag_get_context(
    query: str = Query(..., description="Поисковый запрос"),
    max_tokens: int = Query(2000, description="Максимальное количество токенов"),
):
    """
    Получает контекст для LLM на основе запроса.
    Используется AI-агентами для принятия решений.
    """
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
# MASTERS LIBRARY ENDPOINTS
# ============================================================================
@app.get("/api/v1/masters/catalog", tags=["Masters Library"])
async def get_masters_catalog():
    """
    Возвращает каталог доступных мастер-кадров (Bias/Dark/Flat).
    Используется AI-агентами для подбора калибровок и Frontend для отображения библиотек.
    """
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
    exposure: Optional[float] = Query(None, description="Время экспозиции (DARK)"),
    gain: Optional[int] = Query(None, description="Gain"),
    offset: Optional[int] = Query(None, description="Offset"),
    filter_name: Optional[str] = Query(None, description="Имя фильтра (FLAT)"),
    temp_tolerance: float = Query(2.0, description="Допуск по температуре"),
):
    """
    Ищет наиболее подходящий мастер-кадр по параметрам.
    Используется Calibrator Agent для подбора калибровок.
    """
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
    """
    Запускает pre-flight проверку перед стартом сессии.
    Проверяет 8 gates: Weather, Hardware, Calibration, DiskSpace, APIHealth,
    SafetyMonitor, Sequence, Mode.
    """
    report = await preflight_checker.run_all()
    return report


# ============================================================================
# SECURITY ENDPOINTS (Credential Vault)
# ============================================================================
@app.get("/api/v1/security/vault", tags=["Security"])
async def list_vault_secrets():
    """Возвращает список всех секретов в Vault (без значений)."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")
    return {
        "secrets": credential_vault.list_secrets(),
        "stats": credential_vault.get_stats(),
    }


class SecretRequest(BaseModel):
    name: str
    value: str
    description: Optional[str] = None


@app.post("/api/v1/security/vault/store", tags=["Security"])
async def store_secret(request: SecretRequest):
    """Сохраняет секрет в Vault."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")

    success = credential_vault.store_secret(
        name=request.name,
        value=request.value,
        description=request.description,
    )

    if success:
        return {"status": "success", "message": f"Secret '{request.name}' stored"}
    else:
        raise HTTPException(status_code=500, detail="Failed to store secret")


@app.get("/api/v1/security/vault/{name}", tags=["Security"])
async def get_secret(name: str):
    """Извлекает секрет из Vault (требует аутентификации в production)."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")

    value = credential_vault.get_secret(name)
    if value is None:
        raise HTTPException(status_code=404, detail=f"Secret '{name}' not found")

    return {"name": name, "value": value}


@app.delete("/api/v1/security/vault/{name}", tags=["Security"])
async def delete_secret(name: str):
    """Удаляет секрет из Vault."""
    if not credential_vault:
        raise HTTPException(status_code=503, detail="Credential Vault not initialized")

    success = credential_vault.delete_secret(name)
    if success:
        return {"status": "success", "message": f"Secret '{name}' deleted"}
    else:
        raise HTTPException(status_code=404, detail=f"Secret '{name}' not found")


# ============================================================================
# STORAGE ENDPOINTS (Disk Monitor)
# ============================================================================
@app.get("/api/v1/storage/disk-usage", tags=["Storage"])
async def get_disk_usage():
    """Возвращает информацию об использовании дискового пространства."""
    return await disk_monitor.get_stats()


@app.post("/api/v1/storage/cleanup", tags=["Storage"])
async def apply_retention_policy(
    policy_name: str = Query(..., description="Имя политики"),
):
    """
    Применяет политику удаления старых данных.

    Доступные политики:
    - keep_last_30_days: Хранить сессии за последние 30 дней
    - keep_best_quality: Хранить только сессии с quality_score > 8.0
    - aggressive_cleanup: Удалить RAW, оставить только стеки (7 дней)
    """
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
    """
    Запускает симуляцию сессии (Fake NINA).
    Используется для тестирования AI-агентов без реального оборудования.
    """
    from app.simulation.fake_nina import fake_nina

    await fake_nina.start()
    await fake_nina.start_sequence(target=target, frames=frames)

    # Переключаем Mode Manager в SIMULATION
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

    # Возвращаемся в FULL_AI
    await mode_manager.set_mode(
        OperationMode.FULL_AI, reason="Simulation stopped via API"
    )

    return {"status": "success", "message": "Simulation stopped"}


@app.post("/api/v1/simulation/inject-anomaly", tags=["Simulation"])
async def inject_anomaly(
    anomaly_type: str = Query(..., description="Тип аномалии"),
):
    """
    Инжектирует аномалию для тестирования агентов.

    Доступные типы:
    - hfr_spike: Резкий рост HFR
    - rms_spike: Резкий рост RMS гидирования
    - temp_drift: Дрейф температуры
    - guiding_lost: Потеря гидирования
    - safety_unsafe: Safety Monitor UNSAFE
    """
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

    return {
        "status": "success",
        "message": f"Anomaly '{anomaly_type}' injected",
    }


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
