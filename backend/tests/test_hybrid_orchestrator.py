"""
Тесты для HybridLangGraphOrchestrator.
Покрывает: создание графа, передачу AgentContext, retry logic, финализацию.
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from app.agents.hybrid_langgraph_orchestrator import (
    HybridLangGraphOrchestrator,
    WorkflowStatus,
)


@pytest.fixture
def orchestrator():
    orch = HybridLangGraphOrchestrator()
    orch._initialized = True
    return orch


@pytest.mark.asyncio
async def test_workflow_state_initialization(orchestrator):
    """Проверка начального состояния workflow."""
    state = orchestrator._create_initial_state(
        workflow_type="diagnostic",
        trigger_event="HFR_degradation",
        context={"metric": "hfr"},
        max_retries=2,
    )

    assert state["workflow_type"] == "diagnostic"
    assert state["trigger_event"] == "HFR_degradation"
    assert state["retry_count"] == 0
    assert state["max_retries"] == 2
    assert state["status"] == WorkflowStatus.RUNNING
    assert isinstance(state["errors"], list)


@pytest.mark.asyncio
async def test_retry_decision_logic(orchestrator):
    """Проверка логики retry."""
    state = {
        "retry_count": 0,
        "max_retries": 2,
        "updated_at": datetime.now().isoformat(),
    }

    # Первый вызов: должен вернуть retry
    result_state = await orchestrator._retry_decision_node(state)
    assert result_state["retry_count"] == 1

    # Проверка условия retry
    decision = orchestrator._should_retry(result_state)
    assert decision == "retry"

    # Исчерпание попыток
    result_state["retry_count"] = 2
    decision = orchestrator._should_retry(result_state)
    assert decision == "give_up"


@pytest.mark.asyncio
async def test_finalize_node_with_errors(orchestrator):
    """Финализация при наличии ошибок должна ставить статус FAILED."""
    state = {
        "errors": ["Test error"],
        "executed_actions": [],
        "updated_at": datetime.now().isoformat(),
    }

    result = await orchestrator._finalize_node(state)
    assert result["final_outcome"] == "failed"
    assert result["status"] == WorkflowStatus.FAILED


@pytest.mark.asyncio
async def test_finalize_node_success(orchestrator):
    """Успешная финализация."""
    state = {
        "errors": [],
        "executed_actions": [{"action": "test", "status": "success"}],
        "updated_at": datetime.now().isoformat(),
    }

    result = await orchestrator._finalize_node(state)
    assert result["final_outcome"] == "success"
    assert result["status"] == WorkflowStatus.COMPLETED
