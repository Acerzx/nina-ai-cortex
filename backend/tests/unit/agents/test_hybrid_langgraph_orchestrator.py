"""
Unit tests для HybridLangGraphOrchestrator.
Покрывает: создание графа, передачу AgentContext, retry logic, финализацию.
"""

import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime
from app.agents.hybrid_langgraph_orchestrator import (
    HybridLangGraphOrchestrator,
    WorkflowType,
    WorkflowStatus,
)


class TestHybridLangGraphOrchestrator:
    """Тесты HybridLangGraphOrchestrator."""

    @pytest.fixture
    def orchestrator(self):
        """Создаёт тестовый оркестратор."""
        orch = HybridLangGraphOrchestrator()
        orch._initialized = True
        return orch

    @pytest.mark.asyncio
    async def test_workflow_state_initialization(self, orchestrator):
        """Проверка начального состояния workflow."""
        state = orchestrator._create_initial_state(
            workflow_type=WorkflowType.DIAGNOSTIC,
            trigger_event={"type": "HFR_degradation"},
            context={"metric": "hfr"},
            max_retries=2,
        )

        assert state["workflow_type"] == WorkflowType.DIAGNOSTIC
        assert state["trigger_event"]["type"] == "HFR_degradation"
        assert state["retry_count"] == 0
        assert state["max_retries"] == 2
        assert state["status"] == WorkflowStatus.RUNNING
        assert isinstance(state["errors"], list)

    @pytest.mark.asyncio
    async def test_retry_decision_logic(self, orchestrator):
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
    async def test_finalize_node_with_errors(self, orchestrator):
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
    async def test_finalize_node_success(self, orchestrator):
        """Успешная финализация."""
        state = {
            "errors": [],
            "executed_actions": [{"action": "test", "status": "success"}],
            "updated_at": datetime.now().isoformat(),
        }

        result = await orchestrator._finalize_node(state)
        assert result["final_outcome"] == "success"
        assert result["status"] == WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_decide_workflow_type_diagnostic(self, orchestrator):
        """Определение типа workflow: diagnostic."""
        state = {"workflow_type": WorkflowType.DIAGNOSTIC}

        wf_type = orchestrator._decide_workflow_type(state)
        assert wf_type == "diagnostic"

    @pytest.mark.asyncio
    async def test_decide_workflow_type_post_mortem(self, orchestrator):
        """Определение типа workflow: post_mortem."""
        state = {"workflow_type": WorkflowType.POST_MORTEM}

        wf_type = orchestrator._decide_workflow_type(state)
        assert wf_type == "post_mortem"

    @pytest.mark.asyncio
    async def test_decide_workflow_type_adaptive(self, orchestrator):
        """Определение типа workflow: adaptive."""
        state = {"workflow_type": WorkflowType.ADAPTIVE}

        wf_type = orchestrator._decide_workflow_type(state)
        assert wf_type == "adaptive"

    @pytest.mark.asyncio
    async def test_decide_next_step_success(self, orchestrator):
        """Решение о следующем шаге при успехе."""
        state = {
            "monitoring_metrics": [
                {"outcome": "success", "timestamp": datetime.now().isoformat()}
            ]
        }

        next_step = orchestrator._decide_next_step(state)
        assert next_step == "success"

    @pytest.mark.asyncio
    async def test_decide_next_step_retry(self, orchestrator):
        """Решение о следующем шаге при partial."""
        state = {
            "monitoring_metrics": [
                {"outcome": "partial", "timestamp": datetime.now().isoformat()}
            ]
        }

        next_step = orchestrator._decide_next_step(state)
        assert next_step == "retry"

    @pytest.mark.asyncio
    async def test_decide_next_step_fail(self, orchestrator):
        """Решение о следующем шаге при провале."""
        state = {
            "monitoring_metrics": [
                {"outcome": "failed", "timestamp": datetime.now().isoformat()}
            ]
        }

        next_step = orchestrator._decide_next_step(state)
        assert next_step == "fail"

    @pytest.mark.asyncio
    async def test_start_workflow(self, orchestrator):
        """Запуск нового workflow."""
        workflow_id = await orchestrator.start_workflow(
            workflow_type=WorkflowType.DIAGNOSTIC,
            trigger_event={"type": "test"},
            context={"source": "test"},
            max_retries=3,
        )

        assert workflow_id is not None
        assert workflow_id.startswith("workflow_diagnostic_")
        assert workflow_id in orchestrator.active_workflows

    @pytest.mark.asyncio
    async def test_get_workflow_status(self, orchestrator):
        """Получение статуса workflow."""
        workflow_id = await orchestrator.start_workflow(
            workflow_type=WorkflowType.DIAGNOSTIC,
            trigger_event={"type": "test"},
            context={},
            max_retries=2,
        )

        state = orchestrator.get_workflow_status(workflow_id)

        assert state is not None
        assert state["workflow_id"] == workflow_id
        assert state["workflow_type"] == WorkflowType.DIAGNOSTIC

    @pytest.mark.asyncio
    async def test_get_workflow_status_not_found(self, orchestrator):
        """Статус несуществующего workflow."""
        state = orchestrator.get_workflow_status("nonexistent_id")
        assert state is None

    @pytest.mark.asyncio
    async def test_list_active_workflows(self, orchestrator):
        """Список активных workflows."""
        # Запускаем 3 workflow
        for i in range(3):
            await orchestrator.start_workflow(
                workflow_type=WorkflowType.DIAGNOSTIC,
                trigger_event={"type": f"test_{i}"},
                context={},
                max_retries=2,
            )

        active = orchestrator.list_active_workflows()

        assert len(active) == 3

    @pytest.mark.asyncio
    async def test_diagnostic_analysis_node_with_mock(self, orchestrator):
        """Тест diagnostic_analysis_node с моком Diagnostician."""
        state = {
            "workflow_id": "test_wf",
            "workflow_type": WorkflowType.DIAGNOSTIC,
            "trigger_event": {"type": "HFR_degradation"},
            "context": {"metric": "hfr"},
            "symptoms": [],
            "root_causes": [],
            "diagnostic_confidence": 0.0,
            "errors": [],
            "updated_at": datetime.now().isoformat(),
        }

        # Мокаем Diagnostician Agent
        with patch(
            "app.agents.hybrid_langgraph_orchestrator.DiagnosticianAgent"
        ) as MockDiag:
            mock_instance = MockDiag.return_value
            mock_decision = MagicMock()
            mock_decision.outputs = {
                "symptoms": ["HFR degradation"],
                "root_causes": ["Temperature drift"],
                "confidence": 0.85,
            }
            mock_instance.analyze = AsyncMock(return_value=mock_decision)

            result = await orchestrator._diagnostic_analysis_node(state)

            assert result["symptoms"] == ["HFR degradation"]
            assert result["root_causes"] == ["Temperature drift"]
            assert result["diagnostic_confidence"] == 0.85

    @pytest.mark.asyncio
    async def test_generate_recommendations_node(self, orchestrator):
        """Тест генерации рекомендаций."""
        state = {
            "workflow_type": WorkflowType.DIAGNOSTIC,
            "root_causes": ["Temperature drift", "Wind load"],
            "lessons_learned": [],
            "adaptation_actions": [],
            "recommendations": [],
            "updated_at": datetime.now().isoformat(),
        }

        result = await orchestrator._generate_recommendations_node(state)

        assert len(result["recommendations"]) == 2
        assert "Temperature drift" in result["recommendations"][0]
        assert "Wind load" in result["recommendations"][1]

    @pytest.mark.asyncio
    async def test_graph_structure(self, orchestrator):
        """Проверка структуры графа."""
        assert orchestrator.graph is not None

        # Проверяем наличие основных узлов
        graph_dict = orchestrator.graph.get_graph()
        assert "analyze_context" in graph_dict
        assert "route_workflow" in graph_dict
        assert "diagnostic_analysis" in graph_dict
        assert "post_mortem_analysis" in graph_dict
        assert "adaptive_response" in graph_dict
        assert "generate_recommendations" in graph_dict
        assert "execute_actions" in graph_dict
        assert "monitor_results" in graph_dict
        assert "retry_decision" in graph_dict
        assert "finalize" in graph_dict
