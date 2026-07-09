"""
Hybrid LangGraph Orchestrator
Гибридный оркестратор, объединяющий Complex Diagnostic, Post-Mortem Analysis
и Adaptive Response workflows в единую систему.
Архитектура:
- Автономное выполнение многошаговых workflows
- Периодическая синхронизация с Event-Driven оркестратором
- Интеграция с Decision Audit Trail
- Визуализация через Mermaid (опционально)
ИСПРАВЛЕНО (v4.1): Добавлен импорт state_tracker (был NameError)
"""

from typing import Dict, Any, List, Optional, TypedDict, Literal
from enum import Enum
import asyncio
from datetime import datetime
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, AIMessage
import logging
from app.agents.base_agent import AgentDecision
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.storage.decision_audit import decision_audit
from app.shadow_engine.state_tracker import state_tracker  # ← ДОБАВЛЕНО (v4.1)

logger = logging.getLogger(__name__)


class WorkflowType(str, Enum):
    """Типы поддерживаемых workflows"""

    DIAGNOSTIC = "diagnostic"
    POST_MORTEM = "post_mortem"
    ADAPTIVE = "adaptive"


class WorkflowStatus(str, Enum):
    """Статусы выполнения workflow"""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class HybridWorkflowState(TypedDict):
    """Состояние гибридного workflow."""

    workflow_id: str
    workflow_type: WorkflowType
    status: WorkflowStatus
    created_at: str
    updated_at: str
    trigger_event: Dict[str, Any]
    context: Dict[str, Any]
    symptoms: List[str]
    root_causes: List[str]
    diagnostic_confidence: float
    session_id: Optional[str]
    session_summary: Optional[Dict[str, Any]]
    lessons_learned: List[str]
    current_conditions: Dict[str, Any]
    adaptation_actions: List[Dict[str, Any]]
    monitoring_metrics: List[Dict[str, Any]]
    recommendations: List[str]
    executed_actions: List[Dict[str, Any]]
    final_outcome: Optional[str]
    retry_count: int
    max_retries: int
    errors: List[str]


class HybridLangGraphOrchestrator:
    """
    Гибридный LangGraph оркестратор.
    """

    def __init__(self):
        self.graph = self._build_graph()
        self.active_workflows: Dict[str, HybridWorkflowState] = {}
        logger.info("✅ Hybrid LangGraph Orchestrator initialized")

    def _build_graph(self) -> StateGraph:
        """Построение графа workflow"""
        workflow = StateGraph(HybridWorkflowState)
        workflow.add_node("analyze_context", self._analyze_context_node)
        workflow.add_node("route_workflow", self._route_workflow_node)
        workflow.add_node("diagnostic_analysis", self._diagnostic_analysis_node)
        workflow.add_node("post_mortem_analysis", self._post_mortem_analysis_node)
        workflow.add_node("adaptive_response", self._adaptive_response_node)
        workflow.add_node(
            "generate_recommendations", self._generate_recommendations_node
        )
        workflow.add_node("execute_actions", self._execute_actions_node)
        workflow.add_node("monitor_results", self._monitor_results_node)
        workflow.add_node("retry_decision", self._retry_decision_node)
        workflow.add_node("finalize", self._finalize_node)

        workflow.set_entry_point("analyze_context")
        workflow.add_edge("analyze_context", "route_workflow")
        workflow.add_conditional_edges(
            "route_workflow",
            self._decide_workflow_type,
            {
                "diagnostic": "diagnostic_analysis",
                "post_mortem": "post_mortem_analysis",
                "adaptive": "adaptive_response",
            },
        )
        workflow.add_edge("diagnostic_analysis", "generate_recommendations")
        workflow.add_edge("post_mortem_analysis", "generate_recommendations")
        workflow.add_edge("adaptive_response", "generate_recommendations")
        workflow.add_edge("generate_recommendations", "execute_actions")
        workflow.add_edge("execute_actions", "monitor_results")
        workflow.add_conditional_edges(
            "monitor_results",
            self._decide_next_step,
            {
                "success": "finalize",
                "retry": "retry_decision",
                "fail": "finalize",
            },
        )
        workflow.add_conditional_edges(
            "retry_decision",
            self._should_retry,
            {
                "retry": "execute_actions",
                "give_up": "finalize",
            },
        )
        workflow.add_edge("finalize", END)
        return workflow.compile()

    async def _analyze_context_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"🔍 Analyzing context for workflow {state['workflow_id']}")
        current_state = observatory_state.get_full_state()
        state["context"].update(
            {
                "observatory_state": current_state,
                "analysis_timestamp": datetime.now().isoformat(),
            }
        )
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _route_workflow_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(
            f"🛣️ Routing workflow {state['workflow_id']} "
            f"(type: {state['workflow_type']})"
        )
        return state

    async def _diagnostic_analysis_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"🔬 Running diagnostic analysis for {state['workflow_id']}")
        from app.agents.diagnostician_agent import DiagnosticianAgent
        from app.agents.base_agent import AgentContext

        diagnostician = DiagnosticianAgent()
        context = AgentContext(
            current_metrics=observatory_state.current_metrics,
            weather=observatory_state.weather,
            astronomy=observatory_state.astronomy,
            sequence_state=state_tracker.get_state(),
            safety_status=observatory_state.safety_status,
            active_alerts=observatory_state.active_alerts,
            custom_data={"trigger": state["trigger_event"]},
        )
        analysis_result = await diagnostician.analyze(context)
        if analysis_result and analysis_result.outputs:
            state["symptoms"] = analysis_result.outputs.get("symptoms", [])
            state["root_causes"] = analysis_result.outputs.get("root_causes", [])
            state["diagnostic_confidence"] = analysis_result.outputs.get(
                "confidence", 0.5
            )
        else:
            state["symptoms"] = ["Unknown symptom"]
            state["root_causes"] = ["Root cause analysis failed"]
            state["diagnostic_confidence"] = 0.0
            state["errors"].append("Diagnostic analysis failed")
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _post_mortem_analysis_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"📊 Running post-mortem analysis for {state['workflow_id']}")
        from app.agents.auditor_agent import AuditorAgent
        from app.agents.base_agent import AgentContext

        auditor = AuditorAgent()
        session_id = state["context"].get("session_id", "unknown")
        state["session_id"] = session_id
        context = AgentContext(
            current_metrics=observatory_state.current_metrics,
            weather=observatory_state.weather,
            astronomy=observatory_state.astronomy,
            sequence_state=state_tracker.get_state(),
            safety_status=observatory_state.safety_status,
            active_alerts=[],
            custom_data={"session_id": session_id, "context": state["context"]},
        )
        audit_result = await auditor.analyze(context)
        if audit_result and audit_result.outputs:
            state["session_summary"] = audit_result.outputs.get("summary", {})
            state["lessons_learned"] = audit_result.outputs.get("lessons_learned", [])
        else:
            state["session_summary"] = {"error": "Post-mortem analysis failed"}
            state["lessons_learned"] = []
            state["errors"].append("Post-mortem analysis failed")
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _adaptive_response_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"🔄 Running adaptive response for {state['workflow_id']}")
        state["current_conditions"] = {
            "weather": observatory_state.weather,
            "equipment_status": observatory_state.current_metrics,
            "sequence_progress": state_tracker.get_state(),
        }
        if state["current_conditions"]["weather"].get("cloud_cover", 0) > 70:
            state["adaptation_actions"].append(
                {
                    "action": "pause_sequence",
                    "reason": "High cloud cover detected",
                    "priority": "high",
                }
            )
        if state["current_conditions"]["weather"].get("wind_speed", 0) > 15:
            state["adaptation_actions"].append(
                {
                    "action": "park_mount_if_critical",
                    "reason": "High wind speed detected",
                    "priority": "high",
                }
            )
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _generate_recommendations_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"💡 Generating recommendations for {state['workflow_id']}")
        recommendations = []
        if state["workflow_type"] == WorkflowType.DIAGNOSTIC:
            for cause in state.get("root_causes", []):
                recommendations.append(f"Address root cause: {cause}")
        elif state["workflow_type"] == WorkflowType.POST_MORTEM:
            for lesson in state.get("lessons_learned", []):
                recommendations.append(f"Apply lesson: {lesson}")
        elif state["workflow_type"] == WorkflowType.ADAPTIVE:
            for action in state.get("adaptation_actions", []):
                recommendations.append(f"Execute adaptation: {action['action']}")
        state["recommendations"] = recommendations
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _execute_actions_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"⚡ Executing actions for {state['workflow_id']}")
        executed = []
        for recommendation in state.get("recommendations", []):
            action_record = {
                "recommendation": recommendation,
                "executed_at": datetime.now().isoformat(),
                "status": "simulated",
            }
            executed.append(action_record)
            await event_bus.publish(
                "WORKFLOW_ACTION_EXECUTED",
                {"workflow_id": state["workflow_id"], "action": action_record},
            )
        state["executed_actions"] = executed
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _monitor_results_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"📈 Monitoring results for {state['workflow_id']}")
        metric = {
            "timestamp": datetime.now().isoformat(),
            "outcome": "success",
            "metrics": {},
        }
        state["monitoring_metrics"].append(metric)
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _retry_decision_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        logger.info(f"🔁 Retry decision for {state['workflow_id']}")
        state["retry_count"] += 1
        state["updated_at"] = datetime.now().isoformat()
        return state

    async def _finalize_node(self, state: HybridWorkflowState) -> HybridWorkflowState:
        logger.info(f"✅ Finalizing workflow {state['workflow_id']}")
        if state["errors"]:
            state["final_outcome"] = "failed"
            state["status"] = WorkflowStatus.FAILED
        else:
            state["final_outcome"] = "success"
            state["status"] = WorkflowStatus.COMPLETED
        state["updated_at"] = datetime.now().isoformat()
        decision = AgentDecision(
            agent="HybridLangGraphOrchestrator",
            decision_type=f"WORKFLOW_{state['workflow_type'].upper()}_COMPLETED",
            inputs={"trigger": state["trigger_event"]},
            outputs={
                "workflow_id": state["workflow_id"],
                "recommendations": state["recommendations"],
                "executed_actions": state["executed_actions"],
                "final_outcome": state["final_outcome"],
            },
            rationale=f"Hybrid {state['workflow_type']} workflow completed",
            confidence=0.8 if state["final_outcome"] == "success" else 0.3,
        )
        await decision_audit.log_decision(decision)
        await event_bus.publish(
            "WORKFLOW_COMPLETED",
            {
                "workflow_id": state["workflow_id"],
                "workflow_type": state["workflow_type"],
                "status": state["status"],
                "outcome": state["final_outcome"],
                "recommendations": state["recommendations"],
            },
        )
        return state

    def _decide_workflow_type(self, state: HybridWorkflowState) -> str:
        return state["workflow_type"].value

    def _decide_next_step(self, state: HybridWorkflowState) -> str:
        if not state["monitoring_metrics"]:
            return "fail"
        last_metric = state["monitoring_metrics"][-1]
        outcome = last_metric.get("outcome", "unknown")
        if outcome == "success":
            return "success"
        elif outcome == "partial":
            return "retry"
        else:
            return "fail"

    def _should_retry(self, state: HybridWorkflowState) -> str:
        if state["retry_count"] < state["max_retries"]:
            return "retry"
        else:
            return "give_up"

    async def start_workflow(
        self,
        workflow_type: WorkflowType,
        trigger_event: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> str:
        workflow_id = f"workflow_{workflow_type.value}_{datetime.now().timestamp()}"
        initial_state: HybridWorkflowState = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type,
            "status": WorkflowStatus.RUNNING,
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat(),
            "trigger_event": trigger_event,
            "context": context or {},
            "symptoms": [],
            "root_causes": [],
            "diagnostic_confidence": 0.0,
            "session_id": None,
            "session_summary": None,
            "lessons_learned": [],
            "current_conditions": {},
            "adaptation_actions": [],
            "monitoring_metrics": [],
            "recommendations": [],
            "executed_actions": [],
            "final_outcome": None,
            "retry_count": 0,
            "max_retries": max_retries,
            "errors": [],
        }
        self.active_workflows[workflow_id] = initial_state
        logger.info(f"🚀 Starting workflow {workflow_id} (type: {workflow_type.value})")
        asyncio.create_task(self._run_workflow(workflow_id, initial_state))
        return workflow_id

    async def _run_workflow(self, workflow_id: str, initial_state: HybridWorkflowState):
        try:
            final_state = await self.graph.ainvoke(initial_state)
            self.active_workflows[workflow_id] = final_state
            logger.info(
                f"✅ Workflow {workflow_id} completed with status: {final_state['status']}"
            )
        except Exception as e:
            logger.error(f"❌ Workflow {workflow_id} failed: {e}", exc_info=True)
            if workflow_id in self.active_workflows:
                self.active_workflows[workflow_id]["status"] = WorkflowStatus.FAILED
                self.active_workflows[workflow_id]["errors"].append(str(e))

    def get_workflow_status(self, workflow_id: str) -> Optional[HybridWorkflowState]:
        return self.active_workflows.get(workflow_id)

    def list_active_workflows(self) -> List[str]:
        return [
            wf_id
            for wf_id, state in self.active_workflows.items()
            if state["status"] == WorkflowStatus.RUNNING
        ]


# Singleton instance
hybrid_orchestrator = HybridLangGraphOrchestrator()
