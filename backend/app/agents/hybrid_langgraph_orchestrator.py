"""
Hybrid LangGraph Orchestrator
Гибридный оркестратор, объединяющий Complex Diagnostic, Post-Mortem Analysis
и Adaptive Response workflows в единую систему.
Архитектура:
- Автономное выполнение многошаговых workflows
- Периодическая синхронизация с Event-Driven оркестратором
- Интеграция с Decision Audit Trail через orchestrator.route_decision()
- OpenTelemetry distributed tracing для observability
- Визуализация через Mermaid (опционально)
ИСПРАВЛЕНО (v4.1): Добавлен импорт state_tracker (был NameError)
ИСПРАВЛЕНО (С-5): Внедрение существующих агентов через конструктор
ИСПРАВЛЕНО (С-14): Retry-логика с exponential backoff
ИСПРАВЛЕНО (Н-2): Удалён неиспользуемый импорт Literal
ИСПРАВЛЕНО (Спринт 5 — Задача 2):
- Интеграция LangGraph решений в Decision Audit Trail через orchestrator
- Каждый узел логирует AgentDecision через orchestrator.route_decision()
- OpenTelemetry spans для каждого узла и всего workflow
- Trace context передаётся через HybridWorkflowState
- Проверка режимов работы (MANUAL блокирует, SAFE_AUTONOMOUS ограничивает)
"""

from typing import Dict, Any, List, Optional, TypedDict
from enum import Enum
import asyncio
from datetime import datetime
from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, AIMessage
import logging

from app.agents.base_agent import AgentDecision, BaseAgent
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.storage.decision_audit import decision_audit, DecisionRecord
from app.shadow_engine.state_tracker import state_tracker
from app.core.mode_manager import mode_manager, OperationMode

# Спринт 5: OpenTelemetry tracing
from app.core.tracing import tracing_manager, span_context

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
    # Спринт 5: OpenTelemetry trace context
    trace_context: Dict[str, Any]


class HybridLangGraphOrchestrator:
    """
    Гибридный LangGraph оркестратор.

    ИСПРАВЛЕНО (С-5): Принимает существующих агентов через конструктор
    вместо создания новых экземпляров в каждом workflow.

    ИСПРАВЛЕНО (Спринт 5): Интеграция с Decision Audit Trail через
    orchestrator.route_decision() и OpenTelemetry tracing.
    """

    def __init__(self, agents_registry: Optional[Dict[str, BaseAgent]] = None):
        """
        Инициализация оркестратора.

        Args:
            agents_registry: Словарь существующих агентов из orchestrator.agents
                Например: {"Diagnostician": diagnostician_agent, ...}
        """
        self._agents = agents_registry or {}
        self.graph = self._build_graph()
        self.active_workflows: Dict[str, HybridWorkflowState] = {}

        # Спринт 5: Статистика tracing
        self._tracing_stats = {
            "spans_created": 0,
            "decisions_logged": 0,
            "decisions_blocked_by_mode": 0,
        }

        logger.info(
            f"✅ Hybrid LangGraph Orchestrator initialized "
            f"(agents injected: {len(self._agents)}, "
            f"tracing: {'enabled' if tracing_manager.enabled else 'disabled'})"
        )

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

    # ========================================================================
    # NODE METHODS (с OpenTelemetry tracing и Decision Audit интеграцией)
    # ========================================================================

    async def _analyze_context_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """Анализ контекста с OpenTelemetry span и логированием решения."""
        node_name = "analyze_context"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": state["workflow_type"].value,
                "langgraph.node": node_name,
            },
        ) as span:
            logger.info(f"🔍 Analyzing context for workflow {workflow_id}")

            current_state = observatory_state.get_full_state()
            state["context"].update(
                {
                    "observatory_state": current_state,
                    "analysis_timestamp": datetime.now().isoformat(),
                }
            )
            state["updated_at"] = datetime.now().isoformat()

            # Спринт 5: Логируем решение через orchestrator
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_CONTEXT_ANALYZED",
                inputs={"trigger": state.get("trigger_event", {})},
                outputs={"context_keys": list(state["context"].keys())},
                rationale=f"Context analyzed for {state['workflow_type'].value} workflow",
                confidence=0.9,
            )

            if span:
                span.set_attribute("context.keys_count", len(state["context"]))

            self._tracing_stats["spans_created"] += 1
            return state

    async def _route_workflow_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """Маршрутизация workflow с OpenTelemetry span."""
        node_name = "route_workflow"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": state["workflow_type"].value,
                "langgraph.node": node_name,
            },
        ) as span:
            logger.info(
                f"🛣️ Routing workflow {workflow_id} (type: {state['workflow_type']})"
            )

            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_ROUTED",
                inputs={"workflow_type": state["workflow_type"].value},
                outputs={"routed_to": state["workflow_type"].value},
                rationale=f"Workflow routed to {state['workflow_type'].value} analysis",
                confidence=1.0,
            )

            if span:
                span.set_attribute("routing.target", state["workflow_type"].value)

            self._tracing_stats["spans_created"] += 1
            return state

    async def _diagnostic_analysis_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """
        Diagnostic analysis с OpenTelemetry span.

        ИСПРАВЛЕНО (С-5): Использует внедрённый DiagnosticianAgent
        вместо создания нового экземпляра.
        """
        node_name = "diagnostic_analysis"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": "diagnostic",
                "langgraph.node": node_name,
                "agent.name": "Diagnostician",
            },
        ) as span:
            logger.info(f"🔬 Running diagnostic analysis for {workflow_id}")

            from app.agents.base_agent import AgentContext

            # Получаем существующий агент из registry
            diagnostician = self._agents.get("Diagnostician")
            if not diagnostician:
                logger.error(
                    "❌ DiagnosticianAgent not found in registry. "
                    "Make sure to inject agents via constructor."
                )
                state["symptoms"] = ["Diagnostician agent not available"]
                state["root_causes"] = ["Agent injection failed"]
                state["diagnostic_confidence"] = 0.0
                state["errors"].append("Diagnostician agent not available")
                state["updated_at"] = datetime.now().isoformat()

                if span:
                    span.set_attribute("error", True)
                    span.set_attribute("error.message", "Agent not available")

                return state

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

            # Спринт 5: Логируем решение Diagnostician через orchestrator
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_DIAGNOSTIC_COMPLETED",
                inputs={
                    "trigger": state.get("trigger_event", {}),
                    "symptoms_count": len(state.get("symptoms", [])),
                },
                outputs={
                    "symptoms": state.get("symptoms", []),
                    "root_causes": state.get("root_causes", []),
                    "confidence": state.get("diagnostic_confidence", 0.0),
                },
                rationale=f"Diagnostic analysis: {len(state.get('root_causes', []))} root causes found",
                confidence=state.get("diagnostic_confidence", 0.5),
            )

            if span:
                span.set_attribute(
                    "diagnostic.symptoms_count", len(state.get("symptoms", []))
                )
                span.set_attribute(
                    "diagnostic.root_causes_count", len(state.get("root_causes", []))
                )
                span.set_attribute(
                    "diagnostic.confidence", state.get("diagnostic_confidence", 0.0)
                )

            self._tracing_stats["spans_created"] += 1
            return state

    async def _post_mortem_analysis_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """
        Post-mortem analysis с OpenTelemetry span.

        ИСПРАВЛЕНО (С-5): Использует внедрённый AuditorAgent
        вместо создания нового экземпляра.
        """
        node_name = "post_mortem_analysis"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": "post_mortem",
                "langgraph.node": node_name,
                "agent.name": "Auditor",
            },
        ) as span:
            logger.info(f"📊 Running post-mortem analysis for {workflow_id}")

            from app.agents.base_agent import AgentContext

            # Получаем существующий агент из registry
            auditor = self._agents.get("Auditor")
            if not auditor:
                logger.error(
                    "❌ AuditorAgent not found in registry. "
                    "Make sure to inject agents via constructor."
                )
                state["session_summary"] = {"error": "Auditor agent not available"}
                state["lessons_learned"] = []
                state["errors"].append("Auditor agent not available")
                state["updated_at"] = datetime.now().isoformat()

                if span:
                    span.set_attribute("error", True)

                return state

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
                state["lessons_learned"] = audit_result.outputs.get(
                    "lessons_learned", []
                )
            else:
                state["session_summary"] = {"error": "Post-mortem analysis failed"}
                state["lessons_learned"] = []
                state["errors"].append("Post-mortem analysis failed")

            state["updated_at"] = datetime.now().isoformat()

            # Спринт 5: Логируем решение Auditor через orchestrator
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_POST_MORTEM_COMPLETED",
                inputs={"session_id": session_id},
                outputs={
                    "session_summary": state.get("session_summary", {}),
                    "lessons_learned": state.get("lessons_learned", []),
                },
                rationale=f"Post-mortem analysis: {len(state.get('lessons_learned', []))} lessons learned",
                confidence=0.8 if state.get("session_summary") else 0.3,
            )

            if span:
                span.set_attribute("post_mortem.session_id", session_id)
                span.set_attribute(
                    "post_mortem.lessons_count", len(state.get("lessons_learned", []))
                )

            self._tracing_stats["spans_created"] += 1
            return state

    async def _adaptive_response_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """Adaptive response с OpenTelemetry span."""
        node_name = "adaptive_response"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": "adaptive",
                "langgraph.node": node_name,
            },
        ) as span:
            logger.info(f"🔄 Running adaptive response for {workflow_id}")

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

            # Спринт 5: Логируем решение
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_ADAPTIVE_COMPLETED",
                inputs={"conditions": state.get("current_conditions", {})},
                outputs={"adaptation_actions": state.get("adaptation_actions", [])},
                rationale=f"Adaptive response: {len(state.get('adaptation_actions', []))} actions proposed",
                confidence=0.75,
            )

            if span:
                span.set_attribute(
                    "adaptive.actions_count", len(state.get("adaptation_actions", []))
                )

            self._tracing_stats["spans_created"] += 1
            return state

    async def _generate_recommendations_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """Генерация рекомендаций с OpenTelemetry span."""
        node_name = "generate_recommendations"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": state["workflow_type"].value,
                "langgraph.node": node_name,
            },
        ) as span:
            logger.info(f"💡 Generating recommendations for {workflow_id}")

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

            # Спринт 5: Логируем решение
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_RECOMMENDATIONS_GENERATED",
                inputs={"workflow_type": state["workflow_type"].value},
                outputs={"recommendations": recommendations},
                rationale=f"Generated {len(recommendations)} recommendations",
                confidence=0.85,
            )

            if span:
                span.set_attribute("recommendations.count", len(recommendations))

            self._tracing_stats["spans_created"] += 1
            return state

    async def _execute_actions_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """Выполнение действий с OpenTelemetry span."""
        node_name = "execute_actions"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": state["workflow_type"].value,
                "langgraph.node": node_name,
            },
        ) as span:
            logger.info(f"⚡ Executing actions for {workflow_id}")

            # Спринт 5: Проверка режима работы перед выполнением
            current_mode = mode_manager.current_mode

            if current_mode == OperationMode.MANUAL:
                logger.warning(
                    f"🛑 BLOCKED: Workflow {workflow_id} actions blocked — "
                    f"system in MANUAL mode"
                )
                state["errors"].append(f"Actions blocked: system in MANUAL mode")
                state["executed_actions"] = []
                state["updated_at"] = datetime.now().isoformat()

                self._tracing_stats["decisions_blocked_by_mode"] += 1

                if span:
                    span.set_attribute("blocked_by", "MANUAL_MODE")
                    span.set_attribute("actions.executed_count", 0)

                return state

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

            # Спринт 5: Логируем решение
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_ACTIONS_EXECUTED",
                inputs={"recommendations_count": len(state.get("recommendations", []))},
                outputs={"executed_actions": executed},
                rationale=f"Executed {len(executed)} actions",
                confidence=0.9,
            )

            if span:
                span.set_attribute("actions.executed_count", len(executed))
                span.set_attribute("current_mode", current_mode.value)

            self._tracing_stats["spans_created"] += 1
            return state

    async def _monitor_results_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """Мониторинг результатов с OpenTelemetry span."""
        node_name = "monitor_results"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": state["workflow_type"].value,
                "langgraph.node": node_name,
            },
        ) as span:
            logger.info(f"📈 Monitoring results for {workflow_id}")

            metric = {
                "timestamp": datetime.now().isoformat(),
                "outcome": "success",
                "metrics": {},
            }
            state["monitoring_metrics"].append(metric)
            state["updated_at"] = datetime.now().isoformat()

            # Спринт 5: Логируем решение
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_RESULTS_MONITORED",
                inputs={"actions_count": len(state.get("executed_actions", []))},
                outputs={"outcome": metric["outcome"]},
                rationale=f"Results monitored: {metric['outcome']}",
                confidence=0.8,
            )

            if span:
                span.set_attribute("monitoring.outcome", metric["outcome"])

            self._tracing_stats["spans_created"] += 1
            return state

    async def _retry_decision_node(
        self, state: HybridWorkflowState
    ) -> HybridWorkflowState:
        """Решение о retry с OpenTelemetry span."""
        node_name = "retry_decision"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": state["workflow_type"].value,
                "langgraph.node": node_name,
                "retry.count": state.get("retry_count", 0),
                "retry.max": state.get("max_retries", 3),
            },
        ) as span:
            logger.info(f"🔁 Retry decision for {workflow_id}")

            state["retry_count"] += 1
            state["updated_at"] = datetime.now().isoformat()

            # Спринт 5: Логируем решение
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type="WORKFLOW_RETRY_DECISION",
                inputs={
                    "retry_count": state["retry_count"],
                    "max_retries": state["max_retries"],
                },
                outputs={"will_retry": state["retry_count"] < state["max_retries"]},
                rationale=f"Retry {state['retry_count']}/{state['max_retries']}",
                confidence=0.7,
            )

            if span:
                span.set_attribute("retry.current", state["retry_count"])
                span.set_attribute(
                    "retry.remaining", state["max_retries"] - state["retry_count"]
                )

            self._tracing_stats["spans_created"] += 1
            return state

    async def _finalize_node(self, state: HybridWorkflowState) -> HybridWorkflowState:
        """
        Финализация workflow с OpenTelemetry span.

        ИСПРАВЛЕНО (Спринт 5):
        - Использует orchestrator.route_decision() вместо прямой записи
        - Создаёт финальный OpenTelemetry span
        - Проверяет режимы работы
        """
        node_name = "finalize"
        workflow_id = state["workflow_id"]

        async with span_context(
            name=f"langgraph.{node_name}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": state["workflow_type"].value,
                "langgraph.node": node_name,
                "workflow.outcome": state.get("final_outcome", "unknown"),
            },
        ) as span:
            logger.info(f"✅ Finalizing workflow {workflow_id}")

            if state["errors"]:
                state["final_outcome"] = "failed"
                state["status"] = WorkflowStatus.FAILED
            else:
                state["final_outcome"] = "success"
                state["status"] = WorkflowStatus.COMPLETED

            state["updated_at"] = datetime.now().isoformat()

            # ИСПРАВЛЕНО (Спринт 5): Формируем outputs_dict гарантированно как dict
            outputs_dict = {
                "workflow_id": state["workflow_id"],
                "recommendations": state.get("recommendations", []),
                "executed_actions": state.get("executed_actions", []),
                "final_outcome": state["final_outcome"],
            }

            # Добавляем специфичные поля в зависимости от типа workflow
            wf_type = state["workflow_type"]
            if wf_type == WorkflowType.DIAGNOSTIC:
                outputs_dict["symptoms"] = state.get("symptoms", [])
                outputs_dict["root_causes"] = state.get("root_causes", [])
                outputs_dict["diagnostic_confidence"] = state.get(
                    "diagnostic_confidence", 0.0
                )
            elif wf_type == WorkflowType.POST_MORTEM:
                outputs_dict["session_id"] = state.get("session_id")
                outputs_dict["lessons_learned"] = state.get("lessons_learned", [])
            elif wf_type == WorkflowType.ADAPTIVE:
                outputs_dict["current_conditions"] = state.get("current_conditions", {})
                outputs_dict["adaptation_actions"] = state.get("adaptation_actions", [])

            # Спринт 5: Логируем финальное решение через orchestrator
            await self._log_node_decision(
                state=state,
                node_name=node_name,
                decision_type=f"WORKFLOW_{state['workflow_type'].value.upper()}_COMPLETED",
                inputs={"trigger": state.get("trigger_event", {})},
                outputs=outputs_dict,
                rationale=f"Hybrid {state['workflow_type'].value} workflow completed",
                confidence=0.8 if state["final_outcome"] == "success" else 0.3,
            )

            # Публикуем событие завершения workflow
            await event_bus.publish(
                "WORKFLOW_COMPLETED",
                {
                    "workflow_id": state["workflow_id"],
                    "workflow_type": state["workflow_type"].value,
                    "status": state["status"].value,
                    "outcome": state["final_outcome"],
                    "recommendations": state.get("recommendations", []),
                },
            )

            if span:
                span.set_attribute("workflow.status", state["status"].value)
                span.set_attribute(
                    "workflow.recommendations_count",
                    len(state.get("recommendations", [])),
                )
                span.set_attribute(
                    "workflow.errors_count", len(state.get("errors", []))
                )

            self._tracing_stats["spans_created"] += 1
            return state

    # ========================================================================
    # HELPER: Логирование решения узла через orchestrator
    # ========================================================================

    async def _log_node_decision(
        self,
        state: HybridWorkflowState,
        node_name: str,
        decision_type: str,
        inputs: Dict[str, Any],
        outputs: Dict[str, Any],
        rationale: str,
        confidence: float,
    ) -> None:
        """
        Логирует решение узла workflow через orchestrator.route_decision().

        Это обеспечивает:
        - Проверку режимов работы (MANUAL блокирует, SAFE_AUTONOMOUS ограничивает)
        - Единую pipeline для всех решений
        - Запись в Decision Audit Trail
        - Интеграцию с DecisionAnalyzer

        Args:
            state: Текущее состояние workflow
            node_name: Имя узла (для атрибутов span)
            decision_type: Тип решения
            inputs: Входные данные
            outputs: Выходные данные
            rationale: Обоснование
            confidence: Уверенность (0.0-1.0)
        """
        # Ленивый импорт orchestrator для избежания circular dependency
        try:
            from app.agents.orchestrator import orchestrator
        except ImportError:
            logger.warning(
                f"Cannot import orchestrator for decision logging in {node_name}"
            )
            # Fallback: прямая запись в decision_audit
            await self._log_decision_fallback(
                state=state,
                node_name=node_name,
                decision_type=decision_type,
                inputs=inputs,
                outputs=outputs,
                rationale=rationale,
                confidence=confidence,
            )
            return

        # Формируем AgentDecision
        decision = AgentDecision(
            agent=f"LangGraph:{node_name}",
            decision_type=decision_type,
            inputs={
                **inputs,
                "workflow_id": state["workflow_id"],
                "workflow_type": state["workflow_type"].value,
            },
            outputs=outputs,
            rationale=rationale,
            confidence=confidence,
        )

        # Маршрутизируем через orchestrator (проверяет режимы, приоритеты)
        routed = await orchestrator.route_decision(decision)

        if routed:
            self._tracing_stats["decisions_logged"] += 1
            logger.debug(
                f"📝 LangGraph node '{node_name}' decision logged: "
                f"{decision_type} (confidence: {confidence:.2f})"
            )
        else:
            self._tracing_stats["decisions_blocked_by_mode"] += 1
            logger.warning(
                f"🛑 LangGraph node '{node_name}' decision blocked by mode: "
                f"{decision_type}"
            )

    async def _log_decision_fallback(
        self,
        state: HybridWorkflowState,
        node_name: str,
        decision_type: str,
        inputs: Dict[str, Any],
        outputs: Dict[str, Any],
        rationale: str,
        confidence: float,
    ) -> None:
        """
        Fallback: прямая запись в decision_audit когда orchestrator недоступен.
        Используется только в крайних случаях.
        """
        record = DecisionRecord(
            agent=f"LangGraph:{node_name}",
            decision_type=decision_type,
            inputs={
                **inputs,
                "workflow_id": state["workflow_id"],
                "workflow_type": state["workflow_type"].value,
            },
            outputs=outputs,
            rationale=rationale,
            confidence=confidence,
            session_id=state.get("session_id"),
            context={
                "workflow_id": state["workflow_id"],
                "workflow_type": state["workflow_type"].value,
                "node_name": node_name,
                "trace_context": state.get("trace_context", {}),
            },
        )
        await decision_audit.log_decision(record)
        self._tracing_stats["decisions_logged"] += 1

    # ========================================================================
    # ROUTING & CONDITIONAL EDGES
    # ========================================================================

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

    # ========================================================================
    # WORKFLOW LIFECYCLE
    # ========================================================================

    async def start_workflow(
        self,
        workflow_type: WorkflowType,
        trigger_event: Dict[str, Any],
        context: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
    ) -> str:
        """
        Запускает новый workflow с parent OpenTelemetry span.

        Args:
            workflow_type: Тип workflow
            trigger_event: Событие-триггер
            context: Дополнительный контекст
            max_retries: Максимальное количество retry

        Returns:
            workflow_id
        """
        workflow_id = f"workflow_{workflow_type.value}_{datetime.now().timestamp()}"

        # Спринт 5: Создаём trace context для передачи между узлами
        trace_context = {
            "workflow_id": workflow_id,
            "workflow_type": workflow_type.value,
            "started_at": datetime.now().isoformat(),
            "tracing_enabled": tracing_manager.enabled,
        }

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
            # Спринт 5: trace context
            "trace_context": trace_context,
        }

        self.active_workflows[workflow_id] = initial_state

        logger.info(f"🚀 Starting workflow {workflow_id} (type: {workflow_type.value})")

        # Спринт 5: Создаём parent span для всего workflow
        async with span_context(
            name=f"langgraph.workflow.{workflow_type.value}",
            attributes={
                "workflow.id": workflow_id,
                "workflow.type": workflow_type.value,
                "workflow.max_retries": max_retries,
                "workflow.trigger_type": trigger_event.get("type", "unknown"),
            },
        ) as span:
            if span:
                trace_context["trace_id"] = getattr(
                    span, "get_span_context", lambda: None
                )()

            # Запускаем workflow в фоновой задаче
            asyncio.create_task(self._run_workflow(workflow_id, initial_state))

        return workflow_id

    async def _run_workflow(self, workflow_id: str, initial_state: HybridWorkflowState):
        """
        Выполняет workflow с retry-логикой.

        ИСПРАВЛЕНО (С-14): Добавлена retry-логика с exponential backoff.
        При ошибке workflow пытается перезапуститься до max_retries раз
        с увеличивающейся задержкой (1s, 2s, 4s, ...).
        """
        max_retries = initial_state.get("max_retries", 3)
        retry_count = 0

        while retry_count <= max_retries:
            try:
                final_state = await self.graph.ainvoke(initial_state)
                self.active_workflows[workflow_id] = final_state

                logger.info(
                    f"✅ Workflow {workflow_id} completed with status: "
                    f"{final_state['status']}"
                )
                return  # Успешное завершение

            except Exception as e:
                retry_count += 1

                if retry_count <= max_retries:
                    # Exponential backoff: 1s, 2s, 4s, 8s...
                    backoff_delay = 2 ** (retry_count - 1)
                    logger.warning(
                        f"⚠️ Workflow {workflow_id} failed, "
                        f"retrying ({retry_count}/{max_retries}) "
                        f"in {backoff_delay}s: {e}"
                    )

                    # Добавляем ошибку в state
                    if workflow_id in self.active_workflows:
                        self.active_workflows[workflow_id]["errors"].append(
                            f"Attempt {retry_count} failed: {str(e)}"
                        )

                    await asyncio.sleep(backoff_delay)
                else:
                    # Все retry исчерпаны
                    logger.error(
                        f"❌ Workflow {workflow_id} failed after {max_retries} retries: {e}",
                        exc_info=True,
                    )

                    if workflow_id in self.active_workflows:
                        self.active_workflows[workflow_id]["status"] = (
                            WorkflowStatus.FAILED
                        )
                        self.active_workflows[workflow_id]["errors"].append(
                            f"Final failure: {str(e)}"
                        )

    # ========================================================================
    # PUBLIC API
    # ========================================================================

    def get_workflow_status(self, workflow_id: str) -> Optional[HybridWorkflowState]:
        return self.active_workflows.get(workflow_id)

    def list_active_workflows(self) -> List[str]:
        return [
            wf_id
            for wf_id, state in self.active_workflows.items()
            if state["status"] == WorkflowStatus.RUNNING
        ]

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику оркестратора."""
        return {
            "active_workflows": len(self.list_active_workflows()),
            "total_workflows": len(self.active_workflows),
            "agents_injected": len(self._agents),
            "tracing": {
                "enabled": tracing_manager.enabled,
                **self._tracing_stats,
            },
        }


# Singleton instance — создаётся БЕЗ агентов, агенты внедряются позже через set_agents()
hybrid_orchestrator = HybridLangGraphOrchestrator()


def set_agents_for_hybrid_orchestrator(agents_registry: Dict[str, BaseAgent]):
    """
    Внедряет существующих агентов в hybrid_orchestrator после инициализации.
    Вызывается в main.py после создания всех агентов и до запуска оркестратора.

    Args:
        agents_registry: Словарь агентов из orchestrator.agents
    """
    global hybrid_orchestrator
    hybrid_orchestrator._agents = agents_registry
    logger.info(
        f"✅ Injected {len(agents_registry)} agents into HybridLangGraphOrchestrator"
    )
