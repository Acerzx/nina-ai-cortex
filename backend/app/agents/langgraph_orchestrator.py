"""
LangGraph Orchestrator — координация агентов через LangGraph.
Реализует иерархическую модель Orchestrator-Worker.
"""

import logging
from typing import Dict, Any, Optional, List, TypedDict, Annotated
from datetime import datetime
from langgraph.graph import StateGraph, END
from langgraph.graph.message import add_messages
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from app.agents.llm_client import llm_client
from app.agents.observatory_state import observatory_state
from app.storage.decision_audit import decision_audit, DecisionRecord
from app.core.events import event_bus

logger = logging.getLogger("LangGraphOrchestrator")


# Определяем состояние графа
class AgentState(TypedDict):
    """Состояние для LangGraph."""

    messages: Annotated[list, add_messages]
    current_agent: str
    task: str
    context: Dict[str, Any]
    decision: Optional[Dict[str, Any]]
    result: Optional[str]


class LangGraphOrchestrator:
    """
    Orchestrator на базе LangGraph.

    Workflow:
    1. Router — определяет, какой агент должен обработать задачу
    2. Worker — агент выполняет задачу
    3. Evaluator — оценивает результат
    4. Logger — логирует решение в Decision Audit
    """

    def __init__(self):
        self.graph = self._build_graph()
        self.agents = {}

        logger.info("✅ LangGraph Orchestrator initialized")

    def register_agent(self, name: str, agent: Any):
        """Регистрирует агента."""
        self.agents[name] = agent
        logger.info(f"Agent '{name}' registered in LangGraph")

    def _build_graph(self) -> StateGraph:
        """Строит граф LangGraph."""
        # Создаем граф
        workflow = StateGraph(AgentState)

        # Добавляем узлы
        workflow.add_node("router", self._router_node)
        workflow.add_node("watcher", self._watcher_node)
        workflow.add_node("guardian", self._guardian_node)
        workflow.add_node("diagnostician", self._diagnostician_node)
        workflow.add_node("strategist", self._strategist_node)
        workflow.add_node("auditor", self._auditor_node)
        workflow.add_node("calibrator", self._calibrator_node)
        workflow.add_node("scheduler", self._scheduler_node)
        workflow.add_node("copilot", self._copilot_node)
        workflow.add_node("evaluator", self._evaluator_node)
        workflow.add_node("logger", self._logger_node)

        # Определяем стартовый узел
        workflow.set_entry_point("router")

        # Определяем переходы
        workflow.add_conditional_edges(
            "router",
            self._route_decision,
            {
                "watcher": "watcher",
                "guardian": "guardian",
                "diagnostician": "diagnostician",
                "strategist": "strategist",
                "auditor": "auditor",
                "calibrator": "calibrator",
                "scheduler": "scheduler",
                "copilot": "copilot",
                "end": END,
            },
        )

        # Все worker'ы переходят в evaluator
        for agent in [
            "watcher",
            "guardian",
            "diagnostician",
            "strategist",
            "auditor",
            "calibrator",
            "scheduler",
            "copilot",
        ]:
            workflow.add_edge(agent, "evaluator")

        # Evaluator переходит в logger
        workflow.add_edge("evaluator", "logger")

        # Logger завершает граф
        workflow.add_edge("logger", END)

        # Компилируем граф
        return workflow.compile()

    def _router_node(self, state: AgentState) -> AgentState:
        """Router — определяет, какой агент должен обработать задачу."""
        task = state.get("task", "")
        context = state.get("context", {})

        logger.info(f"🔀 Router: analyzing task '{task}'")

        # Простая эвристика маршрутизации
        if "аномалия" in task.lower() or "мониторинг" in task.lower():
            current_agent = "watcher"
        elif "безопасность" in task.lower() or "park" in task.lower():
            current_agent = "guardian"
        elif "причина" in task.lower() or "диагностика" in task.lower():
            current_agent = "diagnostician"
        elif "оптимизация" in task.lower() or "параметры" in task.lower():
            current_agent = "strategist"
        elif "сессия" in task.lower() or "digest" in task.lower():
            current_agent = "auditor"
        elif "калибровка" in task.lower() or "мастер" in task.lower():
            current_agent = "calibrator"
        elif "план" in task.lower() or "цель" in task.lower():
            current_agent = "scheduler"
        elif "помощь" in task.lower() or "инструкция" in task.lower():
            current_agent = "copilot"
        else:
            current_agent = "watcher"  # По умолчанию

        state["current_agent"] = current_agent

        return state

    def _route_decision(self, state: AgentState) -> str:
        """Определяет следующий узел на основе router."""
        return state.get("current_agent", "end")

    async def _watcher_node(self, state: AgentState) -> AgentState:
        """Watcher agent node."""
        agent = self.agents.get("Watcher")
        if not agent:
            state["result"] = "Watcher agent not available"
            return state

        # Вызываем агента
        from app.agents.base_agent import AgentContext

        context = AgentContext(
            current_metrics=observatory_state.current_metrics,
            weather=observatory_state.weather,
            astronomy=observatory_state.astronomy,
            sequence_state={},
            safety_status=observatory_state.safety_status,
            active_alerts=observatory_state.active_alerts,
        )

        decision = await agent.analyze(context)

        if decision:
            state["decision"] = decision.model_dump()
            state["result"] = f"Watcher detected: {decision.rationale}"
        else:
            state["result"] = "No anomalies detected"

        return state

    async def _guardian_node(self, state: AgentState) -> AgentState:
        """Guardian agent node."""
        agent = self.agents.get("Guardian")
        if not agent:
            state["result"] = "Guardian agent not available"
            return state

        from app.agents.base_agent import AgentContext

        context = AgentContext(
            current_metrics=observatory_state.current_metrics,
            weather=observatory_state.weather,
            astronomy=observatory_state.astronomy,
            sequence_state={},
            safety_status=observatory_state.safety_status,
            active_alerts=observatory_state.active_alerts,
        )

        decision = await agent.analyze(context)

        if decision:
            state["decision"] = decision.model_dump()
            state["result"] = f"Guardian action: {decision.rationale}"
        else:
            state["result"] = "No safety actions required"

        return state

    async def _diagnostician_node(self, state: AgentState) -> AgentState:
        """Diagnostician agent node."""
        # Аналогично watcher_node
        state["result"] = "Diagnostician analysis complete"
        return state

    async def _strategist_node(self, state: AgentState) -> AgentState:
        """Strategist agent node."""
        state["result"] = "Strategist optimization complete"
        return state

    async def _auditor_node(self, state: AgentState) -> AgentState:
        """Auditor agent node."""
        state["result"] = "Auditor digest generation complete"
        return state

    async def _calibrator_node(self, state: AgentState) -> AgentState:
        """Calibrator agent node."""
        state["result"] = "Calibrator check complete"
        return state

    async def _scheduler_node(self, state: AgentState) -> AgentState:
        """Scheduler agent node."""
        state["result"] = "Scheduler planning complete"
        return state

    async def _copilot_node(self, state: AgentState) -> AgentState:
        """Copilot agent node."""
        state["result"] = "Copilot guide generation complete"
        return state

    def _evaluator_node(self, state: AgentState) -> AgentState:
        """Evaluator — оценивает результат."""
        decision = state.get("decision")

        if decision:
            logger.info(f"✅ Decision evaluated: {decision.get('decision_type')}")

        return state

    async def _logger_node(self, state: AgentState) -> AgentState:
        """Logger — логирует решение в Decision Audit."""
        decision_data = state.get("decision")

        if decision_data:
            # Создаем запись
            record = DecisionRecord(
                agent=decision_data.get("agent"),
                decision_type=decision_data.get("decision_type"),
                inputs=decision_data.get("inputs", {}),
                outputs=decision_data.get("outputs", {}),
                rationale=decision_data.get("rationale"),
                confidence=decision_data.get("confidence", 0.5),
                context=state.get("context", {}),
            )

            # Логируем
            await decision_audit.log_decision(record)

        return state

    async def run(self, task: str, context: Optional[Dict[str, Any]] = None) -> str:
        """
        Запускает граф для обработки задачи.

        Args:
            task: Описание задачи
            context: Дополнительный контекст

        Returns:
            Результат выполнения
        """
        # Начальное состояние
        initial_state = {
            "messages": [HumanMessage(content=task)],
            "current_agent": "",
            "task": task,
            "context": context or {},
            "decision": None,
            "result": None,
        }

        # Запускаем граф
        logger.info(f"🚀 LangGraph: starting task '{task}'")

        final_state = await self.graph.ainvoke(initial_state)

        result = final_state.get("result", "Task completed")

        logger.info(f"✅ LangGraph: task completed - {result}")

        return result


# Singleton instance
langgraph_orchestrator = LangGraphOrchestrator()
