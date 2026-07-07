"""
Multi-Agent Swarm — система AI-агентов для автономного управления обсерваторией.

Архитектура (Orchestrator-Worker Pattern):
- Orchestrator: координирует работу всех агентов
- Scheduler: планирование сессий
- Watcher: мониторинг и детекция аномалий
- Diagnostician: root cause analysis
- Strategist: оптимизация параметров
- Guardian: безопасность
- Auditor: post-mortem анализ
- Calibrator: управление мастер-кадрами
- Copilot: интерактивная помощь
- Memory Manager: управление контекстом
"""

from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext

__all__ = [
    "BaseAgent",
    "AgentDecision",
    "AgentContext",
]
