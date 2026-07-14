"""
Decision Analyzer — статистический анализ решений AI-агентов.
Реализация идеи 2 (упрощённая): анализ истории решений для выявления паттернов.

Это **фундамент для будущего Reinforcement Learning**:
- Собирает статистику по каждому агенту и типу решения
- Вычисляет success rate на основе hindsight_verdict
- Генерирует рекомендации по улучшению
- Готов к замене на полноценный RL pipeline в будущем

Архитектура RL (future scope):
    DecisionAnalyzer (сейчас) → RewardFunction → PolicyTrainer → ML Model

Текущая реализация:
    DecisionAnalyzer → Statistical Analysis → Recommendations

Использование:
    from app.analytics.decision_analyzer import decision_analyzer

    # Анализ производительности агента
    perf = await decision_analyzer.analyze_agent_performance("Watcher")

    # Генерация рекомендаций
    recs = await decision_analyzer.generate_recommendations("Watcher")

    # Полная аналитика
    report = await decision_analyzer.generate_full_report()
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from collections import defaultdict
from dataclasses import dataclass, field

from app.core.config import settings
from app.storage.decision_audit import decision_audit
from app.core.events import event_bus

logger = logging.getLogger("DecisionAnalyzer")


@dataclass
class AgentPerformance:
    """Производительность одного агента."""

    agent: str
    total_decisions: int = 0
    correct_decisions: int = 0
    wrong_decisions: int = 0
    suboptimal_decisions: int = 0
    unknown_decisions: int = 0

    # По типам решений
    by_decision_type: Dict[str, Dict[str, int]] = field(default_factory=dict)

    # Средняя уверенность
    avg_confidence: float = 0.0

    @property
    def success_rate(self) -> float:
        """Процент правильных решений."""
        evaluated = (
            self.correct_decisions + self.wrong_decisions + self.suboptimal_decisions
        )
        if evaluated == 0:
            return 0.0
        return self.correct_decisions / evaluated

    @property
    def verdict_rate(self) -> float:
        """Процент решений с оценкой (не UNKNOWN)."""
        if self.total_decisions == 0:
            return 0.0
        evaluated = self.total_decisions - self.unknown_decisions
        return evaluated / self.total_decisions

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent": self.agent,
            "total_decisions": self.total_decisions,
            "correct": self.correct_decisions,
            "wrong": self.wrong_decisions,
            "suboptimal": self.suboptimal_decisions,
            "unknown": self.unknown_decisions,
            "success_rate": round(self.success_rate, 4),
            "verdict_rate": round(self.verdict_rate, 4),
            "avg_confidence": round(self.avg_confidence, 4),
            "by_decision_type": self.by_decision_type,
        }


@dataclass
class Recommendation:
    """Рекомендация по улучшению."""

    agent: str
    decision_type: Optional[str]
    issue: str
    suggestion: str
    priority: str  # HIGH, MEDIUM, LOW
    evidence: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "agent": self.agent,
            "decision_type": self.decision_type,
            "issue": self.issue,
            "suggestion": self.suggestion,
            "priority": self.priority,
            "evidence": self.evidence,
        }


class DecisionAnalyzer:
    """
    Анализатор производительности AI-агентов на основе истории решений.

    Фундамент для будущего RL:
    - Текущая реализация: статистический анализ
    - Будущая реализация: RewardFunction → PolicyTrainer → ML Model

    Метрики:
    - Success rate по агенту и типу решения
    - Средняя уверенность
    - Выявление проблемных паттернов
    - Генерация рекомендаций
    """

    # Пороговые значения для генерации рекомендаций
    LOW_SUCCESS_RATE_THRESHOLD = 0.6  # Ниже 60% — проблема
    LOW_SAMPLE_SIZE = 10  # Минимум решений для статистической значимости
    HIGH_CONFIDENCE_WRONG_THRESHOLD = 0.7  # Уверенные, но неправильные

    # Все известные агенты
    KNOWN_AGENTS = [
        "Watcher",
        "Guardian",
        "Diagnostician",
        "Strategist",
        "Auditor",
        "Calibrator",
        "Copilot",
        "Orchestrator",
        "HybridLangGraphOrchestrator",
    ]

    def __init__(self):
        # Загружаем конфигурацию
        self._enabled = self._load_enabled_flag()

        # Кэш результатов анализа
        self._cache: Dict[str, AgentPerformance] = {}
        self._cache_timestamp: Optional[datetime] = None
        self._cache_ttl_seconds = 300  # 5 минут

        # Статистика
        self._stats = {
            "total_analyses": 0,
            "recommendations_generated": 0,
        }

        logger.info(f"📊 Decision Analyzer initialized (enabled: {self._enabled})")

    def _load_enabled_flag(self) -> bool:
        """Загружает feature flag из settings."""
        try:
            ff = getattr(settings, "feature_flags", None)
            if ff:
                analytics_ff = getattr(ff, "analytics", None)
                if analytics_ff:
                    return getattr(analytics_ff, "decision_analyzer_enabled", True)
        except Exception as e:
            logger.debug(f"Could not load analytics feature flag: {e}")
        return True  # По умолчанию включён

    def _is_cache_valid(self) -> bool:
        """Проверяет валидность кэша."""
        if self._cache_timestamp is None:
            return False
        age = (datetime.now() - self._cache_timestamp).total_seconds()
        return age < self._cache_ttl_seconds

    async def analyze_agent_performance(
        self,
        agent: str,
        days: int = 30,
        force_refresh: bool = False,
    ) -> AgentPerformance:
        """
        Анализирует производительность конкретного агента.

        Args:
            agent: Имя агента
            days: Период анализа (дни)
            force_refresh: Принудительно обновить кэш

        Returns:
            AgentPerformance с полной статистикой
        """
        # Проверяем кэш
        if not force_refresh and self._is_cache_valid() and agent in self._cache:
            return self._cache[agent]

        self._stats["total_analyses"] += 1

        # Получаем решения агента из Decision Audit
        cutoff_date = datetime.now() - timedelta(days=days)

        decisions = await decision_audit.get_decisions(
            agent=agent,
            limit=10000,
        )

        # Фильтруем по дате
        recent_decisions = [
            d for d in decisions if d.timestamp >= cutoff_date.isoformat()
        ]

        # Вычисляем статистику
        performance = AgentPerformance(agent=agent)
        performance.total_decisions = len(recent_decisions)

        if not recent_decisions:
            self._cache[agent] = performance
            return performance

        # Подсчёт по verdict
        confidences = []

        for decision in recent_decisions:
            verdict = decision.hindsight_verdict

            if verdict == "CORRECT":
                performance.correct_decisions += 1
            elif verdict == "WRONG":
                performance.wrong_decisions += 1
            elif verdict == "SUBOPTIMAL":
                performance.suboptimal_decisions += 1
            else:
                performance.unknown_decisions += 1

            # По типам решений
            dt = decision.decision_type
            if dt not in performance.by_decision_type:
                performance.by_decision_type[dt] = {
                    "total": 0,
                    "correct": 0,
                    "wrong": 0,
                    "suboptimal": 0,
                    "unknown": 0,
                }

            performance.by_decision_type[dt]["total"] += 1
            if verdict == "CORRECT":
                performance.by_decision_type[dt]["correct"] += 1
            elif verdict == "WRONG":
                performance.by_decision_type[dt]["wrong"] += 1
            elif verdict == "SUBOPTIMAL":
                performance.by_decision_type[dt]["suboptimal"] += 1
            else:
                performance.by_decision_type[dt]["unknown"] += 1

            # Уверенность
            confidences.append(decision.confidence)

        # Средняя уверенность
        if confidences:
            performance.avg_confidence = sum(confidences) / len(confidences)

        # Кэшируем результат
        self._cache[agent] = performance
        self._cache_timestamp = datetime.now()

        return performance

    async def analyze_all_agents(
        self,
        days: int = 30,
        force_refresh: bool = False,
    ) -> Dict[str, AgentPerformance]:
        """
        Анализирует производительность всех известных агентов.

        Returns:
            Dict {agent_name: AgentPerformance}
        """
        results = {}

        for agent in self.KNOWN_AGENTS:
            perf = await self.analyze_agent_performance(
                agent=agent,
                days=days,
                force_refresh=force_refresh,
            )
            results[agent] = perf

        return results

    async def generate_recommendations(
        self,
        agent: Optional[str] = None,
        days: int = 30,
    ) -> List[Recommendation]:
        """
        Генерирует рекомендации по улучшению на основе анализа.

        Args:
            agent: Конкретный агент (None = все агенты)
            days: Период анализа

        Returns:
            Список рекомендаций
        """
        self._stats["total_analyses"] += 1
        recommendations: List[Recommendation] = []

        # Определяем агентов для анализа
        agents_to_analyze = [agent] if agent else self.KNOWN_AGENTS

        for agent_name in agents_to_analyze:
            perf = await self.analyze_agent_performance(
                agent=agent_name,
                days=days,
            )

            # Пропускаем агентов с малым количеством данных
            if perf.total_decisions < self.LOW_SAMPLE_SIZE:
                continue

            # === Проверка 1: Низкий success rate ===
            if (
                perf.success_rate < self.LOW_SUCCESS_RATE_THRESHOLD
                and perf.success_rate > 0
            ):
                recommendations.append(
                    Recommendation(
                        agent=agent_name,
                        decision_type=None,
                        issue=f"Низкий success rate: {perf.success_rate:.1%}",
                        suggestion=(
                            f"Агент {agent_name} принимает правильные решения только в "
                            f"{perf.success_rate:.1%} случаев. Рассмотрите пересмотр "
                            f"логики принятия решений или улучшение контекста."
                        ),
                        priority="HIGH" if perf.success_rate < 0.4 else "MEDIUM",
                        evidence={
                            "success_rate": perf.success_rate,
                            "total_decisions": perf.total_decisions,
                            "correct": perf.correct_decisions,
                            "wrong": perf.wrong_decisions,
                        },
                    )
                )

            # === Проверка 2: Высокая уверенность при неправильных решениях ===
            if (
                perf.wrong_decisions > 0
                and perf.avg_confidence > self.HIGH_CONFIDENCE_WRONG_THRESHOLD
            ):
                recommendations.append(
                    Recommendation(
                        agent=agent_name,
                        decision_type=None,
                        issue=(
                            f"Высокая уверенность ({perf.avg_confidence:.1%}) "
                            f"при {perf.wrong_decisions} неправильных решениях"
                        ),
                        suggestion=(
                            f"Агент {agent_name} слишком уверен в неправильных решениях. "
                            f"Рассмотрите калибровку уверенности (confidence calibration) "
                            f"или добавление дополнительных проверок."
                        ),
                        priority="MEDIUM",
                        evidence={
                            "avg_confidence": perf.avg_confidence,
                            "wrong_decisions": perf.wrong_decisions,
                        },
                    )
                )

            # === Проверка 3: Проблемные типы решений ===
            for dt, stats in perf.by_decision_type.items():
                dt_total = stats["total"]
                dt_correct = stats["correct"]

                if dt_total < self.LOW_SAMPLE_SIZE:
                    continue

                dt_success_rate = dt_correct / dt_total if dt_total > 0 else 0

                if (
                    dt_success_rate < self.LOW_SUCCESS_RATE_THRESHOLD
                    and dt_success_rate > 0
                ):
                    recommendations.append(
                        Recommendation(
                            agent=agent_name,
                            decision_type=dt,
                            issue=(
                                f"Низкий success rate для {dt}: "
                                f"{dt_success_rate:.1%} ({dt_correct}/{dt_total})"
                            ),
                            suggestion=(
                                f"Тип решения '{dt}' агента {agent_name} имеет "
                                f"низкую эффективность. Рассмотрите пересмотр "
                                f"логики для этого типа."
                            ),
                            priority="MEDIUM",
                            evidence={
                                "decision_type": dt,
                                "success_rate": dt_success_rate,
                                "total": dt_total,
                                "correct": dt_correct,
                            },
                        )
                    )

        self._stats["recommendations_generated"] += len(recommendations)

        # Сортируем по приоритету
        priority_order = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}
        recommendations.sort(key=lambda r: priority_order.get(r.priority, 3))

        return recommendations

    async def generate_full_report(
        self,
        days: int = 30,
    ) -> Dict[str, Any]:
        """
        Генерирует полный отчёт по производительности всех агентов.

        Returns:
            Dict с полной аналитикой
        """
        # Анализируем всех агентов
        all_performance = await self.analyze_all_agents(days=days)

        # Генерируем рекомендации
        recommendations = await self.generate_recommendations(days=days)

        # Общая статистика
        total_decisions = sum(p.total_decisions for p in all_performance.values())
        total_correct = sum(p.correct_decisions for p in all_performance.values())
        total_wrong = sum(p.wrong_decisions for p in all_performance.values())
        total_suboptimal = sum(p.suboptimal_decisions for p in all_performance.values())
        evaluated = total_correct + total_wrong + total_suboptimal

        overall_success_rate = total_correct / evaluated if evaluated > 0 else 0.0

        # Лучший и худший агенты
        best_agent = None
        worst_agent = None
        best_rate = 0.0
        worst_rate = 1.0

        for agent, perf in all_performance.items():
            if perf.total_decisions >= self.LOW_SAMPLE_SIZE and perf.success_rate > 0:
                if perf.success_rate > best_rate:
                    best_rate = perf.success_rate
                    best_agent = agent
                if perf.success_rate < worst_rate:
                    worst_rate = perf.success_rate
                    worst_agent = agent

        report = {
            "report_timestamp": datetime.now().isoformat(),
            "analysis_period_days": days,
            "summary": {
                "total_decisions": total_decisions,
                "total_correct": total_correct,
                "total_wrong": total_wrong,
                "total_suboptimal": total_suboptimal,
                "overall_success_rate": round(overall_success_rate, 4),
                "agents_analyzed": len(
                    [p for p in all_performance.values() if p.total_decisions > 0]
                ),
            },
            "best_performing_agent": {
                "agent": best_agent,
                "success_rate": round(best_rate, 4),
            }
            if best_agent
            else None,
            "worst_performing_agent": {
                "agent": worst_agent,
                "success_rate": round(worst_rate, 4),
            }
            if worst_agent
            else None,
            "agents": {
                agent: perf.to_dict()
                for agent, perf in all_performance.items()
                if perf.total_decisions > 0
            },
            "recommendations": [r.to_dict() for r in recommendations],
            "recommendations_count": {
                "high": sum(1 for r in recommendations if r.priority == "HIGH"),
                "medium": sum(1 for r in recommendations if r.priority == "MEDIUM"),
                "low": sum(1 for r in recommendations if r.priority == "LOW"),
            },
        }

        # Публикуем событие
        await event_bus.publish(
            "ANALYTICS_REPORT_GENERATED",
            {
                "timestamp": report["report_timestamp"],
                "overall_success_rate": overall_success_rate,
                "recommendations_count": len(recommendations),
            },
        )

        return report

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику Decision Analyzer."""
        return {
            **self._stats,
            "enabled": self._enabled,
            "cache_valid": self._is_cache_valid(),
            "cached_agents": list(self._cache.keys()),
            "known_agents": self.KNOWN_AGENTS,
            "thresholds": {
                "low_success_rate": self.LOW_SUCCESS_RATE_THRESHOLD,
                "low_sample_size": self.LOW_SAMPLE_SIZE,
                "high_confidence_wrong": self.HIGH_CONFIDENCE_WRONG_THRESHOLD,
            },
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
decision_analyzer = DecisionAnalyzer()
