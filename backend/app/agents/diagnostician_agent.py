"""
Diagnostician Agent — диагностирует причины проблем (не просто "HFR растет", а "почему").
Анализирует корреляции между метриками, ищет похожие кейсы в RAG, предлагает решения.

ИСПРАВЛЕНО (audit 3.1): добавлен import asyncio для корректной работы
с asyncio.TimeoutError в методе _determine_root_cause.
"""

import asyncio  # ← ДОБАВЛЕНО для asyncio.TimeoutError
import logging
import numpy as np
from typing import Dict, Any, Optional, List, Tuple
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.rag_engine import rag_engine

logger = logging.getLogger("DiagnosticianAgent")


class CorrelationResult(BaseModel):
    """Результат анализа корреляции между двумя метриками."""

    metric1: str
    metric2: str
    correlation_coefficient: float  # Pearson correlation (-1 to 1)
    p_value: Optional[float] = None
    interpretation: str  # "strong_positive", "strong_negative", "weak", "none"
    sample_size: int


class RootCauseAnalysis(BaseModel):
    """Анализ корневой причины проблемы."""

    problem: str
    root_cause: str
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence: List[str]
    similar_cases: List[Dict[str, Any]]
    recommended_actions: List[str]
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class DiagnosticianAgent(BaseAgent):
    """
    Агент диагностики корневых причин проблем.

    Responsibilities:
    - Анализ корреляций между метриками (HFR vs Temperature, RMS vs Wind)
    - Поиск похожих кейсов в RAG (история сессий)
    - Определение root cause проблем
    - Предложение решений на основе исторических данных
    - Интеграция с LLM для сложного анализа

    Примеры анализа:
    - "HFR вырос на 50%" → "Температура упала на 5°C" → "Температурный дрейф фокуса"
    - "RMS по DEC вырос" → "Ветер с севера 12 м/с" → "Ветровая нагрузка на монтировку"
    - "FWHM деградирует" → "Прошло 3 часа с последнего автофокуса" → "Необходим автофокус"
    """

    def __init__(self):
        super().__init__(name="Diagnostician", role="Root Cause Analysis")
        # Пороговые значения для корреляций
        self.correlation_thresholds = {
            "strong": 0.7,  # |r| > 0.7 = сильная корреляция
            "moderate": 0.5,  # |r| > 0.5 = умеренная корреляция
            "weak": 0.3,  # |r| > 0.3 = слабая корреляция
        }
        # Минимальный размер выборки для статистической значимости
        self.min_sample_size = 10

    async def initialize(self):
        """Инициализация агента диагностики."""
        await super().initialize()
        # Подписываемся на алерты от Watcher
        event_bus.subscribe("ALERT", self._on_alert)
        logger.info("✅ Diagnostician Agent initialized")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("ALERT", self._on_alert)
        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует контекст и определяет root cause проблемы.
        Вызывается Orchestrator'ом при получении алерта от Watcher.
        """
        # Получаем активные алерты
        active_alerts = observatory_state.active_alerts
        if not active_alerts:
            return None

        # Анализируем последний алерт
        latest_alert = active_alerts[-1]
        alert_context = latest_alert.get("context", {})

        # Выполняем root cause analysis
        analysis = await self._perform_root_cause_analysis(
            problem=latest_alert.get("message", "Unknown problem"),
            context=alert_context,
        )

        if analysis:
            decision = AgentDecision(
                agent=self.name,
                decision_type="ROOT_CAUSE_IDENTIFIED",
                inputs={"problem": analysis.problem, "context": alert_context},
                outputs={
                    "root_cause": analysis.root_cause,
                    "confidence": analysis.confidence,
                    "recommended_actions": analysis.recommended_actions,
                },
                rationale=f"Root cause identified: {analysis.root_cause}",
                confidence=analysis.confidence,
            )
            self.log_decision(decision)
            return decision
        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет принятое решение (публикует рекомендации)."""
        if decision.decision_type == "ROOT_CAUSE_IDENTIFIED":
            root_cause = decision.outputs.get("root_cause", "")
            recommended_actions = decision.outputs.get("recommended_actions", [])

            # Публикуем рекомендации для других агентов
            await event_bus.publish(
                "DIAGNOSTIC_RECOMMENDATION",
                {
                    "root_cause": root_cause,
                    "confidence": decision.confidence,
                    "recommended_actions": recommended_actions,
                    "timestamp": datetime.now().isoformat(),
                },
            )
            logger.info(f"🔍 Diagnostic recommendation: {root_cause}")
            logger.info(f"   Recommended actions: {recommended_actions}")
            return True
        return False

    async def _on_alert(self, data: Dict[str, Any]) -> None:
        """Обработка алерта от Watcher."""
        level = data.get("level", "INFO")
        # Анализируем только WARNING и CRITICAL алерты
        if level in ("WARNING", "CRITICAL"):
            await self.analyze(
                AgentContext(
                    current_metrics=observatory_state.current_metrics,
                    weather=observatory_state.weather,
                    astronomy=observatory_state.astronomy,
                    sequence_state={},
                    safety_status=observatory_state.safety_status,
                    active_alerts=[data],
                )
            )

    async def _perform_root_cause_analysis(
        self, problem: str, context: Dict[str, Any]
    ) -> Optional[RootCauseAnalysis]:
        """Выполняет полный root cause analysis."""
        # 1. Анализ корреляций между метриками
        correlations = await self._analyze_correlations(problem, context)

        # 2. Поиск похожих кейсов в RAG
        similar_cases = await self._search_similar_cases(problem, context)

        # 3. Определение root cause на основе корреляций и истории
        root_cause, confidence, evidence = await self._determine_root_cause(
            problem, correlations, similar_cases
        )

        # 4. Генерация рекомендаций
        recommended_actions = await self._generate_recommendations(
            root_cause, similar_cases
        )

        return RootCauseAnalysis(
            problem=problem,
            root_cause=root_cause,
            confidence=confidence,
            supporting_evidence=evidence,
            similar_cases=similar_cases,
            recommended_actions=recommended_actions,
        )

    async def _analyze_correlations(
        self, problem: str, context: Dict[str, Any]
    ) -> List[CorrelationResult]:
        """Анализирует корреляции между метриками."""
        correlations = []

        # Определяем проблемную метрику из контекста
        problem_metric = context.get("metric", "")
        if not problem_metric:
            # Если метрика не указана, анализируем все пары
            metrics_to_analyze = [
                "hfr",
                "fwhm",
                "rms_ra",
                "rms_dec",
                "temperature",
                "wind_speed",
            ]
        else:
            # Анализируем корреляции проблемной метрики с другими
            metrics_to_analyze = [problem_metric]

        # Все возможные метрики для корреляции
        all_metrics = [
            "hfr",
            "fwhm",
            "rms_ra",
            "rms_dec",
            "temperature",
            "wind_speed",
            "humidity",
        ]

        for metric1 in metrics_to_analyze:
            for metric2 in all_metrics:
                if metric1 == metric2:
                    continue
                correlation = await self._calculate_correlation(metric1, metric2)
                if correlation:
                    correlations.append(correlation)

        # Сортируем по силе корреляции
        correlations.sort(key=lambda c: abs(c.correlation_coefficient), reverse=True)
        return correlations

    async def _calculate_correlation(
        self, metric1: str, metric2: str
    ) -> Optional[CorrelationResult]:
        """Вычисляет корреляцию Пирсона между двумя метриками."""
        # Получаем историю метрик
        history1 = getattr(observatory_state.history, metric1, None)
        history2 = getattr(observatory_state.history, metric2, None)
        if not history1 or not history2:
            return None

        # Приводим к одинаковой длине
        min_len = min(len(history1), len(history2))
        if min_len < self.min_sample_size:
            return None

        series1 = history1[-min_len:]
        series2 = history2[-min_len:]

        # Вычисляем корреляцию Пирсона
        try:
            correlation_coefficient = np.corrcoef(series1, series2)[0, 1]

            # Интерпретация
            abs_corr = abs(correlation_coefficient)
            if abs_corr > self.correlation_thresholds["strong"]:
                interpretation = (
                    "strong_positive"
                    if correlation_coefficient > 0
                    else "strong_negative"
                )
            elif abs_corr > self.correlation_thresholds["moderate"]:
                interpretation = (
                    "moderate_positive"
                    if correlation_coefficient > 0
                    else "moderate_negative"
                )
            elif abs_corr > self.correlation_thresholds["weak"]:
                interpretation = (
                    "weak_positive" if correlation_coefficient > 0 else "weak_negative"
                )
            else:
                interpretation = "none"

            return CorrelationResult(
                metric1=metric1,
                metric2=metric2,
                correlation_coefficient=float(correlation_coefficient),
                interpretation=interpretation,
                sample_size=min_len,
            )
        except Exception as e:
            logger.debug(f"Failed to calculate correlation {metric1} vs {metric2}: {e}")
            return None

    async def _search_similar_cases(
        self, problem: str, context: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Ищет похожие кейсы в RAG."""
        try:
            # Формируем поисковый запрос
            query = f"Проблема: {problem}. Метрики: {context}"

            # Ищем в RAG
            results = await rag_engine.search(
                query=query, top_k=5, filters={"source": "session_digest"}
            )

            # Форматируем результаты
            similar_cases = []
            for result in results:
                similar_cases.append(
                    {
                        "text": result["text"],
                        "score": result["score"],
                        "metadata": result["metadata"],
                    }
                )
            return similar_cases
        except Exception as e:
            logger.error(f"Failed to search similar cases: {e}")
            return []

    async def _determine_root_cause(
        self,
        problem: str,
        correlations: List[CorrelationResult],
        similar_cases: List[Dict[str, Any]],
    ) -> Tuple[str, float, List[str]]:
        """
        Определяет root cause с помощью LLM (gemma4:31b-cloud → gemma4:e4b).
        """
        from app.agents.llm_provider import llm_provider

        evidence = []
        root_cause = "Неизвестная причина"
        confidence = 0.5

        # === 1. БАЗОВЫЙ АНАЛИЗ КОРРЕЛЯЦИЙ ===
        strong_correlations = [
            c
            for c in correlations
            if abs(c.correlation_coefficient) > self.correlation_thresholds["strong"]
        ]

        if strong_correlations:
            top_correlation = strong_correlations[0]
            if "temperature" in [top_correlation.metric1, top_correlation.metric2]:
                root_cause = "Температурный дрейф фокуса"
                confidence = 0.85
                evidence.append(
                    f"Сильная корреляция с температурой "
                    f"(r={top_correlation.correlation_coefficient:.2f})"
                )
            elif "wind" in [top_correlation.metric1, top_correlation.metric2]:
                root_cause = "Ветровая нагрузка на монтировку"
                confidence = 0.80
                evidence.append(
                    f"Сильная корреляция с ветром "
                    f"(r={top_correlation.correlation_coefficient:.2f})"
                )
            elif "rms" in [top_correlation.metric1, top_correlation.metric2]:
                root_cause = "Проблема с гидированием"
                confidence = 0.75
                evidence.append(
                    f"Сильная корреляция с RMS гидирования "
                    f"(r={top_correlation.correlation_coefficient:.2f})"
                )

        # === 2. LLM АНАЛИЗ (gemma4:31b-cloud → gemma4:e4b) ===
        try:
            context_parts = []
            if similar_cases:
                context_parts.append(
                    "Исторические кейсы:\n"
                    + "\n".join(f"- {c['text'][:200]}" for c in similar_cases[:3])
                )
            if strong_correlations:
                context_parts.append(
                    "Обнаруженные корреляции:\n"
                    + "\n".join(
                        f"- {c.metric1} vs {c.metric2}: "
                        f"r={c.correlation_coefficient:.2f}"
                        for c in strong_correlations
                    )
                )
            context = "\n".join(context_parts)

            prompt = f"""Проблема: {problem}
Текущие метрики: HFR={observatory_state.current_metrics.get("hfr")},
Температура={observatory_state.current_metrics.get("camera_temp")},
Ветер={observatory_state.weather.get("wind_speed")}

{context}

Определи корневую причину проблемы.
Ответь СТРОГО в формате:
КОРНЕВАЯ ПРИЧИНА: [краткое описание]
УВЕРЕННОСТЬ: [число от 0 до 100]
РЕШЕНИЕ: [одно конкретное действие]"""

            response = await llm_provider.generate(
                prompt=prompt,
                system_prompt=(
                    "Ты — агент диагностики проблем обсерватории. "
                    "Отвечай кратко и по делу на русском языке."
                ),
                max_tokens=400,
                temperature=0.2,
            )

            if response and response.content:
                content = response.content
                if "КОРНЕВАЯ ПРИЧИНА:" in content:
                    llm_root_cause = (
                        content.split("КОРНЕВАЯ ПРИЧИНА:")[1].split("\n")[0].strip()
                    )
                    root_cause = llm_root_cause
                    evidence.append(
                        f"LLM анализ ({response.model}, "
                        f"{response.latency_ms:.0f}ms, "
                        f"{'FALLBACK' if response.from_fallback else 'PRIMARY'})"
                    )
                    confidence = min(0.95, confidence + 0.15)
                    logger.info(
                        f"🤖 LLM root cause: {root_cause} "
                        f"(model: {response.model}, "
                        f"latency: {response.latency_ms:.0f}ms)"
                    )

        except asyncio.TimeoutError:
            # ← Теперь asyncio корректно импортирован (audit 3.1)
            logger.warning("⚠️ LLM timeout, using heuristic root cause")
            evidence.append("LLM timeout — использована эвристика")
        except Exception as e:
            logger.error(f"❌ LLM error: {e}")
            evidence.append(f"LLM error: {type(e).__name__}")

        # === 3. FALLBACK НА ЭВРИСТИКИ ===
        if "Неизвестная" in root_cause:
            if "HFR" in problem and "вырос" in problem:
                root_cause = "Естественный дрейф фокуса (требуется автофокус)"
                confidence = 0.6
                evidence.append("HFR деградирует со временем без явной причины")
            elif "RMS" in problem and "вырос" in problem:
                wind_speed = observatory_state.weather.get("wind_speed")
                if wind_speed and wind_speed > 10.0:
                    root_cause = "Ветровая нагрузка"
                    confidence = 0.75
                    evidence.append(f"Высокая скорость ветра: {wind_speed} м/с")

        return root_cause, confidence, evidence

    async def _generate_recommendations(
        self, root_cause: str, similar_cases: List[Dict[str, Any]]
    ) -> List[str]:
        """Генерирует рекомендации на основе root cause."""
        recommendations = []

        # Рекомендации на основе root cause
        if "температурный" in root_cause.lower():
            recommendations.extend(
                [
                    "Запустить автофокус для компенсации температурного коэффициента",
                    "Уменьшить интервал между автофокусами до 30 минут",
                    "Проверить температурный коэффициент в настройках фокусера",
                ]
            )
        elif "ветровая" in root_cause.lower():
            recommendations.extend(
                [
                    "Переключиться на цель в подветренном направлении",
                    "Увеличить агрессивность гидирования",
                    "Рассмотреть возможность паузы до снижения ветра",
                ]
            )
        elif "гидирование" in root_cause.lower():
            recommendations.extend(
                [
                    "Запустить калибровку гида",
                    "Проверить настройки PHD2 (aggressiveness, hysteresis)",
                    "Увеличить частоту дизеринга",
                ]
            )
        elif "дрейф фокуса" in root_cause.lower():
            recommendations.extend(
                [
                    "Запустить автофокус немедленно",
                    "Проверить настройки температурной компенсации",
                    "Рассмотреть более частые автофокусы",
                ]
            )

        # Добавляем рекомендации из похожих кейсов
        if similar_cases:
            for case in similar_cases[:2]:  # Берем топ-2 кейса
                text = case["text"]
                if "рекомендация" in text.lower() or "решение" in text.lower():
                    recommendations.append(f"Из истории: {text[:100]}...")

        return recommendations[:5]  # Максимум 5 рекомендаций

    async def analyze_warning(self, data: Dict[str, Any]) -> None:
        """Анализирует предупреждение (вызывается Orchestrator'ом)."""
        await self._on_alert(data)

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        HOOK: Принимает решение на основе контекста.
        Делегирует существующему analyze() логике.
        """
        active_alerts = observatory_state.active_alerts
        if not active_alerts:
            return None

        latest_alert = active_alerts[-1]
        alert_context = latest_alert.get("context", {})

        analysis = await self._perform_root_cause_analysis(
            problem=latest_alert.get("message", "Unknown problem"),
            context=alert_context,
        )

        if analysis:
            return AgentDecision(
                agent=self.name,
                decision_type="ROOT_CAUSE_IDENTIFIED",
                inputs={"problem": analysis.problem, "context": alert_context},
                outputs={
                    "root_cause": analysis.root_cause,
                    "confidence": analysis.confidence,
                    "recommended_actions": analysis.recommended_actions,
                },
                rationale=f"Root cause identified: {analysis.root_cause}",
                confidence=analysis.confidence,
            )
        return None

    async def _perform_action(self, decision: AgentDecision) -> bool:
        """
        HOOK: Выполняет действие решения.
        Делегирует существующему execute() логике.
        """
        if decision.decision_type == "ROOT_CAUSE_IDENTIFIED":
            root_cause = decision.outputs.get("root_cause", "")
            recommended_actions = decision.outputs.get("recommended_actions", [])

            await event_bus.publish(
                "DIAGNOSTIC_RECOMMENDATION",
                {
                    "root_cause": root_cause,
                    "confidence": decision.confidence,
                    "recommended_actions": recommended_actions,
                    "timestamp": datetime.now().isoformat(),
                },
            )
            logger.info(f"🔍 Diagnostic recommendation: {root_cause}")
            logger.info(f"   Recommended actions: {recommended_actions}")
            return True
        return False
