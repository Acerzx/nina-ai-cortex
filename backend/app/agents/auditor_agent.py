"""
Auditor Agent — анализирует завершенные сессии, генерирует Session Digest, пополняет RAG.
Отвечает за обучение системы на истории.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import json
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.rag_engine import rag_engine

logger = logging.getLogger("AuditorAgent")


class SessionDigest(BaseModel):
    """Структурированный отчет о завершенной сессии."""

    session_id: str
    target: str
    date: str
    filter: str
    exposure_time: float
    gain: int
    temperature: float
    frames_total: int
    frames_accepted: int
    acceptance_rate: float
    avg_hfr: Optional[float] = None
    avg_fwhm: Optional[float] = None
    avg_rms_ra: Optional[float] = None
    avg_rms_dec: Optional[float] = None
    problems: List[Dict[str, str]] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    quality_score: float = Field(ge=0.0, le=10.0)
    detailed_report: Optional[str] = None  # Расширенный отчет от LLM
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class AuditorAgent(BaseAgent):
    """
    Агент post-mortem анализа сессий.

    Responsibilities:
    - Генерация Session Digest после завершения сессии
    - Индексация в RAG для будущего обучения
    - Выявление повторяющихся проблем
    - Генерация рекомендаций для будущих сессий
    - Использование LLM для создания расширенных отчетов

    Trigger:
    - Событие SEQUENCE_STOPPED
    """

    def __init__(self):
        super().__init__(name="Auditor", role="Post-Mortem Analysis")

        # История всех сессий (для выявления паттернов)
        self._session_history: List[SessionDigest] = []

    async def initialize(self):
        """Инициализация агента аудита."""
        await super().initialize()

        # Подписываемся на событие завершения сессии
        event_bus.subscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)

        logger.info("✅ Auditor Agent initialized")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)

        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует завершенную сессию.
        Вызывается Orchestrator'ом при завершении сессии.
        """
        # Генерируем Session Digest
        digest = await self._generate_session_digest()

        if digest:
            decision = AgentDecision(
                agent=self.name,
                decision_type="SESSION_DIGEST_GENERATED",
                inputs={"session_id": digest.session_id},
                outputs={"digest": digest.model_dump()},
                rationale=f"Session Digest generated for {digest.target}",
                confidence=1.0,
            )
            self.log_decision(decision)
            return decision

        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет индексацию Session Digest в RAG."""
        if decision.decision_type == "SESSION_DIGEST_GENERATED":
            digest_data = decision.outputs.get("digest", {})

            try:
                # Индексируем в RAG
                chunks_added = await rag_engine.add_session_digest(digest_data)

                logger.info(f"✅ Session Digest indexed in RAG: {chunks_added} chunks")

                # Сохраняем в историю
                digest = SessionDigest(**digest_data)
                self._session_history.append(digest)

                # Публикуем событие для UI
                await event_bus.publish(
                    "SESSION_DIGEST_READY",
                    {
                        "session_id": digest.session_id,
                        "quality_score": digest.quality_score,
                        "recommendations": digest.recommendations,
                    },
                )

                return True

            except Exception as e:
                logger.error(f"Failed to index Session Digest: {e}")
                return False

        return False

    async def _on_sequence_stopped(self, data: Dict[str, Any]) -> None:
        """Обработка события завершения сессии."""
        logger.info("📊 Sequence stopped, generating Session Digest...")

        # Запускаем генерацию Session Digest
        await self.analyze(
            AgentContext(
                current_metrics=observatory_state.current_metrics,
                weather=observatory_state.weather,
                astronomy=observatory_state.astronomy,
                sequence_state={},
                safety_status=observatory_state.safety_status,
                active_alerts=[],
            )
        )

    async def _generate_session_digest(self) -> Optional[SessionDigest]:
        """Генерирует структурированный Session Digest с использованием LLM."""
        from app.agents.llm_client import llm_client

        # Получаем данные из ObservatoryState
        metrics = observatory_state.current_metrics
        weather = observatory_state.weather
        history = observatory_state.history

        # Определяем параметры сессии
        target = (
            observatory_state.active_targets[0].get("name", "Unknown")
            if observatory_state.active_targets
            else "Unknown"
        )
        filter_name = metrics.get("filter", "Unknown")
        exposure_time = metrics.get("exposure_time", 60.0)
        gain = metrics.get("gain", 85)
        temperature = metrics.get("camera_temp", -15.0)

        # Подсчитываем кадры
        frames_total = len(history.hfr)  # Примерная оценка
        frames_accepted = int(frames_total * 0.9)  # Примерная оценка (90% acceptance)

        # Средние метрики
        avg_hfr = sum(history.hfr) / len(history.hfr) if history.hfr else None
        avg_fwhm = sum(history.fwhm) / len(history.fwhm) if history.fwhm else None
        avg_rms_ra = (
            sum(history.rms_ra) / len(history.rms_ra) if history.rms_ra else None
        )
        avg_rms_dec = (
            sum(history.rms_dec) / len(history.rms_dec) if history.rms_dec else None
        )

        # Выявляем проблемы из активных алертов
        problems = []
        for alert in observatory_state.active_alerts:
            if alert.get("level") in ("WARNING", "CRITICAL"):
                problems.append(
                    {
                        "time": alert.get("timestamp", ""),
                        "issue": alert.get("message", ""),
                        "solution": "Требуется анализ",
                    }
                )

        # Генерируем рекомендации
        recommendations = await self._generate_recommendations(
            avg_hfr, avg_fwhm, avg_rms_ra, avg_rms_dec, weather
        )

        # Рассчитываем quality score
        quality_score = self._calculate_quality_score(
            avg_hfr,
            avg_fwhm,
            avg_rms_ra,
            avg_rms_dec,
            frames_accepted / frames_total if frames_total > 0 else 0,
        )

        # Создаем session_id
        session_id = f"{target}_{datetime.now().strftime('%Y-%m-%d')}"

        # Если LLM доступен, генерируем расширенный текстовый отчет
        detailed_report = None
        if llm_client.is_available():
            session_data = {
                "target": target,
                "filter": filter_name,
                "exposure_time": exposure_time,
                "frames_total": frames_total,
                "frames_accepted": frames_accepted,
                "avg_hfr": avg_hfr,
                "avg_rms_ra": avg_rms_ra,
                "avg_rms_dec": avg_rms_dec,
                "problems": problems,
            }

            context = await self.get_rag_context(f"Сессия {target} {filter_name}")

            detailed_report = await llm_client.generate_session_digest(
                session_data=session_data, problems=problems, context=context
            )

            if detailed_report:
                logger.info("✨ Session Digest enhanced with LLM analysis")

        return SessionDigest(
            session_id=session_id,
            target=target,
            date=datetime.now().strftime("%Y-%m-%d"),
            filter=filter_name,
            exposure_time=exposure_time,
            gain=gain,
            temperature=temperature,
            frames_total=frames_total,
            frames_accepted=frames_accepted,
            acceptance_rate=frames_accepted / frames_total if frames_total > 0 else 0,
            avg_hfr=avg_hfr,
            avg_fwhm=avg_fwhm,
            avg_rms_ra=avg_rms_ra,
            avg_rms_dec=avg_rms_dec,
            problems=problems,
            recommendations=recommendations,
            quality_score=quality_score,
            detailed_report=detailed_report,
        )

    async def _generate_recommendations(
        self,
        avg_hfr: Optional[float],
        avg_fwhm: Optional[float],
        avg_rms_ra: Optional[float],
        avg_rms_dec: Optional[float],
        weather: Dict[str, Any],
    ) -> List[str]:
        """Генерирует рекомендации для будущих сессий."""
        recommendations = []

        # Рекомендации по HFR
        if avg_hfr and avg_hfr > 2.5:
            recommendations.append(
                f"Средний HFR {avg_hfr:.2f}px выше оптимального. "
                "Рассмотрите более частые автофокусы."
            )

        # Рекомендации по RMS
        if avg_rms_ra and avg_rms_ra > 1.5:
            recommendations.append(
                f'RMS по RA {avg_rms_ra:.2f}" высокий. '
                "Проверьте балансировку монтировки и настройки гидирования."
            )

        if avg_rms_dec and avg_rms_dec > 1.5:
            recommendations.append(
                f'RMS по Dec {avg_rms_dec:.2f}" высокий. '
                "Возможно требуется полярное выравнивание."
            )

        # Рекомендации по погоде
        wind_speed = weather.get("wind_speed")
        if wind_speed and wind_speed > 10.0:
            recommendations.append(
                f"Средняя скорость ветра {wind_speed:.1f} м/с. "
                "Избегайте съемки при сильном ветре или выбирайте подветренные цели."
            )

        # Общие рекомендации
        if not recommendations:
            recommendations.append(
                "Сессия прошла без значительных проблем. Продолжайте в том же духе!"
            )

        return recommendations

    def _calculate_quality_score(
        self,
        avg_hfr: Optional[float],
        avg_fwhm: Optional[float],
        avg_rms_ra: Optional[float],
        avg_rms_dec: Optional[float],
        acceptance_rate: float,
    ) -> float:
        """Рассчитывает overall quality score (0-10)."""
        score = 10.0

        # Штраф за высокий HFR
        if avg_hfr:
            if avg_hfr > 3.0:
                score -= 2.0
            elif avg_hfr > 2.5:
                score -= 1.0

        # Штраф за высокий FWHM
        if avg_fwhm:
            if avg_fwhm > 4.0:
                score -= 2.0
            elif avg_fwhm > 3.0:
                score -= 1.0

        # Штраф за высокий RMS
        if avg_rms_ra and avg_rms_ra > 1.5:
            score -= 1.0
        if avg_rms_dec and avg_rms_dec > 1.5:
            score -= 1.0

        # Бонус за высокий acceptance rate
        if acceptance_rate > 0.95:
            score += 1.0
        elif acceptance_rate < 0.80:
            score -= 1.0

        return max(0.0, min(10.0, score))

    async def generate_session_digest(self, data: Dict[str, Any]) -> None:
        """Генерирует Session Digest (вызывается Orchestrator'ом)."""
        await self._on_sequence_stopped(data)

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует завершенную сессию.
        Вызывается Orchestrator'ом при завершении сессии.
        """
        # Делегируем в _make_decision через Template Method
        return await self._make_decision(context)

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        HOOK: Принимает решение на основе контекста.
        Реализация абстрактного метода из BaseAgent.
        """
        # Генерируем Session Digest
        digest = await self._generate_session_digest()

        if digest:
            return AgentDecision(
                agent=self.name,
                decision_type="SESSION_DIGEST_GENERATED",
                inputs={"session_id": digest.session_id},
                outputs={"digest": digest.model_dump()},
                rationale=f"Session Digest generated for {digest.target}",
                confidence=1.0,
            )
        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет индексацию Session Digest в RAG."""
        # Делегируем в _perform_action через Template Method
        return await self._perform_action(decision)

    async def _perform_action(self, decision: AgentDecision) -> bool:
        """
        HOOK: Выполняет действие решения.
        Реализация абстрактного метода из BaseAgent.
        """
        if decision.decision_type == "SESSION_DIGEST_GENERATED":
            digest_data = decision.outputs.get("digest", {})

            try:
                # Индексируем в RAG
                chunks_added = await rag_engine.add_session_digest(digest_data)
                logger.info(f"✅ Session Digest indexed in RAG: {chunks_added} chunks")

                # Сохраняем в историю
                digest = SessionDigest(**digest_data)
                self._session_history.append(digest)

                # Публикуем событие для UI
                await event_bus.publish(
                    "SESSION_DIGEST_READY",
                    {
                        "session_id": digest.session_id,
                        "quality_score": digest.quality_score,
                        "recommendations": digest.recommendations,
                    },
                )

                return True

            except Exception as e:
                logger.error(f"Failed to index Session Digest: {e}")
                return False

        return False
