"""
Auditor Agent — анализирует завершенные сессии, генерирует Session Digest, пополняет RAG.
Отвечает за обучение системы на истории.

ИСПРАВЛЕНО (audit P3 - устранение хардкода):
- Все пороги quality score читаются из settings.auditor.quality_score
- Все пороги рекомендаций читаются из settings.auditor.recommendations
- Значения по умолчанию (gain, temperature, exposure) из settings.observatory_state
- LLM параметры (max_tokens, temperature) из settings.auditor
- Acceptance rate estimate из settings.auditor
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
from app.core.config import settings

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

    ИСПРАВЛЕНО (audit P3):
    - Все пороги и значения по умолчанию из конфигурации
    - НОЛЬ захардкоженных магических чисел

    Trigger:
    - Событие SEQUENCE_STOPPED
    """

    def __init__(self):
        super().__init__(name="Auditor", role="Post-Mortem Analysis")

        # === ИСПРАВЛЕНО (P3): Все параметры из конфигурации ===
        auditor_cfg = settings.auditor
        obs_cfg = settings.observatory_state

        # Quality Score пороги
        self._qs = auditor_cfg.quality_score
        self._base_score = self._qs.base_score

        # Recommendations пороги
        self._rec = auditor_cfg.recommendations

        # LLM параметры
        self._llm_max_tokens: int = auditor_cfg.llm_max_tokens
        self._llm_temperature: float = auditor_cfg.llm_temperature

        # Acceptance rate estimate (когда точные данные недоступны)
        self._default_acceptance_rate: float = (
            auditor_cfg.default_acceptance_rate_estimate
        )

        # Значения по умолчанию для метрик
        self._default_gain: int = int(obs_cfg.default_gain)
        self._default_temperature: float = obs_cfg.default_camera_temp
        self._default_exposure: float = obs_cfg.default_exposure_time

        # История всех сессий (для выявления паттернов)
        self._session_history: List[SessionDigest] = []

        logger.info("🔧 AuditorAgent initialized with config:")
        logger.info(
            f"   Quality score: base={self._base_score}, "
            f"HFR high={self._qs.hfr_threshold_high}, "
            f"HFR med={self._qs.hfr_threshold_medium}"
        )
        logger.info(
            f"   Recommendations: HFR>{self._rec.hfr_warning_threshold}, "
            f"RMS>{self._rec.rms_warning_threshold}, "
            f"Wind>{self._rec.wind_warning_threshold}"
        )
        logger.info(
            f"   LLM: max_tokens={self._llm_max_tokens}, "
            f"temperature={self._llm_temperature}"
        )

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

        # ИСПРАВЛЕНО (P3): Значения по умолчанию из конфигурации
        exposure_time = metrics.get("exposure_time", self._default_exposure)
        gain = metrics.get("gain", self._default_gain)
        temperature = metrics.get("camera_temp", self._default_temperature)

        # Подсчитываем кадры
        frames_total = len(history.hfr)  # Примерная оценка
        # ИСПРАВЛЕНО (P3): acceptance rate из конфигурации
        frames_accepted = int(frames_total * self._default_acceptance_rate)

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

        # Генерируем рекомендации (с порогами из конфигурации)
        recommendations = await self._generate_recommendations(
            avg_hfr, avg_fwhm, avg_rms_ra, avg_rms_dec, weather
        )

        # Рассчитываем quality score (с порогами из конфигурации)
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

            # ИСПРАВЛЕНО (P3): LLM параметры из конфигурации
            detailed_report = await llm_client.generate_session_digest(
                session_data=session_data,
                problems=problems,
                context=context,
                max_tokens=self._llm_max_tokens,
                temperature=self._llm_temperature,
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
        """
        Генерирует рекомендации для будущих сессий.

        ИСПРАВЛЕНО (P3): Все пороги из settings.auditor.recommendations.
        """
        recommendations = []

        # Рекомендации по HFR
        if avg_hfr and avg_hfr > self._rec.hfr_warning_threshold:
            recommendations.append(
                f"Средний HFR {avg_hfr:.2f}px выше оптимального "
                f"(порог: {self._rec.hfr_warning_threshold}px). "
                "Рассмотрите более частые автофокусы."
            )

        # Рекомендации по RMS
        if avg_rms_ra and avg_rms_ra > self._rec.rms_warning_threshold:
            recommendations.append(
                f'RMS по RA {avg_rms_ra:.2f}" высокий '
                f'(порог: {self._rec.rms_warning_threshold}"). '
                "Проверьте балансировку монтировки и настройки гидирования."
            )
        if avg_rms_dec and avg_rms_dec > self._rec.rms_warning_threshold:
            recommendations.append(
                f'RMS по Dec {avg_rms_dec:.2f}" высокий '
                f'(порог: {self._rec.rms_warning_threshold}"). '
                "Возможно требуется полярное выравнивание."
            )

        # Рекомендации по погоде
        wind_speed = weather.get("wind_speed")
        if wind_speed and wind_speed > self._rec.wind_warning_threshold:
            recommendations.append(
                f"Средняя скорость ветра {wind_speed:.1f} м/с "
                f"(порог: {self._rec.wind_warning_threshold} м/с). "
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
        """
        Рассчитывает overall quality score (0-10).

        ИСПРАВЛЕНО (P3): Все пороги и штрафы из settings.auditor.quality_score.
        - base_score: базовый балл (по умолчанию 10.0)
        - hfr_threshold_high/medium: пороги для штрафов по HFR
        - hfr_penalty_high/medium: размер штрафов
        - fwhm_threshold_high/medium: пороги для штрафов по FWHM
        - fwhm_penalty_high/medium: размер штрафов
        - rms_threshold: порог для штрафов по RMS
        - rms_penalty: размер штрафа
        - acceptance_bonus/penalty_threshold: пороги бонусов/штрафов
        - acceptance_bonus/penalty: размер бонусов/штрафов
        """
        qs = self._qs
        score = self._base_score

        # Штраф за высокий HFR
        if avg_hfr:
            if avg_hfr > qs.hfr_threshold_high:
                score -= qs.hfr_penalty_high
            elif avg_hfr > qs.hfr_threshold_medium:
                score -= qs.hfr_penalty_medium

        # Штраф за высокий FWHM
        if avg_fwhm:
            if avg_fwhm > qs.fwhm_threshold_high:
                score -= qs.fwhm_penalty_high
            elif avg_fwhm > qs.fwhm_threshold_medium:
                score -= qs.fwhm_penalty_medium

        # Штраф за высокий RMS
        if avg_rms_ra and avg_rms_ra > qs.rms_threshold:
            score -= qs.rms_penalty
        if avg_rms_dec and avg_rms_dec > qs.rms_threshold:
            score -= qs.rms_penalty

        # Бонус/штраф за acceptance rate
        if acceptance_rate > qs.acceptance_bonus_threshold:
            score += qs.acceptance_bonus
        elif acceptance_rate < qs.acceptance_penalty_threshold:
            score -= qs.acceptance_penalty

        return max(0.0, min(10.0, score))

    async def generate_session_digest(self, data: Dict[str, Any]) -> None:
        """Генерирует Session Digest (вызывается Orchestrator'ом)."""
        await self._on_sequence_stopped(data)
