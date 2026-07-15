"""
Auditor Agent v2 — post-mortem анализ сессий на основе точных данных.

ЭТАП 6 (полный рефакторинг):
- Источники данных: sessions_metadata (SQLite) — точные данные по каждому кадру
- Убраны расчёты из observatory_state.history (были грубой оценкой)
- Собственная формула quality_score (0-10) с весами:
  * avg_hfr: 25% (целевой < 2.5px)
  * avg_fwhm: 15% (целевой < 3.0px)
  * hfr_std: 10% (стабильность)
  * acceptance_rate: 25% (целевой > 90%)
  * hfr_trend: 15% (деградация = плохо)
  * problems: 10% (меньше проблем = лучше)
- LLM для detailed_report (если доступен)
- Интеграция с RAG для индексации Session Digest

Архитектура разделения ролей:
- **sessions_metadata (SQLite)**: точные структурированные данные
  → Каждый кадр с HFR, FWHM, RMS, gain, offset, binning
  → Быстрые SQL-запросы, агрегации
  → Основа для quality_score

- **RAG (Qdrant)**: текстовые дайджесты
  → Семантический поиск для Diagnostician/Copilot
  → Контекст из истории похожих сессий

Trigger:
- Событие SEQUENCE_STOPPED

Использование:
    from app.agents.auditor_agent import auditor_agent

    # Вызывается автоматически при SEQUENCE_STOPPED
    # Или вручную через API
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import json
import numpy as np
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.rag_engine import rag_engine

from backend.app.core.math_utils import calculate_trend

logger = logging.getLogger("AuditorAgent")


class SessionDigest(BaseModel):
    """
    Структурированный отчёт о завершённой сессии.

    Источники данных:
    - sessions_metadata (SQLite) — точные данные по каждому кадру
    - observatory_state — контекст (погода, астрономия)
    - LLM — текстовый detailed_report (если доступен)
    """

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
    hfr_std: Optional[float] = None
    avg_rms_ra: Optional[float] = None
    avg_rms_dec: Optional[float] = None
    hfr_trend: Optional[float] = None
    problems: List[Dict[str, str]] = Field(default_factory=list)
    recommendations: List[str] = Field(default_factory=list)
    quality_score: float = Field(ge=0.0, le=10.0)
    detailed_report: Optional[str] = None
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class AuditorAgent(BaseAgent):
    """
    Агент post-mortem анализа сессий v2.

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

        # Целевые метрики качества (из settings)
        try:
            from app.core.config import settings

            strategist_cfg = getattr(settings, "thresholds", None)
            if strategist_cfg:
                strategist_cfg = getattr(strategist_cfg, "strategist", None)

            self.quality_targets = {
                "hfr_target": (
                    strategist_cfg.hfr_target
                    if strategist_cfg and hasattr(strategist_cfg, "hfr_target")
                    else 2.5
                ),
                "fwhm_target": (
                    strategist_cfg.fwhm_target
                    if strategist_cfg and hasattr(strategist_cfg, "fwhm_target")
                    else 3.0
                ),
                "acceptance_rate_target": (
                    strategist_cfg.acceptance_rate_target
                    if strategist_cfg
                    and hasattr(strategist_cfg, "acceptance_rate_target")
                    else 0.90
                ),
            }
        except Exception as e:
            logger.debug(f"Could not load quality targets from settings: {e}")
            self.quality_targets = {
                "hfr_target": 2.5,
                "fwhm_target": 3.0,
                "acceptance_rate_target": 0.90,
            }

    async def initialize(self):
        """Инициализация агента аудита."""
        await super().initialize()

        # Подписываемся на событие завершения сессии
        event_bus.subscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)

        logger.info(
            f"✅ Auditor Agent v2 initialized "
            f"(quality targets: hfr<{self.quality_targets['hfr_target']}, "
            f"fwhm<{self.quality_targets['fwhm_target']}, "
            f"acceptance>{self.quality_targets['acceptance_rate_target']:.0%})"
        )

    async def shutdown(self):
        """Корректное завершение работы агента."""
        event_bus.unsubscribe("SEQUENCE_STOPPED", self._on_sequence_stopped)
        await super().shutdown()

    async def _on_sequence_stopped(self, data: Dict[str, Any]) -> None:
        """Обработка события завершения сессии."""
        logger.info("📊 Sequence stopped, generating Session Digest...")

        # === НОВОЕ (v4.0): Финализация сессии в sessions_metadata ===
        session_id = data.get("session_id") or data.get("target", "unknown")

        try:
            finalize_result = await sessions_metadata.finalize_session(session_id)
            logger.info(f"📊 Session finalized in metadata: {finalize_result}")
        except Exception as e:
            logger.warning(f"Could not finalize session in metadata: {e}")

        # Запускаем генерацию Session Digest (существующая логика)
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
        """
        Генерирует структурированный Session Digest с использованием LLM.

        Источники данных:
        - sessions_metadata (SQLite) — точные данные по каждому кадру
        - observatory_state — контекст (погода, астрономия)
        - LLM — текстовый detailed_report (если доступен)
        """
        from app.agents.llm_client import llm_client

        # Получаем данные из ObservatoryState
        metrics = observatory_state.current_metrics
        weather = observatory_state.weather

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

        # Создаём session_id
        session_id = f"{target}_{datetime.now().strftime('%Y-%m-%d')}"

        # === ПОЛУЧАЕМ ТОЧНЫЕ ДАННЫЕ ИЗ sessions_metadata ===
        try:
            session_stats = await sessions_metadata.get_session_stats(session_id)

            if "error" in session_stats:
                logger.warning(
                    f"Could not get session stats from metadata: {session_stats['error']}"
                )
                # Fallback на observatory_state.history
                return await self._generate_fallback_digest(
                    session_id, target, filter_name, exposure_time, gain, temperature
                )

            # Извлекаем точные данные
            session_data = session_stats.get("session", {})
            frames_data = session_stats.get("frames", {})
            hfr_data = session_stats.get("hfr", {})
            fwhm_data = session_stats.get("fwhm", {})
            problems_data = session_stats.get("problems", [])

            frames_total = frames_data.get("total", 0)
            frames_accepted = session_data.get("frames_accepted", 0)
            acceptance_rate = session_data.get("acceptance_rate", 0.0)

            avg_hfr = hfr_data.get("avg")
            avg_fwhm = fwhm_data.get("avg")
            hfr_std = hfr_data.get("std")

            avg_rms_ra = session_data.get("avg_rms_ra")
            avg_rms_dec = session_data.get("avg_rms_dec")

            # Вычисляем тренд HFR
            hfr_trend = await self._calculate_hfr_trend(session_id)

            # Форматируем проблемы
            problems = [
                {
                    "time": p.get("timestamp", ""),
                    "issue": p.get("description", ""),
                    "solution": p.get("solution", "Требуется анализ"),
                }
                for p in problems_data
            ]

        except Exception as e:
            logger.error(f"Error getting session data from metadata: {e}")
            # Fallback на observatory_state.history
            return await self._generate_fallback_digest(
                session_id, target, filter_name, exposure_time, gain, temperature
            )

        # Генерируем рекомендации
        recommendations = await self._generate_recommendations(
            avg_hfr, avg_fwhm, avg_rms_ra, avg_rms_dec, weather
        )

        # Рассчитываем quality score (собственная формула)
        quality_score = self._calculate_quality_score(
            avg_hfr=avg_hfr,
            avg_fwhm=avg_fwhm,
            hfr_std=hfr_std,
            acceptance_rate=acceptance_rate,
            hfr_trend=hfr_trend,
            problems_count=len(problems),
        )

        # Если LLM доступен, генерируем расширенный текстовый отчет
        detailed_report = None
        if llm_client.is_available():
            session_data_for_ll = {
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
                session_data=session_data_for_ll, problems=problems, context=context
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
            acceptance_rate=acceptance_rate,
            avg_hfr=avg_hfr,
            avg_fwhm=avg_fwhm,
            hfr_std=hfr_std,
            avg_rms_ra=avg_rms_ra,
            avg_rms_dec=avg_rms_dec,
            hfr_trend=hfr_trend,
            problems=problems,
            recommendations=recommendations,
            quality_score=quality_score,
            detailed_report=detailed_report,
        )

    async def _generate_fallback_digest(
        self,
        session_id: str,
        target: str,
        filter_name: str,
        exposure_time: float,
        gain: int,
        temperature: float,
    ) -> Optional[SessionDigest]:
        """
        Fallback генерация Session Digest из observatory_state.history.
        Используется когда sessions_metadata недоступен.
        """
        logger.warning(
            "Using fallback digest generation from observatory_state.history"
        )

        history = observatory_state.history

        # Подсчитываем кадры (грубая оценка)
        frames_total = len(history.hfr)
        frames_accepted = int(frames_total * 0.9)  # Примерная оценка (90% acceptance)

        # Средние метрики
        avg_hfr = sum(history.hfr) / len(history.hfr) if history.hfr else None
        avg_fwhm = sum(history.fwhm) / len(history.fwhm) if history.fwhm else None
        hfr_std = float(np.std(history.hfr)) if len(history.hfr) >= 3 else None

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
            avg_hfr, avg_fwhm, avg_rms_ra, avg_rms_dec, observatory_state.weather
        )

        # Рассчитываем quality score
        quality_score = self._calculate_quality_score(
            avg_hfr=avg_hfr,
            avg_fwhm=avg_fwhm,
            hfr_std=hfr_std,
            acceptance_rate=frames_accepted / frames_total if frames_total > 0 else 0,
            hfr_trend=None,
            problems_count=len(problems),
        )

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
            hfr_std=hfr_std,
            avg_rms_ra=avg_rms_ra,
            avg_rms_dec=avg_rms_dec,
            hfr_trend=None,
            problems=problems,
            recommendations=recommendations,
            quality_score=quality_score,
            detailed_report=None,
        )

    async def _calculate_hfr_trend(self, session_id: str) -> Optional[float]:
        """
        Вычисляет тренд HFR для сессии (наклон линейной регрессии).
        ИСПРАВЛЕНО (С-4): использует calculate_trend из core.math_utils.
        Returns:
        Положительный тренд → деградация
        Отрицательный тренд → улучшение
        None → недостаточно данных
        """
        try:
            frames = await sessions_metadata.get_frames(session_id, limit=10000)
            if not frames or len(frames) < 5:
                return None
            hfr_values = [f.hfr for f in frames if f.hfr is not None]
            if len(hfr_values) < 5:
                return None
            # ИСПРАВЛЕНО (С-4): единая функция из math_utils
            trend = calculate_trend(hfr_values)
            return trend
        except Exception as e:
            logger.debug(f"Could not calculate HFR trend: {e}")
        return None

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
        if avg_hfr and avg_hfr > self.quality_targets["hfr_target"]:
            recommendations.append(
                f"Средний HFR {avg_hfr:.2f}px выше оптимального. "
                "Рассмотрите более частые автофокусы."
            )

        # Рекомендации по FWHM
        if avg_fwhm and avg_fwhm > self.quality_targets["fwhm_target"]:
            recommendations.append(
                f"Средний FWHM {avg_fwhm:.2f}px выше оптимального. "
                "Проверьте коллимацию и seeing условия."
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
        hfr_std: Optional[float],
        acceptance_rate: float,
        hfr_trend: Optional[float],
        problems_count: int,
    ) -> float:
        """
        Рассчитывает quality score через единый модуль app.core.quality.
        ИСПРАВЛЕНО (С-10): устранено дублирование формулы.
        """
        from backend.app.core.quality import calculate_quality_score

        # Получаем eccentricity из observatory_state (если доступно)
        avg_eccentricity = observatory_state.current_metrics.get("eccentricity")

        # Получаем RMS total из истории
        rms_history = observatory_state.history.rms_ra
        avg_rms_total = None
        if rms_history:
            avg_rms_total = sum(rms_history) / len(rms_history)

        return calculate_quality_score(
            avg_hfr=avg_hfr,
            avg_eccentricity=avg_eccentricity,
            acceptance_rate=acceptance_rate,
            avg_rms_total=avg_rms_total,
            hfr_trend=hfr_trend,
            problems_count=problems_count,
        )

    async def generate_session_digest(self, data: Dict[str, Any]) -> None:
        """Генерирует Session Digest (вызывается Orchestrator'ом)."""
        await self._on_sequence_stopped(data)

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
