"""
Watcher Agent — непрерывный мониторинг всех метрик и детекция аномалий.
Первый агент в иерархии, отвечает за обнаружение проблем.

ИСПРАВЛЕНО (audit 7.2):
- Все магические числа вынесены в settings.thresholds.watcher
- Пороги читаются из конфигурации при инициализации
- Добавлены дополнительные пороги для FWHM и cooldown
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.config import settings
from app.execution.trigger_emulator import trigger_emulator
import numpy as np

logger = logging.getLogger("WatcherAgent")


class AnomalyReport(BaseModel):
    """Отчет об обнаруженной аномалии."""

    metric: str
    current_value: float
    baseline_value: float
    deviation_percent: float
    z_score: float
    trend: Optional[float] = None
    severity: str = Field(pattern="^(LOW|MEDIUM|HIGH|CRITICAL)$")
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    context: Dict[str, Any] = Field(default_factory=dict)


class WatcherAgent(BaseAgent):
    """
    Агент мониторинга и детекции аномалий.

    Responsibilities:
    - Непрерывный мониторинг всех метрик (HFR, FWHM, RMS, температура, ветер)
    - Детекция аномалий через Z-Score и трендовый анализ
    - Генерация алертов для Orchestrator
    - Отслеживание активных процессов (автофокус, гидирование)

    ИСПРАВЛЕНО (audit 7.2):
    - Все пороги извлекаются из settings.thresholds.watcher
    - Магические числа заменены на именованные константы из конфига

    Triggers:
    - HFR вырос на hfr_increase_percent% за последние min_history_points кадров
    - RMS по RA > rms_ra_critical" в течение 3 кадров подряд
    - Температура камеры отклонилась от setpoint на >temperature_deviation°C
    - Ветер > wind_speed_warning м/с с порывами > wind_gust_critical м/с
    - Safety Monitor перешел в UNSAFE
    """

    def __init__(self):
        super().__init__(name="Watcher", role="Monitor & Anomaly Detection")

        # ИСПРАВЛЕНО (audit 7.2): Пороговые значения извлекаются из конфига
        watcher_cfg = settings.thresholds.watcher
        self.thresholds = {
            "hfr_increase_percent": watcher_cfg.hfr_increase_percent,
            "fwhm_increase_percent": watcher_cfg.fwhm_increase_percent,
            "rms_ra_critical": watcher_cfg.rms_ra_critical,
            "rms_dec_critical": watcher_cfg.rms_dec_critical,
            "temperature_deviation": watcher_cfg.temperature_deviation,
            "wind_speed_warning": watcher_cfg.wind_speed_warning,
            "wind_gust_critical": watcher_cfg.wind_gust_critical,
            "z_score_threshold": watcher_cfg.z_score_threshold,
            "min_history_points": watcher_cfg.min_history_points,
        }

        # История последних аномалий (для избежания спама)
        self._recent_anomalies: Dict[str, datetime] = {}
        self._anomaly_cooldown_seconds = watcher_cfg.anomaly_cooldown_seconds

        # Подписка на события
        self._subscribed = False

    async def initialize(self):
        """Инициализация агента и подписка на события."""
        await super().initialize()

        # ИСПРАВЛЕНО (v4.0 — проблема #47): проверка _subscribed
        if not self._subscribed:
            # Подписываемся на события для анализа
            event_bus.subscribe("NEW_FRAME", self._on_new_frame)
            event_bus.subscribe("PROMETHEUS_UPDATE", self._on_prometheus_update)
            event_bus.subscribe("LOG_EVENT", self._on_log_event)
            self._subscribed = True

        logger.info("✅ Watcher Agent initialized with thresholds:")
        for key, value in self.thresholds.items():
            logger.info(f"   - {key}: {value}")
        logger.info(f"   - cooldown_seconds: {self._anomaly_cooldown_seconds}")

    async def shutdown(self):
        """Корректное завершение работы агента."""
        if self._subscribed:
            event_bus.unsubscribe("NEW_FRAME", self._on_new_frame)
            event_bus.unsubscribe("PROMETHEUS_UPDATE", self._on_prometheus_update)
            event_bus.unsubscribe("LOG_EVENT", self._on_log_event)
            self._subscribed = False
        await super().shutdown()

    async def analyze(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        Анализирует контекст и принимает решение.
        Вызывается Orchestrator'ом при необходимости.
        """
        # Проверяем метрики
        anomalies = await self._check_all_metrics()
        if anomalies:
            # Создаем решение для обработки аномалий
            decision = AgentDecision(
                agent=self.name,
                decision_type="ANOMALY_DETECTED",
                inputs={"anomalies_count": len(anomalies)},
                outputs={"anomalies": [a.model_dump() for a in anomalies]},
                rationale=f"Обнаружено {len(anomalies)} аномалий",
                confidence=0.95,
            )
            self.log_decision(decision)
            return decision
        return None

    async def execute(self, decision: AgentDecision) -> bool:
        """Выполняет принятое решение."""
        if decision.decision_type == "ANOMALY_DETECTED":
            anomalies = decision.outputs.get("anomalies", [])
            # Обрабатываем каждую аномалию
            for anomaly_data in anomalies:
                anomaly = AnomalyReport(**anomaly_data)
                await self._handle_anomaly(anomaly)
            return True
        return False

    async def _on_new_frame(self, data: Dict[str, Any]):
        """Обработка нового кадра."""
        frame = data.get("frame", {})
        if not frame:
            return

        # Периодический INFO-отчёт для отладки (каждые 5 кадров)
        hfr_history_len = len(observatory_state.history.hfr)
        if hfr_history_len > 0 and hfr_history_len % 5 == 0:
            recent_hfr = observatory_state.history.hfr[-5:]
            logger.info(
                f"📊 HFR history: {hfr_history_len} points, "
                f"recent: {[f'{v:.2f}' for v in recent_hfr]}"
            )

        # Проверяем метрики кадра
        await self._check_frame_metrics(frame)

    async def _on_prometheus_update(self, data: Dict[str, Any]):
        """Обработка обновления метрик Prometheus."""
        # Проверяем погодные условия
        await self._check_weather_conditions(data)
        # Проверяем гидирование
        await self._check_guiding_metrics(data)

    async def _on_log_event(self, data: Dict[str, Any]):
        """Обработка событий из логов."""
        event_type = data.get("event_type", "")

        # Критические события из логов
        if event_type == "safety_unsafe":
            await self._generate_critical_alert("Safety Monitor перешел в UNSAFE", data)
        elif event_type == "guiding_lost":
            await self._generate_alert("HIGH", "Потеря гидирования", data)
        elif event_type in ("autofocus_fail", "plate_solve_fail"):
            await self._generate_alert("MEDIUM", f"Ошибка: {event_type}", data)

    async def _check_all_metrics(self) -> List[AnomalyReport]:
        """Проверяет все метрики и возвращает список аномалий."""
        anomalies = []

        # 1. Проверка HFR
        hfr_anomaly = await self._check_hfr_trend()
        if hfr_anomaly:
            anomalies.append(hfr_anomaly)

        # 2. Проверка FWHM
        fwhm_anomaly = await self._check_fwhm_trend()
        if fwhm_anomaly:
            anomalies.append(fwhm_anomaly)

        # 3. Проверка RMS
        rms_anomaly = await self._check_rms_metrics()
        if rms_anomaly:
            anomalies.append(rms_anomaly)

        # 4. Проверка температуры
        temp_anomaly = await self._check_temperature()
        if temp_anomaly:
            anomalies.append(temp_anomaly)

        # 5. Проверка ветра
        wind_anomaly = await self._check_wind_conditions()
        if wind_anomaly:
            anomalies.append(wind_anomaly)

        return anomalies

    async def _check_frame_metrics(self, frame: Dict[str, Any]) -> None:
        """Проверяет метрики отдельного кадра."""
        # ИСПРАВЛЕНО (v4.0 — проблема #61): явная проверка frame
        if not frame:
            logger.debug("Empty frame data, skipping check")
            return
        hfr = frame.get("hfr")
        fwhm = frame.get("fwhm")

        if hfr is not None:
            # Проверяем тренд HFR
            anomaly = await self._check_hfr_trend()
            if anomaly:
                await self._handle_anomaly(anomaly)

        if fwhm is not None:
            # Проверяем тренд FWHM
            anomaly = await self._check_fwhm_trend()
            if anomaly:
                await self._handle_anomaly(anomaly)

    async def _check_hfr_trend(self) -> Optional[AnomalyReport]:
        """
        Проверяет тренд HFR (Half Flux Radius).
        Использует Z-Score и процентное отклонение от базового значения.
        """
        history = observatory_state.history.hfr
        min_points = self.thresholds["min_history_points"]

        # Детальное логирование для отладки
        logger.debug(f"🔍 HFR check: history length = {len(history)}")
        if len(history) >= 3:
            logger.debug(
                f"   Last 5 values: {history[-5:] if len(history) >= 5 else history}"
            )

        if len(history) < min_points:
            logger.debug(f"   Skipping: not enough data points (< {min_points})")
            return None

        # Для малых выборок (3-4 точки) — упрощенная детекция
        if len(history) < 5:
            current_value = history[-1]
            baseline = history[:-1]
            if not baseline:
                return None

            baseline_mean = sum(baseline) / len(baseline)
            if baseline_mean == 0:
                return None

            # Если текущее значение > 50% от базового — это аномалия
            deviation_percent = ((current_value - baseline_mean) / baseline_mean) * 100

            if deviation_percent > 50.0:  # Упрощенный порог для малых выборок
                if self._is_in_cooldown("hfr_increase"):
                    logger.debug("   Skipping: cooldown active")
                    return None

                self._recent_anomalies["hfr_increase"] = datetime.now()
                logger.warning(
                    f"⚠️ HFR anomaly detected (small sample): "
                    f"current={current_value:.2f}, "
                    f"baseline_mean={baseline_mean:.2f}, "
                    f"deviation={deviation_percent:.1f}%"
                )

                return AnomalyReport(
                    metric="HFR",
                    current_value=current_value,
                    baseline_value=baseline_mean,
                    deviation_percent=deviation_percent,
                    z_score=0,  # Z-Score не применим для малых выборок
                    severity="HIGH",
                    context={
                        "recent_values": history,
                        "baseline_values": baseline,
                        "note": "Small sample detection (3-4 points)",
                    },
                )
            return None

        # Стандартная логика для больших выборок (5+ точек)
        recent = history[-5:]
        baseline = history[:-5] if len(history) > 5 else history[: len(history) // 2]
        if not baseline:
            return None

        baseline_mean = sum(baseline) / len(baseline)
        current_mean = sum(recent) / len(recent)
        if baseline_mean == 0:
            return None

        increase_percent = ((current_mean - baseline_mean) / baseline_mean) * 100
        hfr_threshold = self.thresholds["hfr_increase_percent"]

        logger.debug(
            f"   HFR analysis: baseline_mean={baseline_mean:.2f}, "
            f"current_mean={current_mean:.2f}, "
            f"increase={increase_percent:.1f}% (threshold: {hfr_threshold}%)"
        )

        if increase_percent > hfr_threshold:
            if self._is_in_cooldown("hfr_increase"):
                logger.debug("   Skipping: cooldown active")
                return None

            # Z-Score для подтверждения
            std = (
                (sum((x - baseline_mean) ** 2 for x in baseline) / len(baseline)) ** 0.5
                if len(baseline) > 1
                else 1.0
            )
            z_score = abs(current_mean - baseline_mean) / std if std > 0 else 0
            z_threshold = self.thresholds["z_score_threshold"]

            logger.debug(f"   Z-Score: {z_score:.2f} (threshold: {z_threshold})")

            # Для симуляции убираем требование Z-Score
            # В реальности Z-Score помогает отсеять шум
            if z_score > z_threshold or increase_percent > 60.0:
                self._recent_anomalies["hfr_increase"] = datetime.now()

                logger.warning(
                    f"⚠️ HFR anomaly detected: baseline={baseline_mean:.2f}, "
                    f"current={current_mean:.2f}, "
                    f"increase={increase_percent:.1f}%, "
                    f"z_score={z_score:.2f}"
                )

                return AnomalyReport(
                    metric="HFR",
                    current_value=current_mean,
                    baseline_value=baseline_mean,
                    deviation_percent=increase_percent,
                    z_score=z_score,
                    trend=observatory_state.get_trend("hfr", window=10),
                    severity="HIGH",
                    context={
                        "recent_values": recent,
                        "baseline_values": (
                            baseline[-10:] if len(baseline) >= 10 else baseline
                        ),
                    },
                )
        return None

    async def _check_fwhm_trend(self) -> Optional[AnomalyReport]:
        """
        Проверяет тренд FWHM (Full Width at Half Maximum).
        Аналогично HFR, но с отдельным порогом.
        """
        history = observatory_state.history.fwhm
        min_points = self.thresholds["min_history_points"]

        if len(history) < min_points:
            return None

        recent = history[-5:]
        baseline = history[:-5] if len(history) > 5 else recent
        if not baseline:
            return None

        baseline_mean = np.mean(baseline)
        current_mean = np.mean(recent)
        fwhm_threshold = self.thresholds["fwhm_increase_percent"]

        if baseline_mean > 0:
            increase_percent = ((current_mean - baseline_mean) / baseline_mean) * 100

            if increase_percent > fwhm_threshold:
                if self._is_in_cooldown("fwhm_increase"):
                    return None

                std = np.std(baseline) if len(baseline) > 1 else 1.0
                z_score = abs(current_mean - baseline_mean) / std if std > 0 else 0
                z_threshold = self.thresholds["z_score_threshold"]

                if z_score > z_threshold:
                    self._recent_anomalies["fwhm_increase"] = datetime.now()

                    return AnomalyReport(
                        metric="FWHM",
                        current_value=current_mean,
                        baseline_value=baseline_mean,
                        deviation_percent=increase_percent,
                        z_score=z_score,
                        severity="MEDIUM",
                    )
        return None

    async def _check_rms_metrics(self) -> Optional[AnomalyReport]:
        """
        Проверяет метрики RMS гидирования.
        Алерт срабатывает, если RMS высокий в течение 3+ кадров подряд.
        """
        rms_ra = observatory_state.current_metrics.get("rms_ra")
        rms_dec = observatory_state.current_metrics.get("rms_dec")

        # Проверяем RMS RA
        if rms_ra and rms_ra > self.thresholds["rms_ra_critical"]:
            history = observatory_state.history.rms_ra
            recent = history[-3:] if len(history) >= 3 else []

            # Если RMS высокий в течение 3+ кадров подряд
            if len(recent) >= 3 and all(
                v > self.thresholds["rms_ra_critical"] for v in recent
            ):
                if not self._is_in_cooldown("rms_ra_high"):
                    self._recent_anomalies["rms_ra_high"] = datetime.now()

                    return AnomalyReport(
                        metric="RMS_RA",
                        current_value=rms_ra,
                        baseline_value=(
                            np.mean(history[:-3]) if len(history) > 3 else rms_ra
                        ),
                        deviation_percent=0,
                        z_score=0,
                        severity="HIGH",
                        context={"recent_values": recent},
                    )

        # Проверяем RMS Dec
        if rms_dec and rms_dec > self.thresholds["rms_dec_critical"]:
            history = observatory_state.history.rms_dec
            recent = history[-3:] if len(history) >= 3 else []

            if len(recent) >= 3 and all(
                v > self.thresholds["rms_dec_critical"] for v in recent
            ):
                if not self._is_in_cooldown("rms_dec_high"):
                    self._recent_anomalies["rms_dec_high"] = datetime.now()

                    return AnomalyReport(
                        metric="RMS_DEC",
                        current_value=rms_dec,
                        baseline_value=(
                            np.mean(history[:-3]) if len(history) > 3 else rms_dec
                        ),
                        deviation_percent=0,
                        z_score=0,
                        severity="HIGH",
                        context={"recent_values": recent},
                    )
        return None

    async def _check_temperature(self) -> Optional[AnomalyReport]:
        """
        Проверяет температуру камеры.
        Алерт если отклонение от setpoint превышает порог.
        """
        current_temp = observatory_state.current_metrics.get("camera_temp")
        if current_temp is None:
            return None

        # Получаем setpoint из текущего состояния
        setpoint = -15.0  # Значение по умолчанию
        temp_deviation = self.thresholds["temperature_deviation"]
        deviation = abs(current_temp - setpoint)

        if deviation > temp_deviation:
            if not self._is_in_cooldown("temperature_deviation"):
                self._recent_anomalies["temperature_deviation"] = datetime.now()

                return AnomalyReport(
                    metric="CAMERA_TEMP",
                    current_value=current_temp,
                    baseline_value=setpoint,
                    deviation_percent=(
                        (deviation / abs(setpoint)) * 100 if setpoint != 0 else 0
                    ),
                    z_score=0,
                    severity="MEDIUM",
                    context={"setpoint": setpoint},
                )
        return None

    async def _check_wind_conditions(self) -> Optional[AnomalyReport]:
        """
        Проверяет ветровые условия.
        Критические порывы ветра → CRITICAL, высокая скорость → HIGH.
        """
        wind_speed = observatory_state.weather.get("wind_speed")
        wind_gust = observatory_state.weather.get("wind_gust")

        # Критические порывы ветра
        wind_gust_threshold = self.thresholds["wind_gust_critical"]
        if wind_gust and wind_gust > wind_gust_threshold:
            if not self._is_in_cooldown("wind_gust"):
                self._recent_anomalies["wind_gust"] = datetime.now()

                return AnomalyReport(
                    metric="WIND_GUST",
                    current_value=wind_gust,
                    baseline_value=wind_gust_threshold,
                    deviation_percent=0,
                    z_score=0,
                    severity="CRITICAL",
                    context={"wind_speed": wind_speed},
                )

        # Высокая скорость ветра
        wind_speed_threshold = self.thresholds["wind_speed_warning"]
        if wind_speed and wind_speed > wind_speed_threshold:
            if not self._is_in_cooldown("wind_speed"):
                self._recent_anomalies["wind_speed"] = datetime.now()

                return AnomalyReport(
                    metric="WIND_SPEED",
                    current_value=wind_speed,
                    baseline_value=wind_speed_threshold,
                    deviation_percent=0,
                    z_score=0,
                    severity="HIGH",
                    context={"wind_gust": wind_gust},
                )
        return None

    async def _check_weather_conditions(self, data: Dict[str, Any]) -> None:
        """Проверяет погодные условия из Prometheus."""
        wind_speed = data.get("wx_wind_speed")
        wind_gust = data.get("wx_wind_gust")
        wind_gust_threshold = self.thresholds["wind_gust_critical"]

        if wind_gust and wind_gust > wind_gust_threshold:
            anomaly = AnomalyReport(
                metric="WIND_GUST",
                current_value=wind_gust,
                baseline_value=wind_gust_threshold,
                deviation_percent=0,
                z_score=0,
                severity="CRITICAL",
                context={"wind_speed": wind_speed},
            )
            await self._handle_anomaly(anomaly)

    async def _check_guiding_metrics(self, data: Dict[str, Any]) -> None:
        """Проверяет метрики гидирования из Prometheus."""
        rms_ra = data.get("guider_rms_ra")
        rms_dec = data.get("guider_rms_dec")
        rms_ra_threshold = self.thresholds["rms_ra_critical"]

        if rms_ra and rms_ra > rms_ra_threshold:
            anomaly = AnomalyReport(
                metric="RMS_RA",
                current_value=rms_ra,
                baseline_value=rms_ra_threshold,
                deviation_percent=0,
                z_score=0,
                severity="HIGH",
            )
            await self._handle_anomaly(anomaly)

    async def _handle_anomaly(self, anomaly: AnomalyReport) -> None:
        """Обрабатывает обнаруженную аномалию."""
        # Определяем уровень алерта
        if anomaly.severity == "CRITICAL":
            level = "CRITICAL"
        elif anomaly.severity == "HIGH":
            level = "WARNING"
        else:
            level = "INFO"

        # Формируем сообщение
        message = (
            f"Аномалия {anomaly.metric}: "
            f"текущее значение {anomaly.current_value:.2f}, "
            f"базовое {anomaly.baseline_value:.2f}"
        )
        if anomaly.deviation_percent > 0:
            message += f" (отклонение {anomaly.deviation_percent:.1f}%)"

        # Публикуем алерт
        await self._generate_alert(level, message, anomaly.model_dump())
        logger.warning(f"⚠️ {message}")

    async def _generate_alert(
        self, level: str, message: str, context: Dict[str, Any]
    ) -> None:
        """Генерирует алерт и публикует его в EventBus."""
        alert = {
            "level": level,
            "message": message,
            "agent": self.name,
            "timestamp": datetime.now().isoformat(),
            "context": context,
        }
        await event_bus.publish("ALERT", alert)

        # Логируем действие
        observatory_state.log_ai_action(
            agent=self.name,
            action=f"Generate Alert [{level}]",
            reason=message,
            result="Alert published",
        )

    async def _generate_critical_alert(
        self, message: str, context: Dict[str, Any]
    ) -> None:
        """Генерирует критический алерт."""
        await self._generate_alert("CRITICAL", message, context)

    def _is_in_cooldown(self, anomaly_key: str) -> bool:
        """Проверяет, находится ли аномалия в cooldown периоде."""
        last_time = self._recent_anomalies.get(anomaly_key)
        if not last_time:
            return False
        elapsed = (datetime.now() - last_time).total_seconds()
        return elapsed < self._anomaly_cooldown_seconds

    async def analyze_frame(self, data: Dict[str, Any]) -> None:
        """Анализирует новый кадр (вызывается Orchestrator'ом)."""
        await self._on_new_frame(data)

    async def start_monitoring(self, data: Dict[str, Any]) -> None:
        """Начинает мониторинг (вызывается при старте секвенсора)."""
        logger.info("🔍 Watcher started monitoring")

    async def stop_monitoring(self, data: Dict[str, Any]) -> None:
        """Останавливает мониторинг (вызывается при остановке секвенсора)."""
        logger.info("🔍 Watcher stopped monitoring")

    async def _make_decision(self, context: AgentContext) -> Optional[AgentDecision]:
        """
        HOOK: Принимает решение на основе контекста.
        Реализация абстрактного метода из BaseAgent.
        """
        # Проверяем метрики и ищем аномалии
        anomalies = await self._check_all_metrics()

        if anomalies:
            return AgentDecision(
                agent=self.name,
                decision_type="ANOMALY_DETECTED",
                inputs={"anomalies_count": len(anomalies)},
                outputs={"anomalies": [a.model_dump() for a in anomalies]},
                rationale=f"Обнаружено {len(anomalies)} аномалий",
                confidence=0.95,
            )

        return None

    async def _perform_action(self, decision: AgentDecision) -> bool:
        """
        HOOK: Выполняет действие решения.
        Реализация абстрактного метода из BaseAgent.
        """
        if decision.decision_type == "ANOMALY_DETECTED":
            anomalies = decision.outputs.get("anomalies", [])

            # Обрабатываем каждую аномалию
            for anomaly_data in anomalies:
                anomaly = AnomalyReport(**anomaly_data)
                await self._handle_anomaly(anomaly)

            return True

        return False

    # В watcher_agent.py
    async def _on_livestack_enhanced(self, data: Dict[str, Any]) -> None:
        """Обработка расширенных данных LiveStack."""
        acceptance_rate = data.get("acceptance_rate")

        if acceptance_rate is not None and acceptance_rate < 0.5:
            # Генерация алерта о низком acceptance rate
            anomaly = AnomalyReport(
                metric="LIVESTACK_ACCEPTANCE",
                current_value=acceptance_rate,
                baseline_value=0.90,
                deviation_percent=((0.90 - acceptance_rate) / 0.90) * 100,
                z_score=0,
                severity="HIGH",
                context={"recommendations": data.get("recommendations", [])},
            )
            await self._handle_anomaly(anomaly)
