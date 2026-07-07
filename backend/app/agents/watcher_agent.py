"""
Watcher Agent — непрерывный мониторинг всех метрик и детекция аномалий.
Первый агент в иерархии, отвечает за обнаружение проблем.
"""

import logging
import asyncio
from typing import Dict, Any, Optional, List
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.base_agent import BaseAgent, AgentDecision, AgentContext
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
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

    Triggers:
    - HFR вырос на 30% за последние 5 кадров
    - RMS по RA > 2.0" в течение 3 кадров подряд
    - Температура камеры отклонилась от setpoint на >2°C
    - Ветер > 15 м/с с порывами > 20 м/с
    - Safety Monitor перешел в UNSAFE
    """

    def __init__(self):
        super().__init__(name="Watcher", role="Monitor & Anomaly Detection")

        # Пороговые значения для детекции аномалий
        self.thresholds = {
            "hfr_increase_percent": 30.0,  # Рост HFR на 30%
            "rms_ra_critical": 2.0,  # RMS RA > 2.0"
            "rms_dec_critical": 2.0,  # RMS Dec > 2.0"
            "temperature_deviation": 2.0,  # Отклонение температуры от setpoint
            "wind_speed_warning": 15.0,  # Ветер > 15 м/с
            "wind_gust_critical": 20.0,  # Порывы > 20 м/с
            "z_score_threshold": 3.0,  # Z-Score для аномалий
        }

        # История последних аномалий (для избежания спама)
        self._recent_anomalies: Dict[str, datetime] = {}
        self._anomaly_cooldown_seconds = 300  # 5 минут между повторными алертами

        # Подписка на события
        self._subscribed = False

    async def initialize(self):
        """Инициализация агента и подписка на события."""
        await super().initialize()

        if not self._subscribed:
            # Подписываемся на события для анализа
            event_bus.subscribe("NEW_FRAME", self._on_new_frame)
            event_bus.subscribe("PROMETHEUS_UPDATE", self._on_prometheus_update)
            event_bus.subscribe("LOG_EVENT", self._on_log_event)
            self._subscribed = True

        logger.info("✅ Watcher Agent initialized with thresholds:")
        for key, value in self.thresholds.items():
            logger.info(f"   - {key}: {value}")

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

        # 2. Проверка RMS
        rms_anomaly = await self._check_rms_metrics()
        if rms_anomaly:
            anomalies.append(rms_anomaly)

        # 3. Проверка температуры
        temp_anomaly = await self._check_temperature()
        if temp_anomaly:
            anomalies.append(temp_anomaly)

        # 4. Проверка ветра
        wind_anomaly = await self._check_wind_conditions()
        if wind_anomaly:
            anomalies.append(wind_anomaly)

        return anomalies

    async def _check_frame_metrics(self, frame: Dict[str, Any]) -> None:
        """Проверяет метрики отдельного кадра."""
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
        """Проверяет тренд HFR (Half Flux Radius)."""
        history = observatory_state.history.hfr

        # ИСПРАВЛЕНО: Поддержка малых выборок (fallback для симуляции)
        if len(history) < 3:
            return None

        # Для малых выборок (< 5 точек) используем простую детекцию выбросов
        if len(history) < 5:
            current_value = history[-1]
            baseline = history[:-1]

            if not baseline:
                return None

            baseline_mean = sum(baseline) / len(baseline)

            # Если текущее значение > 50% от базового — это аномалия
            if baseline_mean > 0 and current_value > baseline_mean * 1.5:
                deviation_percent = (
                    (current_value - baseline_mean) / baseline_mean
                ) * 100

                if not self._is_in_cooldown("hfr_increase"):
                    self._recent_anomalies["hfr_increase"] = datetime.now()

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
                            "note": "Small sample detection (< 5 points)",
                        },
                    )

            return None

        # Стандартная логика для больших выборок (5+ точек)
        recent = history[-5:]
        baseline = history[:-5] if len(history) > 5 else recent

        if not baseline:
            return None

        baseline_mean = sum(baseline) / len(baseline)
        current_mean = sum(recent) / len(recent)

        if baseline_mean > 0:
            increase_percent = ((current_mean - baseline_mean) / baseline_mean) * 100

            if increase_percent > self.thresholds["hfr_increase_percent"]:
                if self._is_in_cooldown("hfr_increase"):
                    return None

                # Z-Score для подтверждения
                std = (
                    (sum((x - baseline_mean) ** 2 for x in baseline) / len(baseline))
                    ** 0.5
                    if len(baseline) > 1
                    else 1.0
                )
                z_score = abs(current_mean - baseline_mean) / std if std > 0 else 0

                if z_score > self.thresholds["z_score_threshold"]:
                    self._recent_anomalies["hfr_increase"] = datetime.now()

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
                            "baseline_values": baseline[-10:]
                            if len(baseline) >= 10
                            else baseline,
                        },
                    )

        return None

    async def _check_fwhm_trend(self) -> Optional[AnomalyReport]:
        """Проверяет тренд FWHM (Full Width at Half Maximum)."""
        history = observatory_state.history.fwhm

        if len(history) < 5:
            return None

        recent = history[-5:]
        baseline = history[:-5] if len(history) > 5 else recent

        if not baseline:
            return None

        baseline_mean = np.mean(baseline)
        current_mean = np.mean(recent)

        if baseline_mean > 0:
            increase_percent = ((current_mean - baseline_mean) / baseline_mean) * 100

            if increase_percent > self.thresholds["hfr_increase_percent"]:
                if self._is_in_cooldown("fwhm_increase"):
                    return None

                std = np.std(baseline) if len(baseline) > 1 else 1.0
                z_score = abs(current_mean - baseline_mean) / std if std > 0 else 0

                if z_score > self.thresholds["z_score_threshold"]:
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
        """Проверяет метрики RMS гидирования."""
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
                        baseline_value=np.mean(history[:-3])
                        if len(history) > 3
                        else rms_ra,
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
                        baseline_value=np.mean(history[:-3])
                        if len(history) > 3
                        else rms_dec,
                        deviation_percent=0,
                        z_score=0,
                        severity="HIGH",
                        context={"recent_values": recent},
                    )

        return None

    async def _check_temperature(self) -> Optional[AnomalyReport]:
        """Проверяет температуру камеры."""
        current_temp = observatory_state.current_metrics.get("camera_temp")

        if current_temp is None:
            return None

        # Получаем setpoint из текущего состояния
        # (в реальности должно быть в ObservatoryState)
        setpoint = -15.0  # Значение по умолчанию

        deviation = abs(current_temp - setpoint)

        if deviation > self.thresholds["temperature_deviation"]:
            if not self._is_in_cooldown("temperature_deviation"):
                self._recent_anomalies["temperature_deviation"] = datetime.now()

                return AnomalyReport(
                    metric="CAMERA_TEMP",
                    current_value=current_temp,
                    baseline_value=setpoint,
                    deviation_percent=(deviation / abs(setpoint)) * 100
                    if setpoint != 0
                    else 0,
                    z_score=0,
                    severity="MEDIUM",
                    context={"setpoint": setpoint},
                )

        return None

    async def _check_wind_conditions(self) -> Optional[AnomalyReport]:
        """Проверяет ветровые условия."""
        wind_speed = observatory_state.weather.get("wind_speed")
        wind_gust = observatory_state.weather.get("wind_gust")

        # Критические порывы ветра
        if wind_gust and wind_gust > self.thresholds["wind_gust_critical"]:
            if not self._is_in_cooldown("wind_gust"):
                self._recent_anomalies["wind_gust"] = datetime.now()

                return AnomalyReport(
                    metric="WIND_GUST",
                    current_value=wind_gust,
                    baseline_value=self.thresholds["wind_gust_critical"],
                    deviation_percent=0,
                    z_score=0,
                    severity="CRITICAL",
                    context={"wind_speed": wind_speed},
                )

        # Высокая скорость ветра
        if wind_speed and wind_speed > self.thresholds["wind_speed_warning"]:
            if not self._is_in_cooldown("wind_speed"):
                self._recent_anomalies["wind_speed"] = datetime.now()

                return AnomalyReport(
                    metric="WIND_SPEED",
                    current_value=wind_speed,
                    baseline_value=self.thresholds["wind_speed_warning"],
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

        if wind_gust and wind_gust > self.thresholds["wind_gust_critical"]:
            anomaly = AnomalyReport(
                metric="WIND_GUST",
                current_value=wind_gust,
                baseline_value=self.thresholds["wind_gust_critical"],
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

        if rms_ra and rms_ra > self.thresholds["rms_ra_critical"]:
            anomaly = AnomalyReport(
                metric="RMS_RA",
                current_value=rms_ra,
                baseline_value=self.thresholds["rms_ra_critical"],
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
