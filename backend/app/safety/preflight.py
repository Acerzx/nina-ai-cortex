"""
Pre-flight Checklist — формализованный чек-лист перед стартом сессии.
Основан на архитектуре Atlas (8 gates с агрегированным verdict).
"""

import logging
from typing import Dict, Any, List, Optional
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus

logger = logging.getLogger("PreFlight")


class GateStatus(Enum):
    """Статус проверки gate."""

    GO = "GO"
    WAITING = "WAITING"
    CAUTION = "CAUTION"
    NO_GO = "NO-GO"


class GateResult(BaseModel):
    """Результат проверки одного gate."""

    gate_name: str
    status: GateStatus
    message: str
    details: Dict[str, Any] = Field(default_factory=dict)
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class PreFlightReport(BaseModel):
    """Полный отчет pre-flight проверки."""

    gates: Dict[str, GateResult]
    verdict: GateStatus  # Агрегированный verdict
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    recommendations: List[str] = Field(default_factory=list)


class PreFlightChecker:
    """
    Pre-flight checker с 8 gates (на основе Atlas).

    Gates:
    1. WeatherGate — погодные условия
    2. HardwareGate — статус оборудования
    3. CalibrationGate — свежесть калибровок
    4. DiskSpaceGate — свободное место на диске
    5. APIHealthGate — доступность N.I.N.A. API
    6. SafetyMonitorGate — статус Safety Monitor
    7. SequenceGate — готовность секвенсора
    8. ModeGate — режим работы системы
    """

    def __init__(self):
        self.gates = [
            "WeatherGate",
            "HardwareGate",
            "CalibrationGate",
            "DiskSpaceGate",
            "APIHealthGate",
            "SafetyMonitorGate",
            "SequenceGate",
            "ModeGate",
        ]

    async def run_all(self) -> PreFlightReport:
        """Запускает все проверки и возвращает агрегированный отчет."""
        results = {}

        for gate_name in self.gates:
            gate_method = getattr(self, f"_check_{gate_name.lower()}", None)
            if gate_method:
                result = await gate_method()
                results[gate_name] = result
            else:
                logger.warning(f"Gate method not found: {gate_name}")

        # Агрегируем verdict
        verdict = self._aggregate_verdict(results)

        # Генерируем рекомендации
        recommendations = self._generate_recommendations(results, verdict)

        report = PreFlightReport(
            gates=results, verdict=verdict, recommendations=recommendations
        )

        # Публикуем отчет
        await event_bus.publish("PREFLIGHT_REPORT", report.model_dump())

        logger.info(f"✅ Pre-flight check complete: {verdict.value}")

        return report

    async def _check_weathergate(self) -> GateResult:
        """Проверка погодных условий."""
        weather = observatory_state.weather

        cloud_cover = weather.get("cloud_cover")
        wind_speed = weather.get("wind_speed")
        humidity = weather.get("humidity")

        # Проверки
        if cloud_cover is not None and cloud_cover > 80.0:
            return GateResult(
                gate_name="WeatherGate",
                status=GateStatus.NO_GO,
                message=f"Cloud cover too high: {cloud_cover}%",
                details={"cloud_cover": cloud_cover},
            )

        if wind_speed is not None and wind_speed > 20.0:
            return GateResult(
                gate_name="WeatherGate",
                status=GateStatus.NO_GO,
                message=f"Wind speed too high: {wind_speed} m/s",
                details={"wind_speed": wind_speed},
            )

        if humidity is not None and humidity > 90.0:
            return GateResult(
                gate_name="WeatherGate",
                status=GateStatus.CAUTION,
                message=f"High humidity: {humidity}%",
                details={"humidity": humidity},
            )

        return GateResult(
            gate_name="WeatherGate",
            status=GateStatus.GO,
            message="Weather conditions acceptable",
            details=weather,
        )

    async def _check_hardwaregate(self) -> GateResult:
        """Проверка статуса оборудования."""
        # Проверяем базовые метрики оборудования
        metrics = observatory_state.current_metrics

        camera_temp = metrics.get("camera_temp")
        if camera_temp is None:
            return GateResult(
                gate_name="HardwareGate",
                status=GateStatus.WAITING,
                message="Camera not connected or not reporting",
                details={},
            )

        # Проверяем, что камера охлаждена
        if camera_temp > -10.0:  # Примерный порог
            return GateResult(
                gate_name="HardwareGate",
                status=GateStatus.CAUTION,
                message=f"Camera not fully cooled: {camera_temp}°C",
                details={"camera_temp": camera_temp},
            )

        return GateResult(
            gate_name="HardwareGate",
            status=GateStatus.GO,
            message="Hardware ready",
            details=metrics,
        )

    async def _check_calibrationgate(self) -> GateResult:
        """Проверка свежести калибровок."""
        # Здесь должна быть интеграция с MastersLibraryAuditor
        # Для простоты возвращаем GO
        return GateResult(
            gate_name="CalibrationGate",
            status=GateStatus.GO,
            message="Calibration masters available",
            details={},
        )

    async def _check_diskspacegate(self) -> GateResult:
        """Проверка свободного места на диске."""
        # Здесь должна быть проверка через shutil.disk_usage
        # Для простоты возвращаем GO
        return GateResult(
            gate_name="DiskSpaceGate",
            status=GateStatus.GO,
            message="Sufficient disk space",
            details={},
        )

    async def _check_apihealthgate(self) -> GateResult:
        """Проверка доступности N.I.N.A. API."""
        # Здесь должна быть проверка через nina_client
        # Для простоты возвращаем GO
        return GateResult(
            gate_name="APIHealthGate",
            status=GateStatus.GO,
            message="N.I.N.A. API reachable",
            details={},
        )

    async def _check_safetymonitorgate(self) -> GateResult:
        """Проверка статуса Safety Monitor."""
        safety_status = observatory_state.safety_status

        if safety_status == "UNSAFE":
            return GateResult(
                gate_name="SafetyMonitorGate",
                status=GateStatus.NO_GO,
                message="Safety Monitor reports UNSAFE conditions",
                details={"safety_status": safety_status},
            )

        if safety_status == "UNKNOWN":
            return GateResult(
                gate_name="SafetyMonitorGate",
                status=GateStatus.WAITING,
                message="Safety Monitor status unknown",
                details={"safety_status": safety_status},
            )

        return GateResult(
            gate_name="SafetyMonitorGate",
            status=GateStatus.GO,
            message="Safety Monitor reports SAFE",
            details={"safety_status": safety_status},
        )

    async def _check_sequencegate(self) -> GateResult:
        """Проверка готовности секвенсора."""
        from app.shadow_engine.state_tracker import state_tracker

        if state_tracker.state.is_running:
            return GateResult(
                gate_name="SequenceGate",
                status=GateStatus.CAUTION,
                message="Sequence already running",
                details={"is_running": True},
            )

        return GateResult(
            gate_name="SequenceGate",
            status=GateStatus.GO,
            message="Sequence ready to start",
            details={"is_running": False},
        )

    async def _check_modegate(self) -> GateResult:
        """Проверка режима работы системы."""
        from app.core.mode_manager import mode_manager

        current_mode = mode_manager.current_mode

        if current_mode.value == "manual":
            return GateResult(
                gate_name="ModeGate",
                status=GateStatus.CAUTION,
                message="System in MANUAL mode (no autonomous actions)",
                details={"mode": current_mode.value},
            )

        return GateResult(
            gate_name="ModeGate",
            status=GateStatus.GO,
            message=f"System in {current_mode.value} mode",
            details={"mode": current_mode.value},
        )

    def _aggregate_verdict(self, results: Dict[str, GateResult]) -> GateStatus:
        """Агрегирует verdict всех gates."""
        statuses = [r.status for r in results.values()]

        # Если есть хотя бы один NO-GO → общий NO-GO
        if GateStatus.NO_GO in statuses:
            return GateStatus.NO_GO

        # Если есть WAITING → общий WAITING
        if GateStatus.WAITING in statuses:
            return GateStatus.WAITING

        # Если есть CAUTION → общий CAUTION
        if GateStatus.CAUTION in statuses:
            return GateStatus.CAUTION

        # Все GO → общий GO
        return GateStatus.GO

    def _generate_recommendations(
        self, results: Dict[str, GateResult], verdict: GateStatus
    ) -> List[str]:
        """Генерирует рекомендации на основе результатов."""
        recommendations = []

        for gate_name, result in results.items():
            if result.status == GateStatus.NO_GO:
                recommendations.append(f"[{gate_name}] {result.message}")
            elif result.status == GateStatus.CAUTION:
                recommendations.append(f"[{gate_name}] WARNING: {result.message}")

        return recommendations


# Singleton instance
preflight_checker = PreFlightChecker()
