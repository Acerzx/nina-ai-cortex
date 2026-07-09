"""
Pre-flight Checklist — формализованный чек-лист перед стартом сессии.
Основан на архитектуре Atlas (8 gates с агрегированным verdict).
ИСПРАВЛЕНО (audit 12.3):
- CalibrationGate теперь проверяет наличие мастеров через MastersLibraryAuditor
- DiskSpaceGate проверяет свободное место через disk_monitor.check_all_disks()
- APIHealthGate проверяет доступность N.I.N.A. API через nina_client.health_check()
- Добавлена проверка оборудования через observatory_state
ИСПРАВЛЕНО (v4.0 — проблемы #20, #34, #35):
- Унифицированы имена порогов (cloud_cover_max вместо cloud_cover_max_percent)
- Порог температуры камеры читается из settings (не хардкод)
- Проверка мастеров учитывает gain, offset, binning
"""

import logging
from typing import Dict, Any, List, Optional
from enum import Enum
from datetime import datetime
from pydantic import BaseModel, Field
from app.agents.observatory_state import observatory_state
from app.core.events import event_bus
from app.core.config import settings
from app.storage.disk_monitor import disk_monitor
from app.execution.nina_client import nina_client

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
    ИСПРАВЛЕНО (v4.0):
    - Все пороги читаются из settings.thresholds.preflight
    - Унифицированы имена порогов
    - Проверка мастеров учитывает все параметры
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

        # Настраиваемые пороги из конфига
        thresholds = getattr(settings, "thresholds", None)
        if thresholds:
            preflight_cfg = getattr(thresholds, "preflight", None)
            if preflight_cfg:
                # ИСПРАВЛЕНО (v4.0 — проблема #20): унифицированные имена
                self.cloud_cover_max = getattr(preflight_cfg, "cloud_cover_max", 80.0)
                self.wind_speed_max = getattr(preflight_cfg, "wind_speed_max", 20.0)
                self.humidity_max = getattr(preflight_cfg, "humidity_max", 90.0)
                self.min_free_disk_space_gb = getattr(
                    preflight_cfg, "min_free_disk_space_gb", 50.0
                )
                # ИСПРАВЛЕНО (v4.0 — проблема #34): настраиваемый порог
                self.camera_cooled_threshold = getattr(
                    preflight_cfg, "camera_cooled_threshold", -10.0
                )
            else:
                # Fallback на дефолтные значения
                self.cloud_cover_max = 80.0
                self.wind_speed_max = 20.0
                self.humidity_max = 90.0
                self.min_free_disk_space_gb = 50.0
                self.camera_cooled_threshold = -10.0
        else:
            # Fallback если thresholds вообще нет
            self.cloud_cover_max = 80.0
            self.wind_speed_max = 20.0
            self.humidity_max = 90.0
            self.min_free_disk_space_gb = 50.0
            self.camera_cooled_threshold = -10.0

        logger.info(
            f"✅ PreFlightChecker initialized "
            f"(cloud_cover_max: {self.cloud_cover_max}%, "
            f"camera_cooled_threshold: {self.camera_cooled_threshold}°C)"
        )

    async def run_all(self) -> PreFlightReport:
        """Запускает все проверки и возвращает агрегированный отчет."""
        results = {}

        for gate_name in self.gates:
            gate_method = getattr(self, f"_check_{gate_name.lower()}", None)
            if gate_method:
                try:
                    result = await gate_method()
                    results[gate_name] = result
                except Exception as e:
                    logger.error(f"Error in {gate_name}: {e}")
                    results[gate_name] = GateResult(
                        gate_name=gate_name,
                        status=GateStatus.CAUTION,
                        message=f"Check failed with error: {e}",
                    )
            else:
                logger.warning(f"Gate method not found: {gate_name}")
                results[gate_name] = GateResult(
                    gate_name=gate_name,
                    status=GateStatus.WAITING,
                    message="Check not implemented",
                )

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

        # Проверки с настраиваемыми порогами
        if cloud_cover is not None and cloud_cover > self.cloud_cover_max:
            return GateResult(
                gate_name="WeatherGate",
                status=GateStatus.NO_GO,
                message=(
                    f"Cloud cover too high: {cloud_cover}% "
                    f"(max {self.cloud_cover_max}%)"
                ),
                details={"cloud_cover": cloud_cover},
            )

        if wind_speed is not None and wind_speed > self.wind_speed_max:
            return GateResult(
                gate_name="WeatherGate",
                status=GateStatus.NO_GO,
                message=(
                    f"Wind speed too high: {wind_speed} m/s "
                    f"(max {self.wind_speed_max} m/s)"
                ),
                details={"wind_speed": wind_speed},
            )

        if humidity is not None and humidity > self.humidity_max:
            return GateResult(
                gate_name="WeatherGate",
                status=GateStatus.CAUTION,
                message=f"High humidity: {humidity}% (max {self.humidity_max}%)",
                details={"humidity": humidity},
            )

        return GateResult(
            gate_name="WeatherGate",
            status=GateStatus.GO,
            message="Weather conditions acceptable",
            details=weather,
        )

    async def _check_hardwaregate(self) -> GateResult:
        """
        Проверка статуса оборудования.
        ИСПРАВЛЕНО (v4.0 — проблема #34): порог температуры из конфига
        """
        metrics = observatory_state.current_metrics
        camera_temp = metrics.get("camera_temp")

        if camera_temp is None:
            return GateResult(
                gate_name="HardwareGate",
                status=GateStatus.WAITING,
                message="Camera not connected or not reporting",
                details={},
            )

        # ИСПРАВЛЕНО: используем настраиваемый порог вместо хардкода -10°C
        if camera_temp > self.camera_cooled_threshold:
            return GateResult(
                gate_name="HardwareGate",
                status=GateStatus.CAUTION,
                message=(
                    f"Camera not fully cooled: {camera_temp}°C "
                    f"(threshold: {self.camera_cooled_threshold}°C)"
                ),
                details={"camera_temp": camera_temp},
            )

        return GateResult(
            gate_name="HardwareGate",
            status=GateStatus.GO,
            message="Hardware ready",
            details=metrics,
        )

    async def _check_calibrationgate(self) -> GateResult:
        """
        ИСПРАВЛЕНО (audit 12.3): Реальная проверка калибровок через
        MastersLibraryAuditor.
        ИСПРАВЛЕНО (v4.0 — проблема #35): проверка соответствия gain/offset/binning

        Проверяет:
        1. Инициализирован ли MastersLibraryAuditor
        2. Есть ли мастера нужных типов (BIAS, DARK, FLAT)
        3. Актуальность мастеров (дата создания)
        4. Соответствие параметров (gain, offset, binning)
        """
        # Импортируем здесь, чтобы избежать циклических зависимостей
        try:
            from app.ingestion.watchers.manager import watcher_manager
        except ImportError:
            return GateResult(
                gate_name="CalibrationGate",
                status=GateStatus.WAITING,
                message="WatcherManager not available",
            )

        auditor = watcher_manager.masters_auditor
        if not auditor:
            return GateResult(
                gate_name="CalibrationGate",
                status=GateStatus.WAITING,
                message="MastersLibraryAuditor not initialized",
            )

        # Получаем текущие параметры съёмки
        current_gain = observatory_state.current_metrics.get("gain")
        current_offset = observatory_state.current_metrics.get("offset")
        current_binning = observatory_state.current_metrics.get("binning")
        current_temp = observatory_state.current_metrics.get("camera_temp", -15.0)
        current_exposure = observatory_state.current_metrics.get("exposure_time", 60.0)
        current_filter = observatory_state.current_metrics.get("filter")

        # Получаем статистику мастеров
        stats = auditor.get_stats()
        total_bias = stats.get("total_bias", 0)
        total_dark = stats.get("total_dark", 0)
        total_flat = stats.get("total_flat", 0)
        total_unknown = stats.get("total_unknown", 0)
        scan_errors = stats.get("scan_errors", 0)

        details = {
            "total_bias": total_bias,
            "total_dark": total_dark,
            "total_flat": total_flat,
            "total_unknown": total_unknown,
            "scan_errors": scan_errors,
            "current_params": {
                "gain": current_gain,
                "offset": current_offset,
                "binning": current_binning,
                "temperature": current_temp,
                "exposure": current_exposure,
                "filter": current_filter,
            },
        }

        # Проверка наличия мастеров
        missing_types = []
        if total_bias == 0:
            missing_types.append("BIAS")
        if total_dark == 0:
            missing_types.append("DARK")
        if total_flat == 0:
            missing_types.append("FLAT")

        if missing_types:
            return GateResult(
                gate_name="CalibrationGate",
                status=GateStatus.NO_GO,
                message=f"Missing calibration masters: {', '.join(missing_types)}",
                details=details,
            )

        # Проверка соответствия параметров
        # ИСПРАВЛЕНО (v4.0 — проблема #35): полная проверка gain/offset/binning
        param_mismatches = []

        if current_gain is not None:
            # Ищем мастера с matching gain
            matching = auditor.find_matching_master(
                image_type="DARK",
                temperature=current_temp,
                exposure=current_exposure,
                gain=current_gain,
                temp_tolerance=2.0,
            )
            if not matching:
                param_mismatches.append(f"DARK with gain={current_gain}")

        if current_offset is not None:
            matching = auditor.find_matching_master(
                image_type="DARK",
                temperature=current_temp,
                exposure=current_exposure,
                gain=current_gain,
                offset=current_offset,
                temp_tolerance=2.0,
            )
            if not matching:
                param_mismatches.append(f"DARK with offset={current_offset}")

        if current_filter:
            matching = auditor.find_matching_master(
                image_type="FLAT",
                temperature=current_temp,
                filter_name=current_filter,
                gain=current_gain,
                temp_tolerance=2.0,
            )
            if not matching:
                param_mismatches.append(f"FLAT with filter={current_filter}")

        if param_mismatches:
            return GateResult(
                gate_name="CalibrationGate",
                status=GateStatus.CAUTION,
                message=(
                    f"No matching masters for current parameters: "
                    f"{'; '.join(param_mismatches)}"
                ),
                details=details,
            )

        # Проверка свежести мастеров
        summary = auditor.get_summary_by_category()
        stale_types = []
        freshness_days = {
            "BIAS": 90,
            "DARK": 30,
            "FLAT": 7,
        }

        for master_type, max_days in freshness_days.items():
            category_summary = summary.get(master_type, {})
            max_date_str = category_summary.get("max_date")
            if max_date_str:
                try:
                    # Парсим дату
                    if "T" in max_date_str:
                        max_date = datetime.fromisoformat(
                            max_date_str.replace("Z", "+00:00")
                        )
                        if max_date.tzinfo:
                            max_date = max_date.replace(tzinfo=None)
                    else:
                        max_date = datetime.strptime(max_date_str[:10], "%Y-%m-%d")

                    age_days = (datetime.now() - max_date).days
                    if age_days > max_days:
                        stale_types.append(
                            f"{master_type} ({age_days}d old, max {max_days}d)"
                        )
                    details[f"{master_type}_age_days"] = age_days
                except (ValueError, TypeError):
                    pass

        if stale_types:
            return GateResult(
                gate_name="CalibrationGate",
                status=GateStatus.CAUTION,
                message=f"Stale calibration masters: {'; '.join(stale_types)}",
                details=details,
            )

        return GateResult(
            gate_name="CalibrationGate",
            status=GateStatus.GO,
            message=(
                f"Calibration masters available and matching: "
                f"{total_bias} BIAS, {total_dark} DARK, {total_flat} FLAT"
            ),
            details=details,
        )

    async def _check_diskspacegate(self) -> GateResult:
        """Проверка свободного места на диске."""
        try:
            disk_usage_list = await disk_monitor.check_all_disks()
        except Exception as e:
            return GateResult(
                gate_name="DiskSpaceGate",
                status=GateStatus.CAUTION,
                message=f"Failed to check disk usage: {e}",
            )

        if not disk_usage_list:
            return GateResult(
                gate_name="DiskSpaceGate",
                status=GateStatus.WAITING,
                message="No disk usage data available",
            )

        details = {
            "disks": [d.model_dump() for d in disk_usage_list],
            "min_free_space_gb": self.min_free_disk_space_gb,
        }

        # Проверяем каждый monitored путь
        critical_disks = []
        warning_disks = []

        for usage in disk_usage_list:
            # Пропускаем недоступные пути (total=0)
            if usage.total_gb == 0:
                continue

            if usage.free_gb < self.min_free_disk_space_gb:
                critical_disks.append(f"{usage.path} ({usage.free_gb:.1f} GB free)")
            elif usage.free_gb < self.min_free_disk_space_gb * 2.5:
                warning_disks.append(f"{usage.path} ({usage.free_gb:.1f} GB free)")

        if critical_disks:
            return GateResult(
                gate_name="DiskSpaceGate",
                status=GateStatus.NO_GO,
                message=(
                    f"Insufficient disk space on: "
                    f"{'; '.join(critical_disks)}. "
                    f"Minimum required: {self.min_free_disk_space_gb} GB"
                ),
                details=details,
            )

        if warning_disks:
            return GateResult(
                gate_name="DiskSpaceGate",
                status=GateStatus.CAUTION,
                message=(f"Low disk space warning on: {'; '.join(warning_disks)}"),
                details=details,
            )

        # Формируем summary
        summary_parts = [
            f"{u.path}: {u.free_gb:.1f} GB free ({u.usage_percent:.0f}% used)"
            for u in disk_usage_list
            if u.total_gb > 0
        ]

        return GateResult(
            gate_name="DiskSpaceGate",
            status=GateStatus.GO,
            message="Sufficient disk space",
            details={**details, "summary": summary_parts},
        )

    async def _check_apihealthgate(self) -> GateResult:
        """Проверка доступности N.I.N.A. API."""
        try:
            is_healthy = await nina_client.health_check()
        except Exception as e:
            logger.debug(f"N.I.N.A. health check error: {e}")
            return GateResult(
                gate_name="APIHealthGate",
                status=GateStatus.NO_GO,
                message=f"N.I.N.A. API unreachable: {type(e).__name__}",
                details={"error": str(e)},
            )

        if is_healthy:
            return GateResult(
                gate_name="APIHealthGate",
                status=GateStatus.GO,
                message="N.I.N.A. API reachable",
                details={"healthy": True},
            )
        else:
            return GateResult(
                gate_name="APIHealthGate",
                status=GateStatus.NO_GO,
                message="N.I.N.A. API not responding",
                details={"healthy": False},
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

        # Проверка наличия теневого графа
        if not state_tracker._shadow_graph:
            return GateResult(
                gate_name="SequenceGate",
                status=GateStatus.WAITING,
                message="Shadow graph not loaded yet",
                details={"shadow_graph_loaded": False},
            )

        return GateResult(
            gate_name="SequenceGate",
            status=GateStatus.GO,
            message="Sequence ready to start",
            details={
                "is_running": False,
                "shadow_graph_loaded": True,
            },
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

        # Проверка здоровья LLM для FULL_AI режима
        if current_mode.value == "full_ai":
            llm_healthy = mode_manager.llm_healthy
            if not llm_healthy:
                return GateResult(
                    gate_name="ModeGate",
                    status=GateStatus.CAUTION,
                    message="FULL_AI mode but LLM is unhealthy",
                    details={
                        "mode": current_mode.value,
                        "llm_healthy": False,
                    },
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
            elif result.status == GateStatus.WAITING:
                recommendations.append(f"[{gate_name}] PENDING: {result.message}")

        # Общие рекомендации
        if verdict == GateStatus.GO:
            recommendations.insert(0, "✅ All gates passed — ready to start")
        elif verdict == GateStatus.NO_GO:
            recommendations.insert(
                0, "❌ Critical issues found — do NOT start sequence"
            )
        elif verdict == GateStatus.WAITING:
            recommendations.insert(0, "⏳ Waiting for some systems to initialize")

        return recommendations


# Singleton instance
preflight_checker = PreFlightChecker()
