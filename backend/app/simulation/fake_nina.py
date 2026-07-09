"""
Fake NINA API — эмулятор N.I.N.A. Advanced API для тестирования.

ИСПРАВЛЕНО (рефакторинг v3):
- Все магические числа (FLUSH_EVERY_FRAMES=10, задержки) вынесены в settings.simulation
- flush_every_frames, flush_every_seconds, frame_delay_seconds читаются из конфига
- Graceful fallback если секция simulation отсутствует в settings
"""

import asyncio
import logging
import random
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import json
import aiofiles
import aiofiles.os

from app.core.events import event_bus
from app.core.config import settings
from typing import TypedDict

logger = logging.getLogger("FakeNina")


class FakeNinaMetrics(TypedDict, total=False):
    """Типизация метрик симулятора."""

    hfr: float
    fwhm: float
    eccentricity: float
    star_count: int
    median_adu: int
    rms_ra: float
    rms_dec: float
    rms_total: float
    camera_temp: float
    focuser_position: int
    rotator_angle: float
    mount_altitude: float
    mount_azimuth: float


class FakeNinaAPI:
    """
    Эмулятор N.I.N.A. Advanced API для тестирования без реального оборудования.

    ИСПРАВЛЕНО (v3):
    - Конфигурация буферизации читается из settings.simulation
    - Асинхронная запись через aiofiles
    - Атомарная запись через temp file + rename
    - asyncio.Lock для защиты от race conditions
    """

    def __init__(self, session_dir: Optional[Path] = None):
        self.session_dir = session_dir or Path("./fake_sessions/test_session")
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # ИСПРАВЛЕНО (v3): Читаем конфигурацию из settings.simulation
        simulation_cfg = getattr(settings, "simulation", None)
        if simulation_cfg:
            self.flush_every_frames = getattr(simulation_cfg, "flush_every_frames", 10)
            self.flush_every_seconds = getattr(
                simulation_cfg, "flush_every_seconds", 30.0
            )
            self.frame_delay_seconds = getattr(
                simulation_cfg, "frame_delay_seconds", 2.0
            )
        else:
            # Fallback на дефолтные значения
            self.flush_every_frames = 10
            self.flush_every_seconds = 30.0
            self.frame_delay_seconds = 2.0

        # Состояние симуляции
        self.sequence_running = False
        self.current_target = "M31"
        self.current_filter = "Ha"
        self.exposure_time = 60.0
        self.gain = 85
        self.temperature_setpoint = -15.0
        self.temperature_actual = -14.8

        # Метрики (с реалистичным шумом)
        # В __init__ изменить типизацию:
        self.metrics: FakeNinaMetrics = {
            "hfr": 2.5,
            "fwhm": 3.0,
            "eccentricity": 0.35,
            "star_count": 150,
            "median_adu": 15000,
            "rms_ra": 0.8,
            "rms_dec": 0.9,
            "rms_total": 1.2,
            "camera_temp": -14.8,
            "focuser_position": 6931,
            "rotator_angle": 180.0,
            "mount_altitude": 45.0,
            "mount_azimuth": 90.0,
        }

        # Счетчики
        self.frame_count = 0
        self.autofocus_triggered = False
        self.dither_triggered = False
        self.meridian_flip_triggered = False

        # Фоновые задачи (ссылки для предотвращения GC)
        self._tasks: List[asyncio.Task] = []

        # Буфер кадров для batch-записи
        self._frame_buffer: List[Dict[str, Any]] = []
        self._buffer_lock = asyncio.Lock()
        self._last_flush_time = datetime.now()

        # Флаги управления
        self._running = False

        logger.info(
            f"🎭 FakeNina initialized "
            f"(flush every {self.flush_every_frames} frames / "
            f"{self.flush_every_seconds}s, "
            f"frame delay: {self.frame_delay_seconds}s)"
        )

    async def start(self):
        """Запускает симуляцию."""
        if self._running:
            logger.warning("Fake NINA API already running")
            return

        self._running = True
        logger.info(f"🎭 Fake NINA API started (session: {self.session_dir})")

        metrics_task = asyncio.create_task(self._generate_metrics_loop())
        self._tasks.append(metrics_task)

        flush_task = asyncio.create_task(self._periodic_flush_loop())
        self._tasks.append(flush_task)

    async def stop(self):
        """Останавливает симуляцию."""
        if not self._running:
            logger.warning("Fake NINA API not running")
            return

        self._running = False

        # Финальный flush перед остановкой
        await self._flush_frame_buffer(reason="stop")

        for task in self._tasks:
            if not task.done():
                task.cancel()

        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # ИСПРАВЛЕНО (v4.0 — проблема #52): Очищаем список задач
        self._tasks.clear()

        logger.info("🎭 Fake NINA API stopped")

    async def start_sequence(self, target: str = "M31", frames: int = 10):
        """Запускает симуляцию секвенсора."""
        if self.sequence_running:
            logger.warning("Sequence already running")
            return

        self.sequence_running = True
        self.current_target = target
        self.frame_count = 0
        self._frame_buffer.clear()
        self._last_flush_time = datetime.now()

        logger.info(f"🚀 Starting fake sequence: {target} ({frames} frames)")

        await event_bus.publish(
            "SEQUENCE_STARTED",
            {"target": target, "start_time": datetime.now().isoformat()},
        )

        sequence_task = asyncio.create_task(self._generate_sequence_loop(frames))
        self._tasks.append(sequence_task)

    async def stop_sequence(self):
        """Останавливает симуляцию секвенсора."""
        if not self.sequence_running:
            logger.warning("Sequence not running")
            return

        self.sequence_running = False
        await self._flush_frame_buffer(reason="stop_sequence")

        logger.info("🛑 Fake sequence stopped")

        await event_bus.publish(
            "SEQUENCE_STOPPED",
            {
                "target": self.current_target,
                "frames_captured": self.frame_count,
                "stop_time": datetime.now().isoformat(),
            },
        )

    async def _generate_metrics_loop(self):
        """Генерирует метрики каждые 3 секунды (как Prometheus)."""
        while self._running:
            try:
                self.metrics["hfr"] += random.gauss(0, 0.05)
                self.metrics["fwhm"] += random.gauss(0, 0.05)
                self.metrics["rms_ra"] += random.gauss(0, 0.02)
                self.metrics["rms_dec"] += random.gauss(0, 0.02)

                self.metrics["hfr"] = max(1.5, min(5.0, self.metrics["hfr"]))
                self.metrics["fwhm"] = max(2.0, min(6.0, self.metrics["fwhm"]))
                self.metrics["rms_ra"] = max(0.3, min(3.0, self.metrics["rms_ra"]))
                self.metrics["rms_dec"] = max(0.3, min(3.0, self.metrics["rms_dec"]))

                temp_diff = self.temperature_setpoint - self.temperature_actual
                self.temperature_actual += temp_diff * 0.1 + random.gauss(0, 0.02)
                self.metrics["camera_temp"] = self.temperature_actual

                await event_bus.publish("PROMETHEUS_UPDATE", self.metrics.copy())
                await asyncio.sleep(3.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics generation: {e}")
                await asyncio.sleep(1.0)

    async def _periodic_flush_loop(self):
        """
        Периодический flush буфера по таймеру.

        ИСПРАВЛЕНО (v3): Интервал читается из settings.simulation.flush_every_seconds
        """
        while self._running:
            try:
                await asyncio.sleep(self.flush_every_seconds)
                elapsed = (datetime.now() - self._last_flush_time).total_seconds()
                if elapsed >= self.flush_every_seconds:
                    await self._flush_frame_buffer(reason="periodic_timer")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in periodic flush: {e}")

    async def _generate_sequence_loop(self, total_frames: int):
        """Генерирует последовательность кадров."""
        try:
            for i in range(total_frames):
                if not self.sequence_running:
                    break

                await self._generate_frame()

                # Периодический flush по количеству кадров
                async with self._buffer_lock:
                    buffer_size = len(self._frame_buffer)
                    if buffer_size >= self.flush_every_frames:
                        await self._flush_frame_buffer(reason="frame_count_threshold")

                # ИСПРАВЛЕНО (v3): Задержка читается из settings.simulation.frame_delay_seconds
                await asyncio.sleep(self.frame_delay_seconds)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in sequence generation: {e}")

    async def _generate_frame(self):
        """
        Генерирует один кадр и публикует события.
        Кадр добавляется в буфер, запись на диск — батчем.
        """
        self.frame_count += 1

        frame_metrics = {
            "hfr": self.metrics["hfr"] + random.gauss(0, 0.1),
            "fwhm": self.metrics["fwhm"] + random.gauss(0, 0.1),
            "stars": int(self.metrics["star_count"] + random.gauss(0, 10)),
            "rms_total": self.metrics["rms_total"] + random.gauss(0, 0.05),
        }

        frame_data = {
            # N.I.N.A. стандарт (ЗАГЛАВНЫЕ)
            "Index": self.frame_count,
            "ExposureTime": self.exposure_time,
            "Filter": self.current_filter,
            "Gain": self.gain,
            "Offset": 10,
            "Temperature": self.temperature_actual,
            "HFR": frame_metrics["hfr"],
            "FWHM": frame_metrics["fwhm"],
            "Stars": frame_metrics["stars"],
            "RmsTotal": frame_metrics["rms_total"],
            "ImageType": "LIGHT",
            "Date": datetime.now().strftime("%Y-%m-%d"),
            "Time": datetime.now().strftime("%H:%M:%S"),
            # Дублируем строчными для совместимости
            "index": self.frame_count,
            "exposure_time": self.exposure_time,
            "filter": self.current_filter,
            "gain": self.gain,
            "temperature": self.temperature_actual,
            "hfr": frame_metrics["hfr"],
            "fwhm": frame_metrics["fwhm"],
            "stars": frame_metrics["stars"],
            "rms_total": frame_metrics["rms_total"],
        }

        async with self._buffer_lock:
            self._frame_buffer.append(frame_data)

        await event_bus.publish(
            "NEW_FRAME", {"session_id": self.session_dir.name, "frame": frame_data}
        )

        logger.info(
            f"📸 Frame #{self.frame_count}: HFR={frame_metrics['hfr']:.2f}, "
            f"FWHM={frame_metrics['fwhm']:.2f}, Stars={frame_metrics['stars']} "
            f"(buffered: {len(self._frame_buffer)})"
        )

    async def _flush_frame_buffer(self, reason: str = "unknown") -> bool:
        """
        Сбрасывает буфер кадров на диск одним батчем.
        ИСПРАВЛЕНО (v4.0 — проблема #23): использует shutil.move через run_in_executor
        вместо aiofiles.os.replace для совместимости с Windows.
        """
        async with self._buffer_lock:
            if not self._frame_buffer:
                return True

            frames_to_write = list(self._frame_buffer)
            self._frame_buffer.clear()
            self._last_flush_time = datetime.now()

        metadata_file = self.session_dir / "ImageMetaData.json"

        try:
            # Читаем существующие данные
            if metadata_file.exists():
                async with aiofiles.open(metadata_file, "r", encoding="utf-8") as f:
                    content = await f.read()
                    try:
                        data = json.loads(content)
                    except json.JSONDecodeError:
                        logger.warning(f"Corrupted metadata file, starting fresh")
                        data = {"Frames": []}
            else:
                data = {"Frames": []}

            # Добавляем новые кадры
            data["Frames"].extend(frames_to_write)

            # Атомарная запись через temp file + move
            temp_path = metadata_file.with_suffix(".json.tmp")
            async with aiofiles.open(temp_path, "w", encoding="utf-8") as f:
                await f.write(json.dumps(data, indent=2, ensure_ascii=False))
                await f.flush()

            # ИСПРАВЛЕНО: используем shutil.move через run_in_executor
            # (aiofiles.os.replace может не работать на Windows)
            import shutil
            from app.core.executors import run_io

            await run_io(shutil.move, str(temp_path), str(metadata_file))

            logger.debug(
                f"💾 Flushed {len(frames_to_write)} frames to disk "
                f"(reason: {reason}, total: {len(data['Frames'])})"
            )
            return True

        except Exception as e:
            logger.error(f"Failed to flush frame buffer: {e}")
            # Возвращаем кадры обратно в буфер
            async with self._buffer_lock:
                self._frame_buffer = frames_to_write + self._frame_buffer
            return False

    async def trigger_autofocus(self):
        """Симулирует запуск автофокуса."""
        if self.autofocus_triggered:
            logger.warning("Autofocus already triggered")
            return

        self.autofocus_triggered = True
        logger.info("🔍 Triggering fake autofocus...")

        await self._flush_frame_buffer(reason="autofocus")

        await event_bus.publish(
            "LOG_EVENT",
            {"event_type": "autofocus_start", "message": "AutoFocus Started"},
        )

        await asyncio.sleep(10.0)

        self.metrics["hfr"] = max(1.8, self.metrics["hfr"] - 0.5)
        self.metrics["fwhm"] = max(2.2, self.metrics["fwhm"] - 0.5)

        await event_bus.publish(
            "LOG_EVENT",
            {
                "event_type": "autofocus_complete",
                "message": f"AutoFocus Completed - HFR: {self.metrics['hfr']:.2f}",
            },
        )

        await event_bus.publish(
            "AUTOFOCUS_REPORT",
            {
                "file": "autofocus_report.json",
                "data": {
                    "hfr_before": self.metrics["hfr"] + 0.5,
                    "hfr_after": self.metrics["hfr"],
                    "position": self.metrics["focuser_position"],
                    "temperature": self.temperature_actual,
                },
            },
        )

        self.autofocus_triggered = False
        logger.info(f"✅ Autofocus complete: HFR improved to {self.metrics['hfr']:.2f}")

    async def trigger_meridian_flip(self):
        """Симулирует Meridian Flip."""
        if self.meridian_flip_triggered:
            logger.warning("Meridian flip already triggered")
            return

        self.meridian_flip_triggered = True
        logger.info("🔄 Triggering fake meridian flip...")

        await self._flush_frame_buffer(reason="meridian_flip")

        await event_bus.publish(
            "MERIDIAN_FLIP_STARTED", {"timestamp": datetime.now().isoformat()}
        )

        await asyncio.sleep(30.0)

        await event_bus.publish(
            "MERIDIAN_FLIP_COMPLETED", {"timestamp": datetime.now().isoformat()}
        )

        self.meridian_flip_triggered = False
        logger.info("✅ Meridian flip complete")

    async def inject_anomaly(self, anomaly_type: str):
        """Инжектирует аномалию для тестирования агентов."""
        logger.warning(f"⚠️ Injecting anomaly: {anomaly_type}")

        if anomaly_type == "hfr_spike":
            self.metrics["hfr"] += 2.0
        elif anomaly_type == "rms_spike":
            self.metrics["rms_ra"] += 1.5
            self.metrics["rms_dec"] += 1.5
        elif anomaly_type == "temp_drift":
            self.temperature_actual += 3.0
        elif anomaly_type == "guiding_lost":
            await event_bus.publish(
                "LOG_EVENT",
                {
                    "event_type": "guiding_lost",
                    "message": "Guiding Lost - guide star not found",
                },
            )
        elif anomaly_type == "safety_unsafe":
            await event_bus.publish(
                "LOG_EVENT",
                {
                    "event_type": "safety_unsafe",
                    "message": "Safety Monitor: Conditions became UNSAFE",
                },
            )
        else:
            logger.error(f"Unknown anomaly type: {anomaly_type}")

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику симулятора."""
        return {
            "running": self._running,
            "sequence_running": self.sequence_running,
            "frame_count": self.frame_count,
            "buffer_size": len(self._frame_buffer),
            "tasks_count": len(self._tasks),
            "current_target": self.current_target,
            "current_filter": self.current_filter,
            "metrics": self.metrics.copy(),
            "config": {
                "flush_every_frames": self.flush_every_frames,
                "flush_every_seconds": self.flush_every_seconds,
                "frame_delay_seconds": self.frame_delay_seconds,
            },
        }


# Singleton instance
fake_nina = FakeNinaAPI()
