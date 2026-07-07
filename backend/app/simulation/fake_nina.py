"""
Fake NINA API — эмулятор N.I.N.A. Advanced API для тестирования.
Генерирует реалистичные метрики и события для тестирования агентов.
"""

import asyncio
import logging
import random
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import json
from app.core.events import event_bus
from app.core.config import settings

logger = logging.getLogger("FakeNina")


class FakeNinaAPI:
    """
    Эмулятор N.I.N.A. Advanced API для тестирования без реального оборудования.

    Возможности:
    - Генерация реалистичных метрик (HFR, FWHM, RMS, температура)
    - Симуляция последовательности кадров
    - Генерация событий (Sequence Started/Stopped, Meridian Flip, Errors)
    - Запись фейковых файлов Session Metadata
    - Симуляция автофокуса и гидирования
    """

    def __init__(self, session_dir: Optional[Path] = None):
        self.session_dir = session_dir or Path("./fake_sessions/test_session")
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # Состояние симуляции
        self.sequence_running = False
        self.current_target = "M31"
        self.current_filter = "Ha"
        self.exposure_time = 60.0
        self.gain = 85
        self.temperature_setpoint = -15.0
        self.temperature_actual = -14.8

        # Метрики (с реалистичным шумом)
        self.metrics = {
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

        # Задачи
        self._metrics_task: Optional[asyncio.Task] = None
        self._sequence_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Запускает симуляцию."""
        if self._running:
            return

        self._running = True
        logger.info(f"🎭 Fake NINA API started (session: {self.session_dir})")

        # Запускаем генерацию метрик
        self._metrics_task = asyncio.create_task(self._generate_metrics_loop())

    async def stop(self):
        """Останавливает симуляцию."""
        self._running = False

        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass

        if self._sequence_task:
            self._sequence_task.cancel()
            try:
                await self._sequence_task
            except asyncio.CancelledError:
                pass

        logger.info("🎭 Fake NINA API stopped")

    async def start_sequence(self, target: str = "M31", frames: int = 10):
        """Запускает симуляцию секвенсора."""
        if self.sequence_running:
            logger.warning("Sequence already running")
            return

        self.sequence_running = True
        self.current_target = target
        self.frame_count = 0

        logger.info(f"🚀 Starting fake sequence: {target} ({frames} frames)")

        # Публикуем событие начала секвенсора
        await event_bus.publish(
            "SEQUENCE_STARTED",
            {"target": target, "start_time": datetime.now().isoformat()},
        )

        # Запускаем генерацию кадров
        self._sequence_task = asyncio.create_task(self._generate_sequence_loop(frames))

    async def stop_sequence(self):
        """Останавливает симуляцию секвенсора."""
        if not self.sequence_running:
            return

        self.sequence_running = False

        if self._sequence_task:
            self._sequence_task.cancel()
            try:
                await self._sequence_task
            except asyncio.CancelledError:
                pass

        logger.info("🛑 Fake sequence stopped")

        # Публикуем событие остановки
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
                # Добавляем реалистичный шум
                self.metrics["hfr"] += random.gauss(0, 0.05)
                self.metrics["fwhm"] += random.gauss(0, 0.05)
                self.metrics["rms_ra"] += random.gauss(0, 0.02)
                self.metrics["rms_dec"] += random.gauss(0, 0.02)

                # Ограничиваем значения
                self.metrics["hfr"] = max(1.5, min(5.0, self.metrics["hfr"]))
                self.metrics["fwhm"] = max(2.0, min(6.0, self.metrics["fwhm"]))
                self.metrics["rms_ra"] = max(0.3, min(3.0, self.metrics["rms_ra"]))
                self.metrics["rms_dec"] = max(0.3, min(3.0, self.metrics["rms_dec"]))

                # Температура дрейфует к setpoint
                temp_diff = self.temperature_setpoint - self.temperature_actual
                self.temperature_actual += temp_diff * 0.1 + random.gauss(0, 0.02)
                self.metrics["camera_temp"] = self.temperature_actual

                # Публикуем метрики
                await event_bus.publish("PROMETHEUS_UPDATE", self.metrics.copy())

                await asyncio.sleep(3.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in metrics generation: {e}")
                await asyncio.sleep(1.0)

    async def _generate_sequence_loop(self, total_frames: int):
        """Генерирует последовательность кадров."""
        try:
            for i in range(total_frames):
                if not self.sequence_running:
                    break

                # Генерируем кадр
                await self._generate_frame()

                # ИСПРАВЛЕНО: Ускоренная симуляция (2 секунды вместо 65)
                # В реальности: exposure_time + overhead (~65s)
                # В симуляции: 2 секунды для быстрого тестирования
                await asyncio.sleep(2.0)  # ← Было: self.exposure_time + 5.0

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in sequence generation: {e}")

    async def _generate_frame(self):
        """
        Генерирует один кадр и публикует события.

        ИСПРАВЛЕНО: Использование обоих регистров ключей для совместимости
        с N.I.N.A. (ЗАГЛАВНЫЕ) и внутренними структурами (строчные).
        """
        self.frame_count += 1

        # Генерируем метрики для кадра
        frame_metrics = {
            "hfr": self.metrics["hfr"] + random.gauss(0, 0.1),
            "fwhm": self.metrics["fwhm"] + random.gauss(0, 0.1),
            "stars": int(self.metrics["star_count"] + random.gauss(0, 10)),
            "rms_total": self.metrics["rms_total"] + random.gauss(0, 0.05),
        }

        # ИСПРАВЛЕНО: Публикуем в обоих регистрах для максимальной совместимости
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

        # Записываем в файл (для SessionWatcher)
        metadata_file = self.session_dir / "ImageMetaData.json"
        await self._update_metadata_file(metadata_file, frame_data)

        # Публикуем событие нового кадра
        await event_bus.publish(
            "NEW_FRAME", {"session_id": self.session_dir.name, "frame": frame_data}
        )

        logger.info(
            f"📸 Frame #{self.frame_count}: HFR={frame_metrics['hfr']:.2f}, "
            f"FWHM={frame_metrics['fwhm']:.2f}, Stars={frame_metrics['stars']}"
        )

    async def _update_metadata_file(self, file_path: Path, frame_data: Dict):
        """Обновляет файл ImageMetaData.json."""
        try:
            # Читаем существующие данные
            if file_path.exists():
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
            else:
                data = {"Frames": []}

            # Добавляем новый кадр
            data["Frames"].append(frame_data)

            # Записываем обратно
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

        except Exception as e:
            logger.error(f"Failed to update metadata file: {e}")

    async def trigger_autofocus(self):
        """Симулирует запуск автофокуса."""
        if self.autofocus_triggered:
            logger.warning("Autofocus already triggered")
            return

        self.autofocus_triggered = True
        logger.info("🔍 Triggering fake autofocus...")

        # Публикуем событие начала автофокуса
        await event_bus.publish(
            "LOG_EVENT",
            {"event_type": "autofocus_start", "message": "AutoFocus Started"},
        )

        # Симулируем процесс автофокуса (10 секунд)
        await asyncio.sleep(10.0)

        # Улучшаем HFR после автофокуса
        self.metrics["hfr"] = max(1.8, self.metrics["hfr"] - 0.5)
        self.metrics["fwhm"] = max(2.2, self.metrics["fwhm"] - 0.5)

        # Публикуем событие завершения
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
            return

        self.meridian_flip_triggered = True
        logger.info("🔄 Triggering fake meridian flip...")

        await event_bus.publish(
            "MERIDIAN_FLIP_STARTED", {"timestamp": datetime.now().isoformat()}
        )

        # Симулируем процесс (30 секунд)
        await asyncio.sleep(30.0)

        await event_bus.publish(
            "MERIDIAN_FLIP_COMPLETED", {"timestamp": datetime.now().isoformat()}
        )

        self.meridian_flip_triggered = False
        logger.info("✅ Meridian flip complete")

    async def inject_anomaly(self, anomaly_type: str):
        """
        Инжектирует аномалию для тестирования агентов.

        Args:
            anomaly_type: Тип аномалии (hfr_spike, rms_spike, temp_drift, etc.)
        """
        logger.warning(f"⚠️ Injecting anomaly: {anomaly_type}")

        if anomaly_type == "hfr_spike":
            # Резкий рост HFR
            self.metrics["hfr"] += 2.0

        elif anomaly_type == "rms_spike":
            # Резкий рост RMS
            self.metrics["rms_ra"] += 1.5
            self.metrics["rms_dec"] += 1.5

        elif anomaly_type == "temp_drift":
            # Дрейф температуры
            self.temperature_actual += 3.0

        elif anomaly_type == "guiding_lost":
            # Потеря гидирования
            await event_bus.publish(
                "LOG_EVENT",
                {
                    "event_type": "guiding_lost",
                    "message": "Guiding Lost - guide star not found",
                },
            )

        elif anomaly_type == "safety_unsafe":
            # Safety Monitor UNSAFE
            await event_bus.publish(
                "LOG_EVENT",
                {
                    "event_type": "safety_unsafe",
                    "message": "Safety Monitor: Conditions became UNSAFE",
                },
            )

        else:
            logger.error(f"Unknown anomaly type: {anomaly_type}")


# Singleton instance
fake_nina = FakeNinaAPI()
