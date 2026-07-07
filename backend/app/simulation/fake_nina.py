"""
Fake NINA API — эмулятор N.I.N.A. Advanced API для тестирования.
Генерирует реалистичные метрики и события для тестирования агентов.

ИСПРАВЛЕНО (audit 12.1):
- Запись в ImageMetaData.json теперь происходит батчами (не каждый кадр)
- Введён in-memory буфер _pending_frames с периодическим flush на диск
- Автоматический flush при stop_sequence и periodically (каждые N кадров / секунд)
- Использование aiofiles для асинхронной записи
- Защита от потери данных при аварийной остановке через final flush
"""

import asyncio
import logging
import random
from typing import Dict, Any, Optional, List
from datetime import datetime
from pathlib import Path
import json
import aiofiles
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
    - Запись фейковых файлов Session Metadata (батчами)
    - Симуляция автофокуса и гидирования

    ИСПРАВЛЕНО (audit 12.1):
    - Батчевая запись метаданных вместо посекундной
    - Настраиваемый flush interval (по кадрам и по времени)
    """

    # ===== Настройки батчевой записи =====
    FLUSH_EVERY_FRAMES: int = 10  # Сбрасывать буфер каждые N кадров
    FLUSH_EVERY_SECONDS: float = 30.0  # Или каждые N секунд
    MAX_PENDING_FRAMES: int = 500  # Максимум кадров в буфере

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

        # ===== ИСПРАВЛЕНО (audit 12.1): буфер для батчевой записи =====
        self._pending_frames: List[Dict[str, Any]] = []
        self._last_flush_time: Optional[datetime] = None
        self._total_flushed: int = 0
        self._flush_lock = asyncio.Lock()

        # Задачи
        self._metrics_task: Optional[asyncio.Task] = None
        self._sequence_task: Optional[asyncio.Task] = None
        self._flush_task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Запускает симуляцию."""
        if self._running:
            return

        self._running = True
        logger.info(f"🎭 Fake NINA API started (session: {self.session_dir})")

        # Запускаем генерацию метрик
        self._metrics_task = asyncio.create_task(self._generate_metrics_loop())

        # ИСПРАВЛЕНО (audit 12.1): задача периодического flush
        self._flush_task = asyncio.create_task(self._periodic_flush_loop())

    async def stop(self):
        """Останавливает симуляцию."""
        self._running = False

        # ИСПРАВЛЕНО (audit 12.1): финальный flush перед остановкой
        await self._flush_pending_frames(reason="stop")

        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass

        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        if self._sequence_task:
            self._sequence_task.cancel()
            try:
                await self._sequence_task
            except asyncio.CancelledError:
                pass

        logger.info(
            f"🎭 Fake NINA API stopped (total frames flushed: {self._total_flushed})"
        )

    async def start_sequence(self, target: str = "M31", frames: int = 10):
        """Запускает симуляцию секвенсора."""
        if self.sequence_running:
            logger.warning("Sequence already running")
            return

        self.sequence_running = True
        self.current_target = target
        self.frame_count = 0
        self._pending_frames.clear()
        self._last_flush_time = datetime.now()

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

        # ИСПРАВЛЕНО (audit 12.1): финальный flush перед публикацией SEQUENCE_STOPPED
        await self._flush_pending_frames(reason="stop_sequence")

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

    async def _periodic_flush_loop(self):
        """
        ИСПРАВЛЕНО (audit 12.1): Периодический flush буфера по таймеру.
        Гарантирует, что данные записываются на диск даже при длинных
        интервалах между кадрами.
        """
        while self._running:
            try:
                await asyncio.sleep(self.FLUSH_EVERY_SECONDS)

                # Проверяем, нужно ли flush по таймеру
                if (
                    self._pending_frames
                    and self._last_flush_time
                    and (datetime.now() - self._last_flush_time).total_seconds()
                    >= self.FLUSH_EVERY_SECONDS
                ):
                    await self._flush_pending_frames(reason="periodic_timer")
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

                # Генерируем кадр
                await self._generate_frame()

                # ИСПРАВЛЕНО: Ускоренная симуляция (2 секунды вместо 65)
                # В реальности: exposure_time + overhead (~65s)
                # В симуляции: 2 секунды для быстрого тестирования
                await asyncio.sleep(2.0)

                # ИСПРАВЛЕНО (audit 12.1): Периодический flush по количеству кадров
                if len(self._pending_frames) >= self.FLUSH_EVERY_FRAMES:
                    await self._flush_pending_frames(reason="frame_count_threshold")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in sequence generation: {e}")

    async def _generate_frame(self):
        """
        Генерирует один кадр и публикует события.

        ИСПРАВЛЕНО (audit 12.1):
        - Кадр добавляется в in-memory буфер _pending_frames
        - Публикация NEW_FRAME происходит сразу (для real-time обработки)
        - Запись на диск — батчем через _flush_pending_frames
        """
        self.frame_count += 1

        # Генерируем метрики для кадра
        frame_metrics = {
            "hfr": self.metrics["hfr"] + random.gauss(0, 0.1),
            "fwhm": self.metrics["fwhm"] + random.gauss(0, 0.1),
            "stars": int(self.metrics["star_count"] + random.gauss(0, 10)),
            "rms_total": self.metrics["rms_total"] + random.gauss(0, 0.05),
        }

        # Формируем данные кадра с обоими регистрами ключей
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

        # ИСПРАВЛЕНО (audit 12.1): Добавляем в буфер вместо немедленной записи
        self._pending_frames.append(frame_data)

        # Защита от переполнения буфера
        if len(self._pending_frames) > self.MAX_PENDING_FRAMES:
            logger.warning(
                f"⚠️ Pending frames buffer exceeded limit "
                f"({len(self._pending_frames)} > {self.MAX_PENDING_FRAMES}), "
                f"forcing flush"
            )
            await self._flush_pending_frames(reason="buffer_overflow")

        # Публикуем событие нового кадра СРАЗУ (для real-time обработки Watcher)
        await event_bus.publish(
            "NEW_FRAME",
            {"session_id": self.session_dir.name, "frame": frame_data},
        )

        logger.info(
            f"📸 Frame #{self.frame_count}: HFR={frame_metrics['hfr']:.2f}, "
            f"FWHM={frame_metrics['fwhm']:.2f}, Stars={frame_metrics['stars']} "
            f"(pending: {len(self._pending_frames)})"
        )

    async def _flush_pending_frames(self, reason: str = "unknown") -> bool:
        """
        ИСПРАВЛЕНО (audit 12.1): Сбрасывает буфер кадров на диск одним батчем.

        Args:
            reason: Причина flush (для логирования)

        Returns:
            True если flush успешен
        """
        async with self._flush_lock:
            if not self._pending_frames:
                return True

            frames_to_write = list(self._pending_frames)
            self._pending_frames.clear()

            metadata_file = self.session_dir / "ImageMetaData.json"

            try:
                # Читаем существующие данные асинхронно
                existing_frames: List[Dict[str, Any]] = []
                if metadata_file.exists():
                    try:
                        async with aiofiles.open(
                            metadata_file, "r", encoding="utf-8"
                        ) as f:
                            content = await f.read()
                            data = json.loads(content)
                            existing_frames = data.get("Frames", [])
                    except (json.JSONDecodeError, IOError) as e:
                        logger.warning(
                            f"Failed to read existing metadata: {e}. "
                            f"Starting with empty list."
                        )

                # Добавляем новые кадры
                existing_frames.extend(frames_to_write)

                # Записываем всё одним батчем
                data = {"Frames": existing_frames}
                async with aiofiles.open(metadata_file, "w", encoding="utf-8") as f:
                    content = json.dumps(data, indent=2, ensure_ascii=False)
                    await f.write(content)

                self._total_flushed += len(frames_to_write)
                self._last_flush_time = datetime.now()

                logger.debug(
                    f"💾 Flushed {len(frames_to_write)} frames to disk "
                    f"(reason: {reason}, total flushed: {self._total_flushed})"
                )
                return True

            except Exception as e:
                # При ошибке записи возвращаем кадры обратно в буфер
                logger.error(
                    f"Failed to flush {len(frames_to_write)} frames: {e}. "
                    f"Returning to buffer."
                )
                self._pending_frames = frames_to_write + self._pending_frames
                return False

    async def trigger_autofocus(self):
        """Симулирует запуск автофокуса."""
        if self.autofocus_triggered:
            logger.warning("Autofocus already triggered")
            return

        self.autofocus_triggered = True
        logger.info("🔍 Triggering fake autofocus...")

        # Flush перед автофокусом для консистентности данных
        await self._flush_pending_frames(reason="autofocus")

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

        # Flush перед flip для консистентности
        await self._flush_pending_frames(reason="meridian_flip")

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

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику симулятора."""
        return {
            "running": self._running,
            "sequence_running": self.sequence_running,
            "frame_count": self.frame_count,
            "total_flushed": self._total_flushed,
            "pending_frames": len(self._pending_frames),
            "flush_config": {
                "every_frames": self.FLUSH_EVERY_FRAMES,
                "every_seconds": self.FLUSH_EVERY_SECONDS,
                "max_pending": self.MAX_PENDING_FRAMES,
            },
            "session_dir": str(self.session_dir),
            "current_target": self.current_target,
        }


# Singleton instance
fake_nina = FakeNinaAPI()
