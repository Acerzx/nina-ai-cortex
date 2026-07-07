"""
Fake PHD2 — эмулятор PHD2 для тестирования гидирования.
"""

import asyncio
import logging
import random
from typing import Dict, Any, Optional
from datetime import datetime
from app.core.events import event_bus

logger = logging.getLogger("FakePhd2")


class FakePhd2:
    """
    Эмулятор PHD2 для тестирования гидирования.
    Генерирует реалистичные RMS и события гидирования.
    """

    def __init__(self):
        self.guiding_active = False
        self.rms_ra = 0.8
        self.rms_dec = 0.9
        self.rms_total = 1.2

        self._task: Optional[asyncio.Task] = None
        self._running = False

    async def start(self):
        """Запускает симуляцию PHD2."""
        if self._running:
            return

        self._running = True
        logger.info("🎭 Fake PHD2 started")

        self._task = asyncio.create_task(self._guiding_loop())

    async def stop(self):
        """Останавливает симуляцию."""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("🎭 Fake PHD2 stopped")

    async def start_guiding(self):
        """Запускает гидирование."""
        if self.guiding_active:
            return

        self.guiding_active = True
        logger.info("🔭 Starting fake guiding...")

        await event_bus.publish(
            "LOG_EVENT", {"event_type": "guiding_start", "message": "Starting Guiding"}
        )

    async def stop_guiding(self):
        """Останавливает гидирование."""
        if not self.guiding_active:
            return

        self.guiding_active = False
        logger.info("🔭 Stopping fake guiding...")

        await event_bus.publish(
            "LOG_EVENT", {"event_type": "guiding_stop", "message": "Guiding Stopped"}
        )

    async def _guiding_loop(self):
        """Генерирует метрики гидирования."""
        while self._running:
            try:
                if self.guiding_active:
                    # Добавляем шум
                    self.rms_ra += random.gauss(0, 0.02)
                    self.rms_dec += random.gauss(0, 0.02)

                    # Ограничиваем значения
                    self.rms_ra = max(0.3, min(3.0, self.rms_ra))
                    self.rms_dec = max(0.3, min(3.0, self.rms_dec))

                    self.rms_total = (self.rms_ra**2 + self.rms_dec**2) ** 0.5

                    # Публикуем метрики
                    await event_bus.publish(
                        "PROMETHEUS_UPDATE",
                        {
                            "guider_rms_ra": self.rms_ra,
                            "guider_rms_dec": self.rms_dec,
                            "guider_rms_total": self.rms_total,
                        },
                    )

                await asyncio.sleep(2.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in guiding loop: {e}")

    async def inject_guiding_error(self):
        """Инжектирует ошибку гидирования."""
        logger.warning("⚠️ Injecting guiding error...")

        self.rms_ra += 2.0
        self.rms_dec += 2.0

        await event_bus.publish(
            "LOG_EVENT",
            {"event_type": "guiding_lost", "message": "Guiding Lost - RMS too high"},
        )


# Singleton instance
fake_phd2 = FakePhd2()
