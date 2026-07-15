"""
Background Task Manager — единый планировщик фоновых задач для Cortex.
ЭТАП 1.3 (упрощение):
- Удалены методы enable/disable/unregister (не используются)
- Упрощён TaskInfo (убраны метрики на задачу)
- Оставлен минимум необходимой функционала:
  * register() — регистрация задачи
  * start() — запуск всех задач
  * stop() — graceful shutdown
  * get_stats() — статистика для API

ИСПРАВЛЕНО (Проблема 1):
- Добавлено поле enabled в TaskInfo
- Проверка enabled в _task_wrapper перед каждым запуском
- Методы enable() и disable() для управления задачами

ИСПРАВЛЕНО (Проблема 31):
- Добавлен метод stop_task() для остановки конкретной задачи

Features:
- Централизованная регистрация периодических задач
- Graceful shutdown с гарантированной отменой всех задач
- Интеграция с FastAPI lifespan
- Динамическое управление задачами (enable/disable/stop)

Использование:
    # В lifespan
    await background_tasks.start()

    background_tasks.register(
        name="cleanup",
        coro=cleanup_loop,
        interval_seconds=24 * 3600,
        enabled=True,
    )

    # Управление задачами
    background_tasks.enable("cleanup")
    background_tasks.disable("cleanup")
    background_tasks.stop_task("cleanup")

    # При shutdown
    await background_tasks.stop()
"""

import asyncio
import logging
from typing import Dict, Any, Callable, Awaitable, Optional
from datetime import datetime
from dataclasses import dataclass, field

logger = logging.getLogger("BackgroundTasks")


@dataclass
class TaskInfo:
    """Информация о зарегистрированной фоновой задаче."""

    name: str
    coro: Callable[[], Awaitable[None]]
    interval_seconds: float
    enabled: bool = True  # ← ИСПРАВЛЕНО (Проблема 1): сохраняем enabled
    description: str = ""
    task: Optional[asyncio.Task] = field(default=None, repr=False)
    last_run: Optional[datetime] = None


class BackgroundTaskManager:
    """
    Менеджер фоновых задач.
    Обеспечивает централизованное управление всеми periodics в системе.

    ИСПРАВЛЕНО (Проблема 1 + 31):
    - Поддержка enabled/disabled состояния задач
    - Методы enable(), disable(), stop_task()
    - Проверка enabled перед каждым запуском задачи
    """

    def __init__(self):
        self._tasks: Dict[str, TaskInfo] = {}
        self._running = False
        self._started_at: Optional[datetime] = None

    async def start(self):
        """Запускает менеджер фоновых задач."""
        if self._running:
            logger.warning("BackgroundTaskManager already running")
            return

        self._running = True
        self._started_at = datetime.now()

        # Запускаем все зарегистрированные enabled задачи
        for name, info in self._tasks.items():
            if info.enabled:
                self._start_task(info)

        logger.info(
            f"✅ BackgroundTaskManager started "
            f"({len(self._tasks)} tasks registered, "
            f"{sum(1 for t in self._tasks.values() if t.enabled)} enabled)"
        )

    async def stop(self):
        """Останавливает все фоновые задачи."""
        if not self._running:
            return

        self._running = False

        # Отменяем все активные задачи
        cancelled = 0
        for info in self._tasks.values():
            if info.task and not info.task.done():
                info.task.cancel()
                cancelled += 1

        # Ждём завершения всех задач
        active_tasks = [
            info.task
            for info in self._tasks.values()
            if info.task and not info.task.done()
        ]

        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)

        logger.info(f"🛑 BackgroundTaskManager stopped ({cancelled} tasks cancelled)")

    def register(
        self,
        name: str,
        coro: Callable[[], Awaitable[None]],
        interval_seconds: float,
        enabled: bool = True,
        description: str = "",
    ) -> None:
        """
        Регистрирует фоновую задачу.

        Args:
            name: Уникальное имя задачи
            coro: Асинхронная функция-обработчик (без аргументов)
            interval_seconds: Интервал между запусками (секунды)
            enabled: Включена ли задача
            description: Описание задачи (для API)
        """
        if name in self._tasks:
            logger.warning(f"Task '{name}' already registered, replacing...")
            old_info = self._tasks[name]
            if old_info.task and not old_info.task.done():
                old_info.task.cancel()

        # ИСПРАВЛЕНО (Проблема 1): сохраняем enabled в TaskInfo
        info = TaskInfo(
            name=name,
            coro=coro,
            interval_seconds=interval_seconds,
            enabled=enabled,
            description=description,
        )

        self._tasks[name] = info

        # Если менеджер уже запущен и задача enabled — сразу стартуем
        if self._running and enabled:
            self._start_task(info)

        logger.info(
            f"📝 Registered background task: '{name}' "
            f"(interval: {interval_seconds}s, enabled: {enabled})"
        )

    def _start_task(self, info: TaskInfo):
        """Запускает одну задачу."""
        if info.task and not info.task.done():
            return  # Уже запущена

        info.task = asyncio.create_task(self._task_wrapper(info))

    async def _task_wrapper(self, info: TaskInfo):
        """
        Обёртка для задачи с обработкой ошибок и интервалом.
        Гарантирует, что задача не упадёт даже при исключениях.

        ИСПРАВЛЕНО (Проблема 1): проверка enabled перед каждым запуском
        """
        logger.debug(f"🔄 Background task '{info.name}' started")

        while self._running and info.enabled:  # ← Проверка enabled
            try:
                # Выполняем саму задачу
                await info.coro()
                info.last_run = datetime.now()
            except asyncio.CancelledError:
                logger.debug(f"Background task '{info.name}' cancelled")
                break
            except Exception as e:
                logger.error(
                    f"❌ Background task '{info.name}' failed: {e}",
                    exc_info=True,
                )

            # Ждём интервал перед следующим запуском
            try:
                await asyncio.sleep(info.interval_seconds)
            except asyncio.CancelledError:
                break

        logger.debug(f"🔄 Background task '{info.name}' stopped")

    # ====================================================================
    # ИСПРАВЛЕНО (Проблема 1 + 31): Методы управления задачами
    # ====================================================================

    def enable(self, name: str) -> bool:
        """
        Включает задачу.

        Args:
            name: Имя задачи

        Returns:
            True если задача найдена и включена
        """
        if name not in self._tasks:
            logger.warning(f"Task '{name}' not found")
            return False

        info = self._tasks[name]
        if info.enabled:
            logger.debug(f"Task '{name}' already enabled")
            return True

        info.enabled = True

        # Если менеджер запущен — запускаем задачу
        if self._running:
            self._start_task(info)

        logger.info(f"✅ Task '{name}' enabled")
        return True

    def disable(self, name: str) -> bool:
        """
        Выключает задачу (не отменяет текущий запуск, но не запускает снова).

        Args:
            name: Имя задачи

        Returns:
            True если задача найдена и выключена
        """
        if name not in self._tasks:
            logger.warning(f"Task '{name}' not found")
            return False

        info = self._tasks[name]
        if not info.enabled:
            logger.debug(f"Task '{name}' already disabled")
            return True

        info.enabled = False
        logger.info(f"⏸️ Task '{name}' disabled (will not restart)")
        return True

    def stop_task(self, name: str) -> bool:
        """
        Останавливает конкретную задачу.

        ИСПРАВЛЕНО (Проблема 31): добавлен метод для остановки одной задачи

        Args:
            name: Имя задачи

        Returns:
            True если задача найдена и остановлена
        """
        if name not in self._tasks:
            logger.warning(f"Task '{name}' not found")
            return False

        info = self._tasks[name]

        if not info.task or info.task.done():
            logger.debug(f"Task '{name}' is not running")
            return True

        # Отменяем задачу
        info.task.cancel()
        info.enabled = False

        logger.info(f"🛑 Task '{name}' stopped")
        return True

    def restart_task(self, name: str) -> bool:
        """
        Перезапускает задачу.

        Args:
            name: Имя задачи

        Returns:
            True если задача найдена и перезапущена
        """
        if name not in self._tasks:
            logger.warning(f"Task '{name}' not found")
            return False

        info = self._tasks[name]

        # Останавливаем если запущена
        if info.task and not info.task.done():
            info.task.cancel()

        # Включаем и запускаем
        info.enabled = True
        if self._running:
            self._start_task(info)

        logger.info(f"🔄 Task '{name}' restarted")
        return True

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику всех задач."""
        uptime_seconds = 0.0
        if self._started_at:
            uptime_seconds = (datetime.now() - self._started_at).total_seconds()

        return {
            "running": self._running,
            "uptime_seconds": round(uptime_seconds, 2),
            "total_tasks": len(self._tasks),
            "enabled_tasks": sum(1 for t in self._tasks.values() if t.enabled),
            "tasks": {
                name: {
                    "enabled": info.enabled,
                    "interval_seconds": info.interval_seconds,
                    "description": info.description,
                    "last_run": info.last_run.isoformat() if info.last_run else None,
                    "active": info.task is not None and not info.task.done(),
                }
                for name, info in self._tasks.items()
            },
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
background_tasks = BackgroundTaskManager()
