"""
Background Task Manager — единый планировщик фоновых задач для Cortex.
Решение архитектурной проблемы #64: отсутствие единого планировщика.

Features:
- Централизованная регистрация периодических задач
- Graceful shutdown с гарантированной отменой всех задач
- Feature flag поддержка (через settings.feature_flags)
- Интеграция с FastAPI lifespan
- Метрики для каждой задачи (успешные/неуспешные запуски)

Использование:
    # В lifespan
    await background_tasks.start()
    background_tasks.register("cleanup", cleanup_loop, interval_hours=24)
    background_tasks.register("health_check", health_check_loop, interval_seconds=300)

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
    enabled: bool = True
    description: str = ""
    task: Optional[asyncio.Task] = field(default=None, repr=False)
    last_run: Optional[datetime] = None
    successful_runs: int = 0
    failed_runs: int = 0
    last_error: Optional[str] = None


class BackgroundTaskManager:
    """
    Менеджер фоновых задач.
    Обеспечивает централизованное управление всеми periodics в системе.
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
            description: Описание задачи
        """
        if name in self._tasks:
            logger.warning(f"Task '{name}' already registered, replacing...")
            old_info = self._tasks[name]
            if old_info.task and not old_info.task.done():
                old_info.task.cancel()

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

    def unregister(self, name: str) -> bool:
        """Удаляет задачу из реестра."""
        if name not in self._tasks:
            return False

        info = self._tasks[name]
        if info.task and not info.task.done():
            info.task.cancel()

        del self._tasks[name]
        logger.info(f"🗑️ Unregistered background task: '{name}'")
        return True

    def enable(self, name: str) -> bool:
        """Включает задачу."""
        if name not in self._tasks:
            return False

        info = self._tasks[name]
        info.enabled = True

        if self._running and (info.task is None or info.task.done()):
            self._start_task(info)

        return True

    def disable(self, name: str) -> bool:
        """Выключает задачу."""
        if name not in self._tasks:
            return False

        info = self._tasks[name]
        info.enabled = False

        if info.task and not info.task.done():
            info.task.cancel()

        return True

    def _start_task(self, info: TaskInfo):
        """Запускает одну задачу."""
        if info.task and not info.task.done():
            return  # Уже запущена

        info.task = asyncio.create_task(self._task_wrapper(info))

    async def _task_wrapper(self, info: TaskInfo):
        """
        Обёртка для задачи с обработкой ошибок и интервалом.
        Гарантирует, что задача не упадёт даже при исключениях.
        """
        logger.debug(f"🔄 Background task '{info.name}' started")

        while self._running and info.enabled:
            try:
                # Выполняем саму задачу
                await info.coro()

                info.last_run = datetime.now()
                info.successful_runs += 1
                info.last_error = None

            except asyncio.CancelledError:
                logger.debug(f"Background task '{info.name}' cancelled")
                break
            except Exception as e:
                info.failed_runs += 1
                info.last_error = f"{type(e).__name__}: {e}"
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
                    "successful_runs": info.successful_runs,
                    "failed_runs": info.failed_runs,
                    "last_error": info.last_error,
                    "active": info.task is not None and not info.task.done(),
                }
                for name, info in self._tasks.items()
            },
        }

    def get_task_info(self, name: str) -> Optional[TaskInfo]:
        """Возвращает информацию о конкретной задаче."""
        return self._tasks.get(name)


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
background_tasks = BackgroundTaskManager()
