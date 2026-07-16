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

ИСПРАВЛЕНО (Спринт 5 — Observability):
- OpenTelemetry spans для каждого запуска задачи
- Prometheus метрики: executions, errors, duration
- Health checks для мониторинга состояния задач
- Trace ID в логах для correlation

Features:
- Централизованная регистрация периодических задач
- Graceful shutdown с гарантированной отменой всех задач
- Интеграция с FastAPI lifespan
- Динамическое управление задачами (enable/disable/stop)
- Полная наблюдаемость через OpenTelemetry и Prometheus

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

    # Health check
    health = background_tasks.health_check()

    # При shutdown
    await background_tasks.stop()
"""

import asyncio
import logging
import time
from typing import Dict, Any, Callable, Awaitable, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, field

# Observability (Спринт 5)
from app.core.tracing import tracing_manager, span_context
from app.core.metrics import cortex_metrics

logger = logging.getLogger("BackgroundTasks")


@dataclass
class TaskInfo:
    """
    Информация о зарегистрированной фоновой задаче.

    ИСПРАВЛЕНО (Проблема 1): добавлено поле enabled
    ИСПРАВЛЕНО (Спринт 5): добавлены поля для метрик и health checks
    """

    name: str
    coro: Callable[[], Awaitable[None]]
    interval_seconds: float
    enabled: bool = True
    description: str = ""
    task: Optional[asyncio.Task] = field(default=None, repr=False)
    last_run: Optional[datetime] = None

    # Спринт 5: метрики задачи
    execution_count: int = 0
    error_count: int = 0
    last_error: Optional[str] = None
    last_error_time: Optional[datetime] = None
    total_execution_time_seconds: float = 0.0


class BackgroundTaskManager:
    """
    Менеджер фоновых задач.

    Обеспечивает централизованное управление всеми periodics в системе.

    ИСПРАВЛЕНО (Проблема 1 + 31):
    - Поддержка enabled/disabled состояния задач
    - Методы enable(), disable(), stop_task()
    - Проверка enabled перед каждым запуском задачи

    ИСПРАВЛЕНО (Спринт 5):
    - OpenTelemetry spans для каждого запуска
    - Prometheus метрики для мониторинга
    - Health checks для обнаружения проблем
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

        # Обновляем Prometheus метрики
        cortex_metrics.background_tasks_total.set_sync(len(self._tasks))
        cortex_metrics.background_tasks_enabled.set_sync(
            sum(1 for t in self._tasks.values() if t.enabled)
        )

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

        # Обновляем Prometheus метрики
        cortex_metrics.background_tasks_total.set_sync(len(self._tasks))
        cortex_metrics.background_tasks_enabled.set_sync(
            sum(1 for t in self._tasks.values() if t.enabled)
        )

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
        Обёртка для задачи с обработкой ошибок, интервалом и Observability.

        Гарантирует, что задача не упадёт даже при исключениях.

        ИСПРАВЛЕНО (Проблема 1): проверка enabled перед каждым запуском
        ИСПРАВЛЕНО (Спринт 5): OpenTelemetry spans и Prometheus метрики
        """
        logger.debug(f"🔄 Background task '{info.name}' started")

        while self._running and info.enabled:  # ← Проверка enabled
            start_time = time.time()
            execution_number = info.execution_count + 1

            try:
                # Спринт 5: OpenTelemetry span для каждого запуска
                async with span_context(
                    f"background_task.{info.name}",
                    attributes={
                        "task.name": info.name,
                        "task.interval_seconds": info.interval_seconds,
                        "task.execution_number": execution_number,
                        "task.description": info.description,
                    },
                ) as span:
                    # Выполняем саму задачу
                    await info.coro()

                    # Успешное выполнение
                    info.last_run = datetime.now()
                    info.execution_count = execution_number
                    execution_time = time.time() - start_time
                    info.total_execution_time_seconds += execution_time

                    # Prometheus метрики
                    cortex_metrics.background_task_executions_total.inc_sync(
                        task_name=info.name, status="success"
                    )
                    cortex_metrics.background_task_duration_seconds.observe_sync(
                        execution_time, task_name=info.name
                    )

                    if span:
                        span.set_attribute(
                            "task.execution_time_seconds", execution_time
                        )
                        span.set_attribute("task.success", True)

                    logger.debug(
                        f"✅ Background task '{info.name}' completed "
                        f"in {execution_time:.2f}s (execution #{execution_number})"
                    )

            except asyncio.CancelledError:
                logger.debug(f"Background task '{info.name}' cancelled")
                break

            except Exception as e:
                execution_time = time.time() - start_time
                error_msg = f"{type(e).__name__}: {str(e)}"

                # Обновляем статистику ошибок
                info.error_count += 1
                info.last_error = error_msg
                info.last_error_time = datetime.now()

                # Prometheus метрики
                cortex_metrics.background_task_executions_total.inc_sync(
                    task_name=info.name, status="error"
                )
                cortex_metrics.background_task_errors_total.inc_sync(
                    task_name=info.name, error_type=type(e).__name__
                )

                if span:
                    span.set_attribute("task.success", False)
                    span.set_attribute("task.error_type", type(e).__name__)
                    span.set_attribute("task.error_message", str(e))
                    span.set_attribute("task.execution_time_seconds", execution_time)

                logger.error(
                    f"❌ Background task '{info.name}' failed: {error_msg}",
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

        # Обновляем Prometheus метрики
        cortex_metrics.background_tasks_enabled.set_sync(
            sum(1 for t in self._tasks.values() if t.enabled)
        )

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

        # Обновляем Prometheus метрики
        cortex_metrics.background_tasks_enabled.set_sync(
            sum(1 for t in self._tasks.values() if t.enabled)
        )

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

        # Обновляем Prometheus метрики
        cortex_metrics.background_tasks_enabled.set_sync(
            sum(1 for t in self._tasks.values() if t.enabled)
        )

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

        # Обновляем Prometheus метрики
        cortex_metrics.background_tasks_enabled.set_sync(
            sum(1 for t in self._tasks.values() if t.enabled)
        )

        if self._running:
            self._start_task(info)

        logger.info(f"🔄 Task '{name}' restarted")
        return True

    # ====================================================================
    # ИСПРАВЛЕНО (Спринт 5): Health Checks
    # ====================================================================

    def health_check(self) -> Dict[str, Any]:
        """
        Спринт 5: Health check всех задач.

        Проверяет:
        - Задача запущена и активна
        - Задача не "застряла" (last_run не слишком старый)
        - Задача не в состоянии постоянной ошибки

        Returns:
            Dict с overall_status и деталями по каждой задаче
        """
        now = datetime.now()
        tasks_health = {}

        for name, info in self._tasks.items():
            health_status = "healthy"
            issues = []

            # Проверка 1: задача должна быть запущена если enabled
            if info.enabled:
                if not info.task or info.task.done():
                    health_status = "unhealthy"
                    issues.append("Task is enabled but not running")

            # Проверка 2: задача не "застряла"
            if info.last_run and info.enabled:
                time_since_last_run = (now - info.last_run).total_seconds()
                # Если прошло больше 2x интервала — задача застряла
                if time_since_last_run > info.interval_seconds * 2:
                    health_status = "degraded"
                    issues.append(
                        f"Task stuck: last run {time_since_last_run:.0f}s ago "
                        f"(expected every {info.interval_seconds}s)"
                    )

            # Проверка 3: задача не в состоянии постоянной ошибки
            if info.error_count > 0 and info.last_error_time:
                time_since_last_error = (now - info.last_error_time).total_seconds()
                error_rate = info.error_count / max(info.execution_count, 1)

                # Если >50% запусков с ошибкой за последние 10 минут
                if error_rate > 0.5 and time_since_last_error < 600:
                    health_status = "unhealthy"
                    issues.append(
                        f"High error rate: {error_rate:.1%} "
                        f"({info.error_count}/{info.execution_count})"
                    )

            tasks_health[name] = {
                "status": health_status,
                "enabled": info.enabled,
                "running": info.task is not None and not info.task.done()
                if info.task
                else False,
                "execution_count": info.execution_count,
                "error_count": info.error_count,
                "error_rate": info.error_count / max(info.execution_count, 1),
                "last_run": info.last_run.isoformat() if info.last_run else None,
                "last_error": info.last_error,
                "last_error_time": info.last_error_time.isoformat()
                if info.last_error_time
                else None,
                "issues": issues,
            }

        # Общая оценка здоровья
        overall_status = "healthy"
        if any(t["status"] == "unhealthy" for t in tasks_health.values()):
            overall_status = "unhealthy"
        elif any(t["status"] == "degraded" for t in tasks_health.values()):
            overall_status = "degraded"

        return {
            "overall_status": overall_status,
            "total_tasks": len(self._tasks),
            "enabled_tasks": sum(1 for t in self._tasks.values() if t.enabled),
            "running_tasks": sum(1 for t in tasks_health.values() if t["running"]),
            "healthy_tasks": sum(
                1 for t in tasks_health.values() if t["status"] == "healthy"
            ),
            "degraded_tasks": sum(
                1 for t in tasks_health.values() if t["status"] == "degraded"
            ),
            "unhealthy_tasks": sum(
                1 for t in tasks_health.values() if t["status"] == "unhealthy"
            ),
            "tasks": tasks_health,
        }

    def get_stats(self) -> Dict[str, Any]:
        """
        Возвращает статистику всех задач с Prometheus метриками.

        ИСПРАВЛЕНО (Спринт 5): добавлен health check
        """
        uptime_seconds = 0.0
        if self._started_at:
            uptime_seconds = (datetime.now() - self._started_at).total_seconds()

        # Спринт 5: добавляем health check
        health = self.health_check()

        return {
            "running": self._running,
            "uptime_seconds": round(uptime_seconds, 2),
            "total_tasks": len(self._tasks),
            "enabled_tasks": sum(1 for t in self._tasks.values() if t.enabled),
            "health": health,
            "tasks": {
                name: {
                    "enabled": info.enabled,
                    "interval_seconds": info.interval_seconds,
                    "description": info.description,
                    "last_run": info.last_run.isoformat() if info.last_run else None,
                    "active": info.task is not None and not info.task.done(),
                    "execution_count": info.execution_count,
                    "error_count": info.error_count,
                    "error_rate": info.error_count / max(info.execution_count, 1),
                    "avg_execution_time_seconds": (
                        info.total_execution_time_seconds / info.execution_count
                        if info.execution_count > 0
                        else 0
                    ),
                    "last_error": info.last_error,
                    "last_error_time": info.last_error_time.isoformat()
                    if info.last_error_time
                    else None,
                }
                for name, info in self._tasks.items()
            },
        }


# ============================================================================
# SINGLETON INSTANCE
# ============================================================================
background_tasks = BackgroundTaskManager()
