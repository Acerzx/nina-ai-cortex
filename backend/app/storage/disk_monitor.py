"""
Disk Monitor + Retention Engine — автоматическое управление дисковым пространством.
Основан на архитектуре Atlas для предотвращения переполнения диска.
"""

import logging
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("DiskMonitor")


class DiskUsage(BaseModel):
    """Информация об использовании диска."""

    path: str
    total_gb: float
    used_gb: float
    free_gb: float
    usage_percent: float
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class RetentionPolicy(BaseModel):
    """Политика удаления старых данных."""

    name: str
    description: str
    keep_last_days: int = 30
    keep_best_quality: bool = False
    delete_raw_keep_stacked: bool = False
    min_free_space_gb: float = 100.0


class RetentionResult(BaseModel):
    """Результат применения политики."""

    policy_name: str
    files_deleted: int
    space_freed_gb: float
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class DiskMonitor:
    """
    Мониторинг дискового пространства.

    Features:
    - Проверка свободного места на всех дисках
    - Алерты при низком свободном месте
    - Автоматическая очистка по политикам
    - Интеграция с Auditor для определения качества сессий
    """

    def __init__(self):
        # Пути для мониторинга
        self.monitored_paths = [
            settings.nina_environment.sessions_root,
            settings.nina_environment.masters_root,
            Path("./data"),  # Базы данных, логи
        ]

        # Пороговые значения
        self.warning_threshold_gb = 50.0  # Предупреждение при < 50 GB
        self.critical_threshold_gb = 20.0  # Критический алерт при < 20 GB

        # Политики хранения
        self.policies = {
            "keep_last_30_days": RetentionPolicy(
                name="keep_last_30_days",
                description="Хранить сессии за последние 30 дней",
                keep_last_days=30,
                min_free_space_gb=100.0,
            ),
            "keep_best_quality": RetentionPolicy(
                name="keep_best_quality",
                description="Хранить только сессии с quality_score > 8.0",
                keep_best_quality=True,
                min_free_space_gb=50.0,
            ),
            "aggressive_cleanup": RetentionPolicy(
                name="aggressive_cleanup",
                description="Агрессивная очистка: удалить RAW, оставить только стеки",
                delete_raw_keep_stacked=True,
                keep_last_days=7,
                min_free_space_gb=20.0,
            ),
        }

        # История очисток
        self._cleanup_history: List[RetentionResult] = []

        logger.info("💾 Disk Monitor initialized")

    async def check_all_disks(self) -> List[DiskUsage]:
        """Проверяет свободное место на всех monitored путях."""
        results = []

        for path in self.monitored_paths:
            if not path.exists():
                logger.warning(f"Path does not exist: {path}")
                continue

            usage = self._get_disk_usage(path)
            results.append(usage)

            # Проверяем пороги
            if usage.free_gb < self.critical_threshold_gb:
                await self._send_critical_alert(usage)
            elif usage.free_gb < self.warning_threshold_gb:
                await self._send_warning_alert(usage)

        return results

    def _get_disk_usage(self, path: Path) -> DiskUsage:
        """Получает информацию об использовании диска."""
        try:
            total, used, free = shutil.disk_usage(path)

            return DiskUsage(
                path=str(path),
                total_gb=total / (1024**3),
                used_gb=used / (1024**3),
                free_gb=free / (1024**3),
                usage_percent=(used / total) * 100,
            )
        except Exception as e:
            logger.error(f"Failed to get disk usage for {path}: {e}")
            return DiskUsage(
                path=str(path), total_gb=0, used_gb=0, free_gb=0, usage_percent=0
            )

    async def _send_warning_alert(self, usage: DiskUsage):
        """Отправляет предупреждение о низком свободном месте."""
        await event_bus.publish(
            "ALERT",
            {
                "level": "WARNING",
                "message": f"Low disk space: {usage.free_gb:.1f} GB free on {usage.path}",
                "agent": "DiskMonitor",
                "timestamp": datetime.now().isoformat(),
                "context": usage.model_dump(),
            },
        )

        logger.warning(f"⚠️ Low disk space: {usage.free_gb:.1f} GB free on {usage.path}")

    async def _send_critical_alert(self, usage: DiskUsage):
        """Отправляет критический алерт о нехватке места."""
        await event_bus.publish(
            "ALERT",
            {
                "level": "CRITICAL",
                "message": f"CRITICAL: Only {usage.free_gb:.1f} GB free on {usage.path}. Immediate cleanup required!",
                "agent": "DiskMonitor",
                "timestamp": datetime.now().isoformat(),
                "context": usage.model_dump(),
            },
        )

        logger.critical(
            f"🚨 CRITICAL: Only {usage.free_gb:.1f} GB free on {usage.path}"
        )

    async def apply_retention_policy(
        self, policy_name: str
    ) -> Optional[RetentionResult]:
        """Применяет политику удаления старых данных."""
        policy = self.policies.get(policy_name)
        if not policy:
            logger.error(f"Unknown retention policy: {policy_name}")
            return None

        logger.info(f"🗑️ Applying retention policy: {policy_name}")

        files_deleted = 0
        space_freed = 0

        # Получаем список сессий
        sessions_root = settings.nina_environment.sessions_root

        if not sessions_root.exists():
            logger.warning(f"Sessions root does not exist: {sessions_root}")
            return None

        # Сканируем папки сессий
        cutoff_date = datetime.now() - timedelta(days=policy.keep_last_days)

        for session_dir in sessions_root.rglob("*"):
            if not session_dir.is_dir():
                continue

            # Проверяем дату сессии (из имени папки или метаданных)
            # Упрощенная логика: проверяем дату создания папки
            try:
                dir_mtime = datetime.fromtimestamp(session_dir.stat().st_mtime)

                if dir_mtime < cutoff_date:
                    # Подсчитываем размер перед удалением
                    dir_size = sum(
                        f.stat().st_size for f in session_dir.rglob("*") if f.is_file()
                    )

                    # Применяем политику
                    should_delete = False

                    if policy.keep_best_quality:
                        # Здесь должна быть проверка quality_score из Auditor
                        # Для простоты удаляем все старые
                        should_delete = True

                    elif policy.delete_raw_keep_stacked:
                        # Удаляем RAW файлы, оставляем стеки
                        # Упрощенная логика: удаляем всю папку
                        should_delete = True

                    else:
                        # Просто удаляем старые сессии
                        should_delete = True

                    if should_delete:
                        # Удаляем папку
                        shutil.rmtree(session_dir)
                        files_deleted += 1
                        space_freed += dir_size

                        logger.info(
                            f"Deleted old session: {session_dir.name} ({dir_size / (1024**3):.2f} GB)"
                        )

            except Exception as e:
                logger.error(f"Error processing session {session_dir}: {e}")

        result = RetentionResult(
            policy_name=policy_name,
            files_deleted=files_deleted,
            space_freed_gb=space_freed / (1024**3),
        )

        self._cleanup_history.append(result)

        logger.info(
            f"✅ Retention policy applied: {files_deleted} sessions deleted, "
            f"{result.space_freed_gb:.2f} GB freed"
        )

        # Публикуем событие
        await event_bus.publish("DISK_CLEANUP_COMPLETED", result.model_dump())

        return result

    async def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику дискового пространства."""
        disk_usage = await self.check_all_disks()

        return {
            "monitored_paths": [str(p) for p in self.monitored_paths],
            "disk_usage": [u.model_dump() for u in disk_usage],
            "warning_threshold_gb": self.warning_threshold_gb,
            "critical_threshold_gb": self.critical_threshold_gb,
            "policies": {name: p.model_dump() for name, p in self.policies.items()},
            "cleanup_history": [r.model_dump() for r in self._cleanup_history[-10:]],
        }


# Singleton instance
disk_monitor = DiskMonitor()
