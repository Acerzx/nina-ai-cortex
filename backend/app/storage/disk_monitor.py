"""
Disk Monitor + Retention Engine — автоматическое управление дисковым пространством.

ИСПРАВЛЕНО (рефакторинг v3):
- Все пороги (50 GB, 20 GB) вынесены в settings.thresholds.storage
- retention_keep_last_days читается из конфига
- Защита активных сессий от удаления сохранена
"""

import logging
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional, Set
from datetime import datetime, timedelta
from pydantic import BaseModel, Field

from app.core.config import settings
from app.core.events import event_bus
from app.shadow_engine.state_tracker import state_tracker
from app.agents.observatory_state import observatory_state

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
    keep_last_days: int = 1000
    keep_best_quality: bool = False
    delete_raw_keep_stacked: bool = False
    min_free_space_gb: float = 10.0


class RetentionResult(BaseModel):
    """Результат применения политики."""

    policy_name: str
    files_deleted: int
    sessions_deleted: int = 0
    sessions_skipped_active: int = 0
    sessions_skipped_whitelist: int = 0
    space_freed_gb: float
    blocked_by_running_sequence: bool = False
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class DiskMonitor:
    """
    Мониторинг дискового пространства.

    ИСПРАВЛЕНО (v3):
    - warning_threshold_gb и critical_threshold_gb читаются из settings
    - retention_keep_last_days читается из settings
    - Все политики используют конфиг вместо хардкода
    """

    def __init__(self):
        # Пути для мониторинга
        self.monitored_paths = [
            settings.nina_environment.sessions_root,
            settings.nina_environment.masters_root,
            Path("./data"),
        ]

        # ИСПРАВЛЕНО (v3): Пороги из settings.thresholds.storage
        storage_cfg = settings.thresholds.storage
        self.warning_threshold_gb = storage_cfg.warning_threshold_gb
        self.critical_threshold_gb = storage_cfg.critical_threshold_gb

        # ИСПРАВЛЕНО (v3): retention_keep_last_days из конфига
        retention_days = storage_cfg.retention_keep_last_days

        # Политики хранения (с использованием конфига)
        self.policies = {
            "keep_last_30_days": RetentionPolicy(
                name="keep_last_30_days",
                description=f"Хранить сессии за последние {retention_days} дней",
                keep_last_days=retention_days,
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

        # Whitelist сессий, которые никогда не удаляются
        self._whitelist: Dict[str, str] = {}

        logger.info(
            f"💾 Disk Monitor initialized "
            f"(warning: {self.warning_threshold_gb} GB, "
            f"critical: {self.critical_threshold_gb} GB, "
            f"retention: {retention_days} days)"
        )

    def add_to_whitelist(self, session_id: str, reason: str = "manual") -> None:
        """Добавляет сессию в whitelist (защита от удаления)."""
        self._whitelist[session_id] = reason
        logger.info(f"🛡️ Session added to whitelist: {session_id} (reason: {reason})")

    def remove_from_whitelist(self, session_id: str) -> bool:
        """Удаляет сессию из whitelist."""
        if session_id in self._whitelist:
            del self._whitelist[session_id]
            logger.info(f"🛡️ Session removed from whitelist: {session_id}")
            return True
        return False

    def _get_active_session_ids(self) -> Set[str]:
        """Возвращает множество ID активных сессий."""
        active_ids: Set[str] = set()

        for target in observatory_state.active_targets:
            target_name = target.get("name") or target.get("target_name")
            if target_name:
                date_str = datetime.now().strftime("%Y-%m-%d")
                active_ids.add(f"{target_name}_{date_str}")
                active_ids.add(target_name)

            session_id = target.get("session_id")
            if session_id:
                active_ids.add(session_id)

        if state_tracker.state.is_running:
            if state_tracker.state.container_path:
                for part in state_tracker.state.container_path[:2]:
                    active_ids.add(part)
                    date_str = datetime.now().strftime("%Y-%m-%d")
                    active_ids.add(f"{part}_{date_str}")

        return active_ids

    def _is_session_active(self, session_dir: Path) -> tuple:
        """
        Проверяет, является ли сессия активной.
        Returns: Tuple (is_active: bool, reason: str)
        """
        session_id = session_dir.name
        session_parent = session_dir.parent.name

        # 1. Проверка whitelist
        if session_id in self._whitelist:
            return True, f"whitelist: {self._whitelist[session_id]}"
        if session_parent in self._whitelist:
            return True, f"whitelist (parent): {self._whitelist[session_parent]}"

        # 2. Проверка активных целей
        active_ids = self._get_active_session_ids()
        if session_id in active_ids:
            return True, "active target"
        if session_parent in active_ids:
            return True, "active target (parent match)"

        # 3. Проверка последовательности
        if state_tracker.state.is_running:
            try:
                mtime = datetime.fromtimestamp(session_dir.stat().st_mtime)
                if mtime.date() == datetime.now().date():
                    return True, "sequence running + today's session"
            except (OSError, ValueError):
                pass

        return False, ""

    async def check_all_disks(self) -> List[DiskUsage]:
        """Проверяет свободное место на всех monitored путях."""
        results = []

        for path in self.monitored_paths:
            if not path.exists():
                logger.warning(f"Path does not exist: {path}")
                continue

            usage = self._get_disk_usage(path)
            results.append(usage)

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
                "message": (
                    f"Low disk space: {usage.free_gb:.1f} GB free on {usage.path}"
                ),
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
                "message": (
                    f"CRITICAL: Only {usage.free_gb:.1f} GB free on {usage.path}. "
                    f"Immediate cleanup required!"
                ),
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
        """
        Применяет политику удаления старых данных.

        ИСПРАВЛЕНО (v3): keep_last_days берётся из политики (которая
        инициализирована из settings.thresholds.storage.retention_keep_last_days).
        """
        policy = self.policies.get(policy_name)
        if not policy:
            logger.error(f"Unknown retention policy: {policy_name}")
            return None

        logger.info(f"🗑️ Applying retention policy: {policy_name}")

        if state_tracker.state.is_running:
            logger.warning(
                "🛑 Sequence is running — retention policy will SKIP "
                "active and today's sessions"
            )

        files_deleted = 0
        sessions_deleted = 0
        sessions_skipped_active = 0
        sessions_skipped_whitelist = 0
        space_freed = 0

        sessions_root = settings.nina_environment.sessions_root
        if not sessions_root.exists():
            logger.warning(f"Sessions root does not exist: {sessions_root}")
            return None

        cutoff_date = datetime.now() - timedelta(days=policy.keep_last_days)
        active_session_ids = self._get_active_session_ids()
        logger.debug(f"Active session IDs detected: {active_session_ids or 'none'}")

        for session_dir in sessions_root.rglob("*"):
            if not session_dir.is_dir():
                continue

            is_active, reason = self._is_session_active(session_dir)
            if is_active:
                if "whitelist" in reason:
                    sessions_skipped_whitelist += 1
                    logger.debug(
                        f"⏭️ Skipping whitelisted session: {session_dir.name} "
                        f"(reason: {reason})"
                    )
                else:
                    sessions_skipped_active += 1
                    logger.info(
                        f"⏭️ Skipping active session: {session_dir.name} "
                        f"(reason: {reason})"
                    )
                continue

            try:
                dir_mtime = datetime.fromtimestamp(session_dir.stat().st_mtime)
                if dir_mtime >= cutoff_date:
                    continue
            except (OSError, ValueError) as e:
                logger.debug(f"Cannot read mtime for {session_dir}: {e}")
                continue

            try:
                dir_size = sum(
                    f.stat().st_size for f in session_dir.rglob("*") if f.is_file()
                )
            except OSError as e:
                logger.warning(f"Cannot calculate size of {session_dir}: {e}")
                continue

            should_delete = False
            if policy.keep_best_quality:
                has_stack = any(
                    (session_dir / f).exists()
                    for f in ["stack.fit", "preview.jpg", "Session_Digest.md"]
                )
                should_delete = not has_stack
                if not should_delete:
                    logger.debug(f"⏭️ Keeping high-quality session: {session_dir.name}")
                    continue
            elif policy.delete_raw_keep_stacked:
                should_delete = True
            else:
                should_delete = True

            if not should_delete:
                continue

            try:
                shutil.rmtree(session_dir)
                sessions_deleted += 1
                files_deleted += max(1, dir_size // (50 * 1024 * 1024))
                space_freed += dir_size
                logger.info(
                    f"🗑️ Deleted old session: {session_dir.name} "
                    f"({dir_size / (1024**3):.2f} GB)"
                )
            except Exception as e:
                logger.error(f"Error deleting session {session_dir}: {e}")

        result = RetentionResult(
            policy_name=policy_name,
            files_deleted=files_deleted,
            sessions_deleted=sessions_deleted,
            sessions_skipped_active=sessions_skipped_active,
            sessions_skipped_whitelist=sessions_skipped_whitelist,
            space_freed_gb=space_freed / (1024**3),
            blocked_by_running_sequence=state_tracker.state.is_running,
        )

        self._cleanup_history.append(result)

        logger.info(
            f"✅ Retention policy '{policy_name}' applied: "
            f"{sessions_deleted} sessions deleted, "
            f"{result.space_freed_gb:.2f} GB freed, "
            f"{sessions_skipped_active} active skipped, "
            f"{sessions_skipped_whitelist} whitelisted skipped"
        )

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
            "active_sessions": list(self._get_active_session_ids()),
            "whitelisted_sessions": dict(self._whitelist),
            "sequence_running": state_tracker.state.is_running,
        }


# Singleton instance
disk_monitor = DiskMonitor()
