"""
Disk Monitor — мониторинг дискового пространства и генерация рекомендаций.
ИСПРАВЛЕНО (v4.2 — критическое):
- УДАЛЕНО автоматическое удаление файлов (shutil.rmtree)
- Теперь ТОЛЬКО мониторит и генерирует рекомендации
- Публикует WARNING/CRITICAL алерты при нехватке места
- Генерирует список рекомендаций для пользователя
- Все пороги читаются из settings.thresholds.storage
- Все параметры вынесены в конфигурацию (v4.2)
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


class DiskRecommendation(BaseModel):
    """Рекомендация по управлению дисковым пространством."""

    priority: str  # CRITICAL, HIGH, MEDIUM, LOW
    category: str  # space, retention, optimization
    message: str
    affected_sessions: List[str] = Field(default_factory=list)
    estimated_space_gb: float = 0.0
    action_required: str = ""
    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())


class RetentionPolicy(BaseModel):
    """Политика рекомендаций по хранению данных."""

    name: str
    description: str
    keep_last_days: int = 30
    keep_best_quality: bool = False
    delete_raw_keep_stacked: bool = False
    min_free_space_gb: float = 100.0


class MonitoringResult(BaseModel):
    """Результат мониторинга дискового пространства."""

    timestamp: str = Field(default_factory=lambda: datetime.now().isoformat())
    disk_usage: List[DiskUsage] = Field(default_factory=list)
    recommendations: List[DiskRecommendation] = Field(default_factory=list)
    alerts_generated: int = 0
    sessions_at_risk: List[str] = Field(default_factory=list)
    total_space_at_risk_gb: float = 0.0


class DiskMonitor:
    """
    Мониторинг дискового пространства.
    ИСПРАВЛЕНО (v4.2): ТОЛЬКО мониторинг и рекомендации, БЕЗ удаления файлов.

    Responsibilities:
    - Периодическая проверка свободного места
    - Генерация WARNING/CRITICAL алертов
    - Создание рекомендаций для пользователя
    - Идентификация сессий, которые можно безопасно удалить

    НЕ делает:
    - Автоматическое удаление файлов
    - Применение retention policies
    - Модификация файловой системы
    """

    def __init__(self):
        # Пути для мониторинга
        self.monitored_paths = [
            settings.nina_environment.sessions_root,
            settings.nina_environment.masters_root,
            Path("./data"),
        ]

        # Пороги из settings.thresholds.storage
        storage_cfg = settings.thresholds.storage
        self.warning_threshold_gb = storage_cfg.warning_threshold_gb
        self.critical_threshold_gb = storage_cfg.critical_threshold_gb

        # Retention days для рекомендаций
        retention_days = storage_cfg.retention_keep_last_days

        # Политики рекомендаций (используются для генерации советов)
        self.policies = {
            "keep_last_30_days": RetentionPolicy(
                name="keep_last_30_days",
                description=f"Рекомендуется хранить сессии за последние {retention_days} дней",
                keep_last_days=retention_days,
                min_free_space_gb=100.0,
            ),
            "keep_best_quality": RetentionPolicy(
                name="keep_best_quality",
                description="Рекомендуется хранить только сессии с quality_score > 8.0",
                keep_best_quality=True,
                min_free_space_gb=50.0,
            ),
        }

        # История мониторинга
        self._monitoring_history: List[MonitoringResult] = []

        # Whitelist сессий, которые никогда не рекомендуются к удалению
        self._whitelist: Dict[str, str] = {}

        logger.info(
            f"💾 Disk Monitor initialized (MONITORING ONLY - NO DELETION)\n"
            f"   Warning threshold: {self.warning_threshold_gb} GB\n"
            f"   Critical threshold: {self.critical_threshold_gb} GB\n"
            f"   Retention recommendation: {retention_days} days"
        )

    def add_to_whitelist(self, session_id: str, reason: str = "manual") -> None:
        """Добавляет сессию в whitelist (защита от рекомендаций к удалению)."""
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

            # Генерируем алерты при нехватке места
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
                    f"Low disk space: {usage.free_gb:.1f} GB free on {usage.path}. "
                    f"Consider cleaning old sessions."
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
                    f"Immediate action required! Check recommendations."
                ),
                "agent": "DiskMonitor",
                "timestamp": datetime.now().isoformat(),
                "context": usage.model_dump(),
            },
        )
        logger.critical(
            f"🚨 CRITICAL: Only {usage.free_gb:.1f} GB free on {usage.path}"
        )

    async def generate_recommendations(
        self, policy_name: str = "keep_last_30_days"
    ) -> MonitoringResult:
        """
        Генерирует рекомендации по управлению дисковым пространством.
        ИСПРАВЛЕНО (v4.2): НЕ удаляет файлы, только рекомендует.

        Args:
            policy_name: Имя политики для генерации рекомендаций

        Returns:
            MonitoringResult с рекомендациями
        """
        policy = self.policies.get(policy_name)
        if not policy:
            logger.error(f"Unknown retention policy: {policy_name}")
            return MonitoringResult()

        logger.info(f"📊 Generating disk space recommendations (policy: {policy_name})")

        # Проверяем диски
        disk_usage = await self.check_all_disks()

        recommendations: List[DiskRecommendation] = []
        sessions_at_risk: List[str] = []
        total_space_at_risk = 0.0

        if state_tracker.state.is_running:
            logger.warning(
                "🛑 Sequence is running — recommendations will EXCLUDE "
                "active and today's sessions"
            )

        sessions_root = settings.nina_environment.sessions_root
        if not sessions_root.exists():
            logger.warning(f"Sessions root does not exist: {sessions_root}")
            return MonitoringResult(disk_usage=disk_usage)

        cutoff_date = datetime.now() - timedelta(days=policy.keep_last_days)
        active_session_ids = self._get_active_session_ids()

        logger.debug(f"Active session IDs detected: {active_session_ids or 'none'}")

        # Сканируем сессии для рекомендаций
        for session_dir in sessions_root.rglob("*"):
            if not session_dir.is_dir():
                continue

            is_active, reason = self._is_session_active(session_dir)
            if is_active:
                logger.debug(
                    f"⏭️ Skipping active session: {session_dir.name} (reason: {reason})"
                )
                continue

            # Проверяем возраст сессии
            try:
                dir_mtime = datetime.fromtimestamp(session_dir.stat().st_mtime)
                if dir_mtime >= cutoff_date:
                    continue  # Сессия новее cutoff — не рекомендуем
            except (OSError, ValueError) as e:
                logger.debug(f"Cannot read mtime for {session_dir}: {e}")
                continue

            # Вычисляем размер сессии
            try:
                dir_size = sum(
                    f.stat().st_size for f in session_dir.rglob("*") if f.is_file()
                )
                dir_size_gb = dir_size / (1024**3)
            except OSError as e:
                logger.warning(f"Cannot calculate size of {session_dir}: {e}")
                continue

            # Генерируем рекомендацию
            recommendation = DiskRecommendation(
                priority="HIGH" if dir_size_gb > 1.0 else "MEDIUM",
                category="retention",
                message=(
                    f"Session '{session_dir.name}' is older than {policy.keep_last_days} days "
                    f"({dir_size_gb:.2f} GB). Consider manual deletion."
                ),
                affected_sessions=[session_dir.name],
                estimated_space_gb=dir_size_gb,
                action_required=f"Manual deletion: {session_dir}",
            )

            recommendations.append(recommendation)
            sessions_at_risk.append(session_dir.name)
            total_space_at_risk += dir_size_gb

        # Сортируем рекомендации по приоритету и размеру
        priority_order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}
        recommendations.sort(
            key=lambda r: (priority_order.get(r.priority, 4), -r.estimated_space_gb)
        )

        # Публикуем событие с рекомендациями
        result = MonitoringResult(
            disk_usage=disk_usage,
            recommendations=recommendations,
            alerts_generated=len(
                [r for r in recommendations if r.priority in ["CRITICAL", "HIGH"]]
            ),
            sessions_at_risk=sessions_at_risk,
            total_space_at_risk_gb=total_space_at_risk,
        )

        self._monitoring_history.append(result)

        if recommendations:
            logger.info(
                f"✅ Generated {len(recommendations)} recommendations:\n"
                f"   Sessions at risk: {len(sessions_at_risk)}\n"
                f"   Total space that can be freed: {total_space_at_risk:.2f} GB\n"
                f"   High priority recommendations: {result.alerts_generated}"
            )

            await event_bus.publish(
                "DISK_RECOMMENDATIONS_GENERATED", result.model_dump()
            )
        else:
            logger.info("✅ No recommendations needed — disk space is adequate")

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
            "monitoring_history": [
                r.model_dump() for r in self._monitoring_history[-10:]
            ],
            "active_sessions": list(self._get_active_session_ids()),
            "whitelisted_sessions": dict(self._whitelist),
            "sequence_running": state_tracker.state.is_running,
            "mode": "MONITORING_ONLY",  # Явно указываем, что удаление отключено
        }


# Singleton instance
disk_monitor = DiskMonitor()
