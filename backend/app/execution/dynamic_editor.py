import json
import shutil
import logging
import asyncio
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime

from app.core.config import settings
from app.shadow_engine.state_tracker import state_tracker

logger = logging.getLogger("DynamicEditor")


class DynamicSequencerEditor:
    """
    Редактирует JSON-проекты Dynamic Sequencer.
    Устраняет Упрощение #25.
    """

    def __init__(self):
        # Путь из settings.yaml (или дефолтный)
        self.projects_root = Path(
            getattr(settings.watchers, "dynamic_sequencer_path", None)
            or Path.home() / "Documents" / "DynamicSequencer" / "Projects"
        )
        self.backup_dir = self.projects_root.parent / "Backups"
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()

    async def list_projects(self) -> List[Dict[str, Any]]:
        """Возвращает список всех доступных проектов."""
        if not self.projects_root.exists():
            logger.warning(
                f"Dynamic Sequencer projects dir not found: {self.projects_root}"
            )
            return []

        projects = []
        for json_file in self.projects_root.glob("*.json"):
            try:
                with open(json_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                projects.append(
                    {
                        "file": json_file.name,
                        "path": str(json_file),
                        "name": data.get("Name", json_file.stem),
                        "targets_count": len(data.get("Targets", [])),
                        "modified": datetime.fromtimestamp(
                            json_file.stat().st_mtime
                        ).isoformat(),
                    }
                )
            except Exception as e:
                logger.error(f"Failed to read project {json_file.name}: {e}")

        return projects

    async def get_project(self, project_name: str) -> Optional[Dict[str, Any]]:
        """Загружает проект по имени."""
        project_file = self.projects_root / f"{project_name}.json"
        if not project_file.exists():
            logger.error(f"Project not found: {project_name}")
            return None

        try:
            with open(project_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load project {project_name}: {e}")
            return None

    async def update_target(
        self,
        project_name: str,
        target_name: str,
        updates: Dict[str, Any],
        reason: str = "AI Optimization",
    ) -> bool:
        """
        Обновляет параметры конкретной цели в проекте.
        Безопасно: создает backup и проверяет состояние секвенсора.
        """
        # КРИТИЧНО: Проверка состояния секвенсора
        if state_tracker.state.is_running:
            logger.warning(
                f"🛑 BLOCKED: Cannot edit project '{project_name}' - sequence is running"
            )
            return False

        async with self._lock:
            project_file = self.projects_root / f"{project_name}.json"
            if not project_file.exists():
                logger.error(f"Project file not found: {project_file}")
                return False

            try:
                # 1. Backup
                backup_name = (
                    f"{project_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                )
                backup_path = self.backup_dir / backup_name
                shutil.copy2(project_file, backup_path)
                logger.info(f"📦 Backup created: {backup_name}")

                # 2. Загрузка
                with open(project_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                # 3. Поиск и обновление цели
                targets = data.get("Targets", [])
                target_found = False
                for target in targets:
                    if (
                        target.get("Name") == target_name
                        or target.get("TargetName") == target_name
                    ):
                        # Применяем обновления (разрешенные поля)
                        allowed_keys = [
                            "active",
                            "priority",
                            "acceptedAmount",
                            "exposureTime",
                            "filter",
                        ]
                        for key, value in updates.items():
                            if key in allowed_keys:
                                old_value = target.get(key)
                                target[key] = value
                                logger.info(
                                    f"✏️ Updated {target_name}.{key}: {old_value} -> {value}"
                                )
                        target_found = True
                        break

                if not target_found:
                    logger.error(
                        f"Target '{target_name}' not found in project '{project_name}'"
                    )
                    return False

                # 4. Валидация и сохранение
                self._validate_project(data)
                with open(project_file, "w", encoding="utf-8") as f:
                    json.dump(data, f, indent=2, ensure_ascii=False)

                logger.info(f"✅ Project '{project_name}' updated. Reason: {reason}")
                return True

            except json.JSONDecodeError as e:
                logger.error(f"❌ Invalid JSON in project: {e}")
                return False
            except Exception as e:
                logger.error(f"❌ Failed to update project: {e}")
                return False

    async def disable_target(
        self, project_name: str, target_name: str, reason: str
    ) -> bool:
        """Отключает цель в проекте (например, при плохой погоде)."""
        return await self.update_target(
            project_name, target_name, {"active": False}, reason=reason
        )

    def _validate_project(self, data: Dict[str, Any]):
        """Базовая валидация структуры проекта."""
        if "Targets" not in data:
            raise ValueError("Project must contain 'Targets' array")
        if not isinstance(data["Targets"], list):
            raise ValueError("'Targets' must be a list")


dynamic_editor = DynamicSequencerEditor()
