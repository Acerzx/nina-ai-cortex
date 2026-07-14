"""
Night Summary Watcher — backup и cross-validation источник.

ЭТАП 6 (доработка):
- Night Summary теперь работает как backup + cross-validation источник
- Основной источник данных для Auditor — sessions_metadata (SQLite)
- Публикация NIGHT_SUMMARY события для UI
- Cross-validation: при получении Night Summary проверяется наличие
  собственных данных Auditor, если данные расходятся — публикуется
  NIGHT_SUMMARY_DISCREPANCY событие

Архитектура разделения ролей:
- **sessions_metadata (SQLite)**: основной источник для Auditor
  → точные данные по каждому кадру
  → быстрая агрегация через SQL
  → основа для quality_score

- **Night Summary (плагин N.I.N.A.)**: backup + cross-validation
  → публикуется для UI (если Frontend хочет показать)
  → cross-validation с собственными расчётами Auditor
  → если данные расходятся — предупреждение

Plugin: https://github.com/isbeorn/nina.plugin.nightsummary
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime
import aiofiles

from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.core.capability_registry import CapabilityRegistry
from app.core.config import settings
from app.storage.sessions_metadata import sessions_metadata

logger = logging.getLogger("NightSummaryWatcher")


class NightSummaryWatcher(BaseFileWatcher):
    """
    Мониторит NightSummary.json как backup + cross-validation источник.
    
    Используется для:
    1. Публикации данных Night Summary в UI (если Frontend хочет показать)
    2. Cross-validation с собственными расчётами Auditor
    3. Если данные расходятся — публикация NIGHT_SUMMARY_DISCREPANCY
    
    НЕ используется как основной источник для Auditor v2.
    """
    
    # Пороги расхождения для cross-validation (в процентах)
    DISCREPANCY_THRESHOLDS = {
        "frames_total": 0.05,      # 5% расхождение в количестве кадров
        "avg_hfr": 0.10,           # 10% расхождение в HFR
        "avg_fwhm": 0.10,          # 10% расхождение в FWHM
        "acceptance_rate": 0.05,   # 5% расхождение в acceptance rate
    }
    
    def __init__(self, registry: CapabilityRegistry):
        super().__init__(
            settings.nina_environment.sessions_root,
            ["NightSummary.json"],
            registry,
        )
        
        # Кэш последних Night Summary (для дедупликации)
        self._last_summary: Dict[str, Dict[str, Any]] = {}
        
        logger.info(
            "📊 NightSummaryWatcher initialized "
            "(backup + cross-validation mode)"
        )
    
    async def process_file(self, path: Path) -> None:
        """Обработка NightSummary.json."""
        if path.name != "NightSummary.json":
            return
        
        session_id = path.parent.name
        
        try:
            async with aiofiles.open(path, "r", encoding="utf-8") as f:
                content = await f.read()
            data = json.loads(content)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in NightSummary.json: {e}")
            return
        except Exception as e:
            logger.error(f"Failed to read NightSummary.json: {e}")
            return
        
        # Дедупликация
        if session_id in self._last_summary:
            if self._last_summary[session_id] == data:
                return
        self._last_summary[session_id] = data
        
        # === 1. Публикуем NIGHT_SUMMARY для UI ===
        await event_bus.publish(
            "NIGHT_SUMMARY",
            {
                "session_id": session_id,
                "source": "plugin",
                "data": data,
                "timestamp": datetime.now().isoformat(),
            },
        )
        
        logger.info(
            f"📊 Night Summary received for {session_id} "
            f"(frames: {data.get('frames_total', 'N/A')})"
        )
        
        # === 2. Cross-validation с sessions_metadata ===
        await self._cross_validate(session_id, data)
    
    async def _cross_validate(
        self,
        session_id: str,
        night_summary_data: Dict[str, Any],
    ) -> None:
        """
        Cross-validation Night Summary с собственными данными sessions_metadata.
        
        Сравнивает:
        - frames_total
        - avg_hfr
        - avg_fwhm
        - acceptance_rate
        
        Если расхождение превышает порог — публикует NIGHT_SUMMARY_DISCREPANCY.
        """
        try:
            # Получаем собственные данные из sessions_metadata
            own_stats = await sessions_metadata.get_session_stats(session_id)
            
            if "error" in own_stats:
                logger.debug(
                    f"Cross-validation skipped: no own data for {session_id}"
                )
                return
            
            # Извлекаем данные для сравнения
            own_session = own_stats.get("session", {})
            own_hfr = own_stats.get("hfr", {})
            own_fwhm = own_stats.get("fwhm", {})
            
            # Маппинг полей Night Summary → sessions_metadata
            comparisons = []
            
            # frames_total
            ns_frames = night_summary_data.get("frames_total")
            own_frames = own_session.get("frames_total")
            if ns_frames is not None and own_frames is not None:
                comparisons.append(
                    self._compare_field(
                        "frames_total", ns_frames, own_frames,
                        self.DISCREPANCY_THRESHOLDS["frames_total"],
                    )
                )
            
            # avg_hfr
            ns_hfr = night_summary_data.get("avg_hfr")
            own_hfr_val = own_hfr.get("avg")
            if ns_hfr is not None and own_hfr_val is not None:
                comparisons.append(
                    self._compare_field(
                        "avg_hfr", ns_hfr, own_hfr_val,
                        self.DISCREPANCY_THRESHOLDS["avg_hfr"],
                    )
                )
            
            # avg_fwhm
            ns_fwhm = night_summary_data.get("avg_fwhm")
            own_fwhm_val = own_fwhm.get("avg")
            if ns_fwhm is not None and own_fwhm_val is not None:
                comparisons.append(
                    self._compare_field(
                        "avg_fwhm", ns_fwhm, own_fwhm_val,
                        self.DISCREPANCY_THRESHOLDS["avg_fwhm"],
                    )
                )
            
            # acceptance_rate
            ns_acceptance = night_summary_data.get("acceptance_rate")
            own_acceptance = own_session.get("acceptance_rate")
            if ns_acceptance is not None and own_acceptance is not None:
                comparisons.append(
                    self._compare_field(
                        "acceptance_rate", ns_acceptance, own_acceptance,
                        self.DISCREPANCY_THRESHOLDS["acceptance_rate"],
                    )
                )
            
            # Проверяем расхождения
            discrepancies = [c for c in comparisons if c["discrepant"]]
            
            if discrepancies:
                logger.warning(
                    f"⚠️ Night Summary discrepancy for {session_id}: "
                    f"{len(discrepancies)} field(s) differ"
                )
                
                await event_bus.publish(
                    "NIGHT_SUMMARY_DISCREPANCY",
                    {
                        "session_id": session_id,
                        "discrepancies": discrepancies,
                        "timestamp": datetime.now().isoformat(),
                    },
                )
            else:
                logger.debug(
                    f"✅ Night Summary cross-validation passed for {session_id}"
                )
        
        except Exception as e:
            logger.error(f"Cross-validation failed for {session_id}: {e}")
    
    def _compare_field(
        self,
        field_name: str,
        night_summary_value: Any,
        own_value: Any,
        threshold: float,
    ) -> Dict[str, Any]:
        """
        Сравнивает одно поле Night Summary с собственным значением.
        
        Returns:
            Dict с результатом сравнения
        """
        try:
            ns_val = float(night_summary_value)
            own_val = float(own_value)
            
            # Избегаем деления на ноль
            if own_val == 0:
                if ns_val == 0:
                    discrepancy_percent = 0.0
                else:
                    discrepancy_percent = 100.0
            else:
                discrepancy_percent = abs(ns_val - own_val) / abs(own_val) * 100
            
            return {
                "field": field_name,
                "night_summary_value": ns_val,
                "own_value": own_val,
                "discrepancy_percent": round(discrepancy_percent, 2),
                "threshold_percent": threshold * 100,
                "discrepant": discrepancy_percent > threshold * 100,
            }
        except (TypeError, ValueError) as e:
            logger.debug(f"Cannot compare {field_name}: {e}")
            return {
                "field": field_name,
                "night_summary_value": night_summary_value,
                "own_value": own_value,
                "discrepancy_percent": None,
                "threshold_percent": threshold * 100,
                "discrepant": False,
                "error": str(e),
            }