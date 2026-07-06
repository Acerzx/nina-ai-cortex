"""
Masters Library Auditor
Сканирует библиотеку мастер-кадров (Bias/Dark/Flat), извлекает FITS-заголовки
и строит каталог доступных калибровочных кадров для AI-агентов.

Архитектурные принципы:
- Использует astropy.io.fits.getheader() для чтения ТОЛЬКО заголовков (без тела)
- Асинхронное сканирование через run_in_executor (не блокирует event loop)
- Извлекает все метаданные: Temp, Gain, Offset, Exposure, Filter, Mean ADU
- Устраняет Упрощение #11
"""

import logging
import asyncio
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
from astropy.io import fits

from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("MastersAuditor")


class MastersLibraryAuditor:
    """
    Сканирует библиотеку мастер-кадров (Bias, Dark, Flat).
    Устраняет Упрощение #11.

    После индексации публикует событие MASTERS_INDEXED с каталогом
    для использования Strategist и LiveStack Watcher'ом.
    """

    def __init__(self):
        self.masters_root = settings.nina_environment.masters_root
        self.catalog: Dict[str, List[Dict[str, Any]]] = {
            "BIAS": [],
            "DARK": [],
            "FLAT": [],
            "UNKNOWN": [],  # Для файлов, где IMAGETYP отсутствует
        }
        self._scan_errors: List[str] = []

    async def scan_library(self):
        """
        Запускает полное сканирование библиотеки при старте Cortex.
        Выполняется в фоне, не блокируя основной event loop.
        """
        if not self.masters_root.exists():
            logger.warning(f"Masters root does not exist: {self.masters_root}")
            return

        logger.info(f"📚 Starting Masters Library audit at {self.masters_root}")
        scan_start = datetime.now()

        # Асинхронный обход через run_in_executor, чтобы не блокировать event loop
        loop = asyncio.get_running_loop()
        fit_files = await loop.run_in_executor(
            None,
            lambda: (
                list(self.masters_root.rglob("*.fit"))
                + list(self.masters_root.rglob("*.fits"))
            ),
        )

        total_files = len(fit_files)
        logger.info(f"   Found {total_files} FITS files to process")

        processed = 0
        skipped = 0

        for fit_path in fit_files:
            try:
                metadata = await self._read_fits_metadata(loop, fit_path)
                if metadata is None:
                    skipped += 1
                    continue

                # Определяем категорию по IMAGETYP
                img_type = metadata.get("image_type", "UNKNOWN")
                category = self._categorize_image_type(img_type)
                metadata["category"] = category

                self.catalog[category].append(metadata)
                processed += 1

                # Периодический прогресс-отчет
                if processed % 50 == 0:
                    logger.info(
                        f"   Progress: {processed}/{total_files} files indexed..."
                    )

            except Exception as e:
                error_msg = f"Error reading {fit_path.name}: {e}"
                self._scan_errors.append(error_msg)
                logger.debug(error_msg)
                skipped += 1

        # Итоговая статистика
        scan_duration = (datetime.now() - scan_start).total_seconds()

        summary = (
            f"✅ Masters audit complete in {scan_duration:.1f}s: "
            f"{len(self.catalog['BIAS'])} Bias, "
            f"{len(self.catalog['DARK'])} Darks, "
            f"{len(self.catalog['FLAT'])} Flats, "
            f"{skipped} skipped/errors"
        )
        logger.info(summary)

        if self._scan_errors:
            logger.warning(
                f"⚠️ {len(self._scan_errors)} files could not be read (check debug log for details)"
            )

        # Публикуем индексированный каталог в EventBus для других компонентов
        await event_bus.publish(
            "MASTERS_INDEXED",
            {
                "catalog": self.catalog,
                "summary": {
                    "total_processed": processed,
                    "total_skipped": skipped,
                    "scan_duration_seconds": scan_duration,
                    "masters_root": str(self.masters_root),
                },
            },
        )

    async def _read_fits_metadata(
        self, loop: asyncio.AbstractEventLoop, fit_path: Path
    ) -> Optional[Dict[str, Any]]:
        """
        Читает ТОЛЬКО заголовки FITS-файла через astropy.io.fits.getheader().
        Не загружает тело файла (данные пикселей), поэтому очень быстро.
        """
        try:
            # astropy.io.fits.getheader читает только заголовки
            header = await loop.run_in_executor(None, fits.getheader, str(fit_path), 0)
        except Exception as e:
            logger.debug(f"Cannot read header from {fit_path.name}: {e}")
            return None

        # Нормализация IMAGETYP: "Light Frame" → "LIGHT", "Master Dark" → "DARK"
        img_type_raw = str(header.get("IMAGETYP", "")).strip().upper()
        img_type_normalized = self._normalize_image_type(img_type_raw)

        metadata = {
            "path": str(fit_path),
            "filename": fit_path.name,
            "relative_path": str(fit_path.relative_to(self.masters_root)),
            "image_type": img_type_normalized,
            "temperature": self._safe_float(header.get("CCD-TEMP"))
            or self._safe_float(header.get("TEMPERAT")),
            "exposure": self._safe_float(header.get("EXPTIME")),
            "filter": str(header.get("FILTER", "")).strip(),
            "gain": self._safe_float(header.get("GAIN")),
            "offset": self._safe_float(header.get("OFFSET")),
            "binning_x": self._safe_int(header.get("XBINNING", header.get("BINX", 1))),
            "binning_y": self._safe_int(header.get("YBINNING", header.get("BINY", 1))),
            "naxis1": self._safe_int(header.get("NAXIS1")),
            "naxis2": self._safe_int(header.get("NAXIS2")),
            "date_obs": str(header.get("DATE-OBS", "")),
            "instrume": str(header.get("INSTRUME", "")),
            "mean_adu": self._safe_float(header.get("MEAN"))
            or self._safe_float(header.get("MEANVAL")),
            "std_adu": self._safe_float(header.get("STDDEV")),
        }

        return metadata

    def _normalize_image_type(self, img_type: str) -> str:
        """Нормализует значения IMAGETYP из FITS-заголовков."""
        img_upper = img_type.upper().strip()

        # Различные варианты написания из N.I.N.A. и PixInsight
        bias_variants = ["BIAS", "BIAS FRAME", "MASTER BIAS", "BIAS_FRAME", "BIASFRAME"]
        dark_variants = [
            "DARK",
            "DARK FRAME",
            "MASTER DARK",
            "DARK_FRAME",
            "DARKFRAME",
            "DARKS",
        ]
        flat_variants = [
            "FLAT",
            "FLAT FRAME",
            "MASTER FLAT",
            "FLAT_FRAME",
            "FLATFRAME",
            "FLATS",
            "SKYFLAT",
        ]
        light_variants = [
            "LIGHT",
            "LIGHT FRAME",
            "LIGHT_FRAME",
            "LIGHTFRAME",
            "SCIENCE",
        ]

        if img_upper in bias_variants:
            return "BIAS"
        if img_upper in dark_variants:
            return "DARK"
        if img_upper in flat_variants:
            return "FLAT"
        if img_upper in light_variants:
            return "LIGHT"

        # Поиск подстрок (для нестандартных имен типа "MASTERDARK_T-15...")
        if "BIAS" in img_upper:
            return "BIAS"
        if "DARK" in img_upper:
            return "DARK"
        if "FLAT" in img_upper:
            return "FLAT"
        if "LIGHT" in img_upper:
            return "LIGHT"

        return "UNKNOWN"

    def _categorize_image_type(self, img_type: str) -> str:
        """Возвращает категорию каталога по типу кадра."""
        return img_type if img_type in ["BIAS", "DARK", "FLAT"] else "UNKNOWN"

    def _safe_float(self, value: Any) -> Optional[float]:
        """Безопасная конвертация в float."""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def _safe_int(self, value: Any, default: int = None) -> Optional[int]:
        """Безопасная конвертация в int."""
        if value is None:
            return default
        try:
            return int(value)
        except (ValueError, TypeError):
            return default

    def get_stats(self) -> Dict[str, Any]:
        """Возвращает статистику индексированной библиотеки для API /health."""
        return {
            "total_bias": len(self.catalog.get("BIAS", [])),
            "total_dark": len(self.catalog.get("DARK", [])),
            "total_flat": len(self.catalog.get("FLAT", [])),
            "total_unknown": len(self.catalog.get("UNKNOWN", [])),
            "scan_errors": len(self._scan_errors),
        }

    def find_matching_master(
        self,
        image_type: str,
        temperature: float,
        exposure: Optional[float] = None,
        gain: Optional[int] = None,
        offset: Optional[int] = None,
        filter_name: Optional[str] = None,
        temp_tolerance: float = 2.0,
    ) -> Optional[Dict[str, Any]]:
        """
        Ищет наиболее подходящий мастер-кадр по параметрам.
        Используется Strategist'ом и LiveStack'ом для подбора калибровок.

        Returns:
            Dict с метаданными лучшего совпадения или None, если не найдено
        """
        category = self._categorize_image_type(image_type)
        candidates = self.catalog.get(category, [])

        if not candidates:
            return None

        # Фильтр 1: тип кадра уже применён
        # Фильтр 2: фильтр (если указан и важен для этого типа)
        if filter_name and category == "FLAT":
            candidates = [
                c for c in candidates if c["filter"].lower() == filter_name.lower()
            ]

        # Фильтр 3: температура с толерантностью
        candidates = [
            c
            for c in candidates
            if c["temperature"] is not None
            and abs(c["temperature"] - temperature) <= temp_tolerance
        ]

        # Фильтр 4: gain
        if gain is not None:
            gain_matches = [c for c in candidates if c["gain"] == gain]
            if gain_matches:
                candidates = gain_matches

        # Фильтр 5: offset
        if offset is not None:
            offset_matches = [c for c in candidates if c["offset"] == offset]
            if offset_matches:
                candidates = offset_matches

        # Фильтр 6: exposure (только для Dark, для Bias exposure ~0, для Flat любое)
        if exposure is not None and category == "DARK":
            exp_matches = [
                c
                for c in candidates
                if c["exposure"] is not None and abs(c["exposure"] - exposure) < 0.1
            ]
            if exp_matches:
                candidates = exp_matches

        if not candidates:
            return None

        # Выбираем "лучший" — с наиболее свежей датой создания файла (последний в списке после сортировки)
        return max(
            candidates, key=lambda c: c.get("date_obs", ""), default=candidates[0]
        )

    def get_summary_by_category(self) -> Dict[str, Dict[str, Any]]:
        """Возвращает детальную сводку по каждой категории мастеров."""
        summary = {}
        for category, items in self.catalog.items():
            if not items:
                continue
            temps = [i["temperature"] for i in items if i["temperature"] is not None]
            exposures = [i["exposure"] for i in items if i["exposure"] is not None]
            filters = list({i["filter"] for i in items if i["filter"]})
            gains = list({int(i["gain"]) for i in items if i["gain"] is not None})

            summary[category] = {
                "count": len(items),
                "temperatures": sorted(set(temps)),
                "exposures": sorted(set(exposures)),
                "filters": filters,
                "gains": sorted(gains),
                "min_date": min(i.get("date_obs", "") for i in items) or None,
                "max_date": max(i.get("date_obs", "") for i in items) or None,
            }
        return summary
