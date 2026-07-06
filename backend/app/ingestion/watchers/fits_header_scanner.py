"""
FITS Header Scanner
Сканирует новые FITS-файлы и извлекает ТОЛЬКО метаданные из заголовков.

Архитектурное правило: Cortex НЕ вычисляет дрейф поля — это задача N.I.N.A.
(через CenterAfterDriftTrigger и FlexureCompensatorTrigger).
Cortex лишь индексирует WCS, MOONANGL, SUNANGLE и другие астрономические
параметры для RAG и аналитики.
"""

import logging
from pathlib import Path
from typing import Optional

from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.fits_header import parse_fits_header
from app.core.capability_registry import CapabilityRegistry

logger = logging.getLogger("FITSScanner")


class FITSHeaderScanner(BaseFileWatcher):
    """
    Сканирует новые FITS-файлы в папках сессий.
    Устраняет Упрощение #10: извлекает WCS, MOONANGL, SUNANGLE для индексации
    в RAG и Masters Library, БЕЗ вычисления дрейфа (это делает N.I.N.A.).

    Исключает из обработки:
    - Временные файлы N.I.N.A. (temp_*, ~*)
    - Не-FITS файлы (пропускает по расширению)
    """

    # Расширения FITS-файлов
    FITS_EXTENSIONS = {".fits", ".fit", ".fts"}

    # Префиксы временных файлов N.I.N.A., которые следует игнорировать
    TEMP_PREFIXES = ("temp_", "~", "tmp_", ".")

    def __init__(self, registry: CapabilityRegistry):
        from app.core.config import settings

        super().__init__(
            watch_path=settings.nina_environment.sessions_root,
            target_files=list(self.FITS_EXTENSIONS),
            registry=registry,
        )

        # Кэш последних WCS-координат для каждой сессии (для справочных целей,
        # НЕ для расчёта дрейфа). Используется Masters Library Auditor и RAG.
        # session_id -> {"ra": float, "dec": float, "file": str}
        self._last_wcs: dict = {}

    async def process_file(self, path: Path) -> None:
        """
        Обработка нового FITS-файла.
        Извлекает ТОЛЬКО заголовки — тело файла не загружается (быстро и безопасно).
        """
        # 1. Проверка расширения
        if path.suffix.lower() not in self.FITS_EXTENSIONS:
            return

        # 2. Игнорируем временные файлы N.I.N.A.
        if any(path.name.startswith(prefix) for prefix in self.TEMP_PREFIXES):
            return

        # 3. Определяем session_id из структуры папок N.I.N.A.:
        #    sessions_root/<telescope_camera>/<target>/<date>/<imagetype>/file.fits
        # session_id = <target>_<date> для уникальности
        try:
            # Безопасное извлечение session_id — учитываем разную глубину вложенности
            relative = path.relative_to(self.watch_path)
            parts = (
                relative.parts
            )  # (telescope_camera, target, date, imagetype, file.fits)

            if len(parts) >= 3:
                # Стандартная структура N.I.N.A.: telescope_camera / target / date / ...
                target = parts[1] if len(parts) > 1 else "unknown"
                date_folder = parts[2] if len(parts) > 2 else "unknown"
                session_id = f"{target}_{date_folder}"
                image_type = parts[3] if len(parts) > 3 else "UNKNOWN"
            else:
                # Файл лежит нестандартно — используем имя родительской папки
                session_id = path.parent.name
                image_type = "UNKNOWN"

        except ValueError:
            # path не находится внутри watch_path (маловероятно, но защитимся)
            session_id = path.parent.name
            image_type = "UNKNOWN"

        # 4. Парсим ТОЛЬКО заголовки через astropy.io.fits.getheader()
        report = parse_fits_header(path)
        if report is None:
            # parse_fits_header уже залогировал ошибку
            return

        # 5. Обновляем справочный кэш последних координат сессии
        # (для индексации, НЕ для расчёта дрейфа!)
        if (
            report.wcs
            and report.wcs.crval1 is not None
            and report.wcs.crval2 is not None
        ):
            prev = self._last_wcs.get(session_id)
            self._last_wcs[session_id] = {
                "ra": report.wcs.crval1,
                "dec": report.wcs.crval2,
                "filter": report.filter_name,
                "file": report.file_name,
            }

            if prev:
                logger.debug(
                    f"📐 WCS update [{session_id}]: "
                    f"RA {prev['ra']:.4f}° → {report.wcs.crval1:.4f}°, "
                    f"Dec {prev['dec']:.4f}° → {report.wcs.crval2:.4f}°"
                )

        # 6. Публикуем событие для RAG, Masters Auditor и AI-агентов
        payload = {
            "session_id": session_id,
            "file_name": report.file_name,
            "file_path": str(path),
            "image_type": image_type,
            "report": report.model_dump(),
        }
        await event_bus.publish("FITS_HEADER_PARSED", payload)

        # 7. Логируем ключевую информацию (уровень DEBUG — чтобы не спамить)
        astro_info = []
        if report.moon_angl is not None:
            astro_info.append(f"Moon {report.moon_angl:.1f}°")
        if report.sun_angle is not None:
            astro_info.append(f"Sun {report.sun_angle:.1f}°")
        if report.filter_name:
            astro_info.append(f"F:{report.filter_name}")
        if report.exposure_time is not None:
            astro_info.append(f"E:{report.exposure_time:.2f}s")

        astro_str = " | ".join(astro_info) if astro_info else "no astro metadata"
        logger.debug(
            f"🔭 FITS scanned: {report.file_name} [{image_type}] — {astro_str}"
        )

    def get_last_position(self, session_id: str) -> Optional[dict]:
        """
        Возвращает последнюю известную позицию цели в сессии.
        Используется Masters Library Auditor для подбора мастеров по координатам.

        Args:
            session_id: Идентификатор сессии (например, "M31_2025-09-17")

        Returns:
            Словарь {"ra", "dec", "filter", "file"} или None
        """
        return self._last_wcs.get(session_id)

    def get_stats(self) -> dict:
        """Возвращает статистику сканера для /health эндпоинта."""
        return {
            "tracked_sessions": len(self._last_wcs),
            "sessions": list(self._last_wcs.keys()),
        }
