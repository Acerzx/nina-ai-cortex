import logging
from pathlib import Path
from typing import Dict, Tuple
from app.ingestion.watchers.base import BaseFileWatcher, event_bus
from app.ingestion.parsers.fits_header import parse_fits_header, angular_separation
from app.core.config import settings

logger = logging.getLogger("FITSScanner")


class FITSHeaderScanner(BaseFileWatcher):
    """
    Сканирует новые FITS-файлы в папках сессий.
    Устраняет Упрощение #10: извлекает WCS, MOONANGL, SUNANGLE и считает дрейф поля.
    """

    def __init__(self):
        super().__init__(
            watch_path=settings.nina_environment.sessions_root,
            target_files=[".fits", ".fit"],  # Нас интересуют только FITS
        )
        # Кэш последних координат для расчета дрейфа (session_id -> (RA, Dec, PixelScale))
        self._last_coords: Dict[str, Tuple[float, float, float]] = {}

    async def process_file(self, path: Path) -> None:
        if path.suffix.lower() not in [".fits", ".fit"]:
            return

        # Игнорируем временные файлы N.I.N.A.
        if path.name.startswith("temp_") or path.name.startswith("~"):
            return

        report = parse_fits_header(path)
        if not report:
            return

        # session_id = имя папки сессии (например, "M31" или "2025-09-17")
        session_id = (
            path.parent.parent.name
            if path.parent.parent != path.parent
            else path.parent.name
        )

        # Расчет дрейфа поля
        if report.wcs and report.wcs.crval1 is not None:
            # Пытаемся определить масштаб пикселя (arcsec/pixel) из CD матрицы
            # Упрощенно: sqrt(CD1_1^2 + CD2_1^2) * 3600
            pixel_scale = 1.0  # По умолчанию
            if report.wcs.cd1_1 and report.wcs.cd2_1:
                pixel_scale = (
                    math.sqrt(report.wcs.cd1_1**2 + report.wcs.cd2_1**2) * 3600
                )
                pixel_scale = abs(pixel_scale)

            last_data = self._last_coords.get(session_id)
            if last_data:
                last_ra, last_dec, last_scale = last_data

                # Дельта в градусах
                delta_ra_deg = report.wcs.crval1 - last_ra
                delta_dec_deg = report.wcs.crval2 - last_dec

                # Переводим в арксекунды с учетом косинуса склонения для RA
                report.drift_ra_arcsec = (
                    delta_ra_deg * 3600 * math.cos(math.radians(report.wcs.crval2))
                )
                report.drift_dec_arcsec = delta_dec_deg * 3600

            # Обновляем кэш
            self._last_coords[session_id] = (
                report.wcs.crval1,
                report.wcs.crval2,
                pixel_scale,
            )

        payload = {
            "session_id": session_id,
            "file_name": report.file_name,
            "report": report.model_dump(),
        }
        await event_bus.publish("FITS_HEADER_PARSED", payload)
        logger.debug(
            f"Scanned FITS: {report.file_name} | Moon: {report.moon_angl} | Sun: {report.sun_angle}"
        )


import math  # Импорт для расчета pixel_scale
