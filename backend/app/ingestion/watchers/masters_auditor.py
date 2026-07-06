import logging
import asyncio
import fitsio
from pathlib import Path
from typing import Dict, List, Any
from app.core.config import settings
from app.core.events import event_bus

logger = logging.getLogger("MastersAuditor")


class MastersLibraryAuditor:
    """
    Сканирует библиотеку мастер-кадров (Bias, Dark, Flat).
    Устраняет Упрощение #11.
    """

    def __init__(self):
        self.masters_root = settings.nina_environment.masters_root
        self.catalog: Dict[str, List[Dict[str, Any]]] = {
            "BIAS": [],
            "DARK": [],
            "FLAT": [],
        }

    async def scan_library(self):
        """Запускает полное сканирование библиотеки при старте Cortex."""
        if not self.masters_root.exists():
            logger.warning(f"Masters root does not exist: {self.masters_root}")
            return

        logger.info(f"Starting Masters Library audit at {self.masters_root}")
        # Асинхронный обход через run_in_executor, чтобы не блокировать event loop
        loop = asyncio.get_running_loop()
        fits_files = await loop.run_in_executor(
            None,
            lambda: (
                list(self.masters_root.rglob("*.fit"))
                + list(self.masters_root.rglob("*.fits"))
            ),
        )

        for fit_path in fits_files:
            try:
                header = await loop.run_in_executor(
                    None, fitsio.read_header, str(fit_path)
                )
                img_type = header.get("IMAGETYP", "UNKNOWN").strip().upper()

                if "BIAS" in img_type:
                    category = "BIAS"
                elif "DARK" in img_type:
                    category = "DARK"
                elif "FLAT" in img_type:
                    category = "FLAT"
                else:
                    continue

                metadata = {
                    "path": str(fit_path),
                    "temperature": header.get("CCD-TEMP") or header.get("TEMPERAT"),
                    "exposure": header.get("EXPTIME"),
                    "filter": header.get("FILTER"),
                    "gain": header.get("GAIN"),
                    "offset": header.get("OFFSET"),
                    "mean_adu": header.get("MEAN"),
                    "date": header.get("DATE-OBS"),
                }
                self.catalog[category].append(metadata)
            except Exception as e:
                logger.debug(f"Failed to read FITS header {fit_path.name}: {e}")

        logger.info(
            f"Masters audit complete: "
            f"{len(self.catalog['BIAS'])} Bias, "
            f"{len(self.catalog['DARK'])} Darks, "
            f"{len(self.catalog['FLAT'])} Flats"
        )

        await event_bus.publish("MASTERS_INDEXED", self.catalog)
