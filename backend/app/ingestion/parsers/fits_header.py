import logging
import math
from pathlib import Path
from typing import Optional
from pydantic import BaseModel
from astropy.io import fits

logger = logging.getLogger("FITSParser")


class WCSData(BaseModel):
    crval1: Optional[float] = None
    crval2: Optional[float] = None
    cd1_1: Optional[float] = None
    cd1_2: Optional[float] = None
    cd2_1: Optional[float] = None
    cd2_2: Optional[float] = None


class FITSHeaderReport(BaseModel):
    file_name: str
    wcs: Optional[WCSData] = None
    moon_angl: Optional[float] = None
    sun_angle: Optional[float] = None
    filter_name: Optional[str] = None
    exposure_time: Optional[float] = None
    temperature: Optional[float] = None


def parse_fits_header(file_path: Path) -> Optional[FITSHeaderReport]:
    """Читает только заголовки FITS-файла через astropy.io.fits"""
    try:
        # astropy.io.fits.getheader читает только заголовки, не загружая данные
        header = fits.getheader(str(file_path), ext=0)

        wcs = WCSData(
            crval1=header.get("CRVAL1"),
            crval2=header.get("CRVAL2"),
            cd1_1=header.get("CD1_1"),
            cd1_2=header.get("CD1_2"),
            cd2_1=header.get("CD2_1"),
            cd2_2=header.get("CD2_2"),
        )

        return FITSHeaderReport(
            file_name=file_path.name,
            wcs=wcs if wcs.crval1 is not None else None,
            moon_angl=header.get("MOONANGL"),
            sun_angle=header.get("SUNANGLE"),
            filter_name=header.get("FILTER"),
            exposure_time=header.get("EXPTIME"),
            temperature=header.get("CCD-TEMP") or header.get("TEMPERAT"),
        )
    except Exception as e:
        logger.error(f"Failed to read FITS header for {file_path.name}: {e}")
        return None
