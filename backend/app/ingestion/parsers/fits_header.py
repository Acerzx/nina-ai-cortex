import logging
import math
import fitsio
from pathlib import Path
from typing import Optional, Dict, Any
from pydantic import BaseModel

logger = logging.getLogger("FITSParser")


class WCSData(BaseModel):
    crval1: Optional[float] = None  # RA
    crval2: Optional[float] = None  # Dec
    cd1_1: Optional[float] = None
    cd1_2: Optional[float] = None
    cd2_1: Optional[float] = None
    cd2_2: Optional[float] = None


class FITSHeaderReport(BaseModel):
    file_name: str
    wcs: Optional[WCSData] = None
    moon_angl: Optional[float] = None  # Угловое расстояние до Луны
    sun_angle: Optional[float] = None  # Угловое расстояние до Солнца
    filter_name: Optional[str] = None
    exposure_time: Optional[float] = None
    temperature: Optional[float] = None

    # Дрейф относительно предыдущего кадра (в арксекундах)
    drift_ra_arcsec: Optional[float] = None
    drift_dec_arcsec: Optional[float] = None


def angular_separation(ra1, dec1, ra2, dec2):
    """Вычисляет угловое расстояние между двумя точками на сфере (в градусах)"""
    ra1, dec1, ra2, dec2 = map(math.radians, [ra1, dec1, ra2, dec2])
    diff_ra = ra2 - ra1
    diff_dec = dec2 - dec1

    a = (
        math.sin(diff_dec / 2) ** 2
        + math.cos(dec1) * math.cos(dec2) * math.sin(diff_ra / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return math.degrees(c)


def parse_fits_header(file_path: Path) -> Optional[FITSHeaderReport]:
    """Читает только заголовки FITS-файла через fitsio"""
    try:
        # read_header читает только заголовки, не загружая данные (очень быстро)
        header = fitsio.read_header(str(file_path))

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
            temperature=header.get("CCD-TEMP"),
        )
    except Exception as e:
        logger.error(f"Failed to read FITS header for {file_path.name}: {e}")
        return None
