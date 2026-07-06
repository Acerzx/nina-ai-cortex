from pydantic import BaseModel, Field, ValidationError
from typing import List, Optional
from datetime import datetime
import json
import logging

logger = logging.getLogger("SessionParser")


class ImageFrame(BaseModel):
    """Модель отдельного кадра из ImageMetaData.json"""

    index: int = Field(alias="Index")
    exposure_time: float = Field(alias="ExposureTime")
    filter_name: str = Field(alias="Filter")
    gain: int = Field(alias="Gain")
    offset: int = Field(alias="Offset")
    temperature: float = Field(alias="Temperature")
    hfr: Optional[float] = Field(alias="HFR", default=None)
    fwhm: Optional[float] = Field(alias="FWHM", default=None)
    stars: Optional[int] = Field(alias="Stars", default=None)
    rms_total: Optional[float] = Field(alias="RmsTotal", default=None)
    rms_ra: Optional[float] = Field(alias="RmsRA", default=None)
    rms_dec: Optional[float] = Field(alias="RmsDec", default=None)
    median: Optional[float] = Field(alias="Median", default=None)
    mean: Optional[float] = Field(alias="Mean", default=None)
    st_dev: Optional[float] = Field(alias="StDev", default=None)
    date: str = Field(alias="Date")
    time: str = Field(alias="Time")
    image_type: str = Field(alias="ImageType")  # LIGHT, FLAT, DARK, BIAS

    class Config:
        populate_by_name = True


class ImageMetaData(BaseModel):
    frames: List[ImageFrame] = Field(default_factory=list)

    @classmethod
    def from_json_file(cls, path: str) -> "ImageMetaData":
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
                # N.I.N.A. обычно сохраняет массив объектов напрямую или в ключе
                if isinstance(data, list):
                    return cls(frames=data)
                return cls(frames=data.get("Frames", data.get("Images", [])))
        except Exception as e:
            logger.error(f"Failed to parse ImageMetaData: {e}")
            return cls(frames=[])


class AcquisitionDetails(BaseModel):
    """Общая информация о сессии"""

    target_name: Optional[str] = Field(alias="TargetName", default=None)
    session_start: Optional[str] = Field(alias="SessionStart", default=None)
    telescope: Optional[str] = Field(alias="Telescope", default=None)
    camera: Optional[str] = Field(alias="Camera", default=None)

    class Config:
        populate_by_name = True

    @classmethod
    def from_json_file(cls, path: str) -> "AcquisitionDetails":
        try:
            with open(path, "r", encoding="utf-8") as f:
                return cls(**json.load(f))
        except Exception as e:
            logger.error(f"Failed to parse AcquisitionDetails: {e}")
            return cls()


class WeatherData(BaseModel):
    """Погодные условия (обновляются регулярно)"""

    temperature: Optional[float] = Field(alias="Temperature", default=None)
    humidity: Optional[float] = Field(alias="Humidity", default=None)
    dew_point: Optional[float] = Field(alias="DewPoint", default=None)
    cloud_cover: Optional[float] = Field(alias="CloudCover", default=None)
    wind_speed: Optional[float] = Field(alias="WindSpeed", default=None)
    wind_direction: Optional[float] = Field(alias="WindDirection", default=None)
    sky_quality: Optional[float] = Field(alias="SkyQuality", default=None)

    class Config:
        populate_by_name = True

    @classmethod
    def from_json_file(cls, path: str) -> "WeatherData":
        try:
            with open(path, "r", encoding="utf-8") as f:
                return cls(**json.load(f))
        except Exception as e:
            logger.error(f"Failed to parse WeatherData: {e}")
            return cls()
