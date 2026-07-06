import re
import logging
from typing import Dict, Any, Optional
from pydantic import BaseModel

logger = logging.getLogger("PrometheusParser")


class ObservatoryMetrics(BaseModel):
    """Полный срез метрик обсерватории из Prometheus"""

    # Camera
    camera_temp: Optional[float] = None
    camera_cooler_power: Optional[float] = None

    # Focuser
    focuser_temp: Optional[float] = None
    focuser_position: Optional[float] = None

    # Guider (КРИТИЧНО для детекции дрейфа вместо собственных расчетов!)
    guider_rms_ra: Optional[float] = None
    guider_rms_dec: Optional[float] = None
    guider_rms_total: Optional[float] = None

    # Mount
    mount_altitude: Optional[float] = None
    mount_azimuth: Optional[float] = None
    mount_slew_active: bool = False
    mount_tracking: bool = False

    # Rotator
    rotator_angle: Optional[float] = None

    # Weather (wx_*)
    wx_temp: Optional[float] = None
    wx_humidity: Optional[float] = None
    wx_dewpoint: Optional[float] = None
    wx_cloud_cover: Optional[float] = None
    wx_wind_speed: Optional[float] = None
    wx_wind_gust: Optional[float] = None
    wx_wind_direction: Optional[float] = None
    wx_sky_quality: Optional[float] = None
    wx_pressure: Optional[float] = None

    # Image Quality (from last solved image)
    image_hfr: Optional[float] = None
    image_fwhm: Optional[float] = None
    image_eccentricity: Optional[float] = None
    image_star_count: Optional[float] = None
    image_median_adu: Optional[float] = None

    # Astrometry
    astro_moon_altitude: Optional[float] = None
    astro_sun_altitude: Optional[float] = None
    astro_moon_angle: Optional[float] = None
    astro_sun_angle: Optional[float] = None

    # Sequence state
    sequence_running: bool = False
    sequence_item_name: Optional[str] = None


def parse_prometheus_text(text: str) -> ObservatoryMetrics:
    """
    Парсит Prometheus text format.
    Формат: metric_name{label="value"} 123.45 1234567890
    """
    metrics = ObservatoryMetrics()

    # Regex для извлечения имени метрики и значения (игнорируем labels и timestamps для простоты)
    # Улучшенный regex: захватывает имя до { или пробела, и последнее число
    pattern = re.compile(
        r"^([a-zA-Z0-9_:]+)(?:\{[^}]*\})?\s+([0-9eE\.\+\-NaN]+)", re.MULTILINE
    )

    for match in pattern.finditer(text):
        name = match.group(1)
        try:
            value_str = match.group(2)
            if value_str in ("NaN", "+Inf", "-Inf"):
                continue
            value = float(value_str)
        except ValueError:
            continue

        # Маппинг метрик Prometheus на нашу модель
        # Camera
        if name == "nina_camera_temperature_celsius":
            metrics.camera_temp = value
        elif name == "nina_camera_cooler_power_percent":
            metrics.camera_cooler_power = value
        # Focuser
        elif name == "nina_focuser_temperature_celsius":
            metrics.focuser_temp = value
        elif name == "nina_focuser_position_steps":
            metrics.focuser_position = value
        # Guider
        elif name == "nina_guider_rms_arcsec":
            metrics.guider_rms_total = value
        elif name == "nina_guider_rms_ra_arcsec":
            metrics.guider_rms_ra = value
        elif name == "nina_guider_rms_dec_arcsec":
            metrics.guider_rms_dec = value
        # Mount
        elif name == "nina_mount_altitude_degrees":
            metrics.mount_altitude = value
        elif name == "nina_mount_azimuth_degrees":
            metrics.mount_azimuth = value
        elif name == "nina_mount_slew_active":
            metrics.mount_slew_active = bool(value)
        elif name == "nina_mount_tracking_active":
            metrics.mount_tracking = bool(value)
        # Rotator
        elif name == "nina_rotator_mechanical_angle_degrees":
            metrics.rotator_angle = value
        # Weather
        elif name == "nina_wx_temperature_celsius":
            metrics.wx_temp = value
        elif name == "nina_wx_humidity_percent":
            metrics.wx_humidity = value
        elif name == "nina_wx_dewpoint_celsius":
            metrics.wx_dewpoint = value
        elif name == "nina_wx_cloud_cover_percent":
            metrics.wx_cloud_cover = value
        elif name == "nina_wx_wind_speed_meters_per_second":
            metrics.wx_wind_speed = value
        elif name == "nina_wx_wind_gust_meters_per_second":
            metrics.wx_wind_gust = value
        elif name == "nina_wx_wind_direction_degrees":
            metrics.wx_wind_direction = value
        elif name == "nina_wx_sky_quality_magnitudes_per_arcsec2":
            metrics.wx_sky_quality = value
        elif name == "nina_wx_pressure_hpa":
            metrics.wx_pressure = value
        # Image
        elif name == "nina_image_hfr_pixels":
            metrics.image_hfr = value
        elif name == "nina_image_fwhm_pixels":
            metrics.image_fwhm = value
        elif name == "nina_image_eccentricity":
            metrics.image_eccentricity = value
        elif name == "nina_image_star_count":
            metrics.image_star_count = value
        elif name == "nina_image_median_adu":
            metrics.image_median_adu = value
        # Astrometry
        elif name == "nina_astro_moon_altitude_degrees":
            metrics.astro_moon_altitude = value
        elif name == "nina_astro_sun_altitude_degrees":
            metrics.astro_sun_altitude = value
        # Sequence
        elif name == "nina_sequence_running":
            metrics.sequence_running = bool(value)

    return metrics
