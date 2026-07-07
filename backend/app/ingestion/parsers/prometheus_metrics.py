"""
Prometheus Metrics Parser
Парсит метрики N.I.N.A. из Prometheus text format (jewzaam плагин).

Обновлено под РЕАЛЬНЫЕ имена метрик из prometheus_sample.txt.
Поддерживает labels (profile_name, host_name, type).
"""

import re
import logging
from typing import Dict, Any, Optional, List
from pydantic import BaseModel

logger = logging.getLogger("PrometheusParser")


class ObservatoryMetrics(BaseModel):
    """Полный срез метрик обсерватории из Prometheus (jewzaam формат)."""

    # === Camera ===
    camera_temp: Optional[float] = None
    camera_cooler_power: Optional[float] = None
    camera_download_timeout_total: Optional[int] = None

    # === Focuser ===
    focuser_temp: Optional[float] = None
    focuser_position: Optional[float] = None
    focuser_temp_comp: bool = False
    focuser_moving: bool = False

    # === Guider (PHD2) ===
    guider_guiding: bool = False
    guider_rms_ra: Optional[float] = None
    guider_rms_dec: Optional[float] = None
    guider_rms_total: Optional[float] = None
    guider_peak_ra: Optional[float] = None
    guider_peak_dec: Optional[float] = None
    guider_dithers_total: Optional[int] = None

    # === Mount ===
    mount_altitude: Optional[float] = None
    mount_azimuth: Optional[float] = None
    mount_ra_hours: Optional[float] = None
    mount_dec: Optional[float] = None
    mount_side_of_pier: Optional[int] = None  # 0=Unknown, 1=East, 2=West
    mount_tracking: bool = False
    mount_slewing: bool = False
    mount_parked: bool = False
    mount_meridian_flip: bool = False

    # === Rotator ===
    rotator_position: Optional[float] = None
    rotator_mechanical_position: Optional[float] = None
    rotator_moving: bool = False
    rotator_synced: bool = False

    # === Filter Wheel ===
    filter_position: Optional[int] = None
    filter_current: Optional[str] = None  # Имя текущего фильтра (из labels)

    # === Weather (nina_weather_*) ===
    wx_temp: Optional[float] = None
    wx_humidity: Optional[float] = None
    wx_dewpoint: Optional[float] = None
    wx_cloud_cover: Optional[float] = None
    wx_wind_speed: Optional[float] = None
    wx_wind_gust: Optional[float] = None
    wx_wind_direction: Optional[float] = None
    wx_pressure: Optional[float] = None
    wx_sky_quality: Optional[float] = None
    wx_rain_rate: Optional[float] = None

    # === Image Quality (from ImageSaved) ===
    image_hfr: Optional[float] = None
    image_stars: Optional[float] = None
    image_rms: Optional[float] = None
    image_camera_temp: Optional[float] = None
    image_mean: Optional[float] = None
    image_median: Optional[float] = None
    image_stdev: Optional[float] = None
    image_mad: Optional[float] = None
    image_min_adu: Optional[float] = None
    image_max_adu: Optional[float] = None
    image_hfr_stdev: Optional[float] = None

    # === Exposure Stats ===
    exposure_total: Optional[int] = None
    last_exposure_time: Optional[float] = None
    last_exposure_filter: Optional[str] = None
    last_exposure_gain: Optional[int] = None
    last_exposure_offset: Optional[int] = None
    last_exposure_binning: Optional[str] = None

    # === Autofocus ===
    autofocus_running: bool = False
    autofocus_success_total: Optional[int] = None
    autofocus_failure_total: Optional[int] = None
    autofocus_final_hfr: Optional[float] = None
    autofocus_duration_seconds: Optional[float] = None
    autofocus_initial_position: Optional[float] = None
    autofocus_calculated_position: Optional[float] = None
    autofocus_initial_hfr: Optional[float] = None
    autofocus_calculated_hfr: Optional[float] = None

    # === Sequence Status (nina_status с labels) ===
    sequence_running: bool = False
    sequence_item_name: Optional[str] = None
    sequence_category: Optional[str] = None
    sequence_started_total: Optional[int] = None
    sequence_completed_total: Optional[int] = None

    # === Equipment Connection (nina_equipment с labels) ===
    equipment_camera: bool = False
    equipment_mount: bool = False
    equipment_focuser: bool = False
    equipment_filterwheel: bool = False
    equipment_guider: bool = False
    equipment_dome: bool = False
    equipment_rotator: bool = False
    equipment_flat_device: bool = False
    equipment_safety_monitor: bool = False
    equipment_weather: bool = False
    equipment_switch: bool = False

    # === Flat Panel ===
    flat_brightness: Optional[float] = None
    flat_light_on: bool = False
    flat_cover_open: bool = False

    # === Safety ===
    safety_is_safe: Optional[bool] = None

    # === Dome ===
    dome_azimuth: Optional[float] = None
    dome_slewing: bool = False
    dome_at_park: bool = False
    dome_shutter_open: bool = False

    # === Metadata ===
    profile_name: Optional[str] = None
    host_name: Optional[str] = None


def _parse_labels(labels_str: str) -> Dict[str, str]:
    """
    Парсит labels в формате {key1="value1",key2="value2"} в словарь.
    """
    if not labels_str:
        return {}

    # Убираем внешние скобки
    labels_str = labels_str.strip("{}")
    if not labels_str:
        return {}

    # Regex для извлечения key="value" пар
    pattern = re.compile(r'(\w+)="([^"]*)"')
    return dict(pattern.findall(labels_str))


def parse_prometheus_text(text: str) -> ObservatoryMetrics:
    """
    Парсит Prometheus text format от плагина jewzaam.

    Формат:
        # HELP metric_name Description
        # TYPE metric_name gauge|counter
        metric_name{label1="value1",label2="value2"} value [timestamp]
    """
    metrics = ObservatoryMetrics()

    # Regex: имя метрики, опциональные labels, значение
    pattern = re.compile(
        r"^([a-zA-Z_][a-zA-Z0-9_:]*)"  # имя метрики
        r"(\{[^}]*\})?"  # опциональные labels
        r"\s+"
        r"([0-9eE\.\+\-]+|NaN|[+-]?Inf)",  # значение
        re.MULTILINE,
    )

    for match in pattern.finditer(text):
        name = match.group(1)
        labels_str = match.group(2) or ""
        value_str = match.group(3)

        # Пропускаем служебные значения
        if value_str in ("NaN", "+Inf", "-Inf"):
            continue

        try:
            value = float(value_str)
        except ValueError:
            continue

        # Парсим labels
        labels = _parse_labels(labels_str)

        # Извлекаем profile_name и host_name из первой метрики
        if metrics.profile_name is None and "profile_name" in labels:
            metrics.profile_name = labels["profile_name"]
        if metrics.host_name is None and "host_name" in labels:
            metrics.host_name = labels["host_name"]

        # === Маппинг под РЕАЛЬНЫЕ имена jewzaam (из prometheus_sample.txt) ===

        # --- Camera ---
        if name == "nina_camera_temperature_celsius":
            metrics.camera_temp = value
        elif name == "nina_camera_cooler_power_percent":
            metrics.camera_cooler_power = value
        elif name == "nina_camera_download_timeout_total":
            metrics.camera_download_timeout_total = int(value)

        # --- Focuser ---
        elif name == "nina_focuser_temperature_celsius":
            metrics.focuser_temp = value
        elif name == "nina_focuser_position":  # БЕЗ _steps!
            metrics.focuser_position = value
        elif name == "nina_focuser_temp_comp":
            metrics.focuser_temp_comp = bool(value)
        elif name == "nina_focuser_moving":
            metrics.focuser_moving = bool(value)

        # --- Guider ---
        elif name == "nina_guider_guiding":
            metrics.guider_guiding = bool(value)
        elif name == "nina_guider_rms_ra_arcsec":
            metrics.guider_rms_ra = value
        elif name == "nina_guider_rms_dec_arcsec":
            metrics.guider_rms_dec = value
        elif name == "nina_guider_rms_total_arcsec":
            metrics.guider_rms_total = value
        elif name == "nina_guider_peak_ra_arcsec":
            metrics.guider_peak_ra = value
        elif name == "nina_guider_peak_dec_arcsec":
            metrics.guider_peak_dec = value
        elif name == "nina_guider_dithers_total":
            metrics.guider_dithers_total = int(value)

        # --- Mount ---
        elif name == "nina_mount_altitude_degrees":
            metrics.mount_altitude = value
        elif name == "nina_mount_azimuth_degrees":
            metrics.mount_azimuth = value
        elif name == "nina_mount_ra_hours":
            metrics.mount_ra_hours = value
        elif name == "nina_mount_dec_degrees":
            metrics.mount_dec = value
        elif name == "nina_mount_side_of_pier":
            metrics.mount_side_of_pier = int(value)
        elif name == "nina_mount_tracking":
            metrics.mount_tracking = bool(value)
        elif name == "nina_mount_slewing":
            metrics.mount_slewing = bool(value)
        elif name == "nina_mount_parked":
            metrics.mount_parked = bool(value)
        elif name == "nina_mount_meridian_flip":
            metrics.mount_meridian_flip = bool(value)

        # --- Rotator ---
        elif name == "nina_rotator_position_degrees":
            metrics.rotator_position = value
        elif name == "nina_rotator_mechanical_position_degrees":
            metrics.rotator_mechanical_position = value
        elif name == "nina_rotator_moving":
            metrics.rotator_moving = bool(value)
        elif name == "nina_rotator_synced":
            metrics.rotator_synced = bool(value)

        # --- Filter Wheel ---
        elif name == "nina_filter_position":
            metrics.filter_position = int(value)
        elif name == "nina_filter_current":
            # filter_name передается как label, value = 1 для текущего фильтра
            if "filter_name" in labels and value == 1.0:
                metrics.filter_current = labels["filter_name"]

        # --- Weather (префикс nina_weather_, НЕ nina_wx_!) ---
        elif name == "nina_weather_temperature_celsius":
            metrics.wx_temp = value
        elif name == "nina_weather_humidity_percent":
            metrics.wx_humidity = value
        elif name == "nina_weather_dew_point_celsius":
            metrics.wx_dewpoint = value
        elif name == "nina_weather_cloud_cover_percent":
            metrics.wx_cloud_cover = value
        elif name == "nina_weather_wind_speed_mps":  # mps, не meters_per_second!
            metrics.wx_wind_speed = value
        elif name == "nina_weather_wind_gust_mps":
            metrics.wx_wind_gust = value
        elif name == "nina_weather_wind_direction_degrees":
            metrics.wx_wind_direction = value
        elif name == "nina_weather_pressure_hpa":
            metrics.wx_pressure = value
        elif name == "nina_weather_sky_quality_mpsas":
            metrics.wx_sky_quality = value
        elif name == "nina_weather_rain_rate_mmh":
            metrics.wx_rain_rate = value

        # --- Image Quality (from ImageSaved) ---
        elif name == "nina_detect_hfr":
            metrics.image_hfr = value
        elif name == "nina_detect_stars":
            metrics.image_stars = value
        elif name == "nina_detect_rms_arcsec":
            metrics.image_rms = value
        elif name == "nina_detect_camera_temperature_celsius":
            metrics.image_camera_temp = value
        elif name == "nina_image_mean":
            metrics.image_mean = value
        elif name == "nina_image_median":
            metrics.image_median = value
        elif name == "nina_image_stdev":
            metrics.image_stdev = value
        elif name == "nina_image_mad":
            metrics.image_mad = value
        elif name == "nina_image_min_adu":
            metrics.image_min_adu = value
        elif name == "nina_image_max_adu":
            metrics.image_max_adu = value
        elif name == "nina_image_hfr_stdev":
            metrics.image_hfr_stdev = value

        # --- Exposure Stats (labels: exposure_time_s, filter, gain, offset, binning) ---
        elif name == "nina_exposure_total":
            metrics.exposure_total = (metrics.exposure_total or 0) + int(value)
            # Сохраняем параметры последней экспозиции из labels
            if "exposure_time_s" in labels:
                metrics.last_exposure_time = float(labels["exposure_time_s"])
            if "filter" in labels:
                metrics.last_exposure_filter = labels["filter"]
            if "gain" in labels:
                metrics.last_exposure_gain = int(labels["gain"])
            if "offset" in labels:
                metrics.last_exposure_offset = int(labels["offset"])
            if "binning" in labels:
                metrics.last_exposure_binning = labels["binning"]

        # --- Autofocus ---
        elif name == "nina_autofocus_running":
            metrics.autofocus_running = bool(value)
        elif name == "nina_autofocus_success_total":
            metrics.autofocus_success_total = int(value)
        elif name == "nina_autofocus_failure_total":
            metrics.autofocus_failure_total = int(value)
        elif name == "nina_autofocus_final_hfr":
            metrics.autofocus_final_hfr = value
        elif name == "nina_autofocus_duration_seconds":
            metrics.autofocus_duration_seconds = value
        elif name == "nina_autofocus_initial_position":
            metrics.autofocus_initial_position = value
        elif name == "nina_autofocus_calculated_position":
            metrics.autofocus_calculated_position = value
        elif name == "nina_autofocus_initial_hfr":
            metrics.autofocus_initial_hfr = value
        elif name == "nina_autofocus_calculated_hfr":
            metrics.autofocus_calculated_hfr = value

        # --- Sequence Status (nina_status с labels category, item) ---
        elif name == "nina_status":
            # nina_status{category="...", item="..."} = 1 when active
            if value == 1.0:
                metrics.sequence_running = True
                if "category" in labels:
                    metrics.sequence_category = labels["category"]
                if "item" in labels:
                    metrics.sequence_item_name = labels["item"]
        elif name == "nina_status_count_started_total":
            metrics.sequence_started_total = int(value)
        elif name == "nina_status_count_completed_total":
            metrics.sequence_completed_total = int(value)

        # --- Equipment Connection (nina_equipment{type="..."}) ---
        elif name == "nina_equipment":
            eq_type = labels.get("type", "")
            connected = bool(value)
            if eq_type == "camera":
                metrics.equipment_camera = connected
            elif eq_type == "telescope":  # telescope = mount
                metrics.equipment_mount = connected
            elif eq_type == "focuser":
                metrics.equipment_focuser = connected
            elif eq_type == "filterwheel":
                metrics.equipment_filterwheel = connected
            elif eq_type == "guider":
                metrics.equipment_guider = connected
            elif eq_type == "dome":
                metrics.equipment_dome = connected
            elif eq_type == "rotator":
                metrics.equipment_rotator = connected
            elif eq_type == "flat_device":
                metrics.equipment_flat_device = connected
            elif eq_type == "safety_monitor":
                metrics.equipment_safety_monitor = connected
            elif eq_type == "weather":
                metrics.equipment_weather = connected
            elif eq_type == "switch":
                metrics.equipment_switch = connected

        # --- Flat Panel ---
        elif name == "nina_flat_brightness":
            metrics.flat_brightness = value
        elif name == "nina_flat_light_on":
            metrics.flat_light_on = bool(value)
        elif name == "nina_flat_cover_open":
            metrics.flat_cover_open = bool(value)

        # --- Safety ---
        elif name == "nina_safety_is_safe":
            metrics.safety_is_safe = bool(value)

        # --- Dome ---
        elif name == "nina_dome_azimuth_degrees":
            metrics.dome_azimuth = value
        elif name == "nina_dome_slewing":
            metrics.dome_slewing = bool(value)
        elif name == "nina_dome_at_park":
            metrics.dome_at_park = bool(value)
        elif name == "nina_dome_shutter_open":
            metrics.dome_shutter_open = bool(value)

    return metrics
