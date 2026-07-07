"""
Providers — поставщики данных из внешних источников.
"""

from app.ingestion.providers.influxdb_metrics import (
    influxdb_metrics_provider,
    InfluxDBMetricsProvider,
)

__all__ = [
    "influxdb_metrics_provider",
    "InfluxDBMetricsProvider",
]
