"""
Unit tests for Hocus Focus parser.
"""

import pytest
from pathlib import Path
from app.ingestion.parsers.hocus_focus import (
    parse_hocus_focus_csv,
    filter_anomalies,
    StarData,
)


class TestHocusFocusParser:
    """Тесты парсера Hocus Focus CSV."""

    def test_parse_valid_csv(self, fixture_path: Path):
        """Тест парсинга валидного CSV файла."""
        csv_path = fixture_path / "hocus_focus" / "valid.csv"
        stars = parse_hocus_focus_csv(csv_path)

        assert len(stars) == 10
        assert all(isinstance(s, StarData) for s in stars)
        assert all(s.fwhm is not None for s in stars)
        assert all(s.hfr is not None for s in stars)
        assert all(0 < s.fwhm < 10 for s in stars if s.fwhm is not None)

    def test_parse_empty_csv(self, fixture_path: Path):
        """Тест обработки пустого CSV файла."""
        csv_path = fixture_path / "hocus_focus" / "empty.csv"
        stars = parse_hocus_focus_csv(csv_path)

        assert len(stars) == 0

    def test_parse_nonexistent_file(self):
        """Тест обработки несуществующего файла."""
        csv_path = Path("/nonexistent/path/file.csv")
        stars = parse_hocus_focus_csv(csv_path)

        assert len(stars) == 0

    def test_filter_anomalies_z_score(self):
        """Тест Z-Score фильтрации аномалий."""
        stars = [
            StarData(X=0, Y=0, FWHM=2.0, HFR=1.5, Eccentricity=0.3),
            StarData(X=1, Y=1, FWHM=2.1, HFR=1.6, Eccentricity=0.35),
            StarData(X=2, Y=2, FWHM=2.2, HFR=1.7, Eccentricity=0.32),
            StarData(X=3, Y=3, FWHM=15.0, HFR=10.0, Eccentricity=0.9),  # Аномалия
        ]

        report = filter_anomalies(stars, z_threshold=3.0)

        assert report.total_stars_detected == 4
        assert report.anomalies_count == 1
        assert report.valid_stars_count == 3
        assert report.median_fwhm is not None
        assert report.median_fwhm < 3.0  # Медиана не должна быть искажена аномалией

    def test_filter_anomalies_eccentricity(self):
        """Тест фильтрации по экстремальному эксцентриситету."""
        stars = [
            StarData(X=0, Y=0, FWHM=2.0, HFR=1.5, Eccentricity=0.3),
            StarData(
                X=1, Y=1, FWHM=2.1, HFR=1.6, Eccentricity=0.9
            ),  # Высокий эксцентриситет
        ]

        report = filter_anomalies(stars, z_threshold=3.0)

        assert report.anomalies_count == 1
        assert report.valid_stars_count == 1
        # Проверяем, что аномальная звезда помечена
        anomaly = [s for s in stars if s.is_anomaly][0]
        assert "Eccentricity" in anomaly.anomaly_reason

    def test_filter_anomalies_empty_list(self):
        """Тест фильтрации пустого списка."""
        stars = []
        report = filter_anomalies(stars)

        assert report.total_stars_detected == 0
        assert report.valid_stars_count == 0
        assert report.anomalies_count == 0
        assert report.median_fwhm is None

    def test_filter_anomalies_small_sample(self):
        """Тест фильтрации с малой выборкой (< 3 звезд)."""
        stars = [
            StarData(X=0, Y=0, FWHM=2.0, HFR=1.5, Eccentricity=0.3),
            StarData(X=1, Y=1, FWHM=2.1, HFR=1.6, Eccentricity=0.35),
        ]

        report = filter_anomalies(stars, z_threshold=3.0)

        # При малой выборке Z-Score не применяется
        assert report.valid_stars_count == 2
        assert report.anomalies_count == 0

    def test_percentiles_calculation(self):
        """Тест расчета процентилей."""
        stars = [
            StarData(X=i, Y=i, FWHM=2.0 + i * 0.1, HFR=1.5 + i * 0.1, Eccentricity=0.3)
            for i in range(10)
        ]

        report = filter_anomalies(stars)

        assert report.fwhm_25th is not None
        assert report.fwhm_75th is not None
        assert report.fwhm_25th < report.median_fwhm < report.fwhm_75th
