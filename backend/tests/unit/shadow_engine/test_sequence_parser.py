"""
Unit tests for Sequence Parser.
Тестирует парсинг Sequence.json из N.I.N.A. Advanced Sequencer.
"""

import pytest
from pathlib import Path
import json
import tempfile

from app.shadow_engine.sequence_parser import SequenceParser


class TestSequenceParser:
    """Тесты Sequence Parser."""

    @pytest.fixture
    def parser(self):
        """Создаёт тестовый parser."""
        return SequenceParser()

    @pytest.fixture
    def simple_sequence(self):
        """Создаёт простую тестовую последовательность."""
        return {
            "$type": "NINA.Legacy.SequenceItem, NINA.Legacy",
            "Name": "Test Sequence",
            "Items": {
                "$values": [
                    {
                        "$type": "NINA.Legacy.Instructions.TakeExposure, NINA.Legacy",
                        "ImageType": "LIGHT",
                        "ExposureTime": 60.0,
                        "Gain": 85,
                        "Offset": 10,
                    },
                    {"$type": "NINA.Legacy.Instructions.RunAutofocus, NINA.Legacy"},
                ]
            },
        }

    @pytest.fixture
    def sequence_with_container(self):
        """Создаёт последовательность с контейнером."""
        return {
            "$type": "NINA.Legacy.SequenceItem, NINA.Legacy",
            "Name": "Test Sequence",
            "Items": {
                "$values": [
                    {
                        "$type": "NINA.Legacy.DeepSkyObjectContainer, NINA.Legacy",
                        "Name": "M31",
                        "Items": {
                            "$values": [
                                {
                                    "$type": "NINA.Legacy.Instructions.TakeExposure, NINA.Legacy",
                                    "ImageType": "LIGHT",
                                    "ExposureTime": 60.0,
                                }
                            ]
                        },
                    }
                ]
            },
        }

    def test_parse_simple_sequence(self, parser, simple_sequence, tmp_path):
        """Тест парсинга простой последовательности."""
        # Создаём временный файл
        seq_file = tmp_path / "sequence.json"
        with open(seq_file, "w", encoding="utf-8") as f:
            json.dump(simple_sequence, f)

        # Мокаем путь к файлу
        parser.sequence_path = seq_file

        # Парсим
        result = parser.parse()

        assert result is not None
        assert "graph" in result
        assert "global_variables" in result
        assert "stats" in result

        # Проверяем статистику
        stats = result["stats"]
        assert stats["total_instructions"] >= 2

    def test_parse_sequence_with_container(
        self, parser, sequence_with_container, tmp_path
    ):
        """Тест парсинга последовательности с контейнером."""
        seq_file = tmp_path / "sequence.json"
        with open(seq_file, "w", encoding="utf-8") as f:
            json.dump(sequence_with_container, f)

        parser.sequence_path = seq_file

        result = parser.parse()

        assert result is not None
        assert result["stats"]["total_containers"] >= 1

    def test_parse_nonexistent_file(self, parser):
        """Тест обработки несуществующего файла."""
        parser.sequence_path = Path("/nonexistent/sequence.json")

        result = parser.parse()

        assert result == {}

    def test_parse_invalid_json(self, parser, tmp_path):
        """Тест обработки невалидного JSON."""
        seq_file = tmp_path / "invalid.json"
        with open(seq_file, "w", encoding="utf-8") as f:
            f.write("{ invalid json }")

        parser.sequence_path = seq_file

        result = parser.parse()

        assert result == {}

    def test_extract_global_variables(self, parser, tmp_path):
        """Тест извлечения глобальных переменных."""
        sequence = {
            "$type": "NINA.Legacy.SequenceItem, NINA.Legacy",
            "Name": "Test",
            "GlobalVariables": [
                {
                    "$type": "NINA.Legacy.GlobalVariable, NINA.Legacy",
                    "Identifier": "EXPOSURE_TIME",
                    "OriginalDefinition": "60.0",
                },
                {
                    "$type": "NINA.Legacy.GlobalVariable, NINA.Legacy",
                    "Identifier": "FILTER",
                    "OriginalDefinition": "Ha",
                },
            ],
        }

        seq_file = tmp_path / "sequence.json"
        with open(seq_file, "w", encoding="utf-8") as f:
            json.dump(sequence, f)

        parser.sequence_path = seq_file

        result = parser.parse()

        assert "global_variables" in result
        global_vars = result["global_variables"]
        assert "EXPOSURE_TIME" in global_vars
        assert global_vars["EXPOSURE_TIME"] == "60.0"
        assert "FILTER" in global_vars
        assert global_vars["FILTER"] == "Ha"

    def test_clean_type(self, parser):
        """Тест извлечения чистого имени типа."""
        full_type = "NINA.Legacy.Instructions.TakeExposure, NINA.Legacy"
        clean = parser._clean_type(full_type)

        assert clean == "TakeExposure"

    def test_to_snake_case(self, parser):
        """Тест преобразования CamelCase в snake_case."""
        assert parser._to_snake_case("ExposureTime") == "exposure_time"
        assert parser._to_snake_case("HFR") == "h_f_r"
        assert parser._to_snake_case("RmsTotal") == "rms_total"

    def test_build_id_map(self, parser):
        """Тест построения карты ID."""
        sequence = {
            "$id": "root",
            "items": [
                {"$id": "item1", "name": "First"},
                {"$id": "item2", "name": "Second"},
            ],
        }

        parser._build_id_map(sequence)

        assert "root" in parser.id_map
        assert "item1" in parser.id_map
        assert "item2" in parser.id_map
        assert parser.id_map["item1"]["name"] == "First"

    def test_resolve_ref(self, parser):
        """Тест разрешения $ref ссылки."""
        # Настраиваем id_map
        parser.id_map = {"ref123": {"name": "Referenced Item", "value": 42}}

        # Тестируем разрешение
        ref_node = {"$ref": "ref123"}
        resolved = parser._resolve_ref(ref_node)

        assert resolved is not None
        assert resolved["name"] == "Referenced Item"
        assert resolved["value"] == 42

    def test_resolve_nonexistent_ref(self, parser):
        """Тест разрешения несуществующей $ref ссылки."""
        parser.id_map = {}

        ref_node = {"$ref": "nonexistent"}
        resolved = parser._resolve_ref(ref_node)

        assert resolved is None

    def test_get_expr(self, parser):
        """Тест извлечения Expression."""
        node = {"ExposureTimeExpression": {"Definition": "EXPOSURE_TIME * 2"}}

        expr = parser._get_expr(node, "ExposureTimeExpression")

        assert expr == "EXPOSURE_TIME * 2"

    def test_parse_instruction(self, parser):
        """Тест парсинга инструкции."""
        instruction = {
            "$id": "instr1",
            "$type": "NINA.Legacy.Instructions.TakeExposure, NINA.Legacy",
            "ImageType": "LIGHT",
            "ExposureTime": 60.0,
            "Gain": 85,
        }

        result = parser._parse_instruction(instruction)

        assert result is not None
        assert result["type"] == "TakeExposure"
        assert result["image_type"] == "LIGHT"
        assert result["exposure_time"] == 60.0
        assert result["gain"] == 85

    def test_parse_container(self, parser):
        """Тест парсинга контейнера."""
        container = {
            "$id": "container1",
            "$type": "NINA.Legacy.DeepSkyObjectContainer, NINA.Legacy",
            "Name": "M31",
            "Items": {"$values": []},
        }

        result = parser._parse_container(container)

        assert result is not None
        assert result["type"] == "DeepSkyObjectContainer"
        assert result["name"] == "M31"

    def test_stats_calculation(self, parser, tmp_path):
        """Тест расчёта статистики."""
        sequence = {
            "$type": "NINA.Legacy.SequenceItem, NINA.Legacy",
            "Name": "Test",
            "Items": {
                "$values": [
                    {
                        "$type": "NINA.Legacy.DeepSkyObjectContainer, NINA.Legacy",
                        "Name": "Container1",
                        "Items": {
                            "$values": [
                                {
                                    "$type": "NINA.Legacy.Instructions.TakeExposure, NINA.Legacy",
                                    "ImageType": "LIGHT",
                                },
                                {
                                    "$type": "NINA.Legacy.Instructions.RunAutofocus, NINA.Legacy"
                                },
                            ]
                        },
                    }
                ]
            },
        }

        seq_file = tmp_path / "sequence.json"
        with open(seq_file, "w", encoding="utf-8") as f:
            json.dump(sequence, f)

        parser.sequence_path = seq_file

        result = parser.parse()

        stats = result["stats"]
        assert stats["total_containers"] >= 1
        assert stats["total_instructions"] >= 2
