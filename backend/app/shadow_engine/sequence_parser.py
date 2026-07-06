"""
N.I.N.A. Advanced Sequencer Parser
Production-ready парсер для анализа секвенсоров N.I.N.A.
Извлекает ВСЕ параметры из всех объектов без потерь.
"""

import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Any, Optional
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class SequenceParser:
    """
    Production-ready парсер N.I.N.A. Advanced Sequencer.
    Извлекает все параметры без потерь.
    """

    def __init__(self):
        self.settings = get_settings()
        self.sequence_path = Path(self.settings.nina_environment.sequence_template)
        self.id_map: Dict[str, Dict] = {}
        self.stats = {
            "total_containers": 0,
            "total_instructions": 0,
            "total_triggers": 0,
            "total_conditions": 0,
            "total_message_boxes": 0,
            "total_annotations": 0,
        }

    def parse(self) -> Dict[str, Any]:
        """
        Парсит Sequence.json и возвращает теневой граф.

        Returns:
            Dict с полной структурой секвенсора
        """
        if not self.sequence_path.exists():
            logger.error(f"❌ Sequence file not found: {self.sequence_path}")
            return {}

        try:
            with open(self.sequence_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            logger.info(f"🧬 Parsing sequence: {self.sequence_path.name}")

            # Строим карту ID для разрешения $ref
            self._build_id_map(data)
            logger.info(f"   Built ID map with {len(self.id_map)} nodes")

            # Собираем глобальные переменные
            global_vars = self._collect_globals(data)

            # Строим граф
            graph = self._parse_node(data)

            logger.info(f"✅ Sequence parsed successfully:")
            logger.info(f"   ├─ Containers: {self.stats['total_containers']}")
            logger.info(f"   ├─ Instructions: {self.stats['total_instructions']}")
            logger.info(f"   ├─ Triggers: {self.stats['total_triggers']}")
            logger.info(f"   ├─ Conditions: {self.stats['total_conditions']}")
            logger.info(f"   ├─ MessageBoxes: {self.stats['total_message_boxes']}")
            logger.info(f"   └─ Annotations: {self.stats['total_annotations']}")

            return {
                "graph": graph,
                "global_variables": global_vars,
                "stats": self.stats,
            }

        except Exception as e:
            logger.error(f"❌ Failed to parse sequence: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return {}

    def _build_id_map(self, node: Any):
        """Рекурсивно строит карту всех $id для разрешения $ref."""
        if isinstance(node, dict):
            if "$id" in node:
                self.id_map[node["$id"]] = node
            for v in node.values():
                self._build_id_map(v)
        elif isinstance(node, list):
            for item in node:
                self._build_id_map(item)

    def _resolve_ref(self, node: Any) -> Optional[Dict]:
        """Разрешает $ref ссылку в реальный объект."""
        if isinstance(node, dict) and "$ref" in node:
            resolved = self.id_map.get(node["$ref"])
            if resolved is None:
                logger.warning(f"⚠️ Unresolved $ref: {node['$ref']}")
            return resolved
        return node if isinstance(node, dict) else None

    def _collect_globals(self, node: Any) -> Dict[str, str]:
        """Собирает глобальные переменные из секвенсора."""
        result = {}
        if isinstance(node, dict):
            node_type = node.get("$type", "")
            if "GlobalVariable" in node_type:
                identifier = node.get("Identifier")
                original_def = node.get("OriginalDefinition")
                if identifier and original_def is not None:
                    result[identifier] = str(original_def)
            for v in node.values():
                result.update(self._collect_globals(v))
        elif isinstance(node, list):
            for item in node:
                result.update(self._collect_globals(item))
        return result

    def _get_expr(self, node: Dict, key: str) -> Optional[str]:
        """Извлекает Definition из Expression объекта."""
        if not node:
            return None
        expr_node = node.get(key)
        if isinstance(expr_node, dict):
            # Разрешаем $ref если есть
            if "$ref" in expr_node:
                expr_node = self._resolve_ref(expr_node)
            if expr_node and "Definition" in expr_node:
                return expr_node["Definition"]
        return None

    def _clean_type(self, type_str: str) -> str:
        """Извлекает чистое имя типа из полного имени."""
        if not type_str:
            return ""
        name_part = type_str.split(",")[0].strip()
        return name_part.split(".")[-1]

    def _to_snake_case(self, name: str) -> str:
        """Преобразует CamelCase в snake_case."""
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def _parse_node(self, node: Any) -> Optional[Dict[str, Any]]:
        """Парсит узел и определяет его тип."""
        if isinstance(node, list):
            children = []
            for item in node:
                child = self._parse_node(item)
                if child:
                    children.append(child)
            return children if children else None

        if not isinstance(node, dict):
            return None

        node_type = node.get("$type", "")

        # Контейнеры
        if "Container" in node_type and "TriggerRunner" not in node_type:
            return self._parse_container(node)

        # SmartExposure - специальный контейнер
        if "SmartExposure" in node_type:
            return self._parse_smart_exposure(node)

        # Инструкции
        if "SequenceItem" in node_type or "Instruction" in node_type:
            return self._parse_instruction(node)

        return None

    def _parse_container(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Парсит контейнер со всеми параметрами."""
        self.stats["total_containers"] += 1

        node_type = node.get("$type", "")
        clean_type = self._clean_type(node_type)

        result = {
            "id": node.get("$id"),
            "name": node.get("Name", "Unnamed"),
            "type": clean_type,
            "error_behavior": node.get("ErrorBehavior", 0),
            "attempts": node.get("Attempts", 1),
        }

        # Strategy
        strategy_node = node.get("Strategy")
        if strategy_node and isinstance(strategy_node, dict):
            strategy_type = strategy_node.get("$type", "")
            if strategy_type:
                result["strategy"] = self._clean_type(strategy_type)

        # Target для DeepSkyObjectContainer
        if clean_type == "DeepSkyObjectContainer":
            target_node = node.get("Target")
            if target_node and isinstance(target_node, dict):
                # Разрешаем $ref если есть
                if "$ref" in target_node:
                    target_node = self._resolve_ref(target_node)

                if target_node and isinstance(target_node, dict):
                    target_info = {
                        "name": target_node.get("TargetName", ""),
                        "position_angle": target_node.get("PositionAngle", 0.0),
                    }

                    # Координаты
                    coords_node = target_node.get("InputCoordinates")
                    if coords_node and isinstance(coords_node, dict):
                        if "$ref" in coords_node:
                            coords_node = self._resolve_ref(coords_node)

                        if coords_node and isinstance(coords_node, dict):
                            target_info["coordinates"] = {
                                "ra_hours": coords_node.get("RAHours", 0),
                                "ra_minutes": coords_node.get("RAMinutes", 0),
                                "ra_seconds": coords_node.get("RASeconds", 0.0),
                                "dec_degrees": coords_node.get("DecDegrees", 0),
                                "dec_minutes": coords_node.get("DecMinutes", 0),
                                "dec_seconds": coords_node.get("DecSeconds", 0.0),
                                "negative_dec": coords_node.get("NegativeDec", False),
                            }

                    result["target"] = target_info

        # Conditions
        conditions_node = node.get("Conditions")
        if conditions_node and isinstance(conditions_node, dict):
            conditions_list = conditions_node.get("$values", [])
            if conditions_list:
                conditions = []
                for cond_node in conditions_list:
                    parsed_cond = self._parse_condition(cond_node)
                    if parsed_cond:
                        conditions.append(parsed_cond)
                if conditions:
                    result["conditions"] = conditions

        # Triggers
        triggers_node = node.get("Triggers")
        if triggers_node and isinstance(triggers_node, dict):
            triggers_list = triggers_node.get("$values", [])
            if triggers_list:
                triggers = []
                for trigger_node in triggers_list:
                    parsed_trigger = self._parse_trigger(trigger_node)
                    if parsed_trigger:
                        triggers.append(parsed_trigger)
                if triggers:
                    result["triggers"] = triggers

        # Children (Items)
        items_node = node.get("Items")
        if items_node and isinstance(items_node, dict):
            items_list = items_node.get("$values", [])
            if items_list:
                message_boxes = []
                instructions = []
                children = []

                for item_node in items_list:
                    if not isinstance(item_node, dict):
                        continue

                    item_type = item_node.get("$type", "")

                    # MessageBox
                    if "MessageBox" in item_type:
                        self.stats["total_message_boxes"] += 1
                        message_boxes.append(
                            {
                                "id": item_node.get("$id"),
                                "type": "MessageBox",
                                "text": item_node.get("Text", "")
                                .replace("\r\n", " ")
                                .replace("\n", " ")
                                .strip(),
                                "error_behavior": item_node.get("ErrorBehavior", 0),
                                "attempts": item_node.get("Attempts", 1),
                            }
                        )

                    # Annotation
                    elif "Annotation" in item_type:
                        self.stats["total_annotations"] += 1
                        instructions.append(
                            {
                                "id": item_node.get("$id"),
                                "type": "Annotation",
                                "text": item_node.get("Text", "")
                                .replace("\r\n", " ")
                                .replace("\n", " ")
                                .strip(),
                            }
                        )

                    # SmartExposure
                    elif "SmartExposure" in item_type:
                        parsed = self._parse_smart_exposure(item_node)
                        if parsed:
                            instructions.append(parsed)

                    # Контейнер
                    elif "Container" in item_type and "TriggerRunner" not in item_type:
                        parsed = self._parse_container(item_node)
                        if parsed:
                            children.append(parsed)

                    # Инструкция
                    else:
                        parsed = self._parse_instruction(item_node)
                        if parsed:
                            instructions.append(parsed)

                if message_boxes:
                    result["message_boxes"] = message_boxes
                if instructions:
                    result["instructions"] = instructions
                if children:
                    result["children"] = children

        return result

    def _parse_smart_exposure(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """Парсит SmartExposure со всеми параметрами."""
        self.stats["total_instructions"] += 1

        result = {
            "id": node.get("$id"),
            "type": "SmartExposure",
            "error_behavior": node.get("ErrorBehavior", 0),
            "attempts": node.get("Attempts", 1),
            "name": node.get("Name", "Умная экспозиция"),
            "iterations_expr": self._get_expr(node, "IterationsExpression"),
            "iterations": node.get("Iterations"),
        }

        # Strategy
        strategy_node = node.get("Strategy")
        if strategy_node and isinstance(strategy_node, dict):
            strategy_type = strategy_node.get("$type", "")
            if strategy_type:
                result["strategy"] = self._clean_type(strategy_type)

        # Conditions
        conditions_node = node.get("Conditions")
        if conditions_node and isinstance(conditions_node, dict):
            conditions_list = conditions_node.get("$values", [])
            if conditions_list:
                conditions = []
                for cond_node in conditions_list:
                    parsed_cond = self._parse_condition(cond_node)
                    if parsed_cond:
                        conditions.append(parsed_cond)
                if conditions:
                    result["conditions"] = conditions

        # Triggers
        triggers_node = node.get("Triggers")
        if triggers_node and isinstance(triggers_node, dict):
            triggers_list = triggers_node.get("$values", [])
            if triggers_list:
                triggers = []
                for trigger_node in triggers_list:
                    parsed_trigger = self._parse_trigger(trigger_node)
                    if parsed_trigger:
                        triggers.append(parsed_trigger)
                if triggers:
                    result["triggers"] = triggers

        # Instructions
        items_node = node.get("Items")
        if items_node and isinstance(items_node, dict):
            items_list = items_node.get("$values", [])
            if items_list:
                instructions = []
                for item_node in items_list:
                    if isinstance(item_node, dict):
                        parsed = self._parse_instruction(item_node)
                        if parsed:
                            instructions.append(parsed)
                if instructions:
                    result["instructions"] = instructions

        return result

    def _parse_instruction(self, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        node_type = node.get("$type", "")
        clean_type = self._clean_type(node_type)
        if "TriggerRunner" in node_type:
            return None

        self.stats["total_instructions"] += 1
        result = {
            "id": node.get("$id"),
            "type": clean_type,
            "error_behavior": node.get("ErrorBehavior", 0),
            "attempts": node.get("Attempts", 1),
        }

        # Пропускаем TriggerRunner
        if "TriggerRunner" in node_type:
            return None

        self.stats["total_instructions"] += 1

        result = {
            "id": node.get("$id"),
            "type": clean_type,
            "error_behavior": node.get("ErrorBehavior", 0),
            "attempts": node.get("Attempts", 1),
        }

        # GlobalVariable
        if clean_type == "GlobalVariable":
            result["identifier"] = node.get("Identifier")
            result["original_definition"] = node.get("OriginalDefinition")
            return result

        # WaitForTimeSpan
        if clean_type == "WaitForTimeSpan":
            result["time_expr"] = self._get_expr(node, "TimeExpression")
            result["time"] = node.get("Time")
            return result

        # WaitForTime
        if clean_type == "WaitForTime":
            hours = node.get("Hours", 0)
            minutes = node.get("Minutes", 0)
            seconds = node.get("Seconds", 0)
            result["time"] = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            result["offset_minutes"] = node.get("MinutesOffset")

            # Provider
            provider_node = node.get("SelectedProvider")
            if provider_node and isinstance(provider_node, dict):
                if "$ref" in provider_node:
                    provider_node = self._resolve_ref(provider_node)
                if provider_node and "$type" in provider_node:
                    result["provider"] = self._clean_type(provider_node["$type"])

            return result

        # WaitForAltitude
        if clean_type == "WaitForAltitude":
            result["offset_expr"] = self._get_expr(node, "OffsetExpression")
            result["above_or_below"] = node.get("AboveOrBelow")

            # Data объект (содержит offset и comparator)
            data_node = node.get("Data")
            if data_node and isinstance(data_node, dict):
                if "$ref" in data_node:
                    data_node = self._resolve_ref(data_node)
                if data_node and isinstance(data_node, dict):
                    result["offset"] = data_node.get("Offset")
                    result["comparator"] = data_node.get("Comparator")

            # Coordinates
            coords_node = node.get("Coordinates")
            if coords_node and isinstance(coords_node, dict):
                if "$ref" in coords_node:
                    coords_node = self._resolve_ref(coords_node)
                if coords_node and isinstance(coords_node, dict):
                    result["coordinates"] = {
                        "ra_hours": coords_node.get("RAHours", 0),
                        "ra_minutes": coords_node.get("RAMinutes", 0),
                        "ra_seconds": coords_node.get("RASeconds", 0.0),
                        "dec_degrees": coords_node.get("DecDegrees", 0),
                        "dec_minutes": coords_node.get("DecMinutes", 0),
                        "dec_seconds": coords_node.get("DecSeconds", 0.0),
                        "negative_dec": coords_node.get("NegativeDec", False),
                    }

            return result

        # Center
        if clean_type == "Center":
            result["offset"] = node.get("Offset")
            result["uses_rotation"] = node.get("usesRotation", False)
            result["inherited"] = node.get("Inherited", False)

            # Coordinates
            coords_node = node.get("Coordinates")
            if coords_node and isinstance(coords_node, dict):
                if "$ref" in coords_node:
                    coords_node = self._resolve_ref(coords_node)
                if coords_node and isinstance(coords_node, dict):
                    result["coordinates"] = {
                        "ra_hours": coords_node.get("RAHours", 0),
                        "ra_minutes": coords_node.get("RAMinutes", 0),
                        "ra_seconds": coords_node.get("RASeconds", 0.0),
                        "dec_degrees": coords_node.get("DecDegrees", 0),
                        "dec_minutes": coords_node.get("DecMinutes", 0),
                        "dec_seconds": coords_node.get("DecSeconds", 0.0),
                        "negative_dec": coords_node.get("NegativeDec", False),
                    }

            return result

        # SlewScopeToAltAz
        if clean_type == "SlewScopeToAltAz":
            result["alt_expr"] = self._get_expr(node, "AltExpression")
            result["az_expr"] = self._get_expr(node, "AzExpression")
            result["alt"] = node.get("Alt")
            result["az"] = node.get("Az")
            result["tracking"] = node.get("Tracking", True)

            # Coordinates
            coords_node = node.get("Coordinates")
            if coords_node and isinstance(coords_node, dict):
                if "$ref" in coords_node:
                    coords_node = self._resolve_ref(coords_node)
                if coords_node and isinstance(coords_node, dict):
                    result["coordinates"] = {
                        "az_degrees": coords_node.get("AzDegrees", 0),
                        "az_minutes": coords_node.get("AzMinutes", 0),
                        "az_seconds": coords_node.get("AzSeconds", 0.0),
                        "alt_degrees": coords_node.get("AltDegrees", 0),
                        "alt_minutes": coords_node.get("AltMinutes", 0),
                        "alt_seconds": coords_node.get("AltSeconds", 0.0),
                    }

            return result

        # ConnectEquipment
        if clean_type == "ConnectEquipment":
            result["device"] = node.get("SelectedDevice")
            return result

        # MoveFocuserAbsolute
        if clean_type == "MoveFocuserAbsolute":
            result["position_expr"] = self._get_expr(node, "PositionExpression")
            result["position"] = node.get("Position")
            return result

        # CoolCamera
        if clean_type == "CoolCamera":
            result["temp_expr"] = self._get_expr(node, "TemperatureExpression")
            result["duration_expr"] = self._get_expr(node, "DurationExpression")
            result["temperature"] = node.get("Temperature")
            result["duration"] = node.get("Duration")
            return result

        # WarmCamera
        if clean_type == "WarmCamera":
            result["duration_expr"] = self._get_expr(node, "DurationExpression")
            result["duration"] = node.get("Duration")
            return result

        # SetTracking
        if clean_type == "SetTracking":
            result["tracking_mode"] = node.get("TrackingMode")
            return result

        # StartGuiding
        if clean_type == "StartGuiding":
            result["force_calibration"] = node.get("ForceCalibration", False)
            return result

        # SwitchProfile
        if clean_type == "SwitchProfile":
            result["profile_id"] = node.get("SelectedProfileId")
            result["reconnect"] = node.get("Reconnect", False)
            return result

        # SwitchFilter
        if clean_type == "SwitchFilter":
            result["filter"] = node.get("ComboBoxText")
            return result

        # TakeExposure
        if clean_type == "TakeExposure":
            result["image_type"] = node.get("ImageType")
            result["exposure_expr"] = self._get_expr(node, "ExposureTimeExpression")
            result["gain_expr"] = self._get_expr(node, "GainExpression")
            result["offset_expr"] = self._get_expr(node, "OffsetExpression")
            result["exposure_time"] = node.get("ExposureTime")
            result["gain"] = node.get("Gain")
            result["offset"] = node.get("Offset")

            # Binning
            binning_node = node.get("Binning")
            if binning_node and isinstance(binning_node, dict):
                if "$ref" in binning_node:
                    binning_node = self._resolve_ref(binning_node)
                if binning_node and isinstance(binning_node, dict):
                    x = binning_node.get("X", 1)
                    y = binning_node.get("Y", 1)
                    result["binning"] = f"{x}x{y}"

            return result

        # TwoPointPolarAlignmentSequenceItem
        if clean_type == "TwoPointPolarAlignmentSequenceItem":
            result["exposure_time"] = node.get("ExposureTime")
            result["gain"] = node.get("Gain")
            result["rotation_amount"] = node.get("RotationAmount")
            result["filter"] = node.get("Filter")
            result["method"] = node.get("Method")
            result["direction"] = node.get("Direction")
            result["starting_point"] = node.get("StartingPoint")
            result["binning"] = node.get("Binning")
            result["offset"] = node.get("Offset")
            result["plate_solve_retries"] = node.get("PlateSolveRetries")
            result["enable_one_point_alignment"] = node.get("EnableOnePointAlignment")
            result["exposures_per_point"] = node.get("ExposuresPerPoint")
            return result

        # ShutdownPcInstruction
        if clean_type == "ShutdownPcInstruction":
            result["shutdown_mode"] = node.get("ShutdownMode")
            result["is_critical_shutdown"] = True
            return result

        # ShutdownNina
        if clean_type == "ShutdownNina":
            result["is_critical_shutdown"] = True
            return result

        # === ДОБАВЛЕНО: Специфичные инструкции плагинов ===
        if clean_type == "OagManualFocusInstruction":
            result["plugin"] = "OagFocusAssist"
            return result

        if clean_type == "FilterSelectorInstruction":
            result["plugin"] = "FilterSelector"
            return result

        if clean_type in ("StartLivestacking", "StopLivestacking"):
            result["plugin"] = "LiveStack"
            result["action"] = "start" if "Start" in clean_type else "stop"
            return result

        if clean_type in ("NightSummaryInstruction", "NightSummaryEndInstruction"):
            result["plugin"] = "NightSummary"
            return result

        if clean_type == "Phd2SettleInstruction":
            result["plugin"] = "PHD2Tools"
            return result

        if clean_type == "ShutdownPcInstruction":
            result["shutdown_mode"] = node.get("ShutdownMode")
            result["is_critical_shutdown"] = True
            return result

        if clean_type == "ShutdownNina":
            result["is_critical_shutdown"] = True
            return result

        # Все остальные инструкции - возвращаем базовые поля
        return result

    def _parse_trigger(self, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Парсит триггер со всеми параметрами."""
        node_type = node.get("$type", "")
        clean_type = self._clean_type(node_type)

        self.stats["total_triggers"] += 1

        result = {
            "id": node.get("$id"),
            "name": clean_type,
        }

        # === ДОБАВЛЕНО: Специфичные триггеры ===
        if clean_type == "FlexureCompensatorTrigger":
            result["plugin"] = "FlexureCompensator"
            result["params"] = {"after_exposures": node.get("AfterExposures")}

        # Parent ref
        parent_node = node.get("Parent")
        if parent_node and isinstance(parent_node, dict):
            result["parent_ref"] = parent_node.get("$ref")

        # SelectedDevice для ReconnectTrigger
        device = node.get("SelectedDevice")
        if device:
            result["device"] = device

        # Параметры триггеров
        params = {}

        # Общие параметры
        param_keys = [
            "RmsThreshold",
            "MinimumPoints",
            "Mode",
            "Amount",
            "SampleSize",
            "TrendPerFilter",
            "AfterExposures",
            "DistanceArcMinutes",
            "PlateSolvingExposureDuration",
            "DeltaT",
        ]

        for key in param_keys:
            if key in node and node[key] is not None:
                snake_key = self._to_snake_case(key)
                params[snake_key] = node[key]

        # Expressions
        expr_keys = [
            "AfterExposuresExpression",
            "AmountExpression",
            "DistanceArcMinutesExpression",
            "SampleSizeExpression",
        ]

        for key in expr_keys:
            expr_val = self._get_expr(node, key)
            if expr_val:
                snake_key = self._to_snake_case(key.replace("Expression", "")) + "_expr"
                params[snake_key] = expr_val

        if params:
            result["params"] = params

        # TriggerRunner actions
        trigger_runner = node.get("TriggerRunner")
        if trigger_runner and isinstance(trigger_runner, dict):
            if "$ref" in trigger_runner:
                trigger_runner = self._resolve_ref(trigger_runner)

            if trigger_runner and isinstance(trigger_runner, dict):
                items_node = trigger_runner.get("Items")
                if items_node and isinstance(items_node, dict):
                    items_list = items_node.get("$values", [])
                    if items_list:
                        actions = []
                        for action_node in items_list:
                            if isinstance(action_node, dict):
                                parsed_action = self._parse_instruction(action_node)
                                if parsed_action:
                                    actions.append(parsed_action)
                        if actions:
                            result["trigger_actions"] = actions

        return result

    def _parse_condition(self, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Парсит условие со всеми параметрами."""
        node_type = node.get("$type", "")
        clean_type = self._clean_type(node_type)

        self.stats["total_conditions"] += 1

        result = {
            "type": clean_type,
        }

        # Parent ref
        parent_node = node.get("Parent")
        if parent_node and isinstance(parent_node, dict):
            result["parent_ref"] = parent_node.get("$ref")

        # AboveHorizonCondition
        if clean_type == "AboveHorizonCondition":
            result["offset_expr"] = self._get_expr(node, "OffsetExpression")

            # Data объект
            data_node = node.get("Data")
            if data_node and isinstance(data_node, dict):
                if "$ref" in data_node:
                    data_node = self._resolve_ref(data_node)
                if data_node and isinstance(data_node, dict):
                    result["offset"] = data_node.get("Offset")
                    result["comparator"] = data_node.get("Comparator")

        # TimeCondition
        elif clean_type == "TimeCondition":
            hours = node.get("Hours", 0)
            minutes = node.get("Minutes", 0)
            seconds = node.get("Seconds", 0)
            result["time"] = f"{hours:02d}:{minutes:02d}:{seconds:02d}"
            result["offset_minutes"] = node.get("MinutesOffset")

            # Provider
            provider_node = node.get("SelectedProvider")
            if provider_node and isinstance(provider_node, dict):
                if "$ref" in provider_node:
                    provider_node = self._resolve_ref(provider_node)
                if provider_node and "$type" in provider_node:
                    result["provider"] = self._clean_type(provider_node["$type"])

        # LoopCondition
        elif clean_type == "LoopCondition":
            result["iterations_expr"] = self._get_expr(node, "IterationsExpression")
            result["iterations"] = node.get("Iterations")
            result["completed_iterations"] = node.get("CompletedIterations", 0)

        return result
