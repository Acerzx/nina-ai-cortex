import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional
from app.core.config import get_settings

logger = logging.getLogger(__name__)


class SequenceParser:
    def __init__(self):
        self.settings = get_settings()
        self.sequence_path = Path(self.settings.nina_environment.sequence_template)
        self.global_variables: Dict[str, Any] = {}
        self.stats = {
            "total_containers": 0,
            "total_instructions": 0,
            "total_triggers": 0,
            "total_conditions": 0,
            "total_message_boxes": 0,
            "total_annotations": 0,
        }
        self._id_map: Dict[str, Dict] = {}

    def parse(self) -> Dict[str, Any]:
        if not self.sequence_path.exists():
            logger.error(f"❌ Sequence file not found: {self.sequence_path}")
            return {}

        try:
            with open(self.sequence_path, "r", encoding="utf-8") as f:
                data = json.load(f)

            self._build_id_map(data)
            logger.info(
                f"🧬 Parsing sequence: {self.sequence_path.name} (ID Map: {len(self._id_map)} nodes)"
            )

            self._collect_globals(data)
            graph = self._parse_container(data)

            logger.info(f"✅ Sequence parsed successfully:")
            logger.info(f"   ├─ Containers: {self.stats['total_containers']}")
            logger.info(f"   ├─ Instructions: {self.stats['total_instructions']}")
            logger.info(f"   ├─ Triggers: {self.stats['total_triggers']}")
            logger.info(f"   ├─ Conditions: {self.stats['total_conditions']}")
            logger.info(f"   ├─ MessageBoxes: {self.stats['total_message_boxes']}")
            logger.info(f"   └─ Annotations: {self.stats['total_annotations']}")

            return {
                "graph": graph,
                "global_variables": self.global_variables,
                "stats": self.stats,
            }

        except Exception as e:
            logger.error(f"❌ Failed to parse sequence: {e}")
            import traceback

            logger.error(traceback.format_exc())
            return {}

    def _build_id_map(self, node: Any):
        if isinstance(node, dict):
            if "$id" in node:
                self._id_map[node["$id"]] = node
            for v in node.values():
                self._build_id_map(v)
        elif isinstance(node, list):
            for item in node:
                self._build_id_map(item)

    def _resolve_ref(self, node: Any) -> Optional[Dict]:
        if isinstance(node, dict) and "$ref" in node:
            return self._id_map.get(node["$ref"])
        return node if isinstance(node, dict) else None

    def _collect_globals(self, node: Any):
        if isinstance(node, dict):
            if "GlobalVariable" in node.get("$type", ""):
                identifier = node.get("Identifier")
                value = node.get("OriginalDefinition")
                if identifier:
                    self.global_variables[identifier] = value
            for v in node.values():
                self._collect_globals(v)
        elif isinstance(node, list):
            for item in node:
                self._collect_globals(item)

    def _clean_type_name(self, type_str: str) -> str:
        if not type_str:
            return ""
        name_part = type_str.split(",")[0].strip()
        return name_part.split(".")[-1]

    def _to_snake_case(self, name: str) -> str:
        import re

        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def _get_expr(self, node: Dict, key: str) -> Optional[str]:
        expr_node = node.get(key)
        if isinstance(expr_node, dict) and "Definition" in expr_node:
            return expr_node["Definition"]
        return None

    def _parse_container(self, node: Dict[str, Any]) -> Dict[str, Any]:
        self.stats["total_containers"] += 1
        node_type = node.get("$type", "")
        clean_type = self._clean_type_name(node_type)

        result = {
            "id": node.get("$id"),
            "name": node.get("Name", "Unnamed"),
            "type": clean_type,
            "strategy": self._clean_type_name(node.get("Strategy", {}).get("$type", ""))
            if isinstance(node.get("Strategy"), dict)
            else None,
            "error_behavior": node.get("ErrorBehavior", 0),
            "attempts": node.get("Attempts", 1),
        }

        # FIX 1: Target extraction
        if clean_type == "DeepSkyObjectContainer" and "Target" in node:
            target_node = self._resolve_ref(node["Target"]) or node["Target"]
            if isinstance(target_node, dict):
                coords_node = self._resolve_ref(
                    target_node.get("InputCoordinates")
                ) or target_node.get("InputCoordinates")
                coords = {}
                if isinstance(coords_node, dict):
                    coords = {
                        "ra_hours": coords_node.get("RAHours", 0),
                        "ra_minutes": coords_node.get("RAMinutes", 0),
                        "ra_seconds": coords_node.get("RASeconds", 0.0),
                        "dec_degrees": coords_node.get("DecDegrees", 0),
                        "dec_minutes": coords_node.get("DecMinutes", 0),
                        "dec_seconds": coords_node.get("DecSeconds", 0.0),
                        "negative_dec": coords_node.get("NegativeDec", False),
                    }
                result["target"] = {
                    "name": target_node.get("TargetName", ""),
                    "position_angle": target_node.get("PositionAngle", 0.0),
                    "coordinates": coords,
                }

        conditions = self._parse_conditions(node.get("Conditions", {}))
        if conditions:
            result["conditions"] = conditions

        triggers = self._parse_triggers(node.get("Triggers", {}))
        if triggers:
            result["triggers"] = triggers

        message_boxes = []
        instructions = []
        children = []

        items_collection = node.get("Items", {})
        if isinstance(items_collection, dict) and "$values" in items_collection:
            for item_node in items_collection["$values"]:
                if not isinstance(item_node, dict):
                    continue

                item_type = item_node.get("$type", "")

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
                # FIX 6: SmartExposure routing
                elif "SmartExposure" in item_type:
                    instructions.append(self._parse_smart_exposure(item_node))
                elif "Container" in item_type and "TriggerRunner" not in item_type:
                    children.append(self._parse_container(item_node))
                else:
                    instr = self._parse_instruction(item_node)
                    if instr:
                        instructions.append(instr)

        if message_boxes:
            result["message_boxes"] = message_boxes
        if instructions:
            result["instructions"] = instructions
        if children:
            result["children"] = children

        return {
            k: v
            for k, v in result.items()
            if v is not None and (not isinstance(v, (list, dict)) or v)
        }

    def _parse_smart_exposure(self, node: Dict[str, Any]) -> Dict[str, Any]:
        self.stats["total_instructions"] += 1

        result = {
            "id": node.get("$id"),
            "type": "SmartExposure",
            "name": node.get("Name", "Smart Exposure"),
            "iterations_expr": self._get_expr(node, "IterationsExpression"),
            "iterations": node.get("Iterations"),
            "error_behavior": node.get("ErrorBehavior", 0),
            "attempts": node.get("Attempts", 1),
        }

        conditions = self._parse_conditions(node.get("Conditions", {}))
        if conditions:
            result["conditions"] = conditions

        triggers = self._parse_triggers(node.get("Triggers", {}))
        if triggers:
            result["triggers"] = triggers

        instructions = []
        items_collection = node.get("Items", {})
        if isinstance(items_collection, dict) and "$values" in items_collection:
            for item_node in items_collection["$values"]:
                if isinstance(item_node, dict):
                    instr = self._parse_instruction(item_node)
                    if instr:
                        instructions.append(instr)

        if instructions:
            result["instructions"] = instructions

        return {
            k: v
            for k, v in result.items()
            if v is not None and (not isinstance(v, (list, dict)) or v)
        }

    def _parse_instruction(self, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.stats["total_instructions"] += 1
        node_type = node.get("$type", "")
        clean_type = self._clean_type_name(node_type)

        if "TriggerRunner" in node_type or not clean_type:
            self.stats["total_instructions"] -= 1
            return None

        result = {
            "id": node.get("$id"),
            "type": clean_type,
            "error_behavior": node.get("ErrorBehavior", 0),
            "attempts": node.get("Attempts", 1),
        }

        # FIX 2: GlobalVariable fields
        if clean_type == "GlobalVariable":
            result["identifier"] = node.get("Identifier")
            result["original_definition"] = node.get("OriginalDefinition")

        elif clean_type == "TakeExposure":
            result["image_type"] = node.get("ImageType")
            result["exposure_expr"] = self._get_expr(node, "ExposureTimeExpression")
            result["exposure_time"] = node.get("ExposureTime")
            result["gain_expr"] = self._get_expr(node, "GainExpression")
            result["gain"] = node.get("Gain")
            result["offset_expr"] = self._get_expr(node, "OffsetExpression")
            result["offset"] = node.get("Offset")

            # FIX 5: Binning string
            binning_node = self._resolve_ref(node.get("Binning")) or node.get("Binning")
            if (
                isinstance(binning_node, dict)
                and "X" in binning_node
                and "Y" in binning_node
            ):
                result["binning"] = f"{binning_node['X']}x{binning_node['Y']}"
            elif isinstance(binning_node, str):
                result["binning"] = binning_node

        elif clean_type == "SwitchFilter":
            result["filter"] = node.get("ComboBoxText")

        elif clean_type == "WaitForTimeSpan":
            result["time_expr"] = self._get_expr(node, "TimeExpression")
            result["time"] = node.get("Time")

        elif clean_type == "WaitForTime":
            h = node.get("Hours", 0)
            m = node.get("Minutes", 0)
            s = node.get("Seconds", 0)
            result["time"] = f"{h:02d}:{m:02d}:{s:02d}"
            result["offset_minutes"] = node.get("MinutesOffset")

            # FIX 3: Provider $ref
            provider_node = self._resolve_ref(node.get("SelectedProvider")) or node.get(
                "SelectedProvider"
            )
            if isinstance(provider_node, dict):
                result["provider"] = self._clean_type_name(
                    provider_node.get("$type", "")
                )

        elif clean_type == "WaitForAltitude":
            result["offset_expr"] = self._get_expr(node, "OffsetExpression")
            data_node = self._resolve_ref(node.get("Data")) or node.get("Data")
            if isinstance(data_node, dict):
                result["offset"] = data_node.get("Offset")
                result["comparator"] = data_node.get("Comparator")
            result["above_or_below"] = node.get("AboveOrBelow")

        elif clean_type in ["ConnectEquipment", "DisconnectEquipment"]:
            result["device"] = node.get("SelectedDevice")

        elif clean_type == "MoveFocuserAbsolute":
            result["position_expr"] = self._get_expr(node, "PositionExpression")
            result["position"] = node.get("Position")

        elif clean_type == "CoolCamera":
            result["temp_expr"] = self._get_expr(node, "TemperatureExpression")
            result["temperature"] = node.get("Temperature")
            result["duration_expr"] = self._get_expr(node, "DurationExpression")
            result["duration"] = node.get("Duration")

        elif clean_type == "WarmCamera":
            result["duration_expr"] = self._get_expr(node, "DurationExpression")
            result["duration"] = node.get("Duration")

        elif clean_type == "SetTracking":
            result["tracking_mode"] = node.get("TrackingMode")

        elif clean_type == "StartGuiding":
            result["force_calibration"] = node.get("ForceCalibration", False)

        elif clean_type == "SlewScopeToAltAz":
            result["alt_expr"] = self._get_expr(node, "AltExpression")
            result["alt"] = node.get("Alt")
            result["az_expr"] = self._get_expr(node, "AzExpression")
            result["az"] = node.get("Az")
            result["tracking"] = node.get("Tracking")

        elif clean_type == "SwitchProfile":
            result["profile_id"] = node.get("SelectedProfileId")
            result["reconnect"] = node.get("Reconnect")

        elif clean_type == "TwoPointPolarAlignmentSequenceItem":
            result["exposure_time"] = node.get("ExposureTime")
            result["gain"] = node.get("Gain")
            result["rotation_amount"] = node.get("RotationAmount")
            result["filter"] = node.get("Filter")
            result["method"] = node.get("Method")
            result["direction"] = node.get("Direction")
            result["starting_point"] = node.get("StartingPoint")
            binning_val = node.get("Binning")
            if isinstance(binning_val, dict):
                result["binning"] = (
                    f"{binning_val.get('X', 1)}x{binning_val.get('Y', 1)}"
                )
            else:
                result["binning"] = binning_val
            result["offset"] = node.get("Offset")
            result["plate_solve_retries"] = node.get("PlateSolveRetries")
            result["enable_one_point_alignment"] = node.get("EnableOnePointAlignment")
            result["exposures_per_point"] = node.get("ExposuresPerPoint")

        # FIX 7: Shutdown instructions
        elif clean_type == "ShutdownPcInstruction":
            result["shutdown_mode"] = node.get("ShutdownMode")
            result["is_critical_shutdown"] = True
        elif clean_type == "ShutdownNina":
            result["is_critical_shutdown"] = True

        return {
            k: v
            for k, v in result.items()
            if v is not None and (not isinstance(v, (list, dict)) or v)
        }

    def _parse_triggers(self, triggers_node: Any) -> List[Dict[str, Any]]:
        result = []
        if not isinstance(triggers_node, dict) or "$values" not in triggers_node:
            return result

        for t_node in triggers_node["$values"]:
            if not isinstance(t_node, dict):
                continue

            t_type = t_node.get("$type", "")
            if "TriggerRunner" in t_type or "ObservableCollection" in t_type:
                continue

            clean_name = self._clean_type_name(t_type)
            if not clean_name:
                continue

            self.stats["total_triggers"] += 1

            t_data = {
                "id": t_node.get("$id"),
                "name": clean_name,
                "parent_ref": t_node.get("Parent", {}).get("$ref")
                if isinstance(t_node.get("Parent"), dict)
                else None,
            }

            device = t_node.get("SelectedDevice")
            if device:
                t_data["device"] = device

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
                if key in t_node and t_node[key] is not None:
                    t_data[self._to_snake_case(key)] = t_node[key]

            expr_keys = [
                "AfterExposuresExpression",
                "AmountExpression",
                "DistanceArcMinutesExpression",
                "SampleSizeExpression",
            ]
            for key in expr_keys:
                expr_val = self._get_expr(t_node, key)
                if expr_val:
                    t_data[
                        self._to_snake_case(key.replace("Expression", "")) + "_expr"
                    ] = expr_val

            # FIX 4: Robust TriggerRunner extraction
            trigger_runner = t_node.get("TriggerRunner")
            if trigger_runner:
                resolved_runner = self._resolve_ref(trigger_runner) or trigger_runner
                if isinstance(resolved_runner, dict):
                    runner_items = resolved_runner.get("Items", {})
                    if isinstance(runner_items, dict) and "$values" in runner_items:
                        runner_instructions = []
                        for ri_node in runner_items["$values"]:
                            if isinstance(ri_node, dict):
                                ri_instr = self._parse_instruction(ri_node)
                                if ri_instr:
                                    runner_instructions.append(ri_instr)
                        if runner_instructions:
                            t_data["trigger_actions"] = runner_instructions

            result.append(t_data)

        return result

    def _parse_conditions(self, conditions_node: Any) -> List[Dict[str, Any]]:
        result = []
        if not isinstance(conditions_node, dict) or "$values" not in conditions_node:
            return result

        for c_node in conditions_node["$values"]:
            if not isinstance(c_node, dict):
                continue

            c_type = c_node.get("$type", "")
            clean_name = self._clean_type_name(c_type)
            if not clean_name:
                continue

            self.stats["total_conditions"] += 1

            c_data = {
                "type": clean_name,
                "parent_ref": c_node.get("Parent", {}).get("$ref")
                if isinstance(c_node.get("Parent"), dict)
                else None,
            }

            if clean_name == "AboveHorizonCondition":
                c_data["offset_expr"] = self._get_expr(c_node, "OffsetExpression")
                data_node = self._resolve_ref(c_node.get("Data")) or c_node.get("Data")
                if isinstance(data_node, dict):
                    c_data["offset"] = data_node.get("Offset")
                    c_data["comparator"] = data_node.get("Comparator")

            elif clean_name == "TimeCondition":
                h = c_node.get("Hours", 0)
                m = c_node.get("Minutes", 0)
                s = c_node.get("Seconds", 0)
                c_data["time"] = f"{h:02d}:{m:02d}:{s:02d}"
                c_data["offset_minutes"] = c_node.get("MinutesOffset")

                provider_node = self._resolve_ref(
                    c_node.get("SelectedProvider")
                ) or c_node.get("SelectedProvider")
                if isinstance(provider_node, dict):
                    c_data["provider"] = self._clean_type_name(
                        provider_node.get("$type", "")
                    )

            elif clean_name == "LoopCondition":
                c_data["iterations_expr"] = self._get_expr(
                    c_node, "IterationsExpression"
                )
                c_data["iterations"] = c_node.get("Iterations")
                c_data["completed_iterations"] = c_node.get("CompletedIterations")

            result.append(c_data)

        return result
