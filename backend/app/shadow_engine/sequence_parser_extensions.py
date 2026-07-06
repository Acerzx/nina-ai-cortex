"""
Расширения для SequenceParser (Устранение Упрощения #14).
Добавляет парсинг специфичных инструкций плагинов из Sequence.json.
"""


def _parse_instruction_extended(self, node: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Расширенный парсер инструкций.
    Добавляет поддержку специфичных инструкций плагинов.
    """
    node_type = node.get("$type", "")
    clean_type = self._clean_type(node_type)

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

    # ===== СПЕЦИФИЧНЫЕ ИНСТРУКЦИИ ПЛАГИНОВ =====

    # OagManualFocusInstruction (OagFocusAssist)
    if clean_type == "OagManualFocusInstruction":
        result["plugin"] = "OagFocusAssist"
        result["is_manual_focus"] = True
        return result

    # FilterSelectorInstruction (FilterSelector)
    if clean_type == "FilterSelectorInstruction":
        result["plugin"] = "FilterSelector"
        result["is_interactive_filter_selection"] = True
        return result

    # StartLivestacking (LiveStack)
    if clean_type == "StartLivestacking":
        result["plugin"] = "LiveStack"
        result["action"] = "start"
        return result

    # StopLivestacking (LiveStack)
    if clean_type == "StopLivestacking":
        result["plugin"] = "LiveStack"
        result["action"] = "stop"
        return result

    # NightSummaryInstruction (Night Summary)
    if clean_type == "NightSummaryInstruction":
        result["plugin"] = "NightSummary"
        result["action"] = "start"
        return result

    # NightSummaryEndInstruction (Night Summary)
    if clean_type == "NightSummaryEndInstruction":
        result["plugin"] = "NightSummary"
        result["action"] = "end"
        return result

    # Phd2SettleInstruction (PHD2 Tools)
    if clean_type == "Phd2SettleInstruction":
        result["plugin"] = "PHD2Tools"
        result["action"] = "settle"
        return result

    # ShutdownPhd2Instruction (PHD2 Tools)
    if clean_type == "ShutdownPhd2Instruction":
        result["plugin"] = "PHD2Tools"
        result["action"] = "shutdown"
        return result

    # SolveAndSync (Plate Solving)
    if clean_type == "SolveAndSync":
        result["action"] = "plate_solve_and_sync"
        return result

    # UnparkScope (Telescope)
    if clean_type == "UnparkScope":
        result["action"] = "unpark"
        return result

    # ParkScope (Telescope)
    if clean_type == "ParkScope":
        result["action"] = "park"
        return result

    # DisconnectAllEquipment (Connect)
    if clean_type == "DisconnectAllEquipment":
        result["action"] = "disconnect_all"
        return result

    # RunAutofocus (Autofocus)
    if clean_type == "RunAutofocus":
        result["action"] = "autofocus"
        return result

    # Dither (Guider)
    if clean_type == "Dither":
        result["action"] = "dither"
        return result

    # StopGuiding (Guider)
    if clean_type == "StopGuiding":
        result["action"] = "stop_guiding"
        return result

    # ===== ТРИГГЕРЫ (расширенный парсинг) =====

    # CenterAfterDriftTrigger (Platesolving)
    if clean_type == "CenterAfterDriftTrigger":
        result["trigger_type"] = "center_after_drift"
        result["distance_arcminutes"] = node.get("DistanceArcMinutes")
        result["distance_arcminutes_expr"] = self._get_expr(
            node, "DistanceArcMinutesExpression"
        )
        result["after_exposures"] = node.get("AfterExposures")
        result["after_exposures_expr"] = self._get_expr(
            node, "AfterExposuresExpression"
        )
        return result

    # MeridianFlipTrigger (MeridianFlip)
    if clean_type == "MeridianFlipTrigger":
        result["trigger_type"] = "meridian_flip"
        return result

    # FlexureCompensatorTrigger (Flexure Compensator)
    if clean_type == "FlexureCompensatorTrigger":
        result["plugin"] = "FlexureCompensator"
        result["trigger_type"] = "flexure_compensation"
        result["after_exposures"] = node.get("AfterExposures")
        result["plate_solving_exposure_duration"] = node.get(
            "PlateSolvingExposureDuration"
        )
        return result

    # AutofocusAfterHFRIncreaseTrigger (Autofocus)
    if clean_type == "AutofocusAfterHFRIncreaseTrigger":
        result["trigger_type"] = "autofocus_after_hfr_increase"
        result["amount"] = node.get("Amount")
        result["amount_expr"] = self._get_expr(node, "AmountExpression")
        result["sample_size"] = node.get("SampleSize")
        result["sample_size_expr"] = self._get_expr(node, "SampleSizeExpression")
        result["trend_per_filter"] = node.get("TrendPerFilter", False)
        return result

    # AutofocusAfterTemperatureChangeTrigger (Autofocus)
    if clean_type == "AutofocusAfterTemperatureChangeTrigger":
        result["trigger_type"] = "autofocus_after_temp_change"
        result["amount"] = node.get("Amount")
        result["amount_expr"] = self._get_expr(node, "AmountExpression")
        result["delta_t"] = node.get("DeltaT")
        result["delta_t_expr"] = self._get_expr(node, "DeltaTExpression")
        return result

    # AutofocusAfterTimeTrigger (Autofocus)
    if clean_type == "AutofocusAfterTimeTrigger":
        result["trigger_type"] = "autofocus_after_time"
        result["amount"] = node.get("Amount")
        result["amount_expr"] = self._get_expr(node, "AmountExpression")
        return result

    # AutofocusAfterFilterChange (Autofocus)
    if clean_type == "AutofocusAfterFilterChange":
        result["trigger_type"] = "autofocus_after_filter_change"
        return result

    # DitherAfterExposures (Guider)
    if clean_type == "DitherAfterExposures":
        result["trigger_type"] = "dither_after_exposures"
        result["after_exposures"] = node.get("AfterExposures")
        result["after_exposures_expr"] = self._get_expr(
            node, "AfterExposuresExpression"
        )
        return result

    # RestartWhenSaturated (PHD2 Tools)
    if clean_type == "RestartWhenSaturated":
        result["plugin"] = "PHD2Tools"
        result["trigger_type"] = "restart_when_saturated"
        return result

    # InterruptWhenRMSAbove (PHD2 Tools)
    if clean_type == "InterruptWhenRMSAbove":
        result["plugin"] = "PHD2Tools"
        result["trigger_type"] = "interrupt_when_rms_above"
        result["rms_threshold"] = node.get("RmsThreshold")
        result["minimum_points"] = node.get("MinimumPoints")
        result["mode"] = node.get("Mode")
        return result

    # Phd2SettleTrigger (PHD2 Tools)
    if clean_type == "Phd2SettleTrigger":
        result["plugin"] = "PHD2Tools"
        result["trigger_type"] = "phd2_settle"
        return result

    # RestoreGuiding (Guider)
    if clean_type == "RestoreGuiding":
        result["trigger_type"] = "restore_guiding"
        return result

    # ReconnectTrigger (Connect)
    if clean_type == "ReconnectTrigger":
        result["trigger_type"] = "reconnect"
        result["device"] = node.get("SelectedDevice")
        return result

    # ReconnectOnDownloadFailure (Connect)
    if clean_type == "ReconnectOnDownloadFailure":
        result["trigger_type"] = "reconnect_on_download_failure"
        return result

    # InjectAutofocusTrigger (Inject Autofocus)
    if clean_type == "InjectAutofocusTrigger":
        result["plugin"] = "InjectAutofocus"
        result["trigger_type"] = "inject_autofocus"
        return result

    # ===== БАЗОВЫЕ ИНСТРУКЦИИ (fallback) =====

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

    # TakeExposure
    if clean_type == "TakeExposure":
        result["image_type"] = node.get("ImageType")
        result["exposure_expr"] = self._get_expr(node, "ExposureTimeExpression")
        result["gain_expr"] = self._get_expr(node, "GainExpression")
        result["offset_expr"] = self._get_expr(node, "OffsetExpression")
        result["exposure_time"] = node.get("ExposureTime")
        result["gain"] = node.get("Gain")
        result["offset"] = node.get("Offset")
        return result

    # SwitchFilter
    if clean_type == "SwitchFilter":
        result["filter"] = node.get("ComboBoxText")
        return result

    # Все остальные инструкции - возвращаем базовые поля
    return result
