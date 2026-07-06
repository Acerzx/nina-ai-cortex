import re
from typing import Dict, List, Optional
from pydantic import BaseModel


class LogEvent(BaseModel):
    timestamp: str
    level: str  # INFO, WARNING, ERROR, FATAL
    source: str  # Имя класса/плагина
    message: str
    event_type: str  # Наш классификатор
    metadata: Dict = {}


# Компилируем regex один раз для производительности
PATTERNS = {
    # Базовые уровни
    "base_log": re.compile(
        r"^(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+[\+\-]\d{2}:\d{2})\s+"
        r"(?P<level>DEBUG|INFO|WARNING|ERROR|FATAL)\s+"
        r"(?P<thread>\d+)\s+"
        r"(?P<source>[\w\.\+]+)\s+"
        r"(?P<message>.*)$"
    ),
    # Триггеры (КРИТИЧНО для понимания действий N.I.N.A.)
    "trigger_fired": re.compile(
        r"Trigger\s+(?P<trigger>[\w\.]+)\s+fired", re.IGNORECASE
    ),
    # Дрейф и коррекция (Вместо собственных расчетов Cortex читает это!)
    "drift_detected": re.compile(
        r"CenterAfterDriftTrigger.*?fired|Drift.*?detected", re.IGNORECASE
    ),
    "flexure_compensation": re.compile(
        r"FlexureCompensator.*?compensating|Flexure.*?detected", re.IGNORECASE
    ),
    # Безопасность
    "safety_unsafe": re.compile(
        r"Safety\s+Monitor.*?UNSAFE|Conditions\s+became\s+UNSAFE", re.IGNORECASE
    ),
    "safety_safe": re.compile(
        r"Safety\s+Monitor.*?SAFE|Conditions\s+became\s+SAFE", re.IGNORECASE
    ),
    # Meridian Flip
    "meridian_flip_start": re.compile(
        r"Meridian\s+Flip\s+Started|Starting\s+Meridian\s+Flip", re.IGNORECASE
    ),
    "meridian_flip_complete": re.compile(r"Meridian\s+Flip\s+Completed", re.IGNORECASE),
    # Plate Solving
    "plate_solve_success": re.compile(
        r"Plate\s+Solve\s+Successful|Solve.*?completed.*?successfully", re.IGNORECASE
    ),
    "plate_solve_fail": re.compile(
        r"Plate\s+Solve\s+Failed|Blind\s+solve\s+failed", re.IGNORECASE
    ),
    # Автофокус
    "autofocus_start": re.compile(
        r"AutoFocus\s+Started|Starting\s+AutoFocus", re.IGNORECASE
    ),
    "autofocus_complete": re.compile(
        r"AutoFocus\s+Completed|HFR.*?new\s+position", re.IGNORECASE
    ),
    "autofocus_fail": re.compile(
        r"AutoFocus\s+Failed|AutoFocus.*?error", re.IGNORECASE
    ),
    # Гидирование
    "guiding_start": re.compile(r"Starting\s+Guiding|Guiding\s+Started", re.IGNORECASE),
    "guiding_lost": re.compile(r"Guiding\s+Lost|Lost\s+guide\s+star", re.IGNORECASE),
    "guiding_settle": re.compile(
        r"Guiding\s+Settled|Phd2Settle.*?completed", re.IGNORECASE
    ),
    # Ошибки оборудования
    "download_failed": re.compile(
        r"Download\s+failed|Image\s+download\s+error", re.IGNORECASE
    ),
    "usb_timeout": re.compile(r"USB\s+Timeout|Timeout.*?camera", re.IGNORECASE),
    "equipment_connect": re.compile(r"Connected\s+(?P<device>[\w\s]+)", re.IGNORECASE),
    "equipment_disconnect": re.compile(
        r"Disconnected\s+(?P<device>[\w\s]+)", re.IGNORECASE
    ),
    # Секвенсор
    "sequence_start": re.compile(
        r"Sequence\s+Started|Starting\s+sequence", re.IGNORECASE
    ),
    "sequence_stop": re.compile(
        r"Sequence\s+Stopped|Sequence\s+completed", re.IGNORECASE
    ),
    "item_start": re.compile(
        r"Sequence\s+Item\s+Started.*?(?P<item>.+)", re.IGNORECASE
    ),
    "item_complete": re.compile(
        r"Sequence\s+Item\s+Completed.*?(?P<item>.+)", re.IGNORECASE
    ),
    # MessageBox (Для Copilot UI)
    "messagebox_shown": re.compile(r"MessageBox\s+shown.*?(?P<text>.+)", re.IGNORECASE),
    # Shutdown (Для Safety Interceptor)
    "shutdown_initiated": re.compile(
        r"Shutdown\s+PC\s+initiated|ShutdownNina.*?executing", re.IGNORECASE
    ),
}


def classify_log_line(line: str) -> Optional[LogEvent]:
    """Классифицирует строку лога и извлекает метаданные"""
    line = line.strip()
    if not line:
        return None

    base_match = PATTERNS["base_log"].match(line)
    if not base_match:
        return None

    data = base_match.groupdict()
    event_type = "generic"
    metadata = {}
    message = data["message"]

    # Проверяем специфичные паттерны
    for pattern_name, pattern in PATTERNS.items():
        if pattern_name == "base_log":
            continue

        match = pattern.search(message)
        if match:
            event_type = pattern_name
            metadata = match.groupdict()
            break  # Берем первый найденный паттерн

    return LogEvent(
        timestamp=data["timestamp"],
        level=data["level"],
        source=data["source"],
        message=message,
        event_type=event_type,
        metadata=metadata,
    )
