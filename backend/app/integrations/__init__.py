"""
Integrations module — интеграции с внешними инструментами.

Содержит:
- siril: Интеграция с Siril для обработки изображений (заглушка)
- (future): PixInsight, ASTAP, и другие инструменты
"""

from app.integrations.siril import (
    SirilBridge,
    ManualSirilBridge,
    siril_bridge,
)

__all__ = [
    "SirilBridge",
    "ManualSirilBridge",
    "siril_bridge",
]
