"""
Simulation Mode — эмуляторы оборудования для тестирования без реальных устройств.
"""

from app.simulation.fake_nina import fake_nina, FakeNinaAPI
from app.simulation.fake_phd2 import fake_phd2, FakePhd2

__all__ = [
    "fake_nina",
    "FakeNinaAPI",
    "fake_phd2",
    "FakePhd2",
]
