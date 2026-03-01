"""
AfyaBima Synthetic Data Generator
----------------------------------
Kenya-specific healthcare claims generator for the AfyaBima fraud detection platform.

Public API:
    AfyaBimaKenyaGenerator  — main generator class
    TemporalController      — 24-month phase timeline
    export_to_csv           — CSV exporter
"""

from data_generation.exporter import export_to_csv
from data_generation.generator import AfyaBimaKenyaGenerator
from data_generation.temporal import TemporalController

__all__ = [
    "AfyaBimaKenyaGenerator",
    "TemporalController",
    "export_to_csv",
]
