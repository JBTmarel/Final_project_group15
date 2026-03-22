from dataclasses import dataclass
from datetime import datetime

@dataclass
class TestMeasurementData:
    id: int
    timi: datetime
    value: float
