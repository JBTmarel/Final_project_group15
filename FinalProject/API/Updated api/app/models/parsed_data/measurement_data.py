# Task E1

from dataclasses import dataclass
from datetime import datetime

@dataclass
class MeasurementData:
    eining_heiti: str
    tegund_maelingar: str
    sendandi_maelingar: str
    timi: datetime
    gildi_kwh: float
    notandi_heiti: str | None