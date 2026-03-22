from pydantic import BaseModel
from typing import Optional
from datetime import datetime

class OrkuMaelingarModel(BaseModel):
    id: int
    eining_heiti: str
    tegund_maelingar: str
    sendandi_maelingar: str
    timi: Optional[datetime] = None
    gildi_kwh: Optional[float] = None
    notandi_heiti: Optional[str] = None