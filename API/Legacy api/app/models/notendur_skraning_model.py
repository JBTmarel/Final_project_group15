from pydantic import BaseModel
from typing import Optional

class NotendurSkraningModel(BaseModel):
    id: int
    heiti: str
    kennitala: str
    eigandi: str
    ar_stofnad: Optional[int] = None
    X_HNIT: Optional[float] = None
    Y_HNIT: Optional[float] = None