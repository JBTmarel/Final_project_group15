from pydantic import BaseModel
from typing import Optional

class OrkuEiningarModel(BaseModel):
    id: int
    heiti: str
    tegund: str
    tegund_stod: Optional[str] = None
    eigandi: Optional[str] = None
    ar_uppsett: Optional[int] = None
    manudir_uppsett: Optional[int] = None
    dagur_uppsett: Optional[int] = None
    X_HNIT: Optional[float] = None
    Y_HNIT: Optional[float] = None
    tengd_stod: Optional[str] = None