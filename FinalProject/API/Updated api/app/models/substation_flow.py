from pydantic import BaseModel

class SubstationFlowModel(BaseModel):
    segment: str
    distance_km: float
    flow_in_kwh: float
    loss_kwh: float
    flow_out_kwh: float
    loss_pct: float
    efficiency_pct: float
    max_capacity_mw: float