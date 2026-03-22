from pydantic import BaseModel

class MonthlyPlantEnergyFlowModel(BaseModel):
    power_plant_source: str
    measurement_type: str
    year: int
    month: int
    total_kwh: float