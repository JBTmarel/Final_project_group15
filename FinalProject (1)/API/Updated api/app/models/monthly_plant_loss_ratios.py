from pydantic import BaseModel

class MonthlyPlantLossRatiosModel(BaseModel):
    power_plant_source: str
    plant_to_substation_loss_ratio: float
    total_system_loss_ratio: float
