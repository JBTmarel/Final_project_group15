from pydantic import BaseModel

class MonthlyCompanyUsageModel(BaseModel):
    power_plant_source: str
    customer_name: str
    year: int
    month: int 
    total_kwh: float