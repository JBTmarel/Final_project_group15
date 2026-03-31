from sqlalchemy import Column, Integer, String, Numeric
from app.db.base import Base

class VMonthlyPlantEnergy(Base):
    __tablename__ = "v_monthly_plant_energy"
    __table_args__ = {"schema": "raforka_updated"}
    power_plant_source = Column(String, primary_key=True)
    year = Column(Integer, primary_key=True)
    month = Column(Integer, primary_key=True)
    production_kwh = Column(Numeric)
    injection_kwh = Column(Numeric)
    attributed_withdrawal_kwh = Column(Numeric)