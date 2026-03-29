from sqlalchemy import Column, Integer, Numeric, DateTime, ForeignKey
from app.db.base import Base

class Production(Base):
    __tablename__ = "production"
    __table_args__ = {"schema": "raforka_updated"}
    power_plant_id = Column(Integer, ForeignKey("raforka_updated.power_plant.power_plant_id"), primary_key=True)
    timestamp = Column(DateTime, primary_key=True)
    value_kwh = Column(Numeric)