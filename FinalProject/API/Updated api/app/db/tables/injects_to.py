from sqlalchemy import Column, Integer, Numeric, DateTime, ForeignKey
from app.db.base import Base

class InjectsTo(Base):
    __tablename__ = "injects_to"
    __table_args__ = {"schema": "raforka_updated"}
    power_plant_id = Column(Integer, primary_key=True)
    production_timestamp = Column(DateTime, primary_key=True)
    substation_id = Column(Integer, ForeignKey("raforka_updated.substation.substation_id"), primary_key=True)
    timestamp = Column(DateTime)
    value_kwh = Column(Numeric)