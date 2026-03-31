from sqlalchemy import Column, Integer, ForeignKey
from app.db.base import Base

class PowerPlant(Base):
    __tablename__ = "power_plant"
    __table_args__ = {"schema": "raforka_updated"}
    power_plant_id = Column(Integer, ForeignKey("raforka_updated.station.id"), primary_key=True)