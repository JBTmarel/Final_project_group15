from sqlalchemy import Column, Integer, ForeignKey
from app.db.base import Base

class Substation(Base):
    __tablename__ = "substation"
    __table_args__ = {"schema": "raforka_updated"}
    substation_id = Column(Integer, ForeignKey("raforka_updated.station.id"), primary_key=True)