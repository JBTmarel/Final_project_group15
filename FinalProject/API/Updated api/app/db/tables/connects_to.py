from sqlalchemy import Column, Integer, Float
from app.db.base import Base

class ConnectsTo(Base):
    __tablename__ = "connects_to"
    __table_args__ = {"schema": "raforka_updated"}
    from_substation_id = Column(Integer, primary_key=True)
    to_substation_id = Column(Integer, primary_key=True)
    distance = Column(Float)
    value_kwh = Column(Float)
    max_capacity_mw = Column(Float)