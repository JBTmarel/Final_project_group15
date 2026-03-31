from sqlalchemy import Column, Integer, Numeric, DateTime, ForeignKey
from app.db.base import Base

class WithdrawsFrom(Base):
    __tablename__ = "withdraws_from"
    __table_args__ = {"schema": "raforka_updated"}
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_id = Column(Integer)
    substation_id = Column(Integer)
    timestamp = Column(DateTime)
    value_kwh = Column(Numeric)
    power_plant_source_id = Column(Integer)