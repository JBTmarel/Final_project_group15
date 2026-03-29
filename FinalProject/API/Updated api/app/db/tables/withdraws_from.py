from sqlalchemy import Column, Integer, Numeric, DateTime, ForeignKey
from app.db.base import Base

class WithdrawsFrom(Base):
    __tablename__ = "withdraws_from"
    __table_args__ = {"schema": "raforka_updated"}
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("raforka_updated.customer.id"))
    substation_id = Column(Integer, ForeignKey("raforka_updated.substation.substation_id"))
    timestamp = Column(DateTime)
    value_kwh = Column(Numeric)