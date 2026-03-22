from sqlalchemy import Column, Integer, Float, DateTime
from app.db.base import Base

class TestMeasurement(Base):
    __tablename__ = "test_measurement"
    __table_args__ = {"schema": "raforka_legacy"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    timi = Column(DateTime, nullable=False)
    value = Column(Float, nullable=False)
