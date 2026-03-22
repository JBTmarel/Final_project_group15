from sqlalchemy import Column, Integer, String, Float, DateTime
from app.db.base import Base

class OrkuMaelingar(Base):
    __tablename__ = "orku_maelingar"
    __table_args__ = {"schema": "raforka_legacy"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    eining_heiti = Column(String(50), nullable=False)
    tegund_maelingar = Column(String(50), nullable=False)
    sendandi_maelingar = Column(String(50), nullable=False)  
    timi = Column(DateTime, nullable=True)
    gildi_kwh = Column(Float, nullable=True)
    notandi_heiti = Column(String(50), nullable=True)

