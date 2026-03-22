from sqlalchemy import Column, Integer, String, Float
from app.db.base import Base

class NotendurSkraning(Base):
    __tablename__ = "notendur_skraning"
    __table_args__ = {"schema": "raforka_legacy"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    heiti = Column(String(100), nullable=False)
    kennitala = Column(String(100), nullable=False)
    eigandi = Column(String(100), nullable=False)  
    ar_stofnad = Column(Integer, nullable=True)
    X_HNIT = Column(Float, nullable=True)
    Y_HNIT = Column(Float, nullable=True)