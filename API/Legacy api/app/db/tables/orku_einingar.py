from sqlalchemy import Column, Integer, String, Float
from app.db.base import Base

class OrkuEiningar(Base):
    __tablename__ = "orku_einingar"
    __table_args__ = {"schema": "raforka_legacy"}

    id = Column(Integer, primary_key=True, autoincrement=True)
    heiti = Column(String(100), nullable=False)
    tegund = Column(String(100), nullable=False)  # 'virkjun' or 'stod'
    tegund_stod = Column(String(100), nullable=True)
    eigandi = Column(String(100), nullable=True)
    ar_uppsett = Column(Integer, nullable=True)
    manudir_uppsett = Column(Integer, nullable=True)
    dagur_uppsett = Column(Integer, nullable=True)
    X_HNIT = Column(Float, nullable=True)
    Y_HNIT = Column(Float, nullable=True)
    tengd_stod = Column(String(100), nullable=True)