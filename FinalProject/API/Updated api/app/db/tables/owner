from sqlalchemy import Column, Integer, String
from app.db.base import Base

class Owner(Base):
    __tablename__ = "owner"
    __table_args__ = {"schema": "raforka_updated"}
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)