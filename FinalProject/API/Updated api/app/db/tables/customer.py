from sqlalchemy import Column, Integer, String, Float, ForeignKey
from app.db.base import Base

class Customer(Base):
    __tablename__ = "customer"
    __table_args__ = {"schema": "raforka_updated"}
    id = Column(Integer, primary_key=True)
    name = Column(String)
    ssn = Column(String)
    founded_year = Column(Integer)
    x_coordinates = Column(Float)
    y_coordinates = Column(Float)
    owner_id = Column(Integer)