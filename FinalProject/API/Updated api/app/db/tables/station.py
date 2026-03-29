from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey
from app.db.base import Base

class Station(Base):
    __tablename__ = "station"
    __table_args__ = {"schema": "raforka_updated"}
    id = Column(Integer, primary_key=True)
    name = Column(String)
    type = Column(String)
    station_type = Column(String)
    installed_date = Column(Date)
    owner_id = Column(Integer)
    x_coordinates = Column(Float)
    y_coordinates = Column(Float)