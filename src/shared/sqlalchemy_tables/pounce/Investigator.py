from sqlalchemy import Column, String, Integer, TIMESTAMP
from .Base import Base
class Investigator(Base):
    __tablename__ = 'investigator'

    invest_id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(256), nullable=False, unique=True)
    first_name = Column(String(128), nullable=False)
    last_name = Column(String(128), nullable=False)
    email = Column(String(45), nullable=False)
    institute = Column(String(128), default=None)
    branch = Column(String(128), default=None)
    uid = Column(String(45), default=None)
    update_date = Column(TIMESTAMP, default=None)
