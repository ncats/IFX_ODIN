from sqlalchemy import Column, String, Integer
from .Base import Base
class Investigator(Base):
    __tablename__ = 'investigator'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(256), nullable=False, unique=True)
    email = Column(String(45), nullable=False)
    institute = Column(String(128), default=None)
    branch = Column(String(128), default=None)
    uid = Column(String(45), default=None)
