from sqlalchemy import Column, Integer, ForeignKey, String

from src.shared.sqlalchemy_tables.pounce.Base import Base


class Demographics(Base):
    __tablename__ = 'demographics'

    id = Column(Integer, primary_key=True, autoincrement=True)
    biosample_id = Column(Integer, ForeignKey('biosample.id'), nullable=False)
    age = Column(Integer)
    race = Column(String(256))
    ethnicity = Column(String(256))
    sex = Column(String(256))
    category = Column(String(256))
    category_value = Column(String(256))

