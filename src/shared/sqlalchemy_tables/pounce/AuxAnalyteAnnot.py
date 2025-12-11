from sqlalchemy import Column, Integer, String
from .Base import Base

class AuxAnalyteAnnot(Base):
    __tablename__ = 'aux_analyte_annot'

    id = Column(Integer, primary_key=True)
    platform = Column(String(64), nullable=False)
    analyte_db_id = Column(Integer, nullable=False)
    data_key = Column(String(45), nullable=False)
    annot_val = Column(String(45), nullable=False)
