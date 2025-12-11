from sqlalchemy import Column, Integer, String, UniqueConstraint, ForeignKey
from .Base import Base

class DataAnalysisAnnotation(Base):
    __tablename__ = 'data_analysis_annotation'

    da_id = Column(Integer, primary_key=True, autoincrement=True)
    expt_id = Column(Integer, ForeignKey("experiment.expt_id"), nullable=False)
    data_key = Column(String(128), nullable=False)
    data_val = Column(String(4096), nullable=False)

UniqueConstraint(DataAnalysisAnnotation.expt_id, DataAnalysisAnnotation.data_key, name='da_expt_id_data_key_uid_idx')
