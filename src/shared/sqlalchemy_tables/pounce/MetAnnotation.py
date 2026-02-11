from sqlalchemy import Column, Integer, String, Float, UniqueConstraint, ForeignKey
from .Base import Base

class MetAnnotation(Base):
    __tablename__ = 'met_annotation'

    id = Column(Integer, primary_key=True, autoincrement=True)
    met_id = Column(String(128), nullable=False)
    expt_id = Column(Integer, ForeignKey('experiment.id'), nullable=False)
    met_name = Column(String(256), nullable=False)
    mz = Column(Float)
    rt = Column(Float)
    ms_platform = Column(String(64))

UniqueConstraint(MetAnnotation.met_id, MetAnnotation.expt_id, name='met_annot_uid_idx')
