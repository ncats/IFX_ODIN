from sqlalchemy import Column, Integer, String, UniqueConstraint, ForeignKey
from .Base import Base
class SampleAnnotation(Base):
    __tablename__ = 'sample_annotation'

    id = Column(Integer, primary_key=True, autoincrement=True)
    sample_id = Column(Integer, ForeignKey('sample.id'), nullable=False)
    expt_id = Column(String(45), nullable=False)
    expt_annot_key = Column(String(64), nullable=False)
    pounce_annot_key = Column(String(64), nullable=False)
    annot_val = Column(String(128), nullable=False)

UniqueConstraint(SampleAnnotation.sample_id, SampleAnnotation.expt_id, SampleAnnotation.pounce_annot_key, name='sample_ann_uid_idx')
