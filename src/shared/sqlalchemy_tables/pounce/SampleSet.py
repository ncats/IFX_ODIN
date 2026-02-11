from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from .Base import Base

class SampleSet(Base):
    __tablename__ = 'sample_set'

    id = Column(Integer, primary_key=True, autoincrement=True)
    set_name = Column(String(256), nullable=False)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)
    expt_id = Column(Integer, ForeignKey('experiment.id'), nullable=False)
    is_full_expt_set = Column(Integer, nullable=False)

UniqueConstraint(SampleSet.set_name, SampleSet.project_id, SampleSet.expt_id, SampleSet.is_full_expt_set, name='s_set_uid_idx')