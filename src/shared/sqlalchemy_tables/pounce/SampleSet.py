from sqlalchemy import Column, Integer, String, TIMESTAMP, ForeignKey, UniqueConstraint, text
from .Base import Base

class SampleSet(Base):
    __tablename__ = 'sample_set'

    sample_set_id = Column(Integer, primary_key=True, autoincrement=True)
    set_name = Column(String(256), nullable=False)
    project_id = Column(Integer, ForeignKey('project.project_id'), nullable=False)
    expt_id = Column(Integer, ForeignKey('experiment.expt_id'), nullable=False)
    is_full_expt_set = Column(Integer, nullable=False)
    update_date = Column(TIMESTAMP, nullable=False, server_default=text('current_timestamp() ON UPDATE current_timestamp()'))

UniqueConstraint(SampleSet.set_name, SampleSet.project_id, SampleSet.expt_id, SampleSet.is_full_expt_set, name='s_set_uid_idx')