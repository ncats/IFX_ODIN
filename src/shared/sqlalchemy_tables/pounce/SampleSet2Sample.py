from sqlalchemy import Column, Integer, ForeignKey, UniqueConstraint
from .Base import Base

class SampleSet2Sample(Base):
    __tablename__ = 'sample_set2sample'

    id = Column(Integer, primary_key=True)
    sample_set_id = Column(Integer, ForeignKey('sample_set.sample_set_id'))
    sample_id = Column(Integer, ForeignKey('sample.sample_id'))
    sample_ordinal = Column(Integer)

UniqueConstraint(SampleSet2Sample.sample_id, SampleSet2Sample.sample_set_id, name='sample_set2s_uid_idx')
