from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, TIMESTAMP
from .Base import Base

class Sample(Base):
    __tablename__ = 'sample'

    sample_id = Column(Integer, primary_key=True, autoincrement=True)
    parent_expt_id = Column(Integer, ForeignKey('experiment.expt_id'), nullable=False)
    parent_expt_name = Column(String(256), nullable=False)
    sample_name = Column(String(256), nullable=False)
    description = Column(String(1024))
    update_date = Column(TIMESTAMP)

UniqueConstraint(Sample.parent_expt_id, Sample.sample_name, name='bid_pexptid_samplename_udix')
