from sqlalchemy import Column, Integer, UniqueConstraint, ForeignKey
from .Base import Base

class BiospecimenSample(Base):
    __tablename__ = 'biospecimen_sample'

    id = Column(Integer, primary_key=True)
    biospecimen_id = Column(Integer, ForeignKey('biospecimen.id'))
    sample_id = Column(Integer, ForeignKey('sample.id'))
    expt_id = Column(Integer, ForeignKey('experiment.id'))

UniqueConstraint(BiospecimenSample.biospecimen_id, BiospecimenSample.sample_id, BiospecimenSample.expt_id, name='biospec_sample_uid_idx')
