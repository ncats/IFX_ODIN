from sqlalchemy import Column, Integer, String, Text, ForeignKey, UniqueConstraint
from .Base import Base

class ProteomicsProfile(Base):
    __tablename__ = 'proteomics_profile'

    prot_profile_id = Column(Integer, primary_key=True, autoincrement=True)
    expt_id = Column(Integer, ForeignKey('experiment.expt_id'), nullable=False)
    sample_set_id = Column(Integer, ForeignKey('sample_set.sample_set_id'))
    sample_set_stat_ready = Column(Integer, ForeignKey('sample_set.sample_set_id'))
    uniprot_acc = Column(String(45), nullable=False)
    raw_data = Column(Text)
    stat_ready_data = Column(Text)
    effect_size_data = Column(Text)


UniqueConstraint(ProteomicsProfile.expt_id, ProteomicsProfile.uniprot_acc, name='record_uid_idx')
