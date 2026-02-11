from sqlalchemy import Column, Integer, String, Double, Text, ForeignKey, UniqueConstraint
from .Base import Base

class MetabolomicsProfile(Base):
    __tablename__ = 'metabolomics_profile'

    id = Column(Integer, primary_key=True, autoincrement=True)
    met_db_id = Column(Integer, nullable=False)
    met_expt_id = Column(String(128), nullable=False)
    met_name = Column(String(256), nullable=False)
    expt_id = Column(Integer, ForeignKey('experiment.id'), nullable=False)
    sample_set_id = Column(Integer, ForeignKey('sample_set.id'))
    sample_set_stat_ready = Column(Integer, ForeignKey('sample_set.id'))
    val_cnt = Column(Integer)
    rt = Column(Double)
    mz = Column(Double)
    ms_platform = Column(String(45))
    raw_data = Column(Text)
    stat_ready_data = Column(Text)
    effect_size_data = Column(Text)

UniqueConstraint(MetabolomicsProfile.met_db_id, MetabolomicsProfile.met_expt_id, MetabolomicsProfile.expt_id, name='met_profile_uid_idx')
