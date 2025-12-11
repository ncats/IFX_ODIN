from sqlalchemy import Column, Integer, Text, TIMESTAMP, text, ForeignKey
from .Base import Base

class ExperimentDataHeader(Base):
    __tablename__ = 'experiment_data_header'
    id = Column(Integer, primary_key=True)
    expt_id = Column(Integer, ForeignKey('experiment.expt_id'), nullable=False)
    raw_data_headers = Column(Text)
    stat_ready_data_headers = Column(Text)
    effect_size_data_headers = Column(Text)
    created = Column(TIMESTAMP, nullable=False, server_default=text('current_timestamp() ON UPDATE current_timestamp()'))
