from sqlalchemy import Column, Integer, String, ForeignKey, Index
from .Base import Base

class ExperimentInvestigator(Base):
    __tablename__ = 'experiment_investigator'

    id = Column(Integer, primary_key=True)
    expt_id = Column(Integer, ForeignKey('experiment.id'), nullable=False)
    investigator_id = Column(Integer, ForeignKey('investigator.id'), nullable=False)
    role = Column(String(32), nullable=False)

Index(None, ExperimentInvestigator.expt_id, ExperimentInvestigator.investigator_id, ExperimentInvestigator.role)
