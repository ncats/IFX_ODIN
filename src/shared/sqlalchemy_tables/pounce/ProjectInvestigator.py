from sqlalchemy import Column, Integer, ForeignKey
from .Base import Base

class ProjectInvestigator(Base):
    __tablename__ = 'project_investigator'
    project_id = Column(Integer, ForeignKey('project.project_id'), primary_key=True)
    invest_id = Column(Integer, ForeignKey('investigator.invest_id'), primary_key=True)


