from sqlalchemy import Column, Integer, ForeignKey, String
from .Base import Base

class ProjectInvestigator(Base):
    __tablename__ = 'project_investigator'
    project_id = Column(Integer, ForeignKey('project.id'), primary_key=True)
    investigator_id = Column(Integer, ForeignKey('investigator.id'), primary_key=True)
    role = Column(String(32), nullable=False)


