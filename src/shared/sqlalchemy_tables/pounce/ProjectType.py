from sqlalchemy import Column, Integer, String, Index, ForeignKey
from .Base import Base

class ProjectType(Base):
    __tablename__ = 'project_type'
    id = Column(Integer, primary_key=True)
    project_type_id = Column(Integer, nullable=False)
    project_type = Column(String(128), nullable=False)
    project_id = Column(Integer, ForeignKey('project.project_id'), nullable=False)

Index(None, ProjectType.project_id)
Index(None, ProjectType.project_type)
Index('ix_project_type_project_type_project_id', ProjectType.project_type, ProjectType.project_id)