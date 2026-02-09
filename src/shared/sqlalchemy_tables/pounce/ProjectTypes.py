from sqlalchemy import Column, Integer, String, Index, ForeignKey, UniqueConstraint
from .Base import Base

class ProjectTypes(Base):
    __tablename__ = 'project_types'
    id = Column(Integer, primary_key=True, autoincrement=True)
    project_type = Column(String(128), nullable=False)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)

Index(None, ProjectTypes.project_id)
Index(None, ProjectTypes.project_type)
UniqueConstraint(ProjectTypes.project_type, ProjectTypes.project_id)


class ProjectKeywords(Base):
    __tablename__ = 'project_keywords'
    id = Column(Integer, primary_key=True, autoincrement=True)
    keyword = Column(String(128), nullable=False)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)

Index(None, ProjectKeywords.project_id)
Index(None, ProjectKeywords.keyword)
UniqueConstraint(ProjectKeywords.keyword, ProjectKeywords.project_id)


class ProjectGroups(Base):
    __tablename__ = 'project_groups'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(128), nullable=False)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)

Index(None, ProjectGroups.project_id)
Index(None, ProjectGroups.name)
UniqueConstraint(ProjectGroups.name, ProjectGroups.project_id)