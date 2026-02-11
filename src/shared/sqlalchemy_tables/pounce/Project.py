from sqlalchemy import Column, String, Index, Integer, BLOB, Enum, Boolean, Text

from src.models.pounce.project import AccessLevel
from .Base import Base

class Project(Base):
    __tablename__ = "project"

    id = Column(Integer, primary_key=True)
    project_display_id = Column(String(32))
    name = Column(String(256), nullable=False, unique=True)
    description = Column(String(1024), nullable=False)
    start_date = Column(String(8))
    privacy_level = Column(Enum(AccessLevel), nullable=False, default=AccessLevel.private)
    rare_disease_focus = Column(Boolean, nullable=False, default=False)
    biosample_preparation = Column(Text)
    doc = Column(BLOB)

Index(None, Project.project_display_id)
