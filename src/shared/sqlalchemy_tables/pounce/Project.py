from sqlalchemy import Column, String, Index, Integer, BLOB, TIMESTAMP, text
from .Base import Base

class Project(Base):
    __tablename__ = "project"

    project_id = Column(Integer, primary_key=True, autoincrement=True)
    project_display_id = Column(String(32))
    name = Column(String(256), nullable=False, unique=True)
    description = Column(String(1024), nullable=False)
    lab_groups = Column(String(45))
    start_date = Column(String(8))
    privacy_level = Column(Integer, nullable=False, default=3)
    doc = Column(BLOB)
    update_date = Column(TIMESTAMP, nullable=False, server_default=text('current_timestamp() ON UPDATE current_timestamp()'))

Index(None, Project.project_display_id)
