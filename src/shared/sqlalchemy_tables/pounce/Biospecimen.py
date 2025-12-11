from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Index
from .Base import Base

class Biospecimen(Base):
    __tablename__ = 'biospecimen'

    biospecimen_id = Column(Integer, primary_key=True, autoincrement=True)
    biospec_ext_id = Column(String(128), nullable=False)
    parent_proj_id = Column(Integer, ForeignKey('project.project_id'), nullable=False)

UniqueConstraint(Biospecimen.biospec_ext_id, Biospecimen.parent_proj_id)
Index(None, Biospecimen.biospec_ext_id)