from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from .Base import Base

class Biosample(Base):
    __tablename__ = 'biosample'

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)
    biospecimen_id = Column(Integer, ForeignKey('biospecimen.id'), nullable=False)
    original_id = Column(String(256), nullable=False)
    type = Column(String(256), nullable=False)

UniqueConstraint(Biosample.id, Biosample.project_id, name='biosamp_id_proj_id_ux')
