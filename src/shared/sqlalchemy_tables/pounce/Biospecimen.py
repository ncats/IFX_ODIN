from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint
from .Base import Base

class Biospecimen(Base):
    __tablename__ = 'biospecimen'

    id = Column(Integer, primary_key=True, autoincrement=True)
    project_id = Column(Integer, ForeignKey('project.id'), nullable=False)
    original_id = Column(String(256), nullable=False)
    type = Column(String(256), nullable=False)
    description = Column(String(4096))
    organism = Column(String(256))
    organism_category = Column(String(256))
    organism_category_value = Column(String(256))
    disease_category = Column(String(256))
    disease_category_value = Column(String(256))
    phenotype_category = Column(String(256))
    phenotype_category_value = Column(String(256))

UniqueConstraint(Biospecimen.project_id, Biospecimen.original_id)

class BiospecimenDisease(Base):
    __tablename__ = 'biospecimen_disease'

    biospecimen_id = Column(Integer, ForeignKey('biospecimen.id'), primary_key=True)
    disease = Column(String(256), primary_key=True)
