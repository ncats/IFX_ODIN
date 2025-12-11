from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Index
from .Base import Base

class BiospecimenAnnotation(Base):
    __tablename__ = 'biospecimen_annotation'

    biospec_annot_id = Column(Integer, primary_key=True, autoincrement=True)
    biospecimen_id = Column(Integer, ForeignKey('biospecimen.biospecimen_id'), nullable=False)
    proj_id = Column(Integer, ForeignKey('project.project_id'), nullable=False)
    biospec_ext_id = Column(String(64), nullable=False)
    pounce_annot_key = Column(String(128), nullable=False)
    expt_annot_key = Column(String(128), nullable=False)
    annot_val = Column(String(256))
    annot_group = Column(String(128))
    value_type = Column(String(64))

UniqueConstraint(BiospecimenAnnotation.biospecimen_id, BiospecimenAnnotation.proj_id, BiospecimenAnnotation.pounce_annot_key, name='bios_uid_idx')
Index(None, BiospecimenAnnotation.biospecimen_id)
Index(None, BiospecimenAnnotation.proj_id)
Index(None, BiospecimenAnnotation.pounce_annot_key)
Index(None, BiospecimenAnnotation.annot_val)