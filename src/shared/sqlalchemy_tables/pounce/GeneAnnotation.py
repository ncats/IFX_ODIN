from sqlalchemy import Column, Integer, String, UniqueConstraint
from .Base import Base

class GeneAnnotation(Base):
    __tablename__ = 'gene_annotation'

    gene_db_id = Column(Integer, primary_key=True, autoincrement=True)
    gene_ext_id = Column(String(45), nullable=False)
    gene_symbol = Column(String(45))

UniqueConstraint(GeneAnnotation.gene_ext_id, GeneAnnotation.gene_symbol, name='gene_annot_uid_idx')
