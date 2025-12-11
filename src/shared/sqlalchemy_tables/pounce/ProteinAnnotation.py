from sqlalchemy import Column, Integer, String, UniqueConstraint
from .Base import Base

class ProteinAnnotation(Base):
    __tablename__ = 'protein_annotation'

    protein_db_id = Column(Integer, primary_key=True, autoincrement=True)
    protein_id = Column(String(45), nullable=False)
    gene_symbol = Column(String(256))

UniqueConstraint(ProteinAnnotation.protein_id, name='protein_id_UNIQUE')
