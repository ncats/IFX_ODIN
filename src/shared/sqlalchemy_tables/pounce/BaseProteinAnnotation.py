from sqlalchemy import Column, Integer, String, Index
from .Base import Base

class BaseProteinAnnotation(Base):
    __tablename__ = "base_protein_annotation"

    uniprot_acc = Column(String(16), primary_key=True)
    is_primary_acc = Column(Integer, nullable=False)
    uniprot_protein_id = Column(String(45), nullable=False)
    protein_name = Column(String(256), primary_key=True, nullable=False)
    gene_name = Column(String(256))
    hgnc_gene_symbol = Column(String(256))

Index(None, BaseProteinAnnotation.uniprot_protein_id)
Index(None, BaseProteinAnnotation.hgnc_gene_symbol)
Index(None, BaseProteinAnnotation.is_primary_acc)
