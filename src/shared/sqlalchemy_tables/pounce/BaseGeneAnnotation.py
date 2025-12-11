from sqlalchemy import Column, Integer, String, Index
from .Base import Base

class BaseGeneAnnotation(Base):
    __tablename__ = "base_gene_annotation"

    id = Column(Integer, primary_key=True)
    ensembl_gene_id = Column(String(45), nullable=False)
    gene_ext_id = Column(String(45))
    hgnc_gene_symbol = Column(String(45))
    chromosome = Column(String(8), nullable=False)
    start_pos = Column(Integer, nullable=False)
    end_pos = Column(Integer, nullable=False)
    strand = Column(Integer, nullable=False)
    gene_biotype = Column(String(45), nullable=False)

Index(None, BaseGeneAnnotation.ensembl_gene_id)