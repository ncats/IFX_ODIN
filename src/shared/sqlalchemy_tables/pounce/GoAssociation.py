from sqlalchemy import Column, String, Index, Integer
from .Base import Base

class GoAssociation(Base):
    __tablename__ = "go_association"
    id = Column(Integer, primary_key=True)
    uniprot_acc = Column(String(16), nullable=False)
    is_primary_acc = Column(Integer, nullable=False)
    hgnc_gene_symbol = Column(String(256))
    go_category = Column(String(8), nullable=False)
    go_id = Column(String(16), nullable=False)
    go_term = Column(String(256), nullable=False)
    evidence_code_source = Column(String(64), nullable=False)

Index(None, GoAssociation.hgnc_gene_symbol)
Index(None, GoAssociation.go_id)
Index(None, GoAssociation.go_term)
Index(None, GoAssociation.uniprot_acc)
Index(None, GoAssociation.is_primary_acc)
