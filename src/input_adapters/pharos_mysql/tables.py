from sqlalchemy import Column, String, Integer, Text, Enum, SmallInteger, ForeignKey, Date, Float
from sqlalchemy.orm import declarative_base
from enum import Enum as PyEnum


class TDL(PyEnum):
    TclinPlus = 'Tclin+'
    Tclin = 'Tclin'
    TchemPlus = 'Tchem+'
    Tchem = 'Tchem'
    Tbio = 'Tbio'
    Tgray = 'Tgray'
    Tdark = 'Tdark'

class GoType(PyEnum):
    Component = 'Component'
    Function = 'Function'
    Process = 'Process'

class Protein(Base):
    __tablename__ = 'protein'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    description = Column(Text, nullable=False)
    uniprot = Column(String(20), nullable=False)
    up_version = Column(Integer)
    geneid = Column(Integer)
    sym = Column(String(20))
    family = Column(String(255))
    chr = Column(String(255))
    seq = Column(Text)
    dtoid = Column(String(13))
    stringid = Column(String(15))
    dtoclass = Column(String(255))
    preferred_symbol = Column(String(20), nullable=False)

class Target(Base):
    __tablename__ = 'target'
    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    ttype = Column(String(255), nullable=False)
    description = Column(Text)
    comment = Column(Text)
    tdl = Column(Enum(TDL))
    idg = Column(SmallInteger, nullable=False, default=0)
    fam = Column(String)
    famext = Column(String(255))

class T2TC(Base):
    __tablename__ = 't2tc'
    target_id = Column(Integer, ForeignKey('target.id'), primary_key=True, nullable=False)
    protein_id = Column(Integer, ForeignKey('protein.id'), primary_key=True)

class GoA(Base):
    __tablename__ = 'goa'
    id = Column(Integer, primary_key=True)
    protein_id = Column(Integer, ForeignKey('protein.id'))
    go_id = Column(String(255), nullable=False)
    go_term = Column(Text)
    evidence = Column(Text)
    goeco = Column(String(255), nullable=False)
    assigned_by = Column(String(50))
    go_type = Column(Enum(GoType))
    go_term_text = Column(Text)

class TDL_info(Base):
    __tablename__ = "tdl_info"
    id = Column(Integer, primary_key=True)
    itype = Column(String(255), nullable=False)
    target_id = Column(Integer, ForeignKey('target.id'))
    protein_id = Column(Integer, ForeignKey('protein.id'))
    string_value = Column(Text)
    number_value = Column(Float)
    integer_value = Column(Integer)
    date_value = Column(Date)
    boolean_value = Column(SmallInteger)

class GeneRif(Base):
    __tablename__ = 'generif'
    id = Column(Integer, primary_key=True)
    protein_id = Column(Integer, ForeignKey('protein.id'), nullable=False)
    gene_id = Column(Integer, nullable=False)
    text = Column(Text, nullable=False)
    date = Column(Date)

class GeneRif2Pubmed(Base):
    __tablename__ = 'generif2pubmed'
    generif_id = Column(Integer, ForeignKey('generif.id'), primary_key=True)
    pubmed_id = Column(Integer, primary_key=True)

class Ligand(Base):
    __tablename__ = 'ncats_ligands'
    id = Column(Integer, primary_key=True)
    identifier = Column(String(255), nullable=False)
    name = Column(Text, nullable=False)
    isDrug = Column(SmallInteger)
    smiles = Column(Text)
    PubChem = Column(String(255))
    ChEMBL = Column(String(255))
    guide_to_pharmacology = Column("Guide to Pharmacology", String(255))
    DrugCentral = Column(String(255))
    description = Column(Text)
    actCnt = Column(Integer)
    targetCount = Column(Integer)
    unii = Column(String(10))
    pt = Column(String(128))

class LigandActivity(Base):
    __tablename__ = 'ncats_ligand_activity'
    id = Column(Integer, primary_key=True)
    ncats_ligand_id = Column(Integer, ForeignKey('ncats_ligands.id'), nullable=False)
    target_id = Column(Integer, ForeignKey('target.id'), nullable=False)
    smiles = Column(Text)
    act_value = Column(Float)
    act_type = Column(String(255))
    action_type = Column(String(255))
    reference = Column(Text)
    reference_source = Column(String(255))
    pubmed_ids = Column(Text)  # pipe delimited list

class DrugActivity(Base):
    __tablename__ = 'drug_activity'
    id = Column(Integer, primary_key=True)
    target_id = Column(Integer, ForeignKey('target.id'), nullable=False)
    drug = Column(String(255), nullable=False)
    act_value = Column(Float)
    act_type = Column(String(255))
    action_type = Column(String(255))
    has_moa = Column(SmallInteger)
    source = Column(String(255))
    reference = Column(Text)
    smiles = Column(Text)
    cmpd_chemblid = Column(String(255))
    nlm_drug_info = Column(Text)
    cmpd_pubchem_cid = Column(Integer)
    dcid = Column(Integer)
    lychi_h4 = Column(String(15))

class PPI(Base):
    __tablename__ = "ncats_ppi"
    id = Column(Integer, primary_key=True)
    ppitypes = Column(String(255), nullable=False)
    protein_id = Column(Integer, ForeignKey('protein.id'), nullable=False)
    other_id = Column(Integer, ForeignKey('protein.id'), nullable=False)
    p_int = Column(Float)
    p_ni = Column(Float)
    p_wrong = Column(Float)
    evidence = Column(String(255))
    interaction_type = Column(String(100))
    score = Column(Integer)
