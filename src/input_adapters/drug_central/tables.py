from sqlalchemy import Column, Integer, Sequence, String, Float, ForeignKey, Text, Boolean, LargeBinary, \
    DateTime, BigInteger, SmallInteger
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Structures(Base):
    __tablename__ = 'structures'

    cd_id = Column(
        Integer,
        primary_key=True,
        nullable=False,
        default=Sequence('structures_cd_id_seq', metadata=Base.metadata).next_value()
    )
    cd_formula = Column(String(100))
    cd_molweight = Column(Float)
    id = Column(Integer, nullable=False, unique=True)
    clogp = Column(Float)
    alogs = Column(Float)
    cas_reg_no = Column(String(50), unique=True)
    tpsa = Column(Float)
    lipinski = Column(Integer)
    name = Column(String(250))
    no_formulations = Column(Integer)
    stem = Column(String(50), ForeignKey('public.inn_stem.stem'))
    molfile = Column(Text)
    mrdef = Column(String(32672))
    enhanced_stereo = Column(Boolean, nullable=False, default=False)
    arom_c = Column(Integer)
    sp3_c = Column(Integer)
    sp2_c = Column(Integer)
    sp_c = Column(Integer)
    halogen = Column(Integer)
    hetero_sp2_c = Column(Integer)
    rotb = Column(Integer)
    molimg = Column(LargeBinary)
    o_n = Column(Integer)
    oh_nh = Column(Integer)
    inchi = Column(String(32672))
    smiles = Column(String(32672))
    rgb = Column(Integer, comment='number of rigid bonds')
    fda_labels = Column(Integer)
    inchikey = Column(String(27))
    status = Column(String(10))


class DBVersion(Base):
    __tablename__ = 'dbversion'
    version = Column(BigInteger, primary_key=True, nullable=False)
    dtime = Column(DateTime, nullable=False)


class ActTableFull(Base):
    __tablename__ = "act_table_full"

    act_id = Column(Integer, primary_key=True, nullable=False)
    struct_id = Column(Integer, ForeignKey("public.structures.id", onupdate="NO ACTION", ondelete="NO ACTION"),
                       nullable=False)
    target_id = Column(Integer, ForeignKey("public.target_dictionary.id", onupdate="NO ACTION", ondelete="NO ACTION"),
                       nullable=False)
    target_name = Column(String(200))
    target_class = Column(String(50), ForeignKey("public.target_class.l1", onupdate="NO ACTION", ondelete="NO ACTION"))
    accession = Column(String(1000))
    gene = Column(String(1000))
    swissprot = Column(String(1000))
    act_value = Column(Float)
    act_unit = Column(String(100))
    act_type = Column(String(100))
    act_comment = Column(String(1000))
    act_source = Column(String(100))
    relation = Column(String(5))
    moa = Column(SmallInteger)
    moa_source = Column(String(100))
    act_source_url = Column(String(500))
    moa_source_url = Column(String(500))
    action_type = Column(String(50),
                         ForeignKey("public.action_type.action_type", onupdate="NO ACTION", ondelete="NO ACTION"))
    first_in_class = Column(SmallInteger)
    tdl = Column(String(500))
    act_ref_id = Column(Integer, ForeignKey("public.reference.id", onupdate="NO ACTION", ondelete="NO ACTION"))
    moa_ref_id = Column(Integer, ForeignKey("public.reference.id", onupdate="NO ACTION", ondelete="NO ACTION"))
    organism = Column(String(150))


class Reference(Base):
    __tablename__ = 'reference'
    id = Column(Integer, primary_key=True)
    pmid = Column(Integer, unique=True)
    doi = Column(String(50), unique=True)
    document_id = Column(String(200), unique=True)
    type = Column(String(50), ForeignKey('ref_type.type'))
    authors = Column(String(4000))
    title = Column(String(500))
    isbn10 = Column(String(10), unique=True)
    url = Column(String(1000))
    journal = Column(String(100))
    volume = Column(String(20))
    issue = Column(String(20))
    dp_year = Column(Integer)
    pages = Column(String(50))
