from sqlalchemy import Column, Integer, String, Float, DateTime, Date
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class MetaboliteClass(Base):
    __tablename__ = 'metabolite_class'
    ramp_id = Column(String(length=32), nullable=False, primary_key=True)
    class_source_id = Column(String(length=32), nullable=False, primary_key=True)
    class_level_name = Column(String(length=128), nullable=False)
    class_name = Column(String(length=128), nullable=False)
    source = Column(String(length=32), nullable=False)

class Analyte(Base):
    __tablename__ = 'analyte'
    rampId = Column(String(length=30), nullable=False, primary_key=True)
    type = Column(String(length=30), nullable=True)

class Pathway(Base):
    __tablename__ = 'pathway'
    pathwayRampId = Column(String(length=30), nullable=False, primary_key=True)
    sourceId = Column(String(length=30), nullable=False)
    type = Column(String(length=30), nullable=False)
    pathwayCategory = Column(String(length=30), nullable=True)
    pathwayName = Column(String(length=250), nullable=False)

class AnalytePathwayRelationship(Base):
    __tablename__ = 'analytehaspathway'
    rampId = Column(String(length=30), nullable=False, primary_key=True)
    pathwayRampId = Column(String(length=30), nullable=False, primary_key=True)
    pathwaySource = Column(String(length=30), nullable=False)

class Catalyzed(Base):
    __tablename__ = 'catalyzed'
    rampCompoundId = Column(String(length=30), nullable=False, primary_key=True)
    rampGeneId = Column(String(length=30), nullable=False, primary_key=True)
    proteinType = Column(String(length=32), nullable=False)

class Ontology(Base):
    __tablename__ = 'ontology'
    rampOntologyId = Column(String(length=30), nullable=False, primary_key=True)
    commonName = Column(String(length=64), nullable=False)
    HMDBOntologyType = Column(String(length=30), nullable=False)
    metCount = Column(Integer, nullable=False)

class AnalyteHasOntology(Base):
    __tablename__ = "analytehasontology"
    rampCompoundId = Column(String(length=30), nullable=False, primary_key=True)
    rampOntologyId = Column(String(length=30), nullable=False, primary_key=True)

class ChemProps(Base):
    __tablename__ = 'chem_props'
    ramp_id = Column(String(length=30), nullable=False, primary_key=True)
    chem_data_source = Column(String(length=32), nullable=False, primary_key=True)
    chem_source_id = Column(String(length=45), nullable=False, primary_key=True)
    iso_smiles = Column(String(length=256), nullable=False)
    inchi_key_prefix = Column(String(length=32), nullable=False)
    inchi_key = Column(String(length=32), nullable=False)
    inchi = Column(String(length=4096), nullable=False)
    mw = Column(Float, nullable=False)
    monoisotop_mass = Column(Float, nullable=False)
    common_name = Column(String(length=1024), nullable=False)
    mol_formula = Column(String(length=64), nullable=False)

class Reaction(Base):
    __tablename__ = 'reaction'
    ramp_rxn_id = Column(String(length=16), nullable=False, primary_key=True)
    rxn_source_id = Column(String(length=16), nullable=False)
    status = Column(Integer, nullable=False)
    is_transport = Column(Integer, nullable=False)
    direction = Column(String(length=8), nullable=False)
    label = Column(String(length=256), nullable=False)
    equation = Column(String(length=256), nullable=False)
    html_equation = Column(String(length=256), nullable=False)
    ec_num = Column(String(length=256), nullable=True)
    has_human_prot = Column(Integer, nullable=False)
    only_human_mets = Column(Integer, nullable=False)

class ReactionClass(Base):
    __tablename__ = 'reaction_ec_class'
    ramp_rxn_id = Column(String(length=16), nullable=False, primary_key=True)
    rxn_source_id = Column(String(length=16), nullable=False)
    rxn_class_ec = Column(String(length=16), nullable=False)
    ec_level = Column(Integer, nullable=False, primary_key=True)
    rxn_class = Column(String(length=256), nullable=False)
    rxn_class_hierarchy = Column(String(length=512), nullable=False)

class ReactionToMetabolite(Base):
    __tablename__ = "reaction2met"
    ramp_rxn_id = Column(String(length=16), nullable=False, primary_key=True)
    rxn_source_id = Column(String(length=16), nullable=False)
    ramp_cmpd_id = Column(String(length=16), nullable=False, primary_key=True)
    substrate_product = Column(Integer, nullable=False)
    met_source_id = Column(String(length=32), nullable=False)
    met_name = Column(String(length=256), nullable=False)
    is_cofactor = Column(Integer, nullable=False)

class ReactionToProtein(Base):
    __tablename__ = "reaction2protein"
    ramp_rxn_id = Column(String(length=16), nullable=False, primary_key=True)
    rxn_source_id = Column(String(length=16), nullable=False)
    ramp_gene_id = Column(String(length=16), nullable=False, primary_key=True)
    uniprot = Column(String(length=16), nullable=False)
    protein_name = Column(String(length=16), nullable=False)
    is_reviewed = Column(Integer, nullable=False)

class Source(Base):
    __tablename__ = "source"
    sourceId = Column(String(length=30), nullable=False, primary_key=True)
    rampId = Column(String(length=30), nullable=False)
    IDtype = Column(String(length=30), nullable=False)
    geneOrCompound = Column(String(length=30), nullable=False)
    commonName = Column(String(length=256), nullable=True)
    priorityHMDBStatus = Column(String(length=32), nullable=True, primary_key=True)
    dataSource = Column(String(length=32), nullable=False, primary_key=True)
    pathwayCount = Column(Integer, nullable=False)

class AnalyteSynonym(Base):
    __tablename__ = "analytesynonym"
    Synonym = Column(String(length=500), nullable=False, primary_key=True)
    rampId = Column(String(length=30), nullable=False, primary_key=True)
    geneOrCompound = Column(String(length=30), nullable=False)
    source = Column(String(length=30), nullable=False, primary_key=True)


class DBVersion(Base):
    __tablename__ = "db_version"
    ramp_version = Column(String(length=20), nullable=False, primary_key=True)
    load_timestamp = Column(DateTime, nullable=False, primary_key=True)
    version_notes = Column(String(length=256), nullable=False)
    met_intersects_json = Column(String(length=10000), nullable=False)
    gene_intersects_json = Column(String(length=10000), nullable=False)
    met_intersects_json_pw_mapped = Column(String(length=10000), nullable=False)
    gene_intersects_json_pw_mapped = Column(String(length=10000), nullable=False)
    db_sql_url = Column(String(length=256), nullable=False)


class VersionInfo(Base):
    __tablename__ = "version_info"
    ramp_db_version = Column(String(length=16), nullable=False, primary_key=True)
    db_mod_date = Column(Date, nullable=False)
    status = Column(String(length=16), nullable=False)
    data_source_id = Column(String(length=32), nullable=False, primary_key=True)
    data_source_name = Column(String(length=128), nullable=False)
    data_source_url = Column(String(length=128), nullable=False)
    data_source_version = Column(String(length=128), nullable=False)

