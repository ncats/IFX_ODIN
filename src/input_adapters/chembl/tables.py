from sqlalchemy import (
    Column, BigInteger, String, ForeignKey, Integer, Index, UniqueConstraint, Text, DECIMAL, Float, SmallInteger,
    PrimaryKeyConstraint, DateTime
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class CompoundRecords(Base):
    __tablename__ = 'compound_records'

    record_id = Column(BigInteger, primary_key=True, nullable=False, comment='Unique ID for a compound/record')
    molregno = Column(BigInteger, ForeignKey('molecule_dictionary.molregno', ondelete='CASCADE'),
                      comment='Foreign key to compounds table (compound structure)')
    doc_id = Column(BigInteger, ForeignKey('docs.doc_id', ondelete='CASCADE'), nullable=False,
                    comment='Foreign key to documents table')
    compound_key = Column(String(250), nullable=True,
                          comment='Key text identifying this compound in the scientific document')
    compound_name = Column(String(4000), nullable=True,
                           comment='Name of this compound recorded in the scientific document')
    src_id = Column(Integer, ForeignKey('source.src_id', ondelete='CASCADE'), nullable=False,
                    comment='Foreign key to source table')
    src_compound_id = Column(String(150), nullable=True,
                             comment='Identifier for the compound in the source database (e.g., pubchem SID)')
    cidx = Column(String(200), nullable=False, comment='The Depositor Defined Compound Identifier.')

    UniqueConstraint('record_id', name='pk_comp_rec_recid')
    Index('fk_comp_rec_molregno', 'molregno')
    Index('idx_comp_rec_cidx', 'cidx')
    Index('fk_comp_rec_docid', 'doc_id')
    Index('idx_comp_rec_ckey', 'compound_key')
    Index('idx_comp_rec_srccpid', 'src_compound_id')
    Index('idx_comp_rec_srcid', 'src_id')


class Activities(Base):
    __tablename__ = 'activities'

    activity_id = Column(BigInteger, primary_key=True, nullable=False, comment='Unique ID for the activity row')
    assay_id = Column(BigInteger, ForeignKey('assays.assay_id', ondelete='CASCADE'), nullable=False,
                      comment='Foreign key to the assays table (containing the assay description)')
    doc_id = Column(BigInteger, ForeignKey('docs.doc_id', ondelete='CASCADE'), nullable=True,
                    comment='Foreign key to documents table (for quick lookup of publication details)')
    record_id = Column(BigInteger, ForeignKey('compound_records.record_id', ondelete='CASCADE'), nullable=False,
                       comment='Foreign key to the compound_records table (containing information on the compound tested)')
    molregno = Column(BigInteger, ForeignKey('molecule_dictionary.molregno', ondelete='CASCADE'), nullable=True,
                      comment='Foreign key to compounds table')
    standard_relation = Column(String(50), nullable=True,
                               comment='Symbol constraining the activity value (e.g. >, <, =)')
    standard_value = Column(DECIMAL(64, 30), nullable=True, comment='Transformed to common units')
    standard_units = Column(String(100), nullable=True, comment="Selected 'Standard' units for data type")
    standard_flag = Column(Integer, nullable=True,
                           comment='Shows whether the standardised columns have been curated/set (1) or default to the published data (0)')
    standard_type = Column(String(250), nullable=True, comment='Standardised version of the published_activity_type')
    activity_comment = Column(String(4000), nullable=True, comment='Additional comments about non-numeric activities')
    data_validity_comment = Column(String(30),
                                   ForeignKey('data_validity_lookup.data_validity_comment', ondelete='CASCADE'),
                                   nullable=True, comment='Comment reflecting whether the values are likely correct')
    potential_duplicate = Column(Integer, nullable=True,
                                 comment='Indicates whether the value is likely a repeat citation')
    pchembl_value = Column(DECIMAL(4, 2), nullable=True, comment='Negative log of selected activity values')
    bao_endpoint = Column(String(11), ForeignKey('bioassay_ontology.bao_id', ondelete='CASCADE'), nullable=True,
                          comment='BioAssay Ontology ID')
    uo_units = Column(String(10), nullable=True, comment='Unit Ontology ID based on standard_units')
    qudt_units = Column(String(70), nullable=True, comment='QUDT Ontology ID based on standard_units')
    toid = Column(Integer, nullable=True, comment='Test Occasion Identifier')
    upper_value = Column(DECIMAL(64, 30), nullable=True, comment='Highest value of the range for the activity')
    standard_upper_value = Column(DECIMAL(64, 30), nullable=True, comment='Standardised version of the highest value')
    src_id = Column(Integer, ForeignKey('source.src_id', ondelete='CASCADE'), nullable=True,
                    comment='Source of the activity value')
    type = Column(String(250), nullable=False, comment='Type of end-point measurement')
    relation = Column(String(50), nullable=True, comment='Symbol constraining the activity value')
    value = Column(DECIMAL(64, 30), nullable=True, comment='Datapoint value as it appears in the dataset')
    units = Column(String(100), nullable=True, comment='Units of measurement in the dataset')
    text_value = Column(String(1000), nullable=True, comment='Additional information about the measurement')
    standard_text_value = Column(String(1000), nullable=True, comment='Standardized additional information')
    action_type = Column(String(50), ForeignKey('action_type.action_type', ondelete='CASCADE'), nullable=True,
                         comment='Effect of the compound on its target')

    Index('fk_data_val_comm', 'data_validity_comment')
    Index('fk_act_action_type', 'action_type')
    Index('fk_act_bao_endpoint', 'bao_endpoint')
    Index('fk_act_doc_id', 'doc_id')
    Index('idx_act_std_type', 'standard_type')
    Index('idx_act_upper', 'upper_value')
    Index('fk_act_molregno', 'molregno')
    Index('idx_act_std_unit', 'standard_units')
    Index('idx_act_val', 'value')
    Index('fk_act_record_id', 'record_id')
    Index('idx_act_std_upper', 'standard_upper_value')
    Index('idx_acc_relation', 'standard_relation')
    Index('idx_act_std_val', 'standard_value')
    Index('fk_act_assay_id', 'assay_id')
    Index('idx_act_pchembl', 'pchembl_value')
    Index('idx_act_text', 'text_value')
    Index('idx_act_units', 'units')
    Index('idx_act_rel', 'relation')
    Index('idx_act_type', 'type')
    Index('idx_act_src_id', 'src_id')
    Index('idx_act_std_text', 'standard_text_value')


class CompoundStructures(Base):
    __tablename__ = 'compound_structures'

    molregno = Column(BigInteger, primary_key=True, nullable=False,
                      comment='Internal Primary Key for the compound structure and foreign key to molecule_dictionary table')
    molfile = Column(Text, comment='MDL Connection table representation of compound')
    standard_inchi = Column(String(4000), nullable=True, comment='IUPAC standard InChI for the compound')
    standard_inchi_key = Column(String(27), nullable=False, unique=True,
                                comment='IUPAC standard InChI key for the compound')
    canonical_smiles = Column(String(4000), nullable=True, comment='Canonical smiles, generated using RDKit')

    UniqueConstraint('molregno', name='compound_structures_pk')
    Index('idx_cmpdstr_stdkey', 'standard_inchi_key')
    ForeignKey('molecule_dictionary.molregno', ondelete='CASCADE', name='fk_cmpdstr_molregno')

class MoleculeDictionary(Base):
    __tablename__ = 'molecule_dictionary'

    molregno = Column(BigInteger, primary_key=True, nullable=False,
                      comment='Internal Primary Key for the molecule')
    pref_name = Column(String(255), nullable=True,
                       comment='Preferred name for the molecule')
    chembl_id = Column(String(20), nullable=False, unique=True,
                       comment='ChEMBL identifier for this compound (for use on web interface etc)')
    max_phase = Column(Float(precision=2), nullable=True,
                       comment='Maximum phase of development reached for the compound across all indications')
    therapeutic_flag = Column(SmallInteger, nullable=False,
                              comment='Indicates therapeutic application (1 = yes, 0 = default)')
    dosed_ingredient = Column(SmallInteger, nullable=False,
                              comment='Indicates if the drug is dosed in this form (1 = yes, 0 = default)')
    structure_type = Column(String(10), nullable=False,
                            comment='Indicates structure type (MOL/SEQ/NONE)')
    chebi_par_id = Column(BigInteger, nullable=True,
                          comment='Preferred ChEBI ID for the compound')
    molecule_type = Column(String(30), nullable=True,
                           comment='Type of molecule (Small molecule, Protein, etc.)')
    first_approval = Column(Integer, nullable=True,
                            comment='Earliest known approval year for the drug')
    oral = Column(SmallInteger, nullable=False,
                  comment='Indicates oral administration (1 = yes, 0 = default)')
    parenteral = Column(SmallInteger, nullable=False,
                        comment='Indicates parenteral administration (1 = yes, 0 = default)')
    topical = Column(SmallInteger, nullable=False,
                     comment='Indicates topical administration (1 = yes, 0 = default)')
    black_box_warning = Column(SmallInteger, nullable=False,
                               comment='Indicates black box warning (1 = yes, 0 = default)')
    first_in_class = Column(SmallInteger, nullable=False,
                            comment='Indicates first approved drug of its class')
    chirality = Column(SmallInteger, nullable=False,
                       comment='Indicates drug chirality (-1 to 2)')
    prodrug = Column(SmallInteger, nullable=False,
                     comment='Indicates prodrug status (-1 to 1)')
    inorganic_flag = Column(SmallInteger, nullable=False,
                            comment='Indicates inorganic molecule (-1 to 1)')
    usan_year = Column(Integer, nullable=True,
                       comment='Year of USAN/INN name application')
    availability_type = Column(SmallInteger, nullable=True,
                               comment='Drug availability type (-2 to 2)')
    usan_stem = Column(String(50), nullable=True,
                       comment='USAN stem, where applicable')
    polymer_flag = Column(SmallInteger, nullable=True,
                          comment='Indicates small molecule polymer (1 = yes, 0 = default)')
    usan_substem = Column(String(50), nullable=True,
                          comment='USAN substem, where applicable')
    usan_stem_definition = Column(String(1000), nullable=True,
                                  comment='Definition of the USAN stem')
    indication_class = Column(String(1000), nullable=True,
                              comment='Indication class assigned to a drug')
    withdrawn_flag = Column(SmallInteger, nullable=False,
                            comment='Indicates approved drug withdrawal for toxicity reasons')
    chemical_probe = Column(SmallInteger, nullable=False,
                            comment='Indicates chemical probe status (1 = yes, 0 = default)')
    natural_product = Column(SmallInteger, nullable=True,
                             comment='Indicates natural product status (1 = yes, 0 = default)')

    UniqueConstraint('chembl_id', name='uk_moldict_chemblid')
    Index('idx_moldict_pref_name', 'pref_name')
    Index('idx_moldict_ther_flag', 'therapeutic_flag')
    Index('idx_moldict_max_phase', 'max_phase')
    ForeignKey('chembl_id_lookup.chembl_id', ondelete='CASCADE', name='fk_moldict_chembl_id')

class Assays(Base):
    __tablename__ = 'assays'

    assay_id = Column(BigInteger, primary_key=True, comment="Unique ID for the assay")
    doc_id = Column(BigInteger, ForeignKey('docs.doc_id', ondelete="CASCADE"), nullable=False, comment="Foreign key to documents table")
    description = Column(String(4000), comment="Description of the reported assay")
    assay_type = Column(String(1), ForeignKey('assay_type.assay_type', ondelete="CASCADE"), comment="Assay classification")
    assay_test_type = Column(String(20), comment="Type of assay system (i.e., in vivo or in vitro)")
    assay_category = Column(String(20), comment="screening, confirmatory, summary, etc.")
    assay_organism = Column(String(250), comment="Name of the organism for the assay system")
    assay_tax_id = Column(BigInteger, comment="NCBI tax ID for the assay organism")
    assay_strain = Column(String(200), comment="Name of specific strain of the assay organism")
    assay_tissue = Column(String(100), comment="Name of tissue used in the assay system")
    assay_cell_type = Column(String(100), comment="Name of cell type or cell line used")
    assay_subcellular_fraction = Column(String(100), comment="Name of subcellular fraction used")
    tid = Column(BigInteger, ForeignKey('target_dictionary.tid', ondelete="CASCADE"), comment="Target identifier")
    relationship_type = Column(String(1), ForeignKey('relationship_type.relationship_type', ondelete="CASCADE"), comment="Relationship between reported and assigned targets")
    confidence_score = Column(Integer, ForeignKey('confidence_score_lookup.confidence_score', ondelete="CASCADE"), comment="Confidence score")
    curated_by = Column(String(32), ForeignKey('curation_lookup.curated_by', ondelete="CASCADE"), comment="Curation level")
    src_id = Column(Integer, ForeignKey('source.src_id', ondelete="CASCADE"), nullable=False, comment="Foreign key to source table")
    src_assay_id = Column(String(50), comment="Identifier for the assay in the source database")
    chembl_id = Column(String(20), ForeignKey('chembl_id_lookup.chembl_id', ondelete="CASCADE"), nullable=False, comment="ChEMBL identifier for this assay")
    cell_id = Column(BigInteger, ForeignKey('cell_dictionary.cell_id', ondelete="CASCADE"), comment="Foreign key to cell dictionary")
    bao_format = Column(String(11), ForeignKey('bioassay_ontology.bao_id', ondelete="CASCADE"), comment="BioAssay Ontology format type ID")
    tissue_id = Column(BigInteger, ForeignKey('tissue_dictionary.tissue_id', ondelete="CASCADE"), comment="Foreign key to tissue dictionary")
    variant_id = Column(BigInteger, ForeignKey('variant_sequences.variant_id', ondelete="CASCADE"), comment="Foreign key to variant_sequences")
    aidx = Column(String(200), nullable=False, comment="Depositor Defined Assay Identifier")

    UniqueConstraint('chembl_id', name='uk_assays_chemblid')
    Index('idx_assays_chembl_id', 'chembl_id')
    Index('fk_assays_confscore', 'confidence_score')
    Index('fk_assays_reltype', 'relationship_type')
    Index('fk_assays_tid', 'tid')
    Index('fk_assays_cur_by', 'curated_by')
    Index('fk_assays_cell_id', 'cell_id')
    Index('fk_assays_tissue_id', 'tissue_id')
    Index('fk_assays_variant_id', 'variant_id')
    Index('idx_assay_assay_id', 'assay_type')
    Index('idx_assays_doc_id', 'doc_id')
    Index('idx_assays_src_id', 'src_id')
    Index('tmp_bao_format', 'bao_format')


class TargetDictionary(Base):
    __tablename__ = "target_dictionary"
    tid = Column(BigInteger, primary_key=True, comment="Unique ID for the target")
    target_type = Column(
        String(30),
        ForeignKey("target_type.target_type", ondelete="CASCADE"),
        comment="Describes whether target is a protein, an organism, a tissue etc. Foreign key to TARGET_TYPE table."
    )
    pref_name = Column(
        String(200),
        nullable=False,
        comment="Preferred target name: manually curated"
    )
    tax_id = Column(
        BigInteger,
        comment="NCBI taxonomy id of target"
    )
    organism = Column(
        String(150),
        comment="Source organism of molecular target or tissue, or the target organism if compound activity is reported in an organism rather than a protein or tissue"
    )
    chembl_id = Column(
        String(20),
        nullable=False,
        unique=True,
        comment="ChEMBL identifier for this target (for use on web interface etc)"
    )
    species_group_flag = Column(
        SmallInteger,
        nullable=False,
        comment="Flag to indicate whether the target represents a group of species, rather than an individual species (e.g., 'Bacterial DHFR'). Where set to 1, indicates that any associated target components will be a representative, rather than a comprehensive set."
    )


class TargetComponents(Base):
    __tablename__ = 'target_components'

    targcomp_id = Column(BigInteger, primary_key=True, autoincrement=True, comment='Primary key.')
    tid = Column(BigInteger, ForeignKey('target_dictionary.tid', ondelete='CASCADE'), nullable=False, comment='Foreign key to the target_dictionary, indicating the target to which the components belong.')
    component_id = Column(BigInteger, ForeignKey('component_sequences.component_id', ondelete='CASCADE'), nullable=False, comment='Foreign key to the component_sequences table, indicating which components belong to the target.')
    homologue = Column(SmallInteger, nullable=False, comment='Indicates that the given component is a homologue of the correct component (e.g., from a different species) when set to 1.')
    PrimaryKeyConstraint('targcomp_id')

class ComponentSequence(Base):
    __tablename__ = 'component_sequences'

    component_id = Column(BigInteger, primary_key=True, autoincrement=False, comment='Primary key. Unique identifier for the component.')
    component_type = Column(String(50), nullable=True, comment="Type of molecular component represented (e.g., 'PROTEIN','DNA','RNA').")
    accession = Column(String(25), nullable=True, comment="Accession for the sequence in the source database from which it was taken (e.g., UniProt accession for proteins).")
    sequence = Column(Text, nullable=True, comment="A representative sequence for the molecular component, as given in the source sequence database.")
    sequence_md5sum = Column(String(32), nullable=True, comment="MD5 checksum of the sequence.")
    description = Column(String(200), nullable=True, comment="Description/name for the molecular component, usually taken from the source sequence database.")
    tax_id = Column(BigInteger, nullable=True, comment="NCBI tax ID for the sequence in the source database (species the protein/nucleic acid sequence comes from).")
    organism = Column(String(150), nullable=True, comment="Name of the organism the sequence comes from.")
    db_source = Column(String(25), nullable=True, comment="The name of the source sequence database from which sequences/accessions are taken.")
    db_version = Column(String(10), nullable=True, comment="The version of the source sequence database from which sequences/accessions were last updated.")

    UniqueConstraint('accession', name='uk_targcomp_seqs_acc')


class Docs(Base):
    __tablename__ = 'docs'

    doc_id = Column(BigInteger, primary_key=True, autoincrement=False, comment='Unique ID for the document')
    journal = Column(String(50), nullable=True, comment="Abbreviated journal name for an article")
    year = Column(Integer, nullable=True, comment="Year of journal article publication")
    volume = Column(String(50), nullable=True, comment="Volume of journal article")
    issue = Column(String(50), nullable=True, comment="Issue of journal article")
    first_page = Column(String(50), nullable=True, comment="First page number of journal article")
    last_page = Column(String(50), nullable=True, comment="Last page number of journal article")
    pubmed_id = Column(BigInteger, nullable=True, comment="NIH pubmed record ID, where available")
    doi = Column(String(100), nullable=True, comment="Digital object identifier for this reference")
    chembl_id = Column(String(20), nullable=False, comment="ChEMBL identifier for this document")
    title = Column(String(500), nullable=True, comment="Document title (e.g., Publication title or description of dataset)")
    doc_type = Column(String(50), nullable=False, comment="Type of the document (e.g., Publication, Deposited dataset)")
    authors = Column(String(4000), nullable=True, comment="For a deposited dataset, the authors carrying out the screening and/or submitting the dataset.")
    abstract = Column(Text, nullable=True, comment="For a deposited dataset, a brief description of the dataset.")
    patent_id = Column(String(20), nullable=True, comment="Patent ID for this document")
    ridx = Column(String(200), nullable=False, comment="The Depositor Defined Reference Identifier")
    src_id = Column(Integer, ForeignKey('source.src_id'), nullable=False, comment="Foreign key to Source table, indicating the source of this document")
    chembl_release_id = Column(Integer, ForeignKey('chembl_release.chembl_release_id'), nullable=True, comment="Foreign key to chembl_release table")

    UniqueConstraint('chembl_id', name='uk_docs_chemblid')

class Version(Base):
    __tablename__ = 'version'

    name = Column(String(50), primary_key=True, nullable=False,
                  comment='Name of release version')
    creation_date = Column(DateTime, nullable=True,
                           comment='Date database created')
    comments = Column(String(2000), nullable=True,
                      comment='Description of release version')
