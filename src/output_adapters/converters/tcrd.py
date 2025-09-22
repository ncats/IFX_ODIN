import hashlib
from typing import Union, List

from src.constants import Prefix
from src.input_adapters.pharos_mysql.new_tables import Protein, Xref, Alias, Target, TDL_info, T2TC, GO, GOParent, GoA, \
    GeneRif, GeneRif2Pubmed, Protein2Pubmed, Ligand, LigandActivity
from src.models.go_term import GoType
from src.models.node import EquivalentId


def goterm_converter(obj: dict) -> GO:
    def get_namespace(obj: dict):
        if obj['type'] == GoType.Process:
            return 'biological_process'
        if obj['type'] == GoType.Function:
            return 'molecular_function'
        if obj['type'] == GoType.Component:
            return 'cellular_component'
        return 'unknown'
    return GO(
        go_id=obj['id'],
        name=obj['term'],
        namespace=get_namespace(obj),
        def_=obj['definition'],
        provenance = obj['provenance'])


def goterm_parent_converter(obj: dict) -> GOParent:
    return GOParent(
        go_id=obj['start_id'],
        parent_id=obj['end_id'],
        provenance = obj['provenance'])


def protein_converter(obj: dict) -> Protein:
    gene_ids = [p for p in obj['xref'] if p.startswith('NCBIGene:')]
    if len(gene_ids) > 0:
        gene_id = gene_ids[0].split(':')[-1]
    else:
        gene_id = None
    ensembl_ids = [p for p in obj['xref'] if p.startswith('ENSEMBL:ENSP')]
    if len(ensembl_ids) > 0 and gene_id is None:
        string_id = ensembl_ids[0].split(':')[-1]
    else:
        string_id = None

    return Protein(
        id = obj['id'],
        description = obj['name'],
        uniprot = obj['uniprot_id'],
        sym = obj['symbol'],
        geneid = gene_id,
        stringid = string_id,
        seq = obj['sequence'],
        provenance = obj['provenance'],
        preferred_symbol = obj['preferred_symbol']
    )

def tdl_info_converter(obj: dict) -> TDL_info:
    tdl_infos = []
    if 'antibody_count' in obj and obj['antibody_count'] is not None:
        antibody_count = int(obj['antibody_count'])
        if antibody_count > 0:
            tdl_infos.append(TDL_info(
                itype="Ab Count",
                protein_id=obj['id'],
                integer_value=antibody_count
            ))
    if 'pm_score' in obj and obj['pm_score'] is not None:
        pm_score = float(obj['pm_score'])
        tdl_infos.append(TDL_info(
            itype="JensenLab PubMed Score",
            protein_id=obj['id'],
            number_value=pm_score
        ))
    return tdl_infos

def protein_alias_converter(obj: dict) -> List[Union[Xref, Alias]]:
    aliases = []
    protein_id = obj['id']

    ids = [EquivalentId.parse(x) for x in obj['xref']]

    symbols = [x for x in ids if x.type in (Prefix.OldSymbol, Prefix.Symbol)]
    uniprot_ids = [x for x in ids if x.type == Prefix.UniProtKB]
    ncbi_ids = [x for x in ids if x.type == Prefix.NCBIGene]

    for s in symbols:
        aliases.append(Alias(protein_id=protein_id, type='symbol', value=s.id))
    for u in uniprot_ids:
        aliases.append(Alias(protein_id=protein_id, type='uniprot', value=u.id))
    for n in ncbi_ids:
        aliases.append(Alias(protein_id=protein_id, type='NCBI Gene ID', value=n.id))

    for a in ids:
        if a.type == Prefix.ENSEMBL:
            typeText = "Ensembl"
        else:
            typeText = a.type.value
        aliases.append(
            Xref(protein_id=protein_id,
                       xtype=typeText,
                       value=a.id)
        )
    return aliases

def t2tc_converter(obj: dict) -> T2TC:
    return T2TC(
        target_id=obj['id'],
        protein_id=obj['id'],
        provenance = obj['provenance'])


def target_converter(obj: dict) -> Target:
    return Target(
        id = obj['id'],
        name = obj['name'],
        ttype = 'Single Protein',
        fam = obj['idg_family'],
        tdl = obj['tdl'],
        provenance = obj['provenance']
    )

def goa_converter(obj: dict) -> List[GoA]:
    return [
        GoA(
            protein_id = obj['start_id'],
            go_id = obj['end_id'],
            evidence = e['abbreviation'],
            goeco = e['evidence'],
            assigned_by = e['assigned_by'],
            go_term = f"{obj['end_node']['type']}:{obj['end_node']['term']}",
            go_type = {'C': 'Component', 'P': 'Process', 'F': 'Function'}.get(obj['end_node']['type'], obj['end_node']['type']),
            go_term_text = obj['end_node']['term'],
            provenance = obj['provenance']
        )
        for e in obj['evidence']]

def get_generif_hash(obj: dict) -> str:
    data_to_hash = f"{obj['start_id']}-{obj['gene_id']}-{obj['date']}-{obj['end_node']['text']}"
    hashed_id = hashlib.sha256(data_to_hash.encode('utf-8')).hexdigest()
    return hashed_id

def generif_converter(obj: dict) -> GeneRif:
    hashed_id = get_generif_hash(obj)

    return GeneRif(
        id=hashed_id,
        protein_id=obj['start_id'],
        gene_id=obj['gene_id'],
        date=obj['date'],
        text=obj['end_node']['text'],
        provenance=obj['provenance']
    )
setattr(generif_converter, 'merge_anyway', True)

def generif_assoc_converter(obj: dict) -> List[GeneRif2Pubmed]:
    hashed_id = get_generif_hash(obj)
    pubmed_links = []
    for pmid in obj['pmids']:
        pubmed_link = GeneRif2Pubmed(
            generif_id=hashed_id,
            pubmed_id=pmid,
            provenance=obj['provenance']
        )
        pubmed_links.append(pubmed_link)

    return pubmed_links
setattr(generif_assoc_converter, 'merge_anyway', True)

def p2p_converter(obj: dict) -> List[Protein2Pubmed]:
    pubmed_links = []
    for pmid in obj['pmids']:
        pubmed_link = Protein2Pubmed(
            protein_id=obj['start_id'],
            pubmed_id=pmid,
            gene_id=obj['gene_id'],
            source='NCBI',
            provenance=obj['provenance']
        )
        pubmed_links.append(pubmed_link)
    return pubmed_links

setattr(p2p_converter, 'merge_anyway', True)


# actCnt = Column(Integer)
# targetCount = Column(Integer)
# pt = Column(String(128))

def ligand_converter(obj: dict) -> Ligand:
    xrefs = [
        EquivalentId.parse(eq_id)
        for eq_id in (obj.get('xref') or [])
    ]
    PubChemIDs = [x.id for x in xrefs if x.type == Prefix.PUBCHEM_COMPOUND]
    ChemblIDs = [x.id for x in xrefs if x.type == Prefix.CHEMBL_COMPOUND]
    IUPHARIDs = [x.id for x in xrefs if x.type == Prefix.GTOPDB]
    DCIDs = [x.id for x in xrefs if x.type == Prefix.DrugCentral]
    UNIIs = [x.id for x in xrefs if x.type == Prefix.UNII]
    return Ligand(
        id = obj.get('id'),
        identifier=obj.get('id'),
        name = obj.get('name'),
        isDrug = obj.get('isDrug', False),
        smiles=obj.get('smiles'),
        description=obj.get('description'),
        PubChem=",".join(PubChemIDs),
        ChEMBL=",".join(ChemblIDs),
        guide_to_pharmacology=",".join(IUPHARIDs),
        DrugCentral=",".join(DCIDs),
        unii=",".join(UNIIs),
        provenance=obj['provenance']
    )



    # id = Column(Integer, primary_key=True)
    # ncats_ligand_id = Column(String(255), ForeignKey('ncats_ligands.id'), nullable=False)
    # target_id = Column(String(18), ForeignKey('target.id'), nullable=False)

    # act_value = Column(Float)
    # act_type = Column(String(255))
    # action_type = Column(String(255))
    # reference = Column(Text)
    # reference_source = Column(String(255))
    # pubmed_ids = Column(Text)  # pipe delimited list


# :@dataclass
# @facets(category_fields=["act_type", "has_moa", "action_type"],
#         numeric_fields=["act_value"])
# class ActivityDetails:
#     ref_id: Optional[int] = None
#     act_value: Optional[float] = None
#     act_type: Optional[str] = None
#     action_type: Optional[str] = None
#     has_moa: Optional[bool] = None
#     reference: Optional[str] = None
#     act_pmids: Optional[List[int]] = field(default_factory=list)
#     moa_pmid: Optional[int] = None
#     act_source: Optional[str] = None
#     moa_source: Optional[str] = None
#     assay_type: Optional[str] = None
#     comment: Optional[str] = None

def ligand_edge_converter(obj: dict) -> List[LigandActivity]:
    activity_objects = []

    for detail in obj.get('details', []):
        pubmed_ids = detail.get('act_pmids') or []
        if detail.get('moa_pmid'):
            pubmed_ids.append(detail.get('moa_pmid'))
        pubmed_ids = [str(i) for i in pubmed_ids]
        activity_object = LigandActivity(
            ncats_ligand_id = obj['end_id'],
            target_id = obj['start_id'],
            act_value=detail.get('act_value'),
            act_type=detail.get('act_type'),
            action_type=detail.get('action_type'),
            reference=detail.get('reference'),
            pubmed_ids="|".join(list(set(pubmed_ids))),
            provenance=obj['provenance']
        )
        activity_objects.append(activity_object)

    return activity_objects
