from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import List, Optional, Dict

from src.models.node import Node, Relationship


@dataclass
class Ligand(Node):
    name: Optional[str] = None
    smiles: Optional[str] = None
    originator: Optional[str] = None
    originator_uri: Optional[str] = None
    originator_comment: Optional[str] = None
    compound_description: Optional[str] = None
    description_uri: Optional[str] = None
    description_comment: Optional[str] = None
    in_vivo_use_guide: Optional[str] = None
    in_vivo_use_route: Optional[str] = None
    in_vivo_use_route_other: Optional[str] = None
    in_vivo_uri: Optional[str] = None
    in_vivo_comment: Optional[str] = None
    in_vitro_use_guide: Optional[str] = None
    emolecules_id: Optional[str] = None
    bindingdb_id: Optional[str] = None
    in_vitro_uri: Optional[str] = None
    in_vitro_comment: Optional[str] = None
    structure_alert: Optional[bool] = None
    cns: Optional[str] = None
    cns_uri: Optional[str] = None
    cns_comment: Optional[str] = None
    curated_by: Optional[str] = None
    drugbank_id: Optional[str] = None
    molport_id: Optional[str] = None
    modified_datetime: Optional[datetime] = None
    qcd: Optional[bool] = None
    unii: Optional[str] = None
    cas: Optional[str] = None
    chembl_id: Optional[str] = None
    kegg_id: Optional[str] = None
    iupac: Optional[str] = None
    cid: Optional[int] = None
    wikiurl: Optional[str] = None
    chemspider_id: Optional[int] = None
    compound_id: Optional[int] = None
    synonyms: Optional[List[str]] = None
    pmids: Optional[List[int]] = None
    patent: Optional[List[str]] = None

@dataclass
class Database(Node):
    pass

@dataclass
class Vendor(Node):
    pass

@dataclass
class LigandSourceRelationship(Relationship):
    start_node: Ligand
    substance_id: str = None
    substance_url: Optional[str] = None

@dataclass
class LigandDatabaseRelationship(LigandSourceRelationship):
    end_node: Database

@dataclass
class LigandVendorRelationship(LigandSourceRelationship):
    end_node: Vendor


@dataclass
class Condition(Node):
    name: Optional[str] = None
    do_id: Optional[int] = None
    do_label: Optional[str] = None

@dataclass
class LigandConditionDetails:
    name: Optional[str] = None
    condition_uri: Optional[str] = None
    condition_comment: Optional[str] = None
    condition_treatment_modality: Optional[str] = None
    condition_do_imprecise: Optional[bool] = None
    condition_mesh_imprecise: Optional[bool] = None
    condition_highest_phase: Optional[str] = None
    highest_phase_discontinued: Optional[bool] = None
    highest_phase_uri: Optional[str] = None
    highest_phase_comment: Optional[str] = None
    is_product_manual: Optional[bool] = None
    product_discontinued: Optional[bool] = None
    is_product_date_unknown: Optional[bool] = None
    product_name: Optional[str] = None
    product_date: Optional[str] = None
    is_fda_use: Optional[bool] = None
    fda_use: Optional[str] = None
    fda_use_uri: Optional[str] = None
    fda_use_comment: Optional[str] = None
    offlabel_use: Optional[str] = None
    offlabel_use_uri: Optional[str] = None
    offlabel_use_comment: Optional[str] = None
    clinical_trial: Optional[str] = None
    mesh_id: Optional[str] = None
    mesh_label: Optional[str] = None

    def to_dict(self) -> Dict[str, str]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict):
        return cls(**data)


@dataclass
class LigandConditionRelationship(Relationship):
    start_node: Ligand
    end_node: Condition
    details: List[LigandConditionDetails] = field(default_factory=list)






#
# "conditions":[
#     {
#         "condition_id":6,
#         "name":"Myelodysplasia",
#         "condition_uri":"https://clinicaltrials.gov/ct2/show/NCT02765997",
#         "condition_comment":"De-differentiated cancer cell",
#         "product_name":"Unknown",
#         "condition_highest_phase":"Phase II ",
#         "highest_phase_uri":"https://clinicaltrials.gov/ct2/show/NCT02765997",
#         "fda_use":"Unknown",
#         "offlabel_use":"Unknown",
#         "offlabel_use_uri":"Unknown",
#         "offlabel_use_comment":" ",
#         "clinical_trial":"NCT02765997",
#         "condition_treatment_modality":"Primary",
#         "condition_do_imprecise":false,
#         "condition_mesh_imprecise":false,
#         "mesh_id":"D009190",
#         "mesh_label":"Myelodysplastic Syndromes",
#         "do_id":"50908",
#         "do_label":"myelodysplastic syndrome",
#         "is_fda_use":false,
#         "fda_use_uri":"Unknown",
#         "is_product_manual":false,
#         "highest_phase_discontinued":false,
#         "product_discontinued":false,
#         "is_product_date_unknown":true
#     },
# ],
# "targets":[
#     {
#         "target_id":4,
#         "primary_target_id":"P35869",
#         "target_primary_target_type":"UniProt",
#         "primary_target_uri":"http://www.ncbi.nlm.nih.gov/pubmed/20688981",
#         "target_primary_potency_type":"IC50",
#         "target_primary_potency_value":"127",
#         "target_primary_potency_dimensions":"nM",
#         "primary_potency_uri":"http://www.ncbi.nlm.nih.gov/pubmed/20688981",
#         "target_pharmacology":"Antagonist",
#         "primary_target_label":"Aryl hydrocarbon receptor",
#         "primary_target_gene_symbol":"AHR",
#         "primary_target_gene_id":"196",
#         "primary_target_organism":"Homo sapiens (Human)"
#     },
# ],
# "ddi":[
#     {
#         "ddi_id":20698,
#         "ddi_concentration":"2.6837",
#         "ddi_relation":"inhibitor",
#         "ddi_reported_magnitude":"yes",
#         "ddi_target":"CYP2C9",
#         "ddi_type":"IC50",
#         "ddi_url":"https://pubchem.ncbi.nlm.nih.gov/bioassay/1645842#sid=440681310"
#     },
# ],
# "sourcing":[
#     {
#         "sourcing_id":1,
#         "sourcing_vendor_name":"1PlusChem LLC",
#         "sourcing_vendor_substance_url":"https://www.1pchem.com/search?query=1P000JVS",
#         "sourcing_vendor_type":"vendor",
#         "sourcing_vendor_substance_id":"1P000JVS"
#     }
# ]
