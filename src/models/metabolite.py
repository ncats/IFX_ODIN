from dataclasses import dataclass, field
from src.models.analyte import Analyte
from src.models.gene import Gene
from src.models.reaction import Reaction


@dataclass
class Metabolite(Analyte):
    chem_data_source: [str] = field(default_factory=list)
    chem_source_id: [str] = field(default_factory=list)
    iso_smiles: [str] = field(default_factory=list)
    inchi_key_prefix: [str] = field(default_factory=list)
    inchi_key: [str] = field(default_factory=list)
    inchi: [str] = field(default_factory=list)
    mw: [float] = field(default_factory=list)
    monoisotop_mass: [float] = field(default_factory=list)
    common_name: [str] = field(default_factory=list)
    mol_formula: [str] = field(default_factory=list)



@dataclass
class MetaboliteGeneRelationship:
    metabolite: Metabolite
    gene: Gene

@dataclass
class MetaboliteReactionRelationship:
    metabolite: Metabolite
    reaction: Reaction
    substrate_product: int
    is_cofactor: bool