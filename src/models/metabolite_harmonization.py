from dataclasses import dataclass, field
from typing import List, Optional

from src.core.decorators import facets, indexed, search
from src.models.node import Node, Relationship


@dataclass(frozen=True)
class MetaboliteName:
    value: str
    source: str
    source_field: Optional[str] = None

    def to_dict(self):
        return {
            "value": self.value,
            "source": self.source,
            "source_field": self.source_field,
        }


@dataclass(frozen=True)
class MetaboliteChemProps:
    source: str
    source_id: str
    iso_smiles: Optional[str] = None
    canonical_smiles: Optional[str] = None
    isomeric_smiles: Optional[str] = None
    inchi_key_prefix: Optional[str] = None
    inchi_key: Optional[str] = None
    inchi: Optional[str] = None
    mw: Optional[str] = None
    monoisotopic_mass: Optional[str] = None
    common_name: Optional[str] = None
    iupac_name: Optional[str] = None
    molecular_formula: Optional[str] = None

    def to_dict(self):
        return {
            "source": self.source,
            "source_id": self.source_id,
            "iso_smiles": self.iso_smiles,
            "canonical_smiles": self.canonical_smiles,
            "isomeric_smiles": self.isomeric_smiles,
            "inchi_key_prefix": self.inchi_key_prefix,
            "inchi_key": self.inchi_key,
            "inchi": self.inchi,
            "mw": self.mw,
            "monoisotopic_mass": self.monoisotopic_mass,
            "common_name": self.common_name,
            "iupac_name": self.iupac_name,
            "molecular_formula": self.molecular_formula,
        }


@dataclass
@indexed(fields=["id"])
@facets(category_fields=["prefix"])
@search(text_fields=["id", "names", "synonyms"])
class MetaboliteIdentifier(Node):
    prefix: Optional[str] = None
    names: List[MetaboliteName] = field(default_factory=list)
    synonyms: List[MetaboliteName] = field(default_factory=list)
    chem_props: List[MetaboliteChemProps] = field(default_factory=list)

    def __post_init__(self):
        if self.prefix is None and self.id:
            self.prefix = self.id.split(":", 1)[0]


@dataclass(frozen=True)
class MetaboliteIdentifierMappingDetail:
    source: str
    source_field: str
    source_id: str

    def to_dict(self):
        return {
            "source": self.source,
            "source_field": self.source_field,
            "source_id": self.source_id,
        }


@dataclass
class MetaboliteIdentifierMappingEdge(Relationship):
    start_node: MetaboliteIdentifier
    end_node: MetaboliteIdentifier
    details: List[MetaboliteIdentifierMappingDetail] = field(default_factory=list)


@dataclass(frozen=True)
class HmdbGoClassification:
    category: Optional[str] = None
    description: Optional[str] = None
    go_id: Optional[str] = None

    def to_dict(self):
        return {
            "category": self.category,
            "description": self.description,
            "go_id": self.go_id,
        }


@dataclass(frozen=True)
class HmdbPfam:
    name: Optional[str] = None
    pfam_id: Optional[str] = None

    def to_dict(self):
        return {
            "name": self.name,
            "pfam_id": self.pfam_id,
        }


@dataclass(frozen=True)
class HmdbProteinReference:
    pubmed_id: Optional[str] = None
    reference_text: Optional[str] = None

    def to_dict(self):
        return {
            "pubmed_id": self.pubmed_id,
            "reference_text": self.reference_text,
        }


@dataclass
@indexed(fields=["id", "hmdb_accession"])
@facets(category_fields=["prefix", "protein_type"])
@search(text_fields=["id", "hmdb_accession", "name", "synonyms", "gene_name", "uniprot_name"])
class ProteinIdentifier(Node):
    hmdb_accession: Optional[str] = None
    secondary_accessions: List[str] = field(default_factory=list)
    prefix: Optional[str] = None
    name: Optional[str] = None
    synonyms: List[str] = field(default_factory=list)
    protein_type: Optional[str] = None
    gene_name: Optional[str] = None
    uniprot_name: Optional[str] = None
    general_function: Optional[str] = None
    specific_function: Optional[str] = None
    genbank_protein_id: Optional[str] = None
    genbank_gene_id: Optional[str] = None
    genecard_id: Optional[str] = None
    geneatlas_id: Optional[str] = None
    hgnc_id: Optional[str] = None
    subcellular_locations: List[str] = field(default_factory=list)
    go_classifications: List[HmdbGoClassification] = field(default_factory=list)
    pfams: List[HmdbPfam] = field(default_factory=list)
    pdb_ids: List[str] = field(default_factory=list)
    residue_number: Optional[str] = None
    molecular_weight: Optional[str] = None
    theoretical_pi: Optional[str] = None
    transmembrane_regions: List[str] = field(default_factory=list)
    signal_regions: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.prefix is None and self.id:
            self.prefix = self.id.split(":", 1)[0]


@dataclass
@indexed(fields=["id"])
@facets(category_fields=["prefix"])
@search(text_fields=["id", "names"])
class GeneIdentifier(Node):
    prefix: Optional[str] = None
    names: List[str] = field(default_factory=list)

    def __post_init__(self):
        if self.prefix is None and self.id:
            self.prefix = self.id.split(":", 1)[0]


@dataclass(frozen=True)
class HmdbMetaboliteProteinAssociationDetail:
    source: str
    source_field: str
    hmdb_metabolite_accession: str
    hmdb_protein_accession: str
    metabolite_name: Optional[str] = None
    protein_type: Optional[str] = None
    references: List[HmdbProteinReference] = field(default_factory=list)

    def to_dict(self):
        return {
            "source": self.source,
            "source_field": self.source_field,
            "hmdb_metabolite_accession": self.hmdb_metabolite_accession,
            "hmdb_protein_accession": self.hmdb_protein_accession,
            "metabolite_name": self.metabolite_name,
            "protein_type": self.protein_type,
            "references": [reference.to_dict() for reference in self.references],
        }


@dataclass
class HmdbMetaboliteProteinAssociationEdge(Relationship):
    start_node: MetaboliteIdentifier
    end_node: ProteinIdentifier
    details: List[HmdbMetaboliteProteinAssociationDetail] = field(default_factory=list)


@dataclass(frozen=True)
class PathwayName:
    value: str
    source: str
    source_field: Optional[str] = None

    def to_dict(self):
        return {
            "value": self.value,
            "source": self.source,
            "source_field": self.source_field,
        }


@dataclass
@indexed(fields=["id", "stable_id"])
@facets(category_fields=["prefix", "category"])
@search(text_fields=["id", "stable_id", "names"])
class PathwayIdentifier(Node):
    prefix: Optional[str] = None
    category: Optional[str] = None
    stable_id: Optional[str] = None
    url: Optional[str] = None
    names: List[PathwayName] = field(default_factory=list)

    def __post_init__(self):
        if self.prefix is None and self.id:
            self.prefix = self.id.split(":", 1)[0]
        if self.stable_id is None and self.id and self.prefix == "Reactome" and ":" in self.id:
            self.stable_id = self.id.split(":", 1)[1]


@dataclass(frozen=True)
class MetabolitePathwayDetail:
    source: str
    source_field: str
    metabolite_id: str
    pathway_id: str
    pathway_name: Optional[str] = None
    pathway_category: Optional[str] = None
    hmdb_metabolite_accession: Optional[str] = None
    url: Optional[str] = None
    evidence_code: Optional[str] = None
    species: Optional[str] = None

    def to_dict(self):
        return {
            "source": self.source,
            "source_field": self.source_field,
            "metabolite_id": self.metabolite_id,
            "pathway_id": self.pathway_id,
            "pathway_name": self.pathway_name,
            "pathway_category": self.pathway_category,
            "hmdb_metabolite_accession": self.hmdb_metabolite_accession,
            "url": self.url,
            "evidence_code": self.evidence_code,
            "species": self.species,
        }


@dataclass
class MetabolitePathwayEdge(Relationship):
    start_node: MetaboliteIdentifier
    end_node: PathwayIdentifier
    details: List[MetabolitePathwayDetail] = field(default_factory=list)


@dataclass(frozen=True)
class ProteinPathwayDetail:
    source: str
    source_field: str
    protein_id: str
    pathway_id: str
    pathway_name: Optional[str] = None
    pathway_category: Optional[str] = None
    hmdb_protein_accession: Optional[str] = None
    url: Optional[str] = None
    evidence_code: Optional[str] = None
    species: Optional[str] = None

    def to_dict(self):
        return {
            "source": self.source,
            "source_field": self.source_field,
            "protein_id": self.protein_id,
            "pathway_id": self.pathway_id,
            "pathway_name": self.pathway_name,
            "pathway_category": self.pathway_category,
            "hmdb_protein_accession": self.hmdb_protein_accession,
            "url": self.url,
            "evidence_code": self.evidence_code,
            "species": self.species,
        }


@dataclass
class ProteinPathwayEdge(Relationship):
    start_node: ProteinIdentifier
    end_node: PathwayIdentifier
    details: List[ProteinPathwayDetail] = field(default_factory=list)


@dataclass(frozen=True)
class GenePathwayDetail:
    source: str
    source_field: str
    gene_id: str
    pathway_id: str
    pathway_name: Optional[str] = None
    url: Optional[str] = None
    species: Optional[str] = None

    def to_dict(self):
        return {
            "source": self.source,
            "source_field": self.source_field,
            "gene_id": self.gene_id,
            "pathway_id": self.pathway_id,
            "pathway_name": self.pathway_name,
            "url": self.url,
            "species": self.species,
        }


@dataclass
class GenePathwayEdge(Relationship):
    start_node: GeneIdentifier
    end_node: PathwayIdentifier
    details: List[GenePathwayDetail] = field(default_factory=list)


@dataclass(frozen=True)
class ReactomePathwayParentDetail:
    source: str
    source_field: str
    parent_pathway_id: str
    child_pathway_id: str

    def to_dict(self):
        return {
            "source": self.source,
            "source_field": self.source_field,
            "parent_pathway_id": self.parent_pathway_id,
            "child_pathway_id": self.child_pathway_id,
        }


@dataclass
class ReactomePathwayParentEdge(Relationship):
    start_node: PathwayIdentifier
    end_node: PathwayIdentifier
    details: List[ReactomePathwayParentDetail] = field(default_factory=list)
