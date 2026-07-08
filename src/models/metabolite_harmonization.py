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
