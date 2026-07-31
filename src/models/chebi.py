from dataclasses import asdict, dataclass, field
from typing import List, Optional

from src.core.decorators import facets, indexed, search
from src.models.node import Node, Relationship


@dataclass
class Synonym:
    value: str
    scope: Optional[str] = None
    type: Optional[str] = None
    references: List[str] = field(default_factory=list)
    raw: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Xref:
    value: str
    prefix: Optional[str] = None
    source: Optional[str] = None
    raw: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Property:
    predicate: str
    value: str
    datatype: Optional[str] = None
    raw: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
class Predicate:
    predicate: str
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    raw: Optional[str] = None

    def to_dict(self):
        return asdict(self)


@dataclass
@indexed(fields=["id", "subsets"])
@facets(category_fields=["subsets", "is_obsolete"])
@search(text_fields=["id", "name", "inchi_key"])
class Term(Node):
    name: Optional[str] = None
    definition: Optional[str] = None
    definition_references: List[str] = field(default_factory=list)
    subsets: List[str] = field(default_factory=list)
    alt_ids: List[str] = field(default_factory=list)
    synonyms: List[Synonym] = field(default_factory=list)
    xrefs: List[Xref] = field(default_factory=list)
    properties: List[Property] = field(default_factory=list)
    is_obsolete: bool = False

    # Source-derived convenience fields for indexing/search in QA Browser.
    synonym_text: Optional[str] = None
    xref_text: Optional[str] = None
    charge: Optional[str] = None
    formula: Optional[str] = None
    mass: Optional[str] = None
    monoisotopic_mass: Optional[str] = None
    smiles: Optional[str] = None
    inchi: Optional[str] = None
    inchi_key: Optional[str] = None
    wurcs: Optional[str] = None


@dataclass
@indexed(fields=["id", "subsets"])
@facets(category_fields=["subsets", "is_obsolete"])
@search(text_fields=["id", "name", "inchi_key"])
class ChemicalEntity(Term):
    pass


@dataclass
@indexed(fields=["id", "subsets"])
@facets(category_fields=["subsets", "is_obsolete"])
@search(text_fields=["id", "name"])
class Role(Term):
    pass


@dataclass
@indexed(fields=["id", "subsets"])
@facets(category_fields=["subsets", "is_obsolete"])
@search(text_fields=["id", "name"])
class Application(Role):
    pass


@dataclass
@indexed(fields=["id", "subsets"])
@facets(category_fields=["subsets", "is_obsolete"])
@search(text_fields=["id", "name"])
class BiologicalRole(Role):
    pass


@dataclass
@indexed(fields=["id", "subsets"])
@facets(category_fields=["subsets", "is_obsolete"])
@search(text_fields=["id", "name"])
class ChemicalRole(Role):
    pass


@dataclass
class SourceEdge(Relationship):
    start_node: Term
    end_node: Term
    source_predicate: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    raw: Optional[str] = None


@dataclass
@search(text_fields=["source_label", "target_label"])
class IsAEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class HasFunctionalParentEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class IsConjugateBaseOfEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class IsConjugateAcidOfEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class HasPartEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class IsEnantiomerOfEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class IsTautomerOfEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class HasParentHydrideEdge(SourceEdge):
    pass


@dataclass
@search(text_fields=["source_label", "target_label"])
class IsSubstituentGroupFromEdge(SourceEdge):
    start_node: ChemicalEntity
    end_node: Term


@dataclass
@search(text_fields=["source_label", "target_label"])
class HasRoleEdge(Relationship):
    start_node: Term
    end_node: Role
    source_predicate: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    raw: Optional[str] = None


@dataclass
@search(text_fields=["source_label", "target_label"])
class HasApplicationEdge(Relationship):
    start_node: Term
    end_node: Application
    source_predicate: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    raw: Optional[str] = None


@dataclass
@search(text_fields=["source_label", "target_label"])
class HasBiologicalRoleEdge(Relationship):
    start_node: Term
    end_node: BiologicalRole
    source_predicate: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    raw: Optional[str] = None


@dataclass
@search(text_fields=["source_label", "target_label"])
class HasChemicalRoleEdge(Relationship):
    start_node: Term
    end_node: ChemicalRole
    source_predicate: Optional[str] = None
    source_label: Optional[str] = None
    target_label: Optional[str] = None
    raw: Optional[str] = None
