from dataclasses import dataclass
from typing import Optional

from src.core.decorators import facets
from src.models.node import Node, Relationship


@dataclass
@facets(category_fields=['resolution_source', 'review_flag', 'review_reason'], numeric_fields=['confidence'])
class CureNode(Node):
    name: str = None
    resolution_source: Optional[str] = None
    confidence: float = None
    review_flag: Optional[bool] = None
    review_reason: Optional[str] = None

@dataclass
class SequenceVariant(CureNode):
    pass

@dataclass
class Disease(CureNode):
    pass

@dataclass
class Drug(CureNode):
    pass

@dataclass
class AdverseEvent(CureNode):
    pass

@dataclass
class Gene(CureNode):
    pass

@dataclass
class PhenotypicFeature(CureNode):
    pass


@dataclass
@facets(category_fields=['biolink_label', 'biolink_category'])
class CureEdge(Relationship):
    report_id: str = None
    pmid: Optional[int] = None
    link: str = None
    outcome: Optional[str] = None
    biolink_label: str = None
    biolink_category: str = None

@dataclass
class DiseasePhenotypicFeatureEdge(CureEdge):
    start_node: Disease
    end_node: PhenotypicFeature

@dataclass
class GeneDiseaseEdge(CureEdge):
    start_node: Gene
    end_node: Disease

@dataclass
class DrugAdverseEventEdge(CureEdge):
    start_node: Drug
    end_node: AdverseEvent

@dataclass
class GeneSequenceVariantEdge(CureEdge):
    start_node: Gene
    end_node: SequenceVariant

@dataclass
class DrugPhenotypicFeatureEdge(CureEdge):
    start_node: Drug
    end_node: PhenotypicFeature

@dataclass
class DrugDiseaseEdge(CureEdge):
    start_node: Drug
    end_node: Disease

@dataclass
class SequenceVariantDiseaseEdge(CureEdge):
    start_node: SequenceVariant
    end_node: Disease