from dataclasses import dataclass, field
from typing import List, Optional

from src.core.decorators import search
from src.models.cure.pasc.condition import Condition
from src.models.cure.rasopathies.clinical_context import ClinicalContext
from src.models.gene import Gene
from src.models.node import Node, Relationship


@dataclass
@search(text_fields=["diagnosis_methods"])
class Diagnosis(Node):
    diagnosis_methods: List[str] = field(default_factory=list)


@dataclass
@search(text_fields=["source_gene_symbol", "nucleotide_change", "protein_change", "variant_label"])
class GeneVariant(Node):
    source_gene_symbol: Optional[str] = None
    nucleotide_change: Optional[str] = None
    protein_change: Optional[str] = None
    variant_label: Optional[str] = None


@dataclass
class ClinicalContextDiagnosisEdge(Relationship):
    start_node: ClinicalContext = None
    end_node: Diagnosis = None


@dataclass
class DiagnosisConditionEdge(Relationship):
    start_node: Diagnosis = None
    end_node: Condition = None


@dataclass
class DiagnosisGeneEdge(Relationship):
    start_node: Diagnosis = None
    end_node: Gene = None


@dataclass
class DiagnosisGeneVariantEdge(Relationship):
    start_node: Diagnosis = None
    end_node: GeneVariant = None


@dataclass
class GeneGeneVariantEdge(Relationship):
    start_node: Gene = None
    end_node: GeneVariant = None
