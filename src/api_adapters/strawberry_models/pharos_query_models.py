from typing import Optional, List, Literal, Dict

import strawberry
from strawberry import Info

from src.api_adapters.strawberry_models.class_generators import make_linked_list_result_type
from src.api_adapters.strawberry_models.input_types import FilterOption, ListFilterSettings, LinkedListFilterSettings
from src.api_adapters.strawberry_models.shared_query_models import Provenance, generate_resolvers
from src.interfaces.result_types import LinkedListQueryContext
from src.models.analyte import Synonym
from src.models.gene import Gene as GeneBase, GeneticLocation
from src.models.generif import GeneGeneRifEdge as GeneGeneRifEdgeBase, GeneRif as GeneRifBase
from src.models.go_term import GoTerm as GoTermBase, ProteinGoTermEdge as ProteinGoTermEdgeBase, \
    GoEvidence as GoEvidenceBase, GoTermHasParent as GoTermHasParentBase
from src.models.ligand import Ligand as LigandBase, ProteinLigandEdge as ProteinLigandEdgeBase, ActivityDetails
from src.models.node import Node, EquivalentId, Relationship
from src.models.protein import Protein as ProteinBase, TDLMetadata
from src.models.transcript import Transcript as TranscriptBase, TranscriptLocation, IsoformProteinEdge as IsoformProteinEdgeBase, \
    GeneProteinEdge as GeneProteinEdgeBase, \
    TranscriptProteinEdge as TranscriptProteinEdgeBase, GeneTranscriptEdge as GeneTranscriptEdgeBase


Node = strawberry.type(Node)
EquivalentId = strawberry.type(EquivalentId)
Synonym = strawberry.type(Synonym)
GeneticLocation = strawberry.type(GeneticLocation)
ActivityDetails = strawberry.type(ActivityDetails)
Relationship = strawberry.type(Relationship)
TranscriptLocation = strawberry.type(TranscriptLocation)
TDLMetadata = strawberry.type(TDLMetadata)

@strawberry.type
class GeneRif(GeneRifBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def genes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GeneRifGeneQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=None,
            dest_data_model="GeneRif",
            edge_model="GeneGeneRifEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

def go_terms(root, info: Info, type: Literal["P", "F", "C"], filter: Optional[
    LinkedListFilterSettings] = None) -> "ProteinGoTermQueryResult":
    api = info.context["api"]
    default_filter = LinkedListFilterSettings(node_filter=ListFilterSettings(settings=[
        FilterOption(field="type", allowed_values=[type])]))
    filter = LinkedListFilterSettings.merge(filter, default_filter)

    context = LinkedListQueryContext(
        source_data_model="Protein",
        source_id=root.id,
        dest_data_model="GoTerm",
        edge_model="ProteinGoTermEdge",
        dest_id=None,
        filter=filter
    )
    result = api.get_linked_list(context)
    return result


@strawberry.type
class Protein(ProteinBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def isoforms(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinIsoformQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="Protein",
            edge_model="IsoformProteinEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def canonical(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinIsoformQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="IsoformProteinEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def go_functions(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinGoTermQueryResult":
        return go_terms(root, info=info, type="F", filter=filter)

    @strawberry.field()
    def go_processes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinGoTermQueryResult":
        return go_terms(root, info=info, type="P", filter=filter)

    @strawberry.field()
    def go_components(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinGoTermQueryResult":
        return go_terms(root, info=info, type="C", filter=filter)

    @strawberry.field()
    def genes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinGeneQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=None,
            dest_data_model="Protein",
            edge_model="GeneProteinEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def transcripts(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinTranscriptQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Transcript",
            source_id=None,
            dest_data_model="Protein",
            edge_model="TranscriptProteinEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def ligands(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinLigandQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=root.id,
            dest_data_model="Ligand",
            edge_model="ProteinLigandEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result


@strawberry.type
class Transcript(TranscriptBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def proteins(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "TranscriptProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Transcript",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="TranscriptProteinEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def genes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "TranscriptGeneQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=None,
            dest_data_model="Transcript",
            edge_model="GeneTranscriptEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Gene(GeneBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def transcripts(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GeneTranscriptQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=root.id,
            dest_data_model="Transcript",
            edge_model="GeneTranscriptEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def proteins(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GeneProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="GeneProteinEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def geneRifs(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GeneGeneRifQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=root.id,
            dest_data_model="GeneRif",
            edge_model="GeneGeneRifEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Ligand(LigandBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def proteins(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "LigandProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="Ligand",
            edge_model="ProteinLigandEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class GoTerm(GoTermBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def proteins(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GoTermProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="GoTerm",
            edge_model="ProteinGoTermEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def parents(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GoTermGoTermQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="GoTerm",
            source_id=root.id,
            dest_data_model="GoTerm",
            edge_model="GoTermHasParent",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def children(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GoTermGoTermQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="GoTerm",
            source_id=None,
            dest_data_model="GoTerm",
            edge_model="GoTermHasParent",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class GoEvidence(GoEvidenceBase):
    @strawberry.field()
    def abbreviation(self) -> str:
        return GoEvidenceBase.abbreviation(self)

    @strawberry.field()
    def category(self) -> str:
        return GoEvidenceBase.category(self)

    @strawberry.field()
    def text(self) -> str:
        return GoEvidenceBase.text(self)

@strawberry.type
class ProteinGoTermEdge(ProteinGoTermEdgeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Protein
    end_node: GoTerm
    evidence: List[GoEvidence]


@strawberry.type
class GoTermHasParent(GoTermHasParentBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: GoTerm
    end_node: GoTerm


@strawberry.type
class IsoformProteinEdge(IsoformProteinEdgeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Protein
    end_node: Protein

@strawberry.type
class GeneProteinEdge(GeneProteinEdgeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Gene
    end_node: Protein

@strawberry.type
class TranscriptProteinEdge(TranscriptProteinEdgeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Transcript
    end_node: Protein

@strawberry.type
class GeneTranscriptEdge(GeneTranscriptEdgeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Gene
    end_node: Transcript

@strawberry.type
class ProteinLigandEdge(ProteinLigandEdgeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Protein
    end_node: Ligand

@strawberry.type
class GeneGeneRifEdge(GeneGeneRifEdgeBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Gene
    end_node: GeneRif
    pmids: List[str]

ProteinGeneQueryResult = make_linked_list_result_type("ProteinGeneQueryResult", "ProteinGeneDetails", GeneProteinEdge, Gene)
ProteinTranscriptQueryResult = make_linked_list_result_type("ProteinTranscriptQueryResult", "ProteinTranscriptDetails", TranscriptProteinEdge, Transcript)
ProteinGoTermQueryResult = make_linked_list_result_type("ProteinGoTermQueryResult", "ProteinGoTermDetails", ProteinGoTermEdge, GoTerm)
ProteinIsoformQueryResult = make_linked_list_result_type("ProteinIsoformQueryResult", "ProteinIsoformDetails", IsoformProteinEdge, Protein)
ProteinLigandQueryResult = make_linked_list_result_type("ProteinLigandQueryResult", "ProteinLigandDetails", ProteinLigandEdge, Ligand)

GoTermProteinQueryResult = make_linked_list_result_type("GoTermProteinQueryResult", "GoTermProteinDetails", ProteinGoTermEdge, Protein)
GoTermGoTermQueryResult = make_linked_list_result_type("GoTermGoTermQueryResult", "GoTermGoTermDetails", GoTermHasParent, GoTerm)
TranscriptProteinQueryResult = make_linked_list_result_type("TranscriptProteinQueryResult", "TranscriptProteinDetails", TranscriptProteinEdge, Protein)
TranscriptGeneQueryResult = make_linked_list_result_type("TranscriptGeneQueryResult", "TranscriptGeneDetails", GeneTranscriptEdge, Gene)

GeneProteinQueryResult = make_linked_list_result_type("GeneProteinQueryResult", "GeneProteinDetails", GeneProteinEdge, Protein)
GeneTranscriptQueryResult = make_linked_list_result_type("GeneTranscriptQueryResult", "GeneTranscriptDetails", GeneTranscriptEdge, Transcript)
GeneGeneRifQueryResult = make_linked_list_result_type("GeneGeneRifQueryResult", "GeneGeneRifDetails", GeneGeneRifEdge, GeneRif)
GeneRifGeneQueryResult = make_linked_list_result_type("GeneRifGeneQueryResult", "GeneRifGeneDetails", GeneGeneRifEdge, Gene)

LigandProteinQueryResult = make_linked_list_result_type("LigandProteinQueryResult", "LigandProteinDetails", ProteinLigandEdge, Protein)

ENDPOINTS: Dict[type, Dict[str, str]] = {
    Protein: {
        "list": "proteins",
        "details": "resolve_protein",
        "sortby": {"uniprot_reviewed": "desc", "uniprot_canonical": "desc", "mapping_ratio": "desc"}
    },
    Gene: {
        "list": "genes",
        "details": "resolve_gene",
        "sortby": {"mapping_ratio": "desc"}
    },
    Transcript: {
        "list": "transcripts",
        "details": "resolve_transcript",
        "sortby": {"mapping_ratio": "desc"}
    },
    Ligand: {
        "list": "ligands",
        "details": "resolve_ligand"
    },
    GoTerm: {
        "list": "go_terms",
        "details": "resolve_go_term"
    },
    GeneRif: {
        "list": "gene_rifs",
        "details": "resolve_generif"
    }
}

EDGES : Dict[type, str] = {
    ProteinGoTermEdge: "protein_go_term_edges",
    IsoformProteinEdge: "isoform_protein_edges",
    GeneProteinEdge: "gene_protein_edges",
    TranscriptProteinEdge: "transcript_protein_edges",
    GeneTranscriptEdge: "gene_transcript_edges",
    ProteinLigandEdge: "protein_ligand_edges",
    GeneGeneRifEdge: "gene_generif_edges"
}

def Query(url):
    resolvers = generate_resolvers(ENDPOINTS, EDGES, url)
    return strawberry.type(type("Query", (), resolvers))