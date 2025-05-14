from typing import Optional, List, Literal

import strawberry
from strawberry import Info

from src.api_adapters.strawberry_models.class_generators import make_linked_list_result_type, make_resolve_result_type, \
    make_details_result_type, make_list_result_type
from src.api_adapters.strawberry_models.input_types import FilterOption, ListFilterSettings, LinkedListFilterSettings, \
    ListQueryContext
from src.api_adapters.strawberry_models.shared_query_models import Provenance
from src.interfaces.result_types import LinkedListQueryContext
from src.interfaces.simple_enum import NodeLabel, RelationshipLabel
from src.models.analyte import Synonym
from src.models.gene import Gene as GeneBase, GeneticLocation
from src.models.generif import GeneGeneRifRelationship, GeneRif as GeneRifBase
from src.models.go_term import GoTerm as GoTermBase, ProteinGoTermRelationship as ProteinGoTermRelationshipBase, \
    GoEvidence as GoEvidenceBase
from src.models.ligand import Ligand as LigandBase, ProteinLigandRelationship, ActivityDetails
from src.models.node import Node, EquivalentId, Relationship
from src.models.protein import Protein as ProteinBase
from src.models.transcript import Transcript as TranscriptBase, TranscriptLocation, IsoformProteinRelationship, \
    GeneProteinRelationship, \
    TranscriptProteinRelationship, GeneTranscriptRelationship

NodeLabel = strawberry.type(NodeLabel)
Node = strawberry.type(Node)
EquivalentId = strawberry.type(EquivalentId)
Synonym = strawberry.type(Synonym)
GeneticLocation = strawberry.type(GeneticLocation)
ActivityDetails = strawberry.type(ActivityDetails)

IsoformProteinRelationship = strawberry.type(IsoformProteinRelationship)
GeneProteinRelationship = strawberry.type(GeneProteinRelationship)
TranscriptProteinRelationship = strawberry.type(TranscriptProteinRelationship)
GeneTranscriptRelationship = strawberry.type(GeneTranscriptRelationship)
ProteinLigandRelationship = strawberry.type(ProteinLigandRelationship)
GeneGeneRifRelationship = strawberry.type(GeneGeneRifRelationship)
RelationshipLabel = strawberry.type(RelationshipLabel)
Relationship = strawberry.type(Relationship)

TranscriptLocation = strawberry.type(TranscriptLocation)


@strawberry.type
class GeneRif(GeneRifBase):
    pmids: List[str]

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
        edge_model="ProteinGoTermRelationship",
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
            edge_model="IsoformProteinRelationship",
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
            edge_model="IsoformProteinRelationship",
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
            edge_model="GeneProteinRelationship",
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
            edge_model="TranscriptProteinRelationship",
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
            edge_model="ProteinLigandRelationship",
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
            edge_model="TranscriptProteinRelationship",
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
            edge_model="GeneTranscriptRelationship",
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
            edge_model="GeneTranscriptRelationship",
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
            edge_model="GeneProteinRelationship",
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
            edge_model="GeneGeneRifRelationship",
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
            edge_model="ProteinLigandRelationship",
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
            edge_model="ProteinGoTermRelationship",
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
class ProteinGoTermRelationship(ProteinGoTermRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    evidence: List[GoEvidence]


ProteinListResult = make_list_result_type("ProteinListResult", Protein)
GeneListResult = make_list_result_type("GeneListResult", Gene)
TranscriptListResult = make_list_result_type("TranscriptListResult", Transcript)
LigandListResult = make_list_result_type("LigandListResult", Ligand)
GoTermListResult = make_list_result_type("GoTermListResult", GoTerm)

ResolveProteinResult = make_resolve_result_type("ResolveProteinResult", Protein)
ResolveGeneResult = make_resolve_result_type("ResolveGeneResult", Gene)
ResolveTranscriptResult = make_resolve_result_type("ResolveTranscriptResult", Transcript)
ResolveLigandResult = make_resolve_result_type("ResolveLigandResult", Ligand)

GoTermResult = make_details_result_type("GoTermResult", GoTerm)

ProteinGeneQueryResult = make_linked_list_result_type("ProteinGeneQueryResult", "ProteinGeneDetails", GeneProteinRelationship, Gene)
ProteinTranscriptQueryResult = make_linked_list_result_type("ProteinTranscriptQueryResult", "ProteinTranscriptDetails", TranscriptProteinRelationship, Transcript)
ProteinGoTermQueryResult = make_linked_list_result_type("ProteinGoTermQueryResult", "ProteinGoTermDetails", ProteinGoTermRelationship, GoTerm)
ProteinIsoformQueryResult = make_linked_list_result_type("ProteinIsoformQueryResult", "ProteinIsoformDetails", IsoformProteinRelationship, Protein)
ProteinLigandQueryResult = make_linked_list_result_type("ProteinLigandQueryResult", "ProteinLigandDetails", ProteinLigandRelationship, Ligand)

GoTermProteinQueryResult = make_linked_list_result_type("GoTermProteinQueryResult", "GoTermProteinDetails", ProteinGoTermRelationship, Protein)

TranscriptProteinQueryResult = make_linked_list_result_type("TranscriptProteinQueryResult", "TranscriptProteinDetails", TranscriptProteinRelationship, Protein)
TranscriptGeneQueryResult = make_linked_list_result_type("TranscriptGeneQueryResult", "TranscriptGeneDetails", GeneTranscriptRelationship, Gene)

GeneProteinQueryResult = make_linked_list_result_type("GeneProteinQueryResult", "GeneProteinDetails", GeneProteinRelationship, Protein)
GeneTranscriptQueryResult = make_linked_list_result_type("GeneTranscriptQueryResult", "GeneTranscriptDetails", GeneTranscriptRelationship, Transcript)
GeneGeneRifQueryResult = make_linked_list_result_type("GeneGeneRifQueryResult", "GeneGeneRifDetails", GeneGeneRifRelationship, GeneRif)

LigandProteinQueryResult = make_linked_list_result_type("LigandProteinQueryResult", "LigandProteinDetails", ProteinLigandRelationship, Protein)



@strawberry.type
class Query:
    @strawberry.field()
    def proteins(self, info: Info, filter: Optional[ListFilterSettings] = None) -> ProteinListResult:
        api = info.context["api"]
        context = ListQueryContext(
            source_data_model="Protein",
            filter = filter
        )
        result = api.get_list_obj(context)
        return result

    @strawberry.field()
    def genes(self, info: Info, filter: Optional[ListFilterSettings] = None) -> GeneListResult:
        api = info.context["api"]
        context = ListQueryContext(
            source_data_model="Gene",
            filter = filter
        )
        result = api.get_list_obj(context)
        return result

    @strawberry.field()
    def transcripts(self, info: Info, filter: Optional[ListFilterSettings] = None) -> TranscriptListResult:
        api = info.context["api"]
        context = ListQueryContext(
            source_data_model="Transcript",
            filter = filter
        )
        result = api.get_list_obj(context)
        return result

    @strawberry.field()
    def ligands(self, info: Info, filter: Optional[ListFilterSettings] = None) -> LigandListResult:
        api = info.context["api"]
        context = ListQueryContext(
            source_data_model="Ligand",
            filter = filter
        )
        result = api.get_list_obj(context)
        return result

    @strawberry.field()
    def go_terms(self, info: Info, filter: Optional[ListFilterSettings] = None) -> GoTermListResult:
        api = info.context["api"]
        context = ListQueryContext(
            source_data_model="GoTerm",
            filter = filter
        )
        result = api.get_list_obj(context)
        return result

    @strawberry.field()
    def resolve_protein(self, info: Info, id: str) -> ResolveProteinResult:
        api = info.context["api"]
        result = api.resolve_id("Protein", id=id, sortby={"uniprot_reviewed": "desc", "uniprot_canonical": "desc", "mapping_ratio": "desc"})
        return result

    @strawberry.field()
    def resolve_gene(self, info: Info, id: str) -> ResolveGeneResult:
        api = info.context["api"]
        result = api.resolve_id("Gene", id=id, sortby={"mapping_ratio": "desc"})
        return result

    @strawberry.field()
    def resolve_transcript(self, info: Info, id: str) -> ResolveTranscriptResult:
        api = info.context["api"]
        result = api.resolve_id("Transcript", id=id, sortby={"mapping_ratio": "desc"})
        return result

    @strawberry.field()
    def resolve_ligand(self, info: Info, id: str) -> ResolveLigandResult:
        api = info.context["api"]
        result = api.resolve_id("Ligand", id=id)
        return result

    @strawberry.field()
    def go_term(self, info: Info, id: str) -> GoTermResult:
        api = info.context["api"]
        result = api.get_details("GoTerm", id=id)
        return result
