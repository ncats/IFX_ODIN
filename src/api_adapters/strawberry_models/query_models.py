from typing import Optional, Type, List

import strawberry

from src.interfaces.result_types import ResolveResult, DetailsQueryResult, LinkedListQueryResult, LinkDetails
from src.interfaces.simple_enum import NodeLabel, RelationshipLabel
from src.models.analyte import Synonym
from src.models.gene import Gene as GeneBase, GeneticLocation
from src.models.generif import GeneGeneRifRelationship, GeneRif as GeneRifBase
from src.models.go_term import GoTerm as GoTermBase, ProteinGoTermRelationship as ProteinGoTermRelationshipBase, GoEvidence as GoEvidenceBase
from src.models.ligand import Ligand as LigandBase, ProteinLigandRelationship, ActivityDetails
from src.models.node import Node, EquivalentId, Relationship
from src.models.protein import Protein as ProteinBase
from src.models.transcript import Transcript as TranscriptBase, TranscriptLocation, IsoformProteinRelationship, \
    GeneProteinRelationship, \
    TranscriptProteinRelationship, GeneTranscriptRelationship
from src.use_cases.build_from_yaml import HostDashboardFromYaml

NodeLabel = strawberry.type(NodeLabel)
Node = strawberry.type(Node)
EquivalentId = strawberry.type(EquivalentId)
Synonym = strawberry.type(Synonym)
GeneticLocation = strawberry.type(GeneticLocation)
IsoformProteinRelationship = strawberry.type(IsoformProteinRelationship)
GeneProteinRelationship = strawberry.type(GeneProteinRelationship)
TranscriptProteinRelationship = strawberry.type(TranscriptProteinRelationship)
GeneTranscriptRelationship = strawberry.type(GeneTranscriptRelationship)
ActivityDetails = strawberry.type(ActivityDetails)
ProteinLigandRelationship = strawberry.type(ProteinLigandRelationship)

@strawberry.type
class GeneRif(GeneRifBase):
    pmids: List[str]

GeneGeneRifRelationship = strawberry.type(GeneGeneRifRelationship)

RelationshipLabel = strawberry.type(RelationshipLabel)
Relationship = strawberry.type(Relationship)
LinkDetails = strawberry.type(LinkDetails)

TranscriptLocation = strawberry.type(TranscriptLocation)

@strawberry.type
class Protein(ProteinBase):

    @strawberry.field
    def isoforms(root, top: int = 10, skip: int = 0) -> "ProteinIsoformQueryResult":
        result = api.get_linked_list(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="Protein",
            edge_model="IsoformProteinRelationship",
            dest_id=root.id,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def canonical(root, top: int = 10, skip: int = 0) -> "ProteinIsoformQueryResult":
        result = api.get_linked_list(
            source_data_model="Protein",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="IsoformProteinRelationship",
            dest_id=None,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def go_terms(root, top: int = 10, skip: int = 0) -> "ProteinGoTermQueryResult":
        result = api.get_linked_list(
            source_data_model="Protein",
            source_id=root.id,
            dest_data_model="GoTerm",
            edge_model="ProteinGoTermRelationship",
            dest_id=None,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def genes(root, top: int = 10, skip: int = 0) -> "ProteinGeneQueryResult":
        result = api.get_linked_list(
            source_data_model="Gene",
            source_id=None,
            dest_data_model="Protein",
            edge_model="GeneProteinRelationship",
            dest_id=root.id,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def transcripts(root, top: int = 10, skip: int = 0) -> "ProteinTranscriptQueryResult":
        result = api.get_linked_list(
            source_data_model="Transcript",
            source_id=None,
            dest_data_model="Protein",
            edge_model="TranscriptProteinRelationship",
            dest_id=root.id,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def ligands(root, top: int = 10, skip: int = 0) -> "ProteinLigandQueryResult":
        result = api.get_linked_list(
            source_data_model="Protein",
            source_id=root.id,
            dest_data_model="Ligand",
            edge_model="ProteinLigandRelationship",
            dest_id=None,
            top=top,
            skip=skip)
        return result


@strawberry.type
class Transcript(TranscriptBase):
    @strawberry.field
    def proteins(root, top: int = 10, skip: int = 0) -> "TranscriptProteinQueryResult":
        result = api.get_linked_list(
            source_data_model="Transcript",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="TranscriptProteinRelationship",
            dest_id=None,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def genes(root, top: int = 10, skip: int = 0) -> "TranscriptGeneQueryResult":
        result = api.get_linked_list(
            source_data_model="Gene",
            source_id=None,
            dest_data_model="Transcript",
            edge_model="GeneTranscriptRelationship",
            dest_id=root.id,
            top=top,
            skip=skip)
        return result

@strawberry.type
class Gene(GeneBase):
    @strawberry.field
    def transcripts(root, top: int = 10, skip: int = 0) -> "GeneTranscriptQueryResult":
        result = api.get_linked_list(
            source_data_model="Gene",
            source_id=root.id,
            dest_data_model="Transcript",
            edge_model="GeneTranscriptRelationship",
            dest_id=None,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def proteins(root, top: int = 10, skip: int = 0) -> "GeneProteinQueryResult":
        result = api.get_linked_list(
            source_data_model="Gene",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="GeneProteinRelationship",
            dest_id=None,
            top=top,
            skip=skip)
        return result

    @strawberry.field
    def geneRifs(root, top: int = 10, skip: int = 0) -> "GeneGeneRifQueryResult":
        result = api.get_linked_list(
            source_data_model="Gene",
            source_id=root.id,
            dest_data_model="GeneRif",
            edge_model="GeneGeneRifRelationship",
            dest_id=None,
            top=top,
            skip=skip)
        return result

@strawberry.type
class Ligand(LigandBase):
    @strawberry.field
    def targets(root, top: int = 10, skip: int = 0) -> "LigandProteinQueryResult":
        result = api.get_linked_list(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="Ligand",
            edge_model="ProteinLigandRelationship",
            dest_id=root.id,
            top=top,
            skip=skip)
        return result

yaml_file = "./src/use_cases/api/pharos_prod_dashboard.yaml"
dashboard = HostDashboardFromYaml(yaml_file=yaml_file)
api = dashboard.api_adapter




@strawberry.type
class GoTerm(GoTermBase):
    @strawberry.field
    def proteins(root, top: int = 10, skip: int = 0) -> "GoTermProteinQueryResult":
        protein_list = api.get_linked_list(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="GoTerm",
            edge_model="ProteinGoTermRelationship",
            dest_id=root.id,
            top=top,
            skip=skip)
        return protein_list

@strawberry.type
class GoEvidence(GoEvidenceBase):
    @strawberry.field
    def abbreviation(self) -> str:
        return GoEvidenceBase.abbreviation(self)

    @strawberry.field
    def category(self) -> str:
        return GoEvidenceBase.category(self)

    @strawberry.field
    def text(self) -> str:
        return GoEvidenceBase.text(self)

@strawberry.type
class ProteinGoTermRelationship(ProteinGoTermRelationshipBase):
    evidence: List[GoEvidence]




def make_linked_list_result_type(query_class_name: str, details_class_name: str, edge_type: Type, node_type: Type):
    def make_linked_details_type(details_class_name: str, edge_type: Type, node_type: Type):
        def edge_field(root) -> edge_type:
            return root.edge
        def node_field(root) -> node_type:
            return root.node

        new_class = type(
            details_class_name,
            (LinkDetails,),
            {
                "edge": strawberry.field(resolver = edge_field),
                "node": strawberry.field(resolver = node_field)
            }
        )
        return strawberry.type(new_class)
    details_class = make_linked_details_type(details_class_name, edge_type, node_type)
    def list_field(root) -> List[details_class]:
        return root.list

    new_class = type(
        query_class_name,
        (LinkedListQueryResult,),
        {
            "list": strawberry.field(resolver=list_field)
        }
    )
    return strawberry.type(new_class)


def make_resolve_result_type(class_name: str, match_type: Type):
    # Create a new type dynamically
    def match_field(root) -> Optional[match_type]:
        return root.match

    def other_matches_field(root) -> Optional[List[match_type]]:
        return root.other_matches

    new_class = type(
        class_name,
        (ResolveResult,),
        {
            "match": strawberry.field(resolver=match_field),
            "other_matches": strawberry.field(resolver=other_matches_field),
        },
    )
    return strawberry.type(new_class)

def make_details_result_type(class_name: str, match_type: Type):
    def details_field(root) -> Optional[match_type]:
        return root.details

    new_class = type(
        class_name, (DetailsQueryResult,),
        {
            "details": strawberry.field(resolver=details_field)
        }
    )
    return strawberry.type(new_class)


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

TranscriptProteinQueryResult = make_linked_list_result_type("TranscriptProteinQueryResult","TranscriptProteinDetails",TranscriptProteinRelationship,Protein)
TranscriptGeneQueryResult = make_linked_list_result_type("TranscriptGeneQueryResult","TranscriptGeneDetails", GeneTranscriptRelationship, Gene)

GeneProteinQueryResult = make_linked_list_result_type("GeneProteinQueryResult", "GeneProteinDetails", GeneProteinRelationship, Protein)
GeneTranscriptQueryResult = make_linked_list_result_type("GeneTranscriptQueryResult", "GeneTranscriptDetails", GeneTranscriptRelationship, Transcript)
GeneGeneRifQueryResult = make_linked_list_result_type("GeneGeneRifQueryResult", "GeneGeneRifDetails", GeneGeneRifRelationship, GeneRif)

LigandProteinQueryResult = make_linked_list_result_type("LigandProteinQueryResult", "LigandProteinDetails", ProteinLigandRelationship, Protein)
