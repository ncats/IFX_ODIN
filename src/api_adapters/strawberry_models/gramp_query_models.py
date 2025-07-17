from typing import Dict, Optional

import strawberry
from strawberry import Info

from src.api_adapters.strawberry_models.class_generators import make_linked_list_result_type
from src.api_adapters.strawberry_models.input_types import LinkedListFilterSettings
from src.interfaces.result_types import LinkedListQueryContext
from src.models.metabolite import Metabolite as MetaboliteBase, \
    MetaboliteChemPropsRelationship as MetaboliteChemPropsRelationshipBase, \
    MetaboliteProteinRelationship as MetaboliteProteinRelationshipBase, MetaboliteReactionRelationship as MetaboliteReactionRelationshipBase
from src.models.metabolite_chem_props import MetaboliteChemProps as MetaboliteChemPropsBase
from src.models.protein import Protein as ProteinBase, ProteinReactionRelationship as ProteinReactionRelationshipBase
from src.models.metabolite_class import MetaboliteClass as MetaboliteClassBase, MetaboliteClassRelationship as MetaboliteClassRelationshipBase
from src.models.pathway import Pathway as PathwayBase, AnalytePathwayRelationship as AnalytePathwayRelationshipBase
from src.models.ontology import Ontology as OntologyBase, MetaboliteOntologyRelationship as MetaboliteOntologyRelationshipBase
from src.models.reaction import Reaction as ReactionBase, ReactionClass as ReactionClassBase, \
    ReactionReactionClassRelationship as ReactionReactionClassRelationshipBase, ReactionClassParentRelationship as ReactionClassParentRelationshipBase
from src.models.version import DataVersion as DataVersionBase

from src.api_adapters.strawberry_models.shared_query_models import generate_resolvers, Provenance


@strawberry.type
class Metabolite(MetaboliteBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def classes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetaboliteClassQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=root.id,
            dest_data_model="MetaboliteClass",
            edge_model="MetaboliteClassRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def ontologies(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetaboliteOntologyQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=root.id,
            dest_data_model="Ontology",
            edge_model="MetaboliteOntologyRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def pathways(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetabolitePathwayQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=root.id,
            dest_data_model="Pathway",
            edge_model="AnalytePathwayRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def chem_props(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetaboliteChemPropsQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=root.id,
            dest_data_model="MetaboliteChemProps",
            edge_model="MetaboliteChemPropsRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def enzymes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetaboliteProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=root.id,
            dest_data_model="Protein",
            edge_model="MetaboliteProteinRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def reactions(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetaboliteReactionQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=root.id,
            dest_data_model="Reaction",
            edge_model="MetaboliteReactionRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class MetaboliteChemProps(MetaboliteChemPropsBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

@strawberry.type
class Protein(ProteinBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def pathways(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinPathwayQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=root.id,
            dest_data_model="Pathway",
            edge_model="AnalytePathwayRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def substrates(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinMetaboliteQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=None,
            dest_data_model="Protein",
            edge_model="MetaboliteProteinRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def reactions(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ProteinReactionQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=root.id,
            dest_data_model="Reaction",
            edge_model="ProteinReactionRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class MetaboliteClass(MetaboliteClassBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)


    @strawberry.field()
    def metabolites(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "MetaboliteClassMetaboliteQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=None,
            dest_data_model="MetaboliteClass",
            edge_model="MetaboliteClassRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Pathway(PathwayBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def metabolites(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "PathwayMetaboliteQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=None,
            dest_data_model="Pathway",
            edge_model="AnalytePathwayRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def proteins(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "PathwayProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="Pathway",
            edge_model="AnalytePathwayRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result



@strawberry.type
class Ontology(OntologyBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def metabolites(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "OntologyMetaboliteQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=None,
            dest_data_model="Ontology",
            edge_model="MetaboliteOntologyRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Reaction(ReactionBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def metabolites(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ReactionMetaboliteQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Metabolite",
            source_id=None,
            dest_data_model="Reaction",
            edge_model="MetaboliteReactionRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def proteins(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ReactionProteinQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Protein",
            source_id=None,
            dest_data_model="Reaction",
            edge_model="ProteinReactionRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def classes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ReactionReactionClassQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Reaction",
            source_id=root.id,
            dest_data_model="ReactionClass",
            edge_model="ReactionReactionClassRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result


@strawberry.type
class DataVersion(DataVersionBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

@strawberry.type
class ReactionClass(ReactionClassBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def reactions(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ReactionClassReactionQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Reaction",
            source_id=None,
            dest_data_model="ReactionClass",
            edge_model="ReactionReactionClassRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def parents(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ReactionClassParentQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="ReactionClass",
            source_id=root.id,
            dest_data_model="ReactionClass",
            edge_model="ReactionClassParentRelationship",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def children(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "ReactionClassParentQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="ReactionClass",
            source_id=None,
            dest_data_model="ReactionClass",
            edge_model="ReactionClassParentRelationship",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class MetaboliteClassRelationship(MetaboliteClassRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Metabolite
    end_node: MetaboliteClass

@strawberry.type
class MetaboliteOntologyRelationship(MetaboliteOntologyRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Metabolite
    end_node: Ontology

@strawberry.type
class MetabolitePathwayRelationship(AnalytePathwayRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Metabolite
    end_node: Pathway

@strawberry.type
class ProteinPathwayRelationship(AnalytePathwayRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Protein
    end_node: Pathway

@strawberry.type
class MetaboliteChemPropsRelationship(MetaboliteChemPropsRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Metabolite
    end_node: MetaboliteChemProps

@strawberry.type
class MetaboliteProteinRelationship(MetaboliteProteinRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Metabolite
    end_node: Protein

@strawberry.type
class MetaboliteReactionRelationship(MetaboliteReactionRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Metabolite
    end_node: Reaction

@strawberry.type
class ProteinReactionRelationship(ProteinReactionRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Protein
    end_node: Reaction

@strawberry.type
class ReactionReactionClassRelationship(ReactionReactionClassRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: Reaction
    end_node: ReactionClass

@strawberry.type
class ReactionClassParentRelationship(ReactionClassParentRelationshipBase):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    start_node: ReactionClass
    end_node: ReactionClass


MetaboliteClassQueryResult = make_linked_list_result_type("MetaboliteClassQueryResult", "MetaboliteClassDetails", MetaboliteClassRelationship, MetaboliteClass)
MetaboliteClassMetaboliteQueryResult = make_linked_list_result_type("MetaboliteClassMetaboliteQueryResult", "ClassMetaboliteDetails", MetaboliteClassRelationship, Metabolite)
MetaboliteOntologyQueryResult = make_linked_list_result_type("MetaboliteOntologyQueryResult", "MetaboliteOntologyDetails", MetaboliteOntologyRelationship, Ontology)
OntologyMetaboliteQueryResult = make_linked_list_result_type("OntologyMetaboliteQueryResult", "OntologyMetaboliteDetails", MetaboliteOntologyRelationship, Metabolite)
PathwayMetaboliteQueryResult = make_linked_list_result_type("PathwayMetaboliteQueryResult", "PathwayMetaboliteDetails", MetabolitePathwayRelationship, Metabolite)
MetabolitePathwayQueryResult = make_linked_list_result_type("MetabolitePathwayQueryResult", "MetabolitePathwayDetails", MetabolitePathwayRelationship, Pathway)
PathwayProteinQueryResult = make_linked_list_result_type("PathwayProteinQueryResult", "PathwayProteinDetails", ProteinPathwayRelationship, Protein)
ProteinPathwayQueryResult = make_linked_list_result_type("ProteinPathwayQueryResult", "ProteinPathwayDetails", ProteinPathwayRelationship, Pathway)
MetaboliteChemPropsQueryResult = make_linked_list_result_type("MetaboliteChemPropsQueryResult", "MetaboliteChemPropsDetails", MetaboliteChemPropsRelationship, MetaboliteChemProps)
MetaboliteProteinQueryResult = make_linked_list_result_type("MetaboliteProteinQueryResult", "MetaboliteProteinDetails", MetaboliteProteinRelationship, Protein)
ProteinMetaboliteQueryResult = make_linked_list_result_type("ProteinMetaboliteQueryResult", "ProteinMetaboliteDetails", MetaboliteProteinRelationship, Metabolite)
MetaboliteReactionQueryResult = make_linked_list_result_type("MetaboliteReactionQueryResult", "MetaboliteReactionDetails", MetaboliteReactionRelationship, Reaction)
ReactionMetaboliteQueryResult = make_linked_list_result_type("ReactionMetaboliteQueryResult", "ReactionMetaboliteDetails", MetaboliteReactionRelationship, Metabolite)
ProteinReactionQueryResult = make_linked_list_result_type("ProteinReactionQueryResult", "ProteinReactionDetails", ProteinReactionRelationship, Reaction)
ReactionProteinQueryResult = make_linked_list_result_type("ReactionProteinQueryResult", "ReactionProteinDetails", ProteinReactionRelationship, Protein)
ReactionReactionClassQueryResult = make_linked_list_result_type("ReactionReactionClassQueryResult", "ReactionReactionClassDetails", ReactionReactionClassRelationship, ReactionClass)
ReactionClassReactionQueryResult = make_linked_list_result_type("ReactionClassReactionQueryResult", "ReactionClassReactionDetails", ReactionReactionClassRelationship, Reaction)
ReactionClassParentQueryResult = make_linked_list_result_type("ReactionClassParentQueryResult", "ReactionClassParentDetails", ReactionClassParentRelationship, ReactionClass)

ENDPOINTS: Dict[type, Dict[str, str]] = {
    Metabolite: {
        "list": "metabolites",
        "details": "resolve_metabolite"
    },
    Protein: {
        "list": "proteins",
        "details": "resolve_protein"
    },
    MetaboliteClass: {
        "list": "metabolite_classes",
        "details": "resolve_metabolite_class"
    },
    Pathway: {
        "list": "pathways",
        "details": "resolve_pathway"
    },
    Ontology: {
        "list": "ontologies",
        "details": "resolve_ontology"
    },
    Reaction: {
        "list": "reactions",
        "details": "resolve_reaction"
    },
    ReactionClass: {
        "list": "reaction_classes",
        "details": "resolve_reaction_class"
    },
    DataVersion: {
        "list": "data_versions"
    }
}

EDGES : Dict[type, str] = {
    # MetaboliteClassRelationship: "metabolite_class_edges",
}

def Query(url):
    resolvers = generate_resolvers(ENDPOINTS, EDGES, url)
    return strawberry.type(type("Query", (), resolvers))