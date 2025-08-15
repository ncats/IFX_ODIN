from typing import Optional, Dict

import strawberry
from strawberry import Info

from src.api_adapters.strawberry_models.class_generators import make_linked_list_result_type
from src.api_adapters.strawberry_models.input_types import LinkedListFilterSettings
from src.api_adapters.strawberry_models.shared_query_models import Provenance, generate_resolvers
from src.interfaces.result_types import LinkedListQueryContext
from src.models.node import EquivalentId
from src.models.cure.models import CureNode, CureEdge
from src.interfaces.simple_enum import NodeLabel, RelationshipLabel
from src.models.analyte import Synonym

# base classes
NodeLabel = strawberry.type(NodeLabel)
RelationshipLabel = strawberry.type(RelationshipLabel)
EquivalentId = strawberry.type(EquivalentId)
Synonym = strawberry.type(Synonym)

CureNode = strawberry.type(CureNode)
CureEdge = strawberry.type(CureEdge)

# node classes
@strawberry.type
class SequenceVariant(CureNode):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def diseases(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "VariantDiseaseQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="SequenceVariant",
            source_id=root.id,
            dest_data_model="Disease",
            edge_model="SequenceVariantDiseaseEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def genes(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "VariantGeneQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=None,
            dest_data_model="SequenceVariant",
            edge_model="GeneSequenceVariantEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result



@strawberry.type
class Disease(CureNode):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)


    @strawberry.field()
    def sequence_variants(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "DiseaseVariantQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="SequenceVariant",
            source_id=None,
            dest_data_model="Disease",
            edge_model="SequenceVariantDiseaseEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def phenotypic_features(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "DiseasePhenotypeQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Disease",
            source_id=root.id,
            dest_data_model="PhenotypicFeature",
            edge_model="DiseasePhenotypicFeatureEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def drugs(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "DiseaseDrugQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Drug",
            source_id=None,
            dest_data_model="Disease",
            edge_model="DrugDiseaseEdge",
            dest_id=self.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result



@strawberry.type
class Drug(CureNode):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def diseases(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "DrugDiseaseQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Drug",
            source_id=self.id,
            dest_data_model="Disease",
            edge_model="DrugDiseaseEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def adverse_events(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "DrugAdverseEventQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Drug",
            source_id=self.id,
            dest_data_model="AdverseEvent",
            edge_model="DrugAdverseEventEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def phenotypic_features(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "DrugPhenotypeQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Drug",
            source_id=self.id,
            dest_data_model="PhenotypicFeature",
            edge_model="DrugPhenotypicFeatureEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def genes(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "DrugGeneQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=None,
            dest_data_model="Drug",
            edge_model="GeneDiseaseEdge",
            dest_id=self.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result



@strawberry.type
class AdverseEvent(CureNode):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def drugs(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "AdverseEventDrugQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Drug",
            source_id=None,
            dest_data_model="AdverseEvent",
            edge_model="DrugAdverseEventEdge",
            dest_id=self.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class Gene(CureNode):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)

    @strawberry.field()
    def sequence_variants(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GeneVariantQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=root.id,
            dest_data_model="SequenceVariant",
            edge_model="GeneSequenceVariantEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def genes(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "GeneDrugQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Gene",
            source_id=self.id,
            dest_data_model="Drug",
            edge_model="GeneDiseaseEdge",
            dest_id=None,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

@strawberry.type
class PhenotypicFeature(CureNode):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)


    @strawberry.field()
    def diseases(root, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "PhenotypeDiseaseQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Disease",
            source_id=None,
            dest_data_model="PhenotypicFeature",
            edge_model="DiseasePhenotypicFeatureEdge",
            dest_id=root.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

    @strawberry.field()
    def drugs(self, info: Info, filter: Optional[LinkedListFilterSettings] = None) -> "PhenotypeDrugQueryResult":
        api = info.context["api"]
        context = LinkedListQueryContext(
            source_data_model="Drug",
            source_id=None,
            dest_data_model="PhenotypicFeature",
            edge_model="DrugPhenotypicFeatureEdge",
            dest_id=self.id,
            filter=filter
        )
        result = api.get_linked_list(context)
        return result

# edge classes
@strawberry.type
class DiseasePhenotypicFeatureEdge(CureEdge):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Disease
    end_node: PhenotypicFeature

@strawberry.type
class GeneDiseaseEdge(CureEdge):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Gene
    end_node: Disease

@strawberry.type
class DrugAdverseEventEdge(CureEdge):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Drug
    end_node: AdverseEvent

@strawberry.type
class GeneSequenceVariantEdge(CureEdge):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Gene
    end_node: SequenceVariant

@strawberry.type
class DrugPhenotypicFeatureEdge(CureEdge):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Drug
    end_node: PhenotypicFeature

@strawberry.type
class DrugDiseaseEdge(CureEdge):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: Drug
    end_node: Disease

@strawberry.type
class SequenceVariantDiseaseEdge(CureEdge):
    @strawberry.field()
    def provenance(root) -> Provenance:
        return Provenance.parse_provenance_fields(root)
    start_node: SequenceVariant
    end_node: Disease

VariantDiseaseQueryResult = make_linked_list_result_type("VariantDiseaseQueryResult", "VariantDiseaseLinkDetails", SequenceVariantDiseaseEdge, Disease)
DiseaseVariantQueryResult = make_linked_list_result_type("DiseaseVariantQueryResult", "DiseaseVariantLinkDetails", SequenceVariantDiseaseEdge, SequenceVariant)

VariantGeneQueryResult = make_linked_list_result_type("VariantGeneQueryResult", "VariantGeneLinkDetails", GeneSequenceVariantEdge, Gene)
GeneVariantQueryResult = make_linked_list_result_type("GeneVariantQueryResult", "GeneVariantLinkDetails", GeneSequenceVariantEdge, SequenceVariant)

PhenotypeDiseaseQueryResult = make_linked_list_result_type("PhenotypeDiseaseQueryResult", "PhenotypeDiseaseLinkDetails", DiseasePhenotypicFeatureEdge, Disease)
DiseasePhenotypeQueryResult = make_linked_list_result_type("DiseasePhenotypeQueryResult", "DiseasePhenotypeLinkDetails", DiseasePhenotypicFeatureEdge, PhenotypicFeature)

DrugDiseaseQueryResult = make_linked_list_result_type("DrugDiseaseQueryResult", "DrugDiseaseLinkDetails", DrugDiseaseEdge, Disease)
DiseaseDrugQueryResult = make_linked_list_result_type("DiseaseDrugQueryResult", "DiseaseDrugLinkDetails", DrugDiseaseEdge, Drug)

DrugAdverseEventQueryResult = make_linked_list_result_type("DrugAdverseEventQueryResult", "DrugAdverseEventLinkDetails", DrugAdverseEventEdge, AdverseEvent)
AdverseEventDrugQueryResult = make_linked_list_result_type("AdverseEventDrugQueryResult", "AdverseEventDrugLinkDetails", DrugAdverseEventEdge, Drug)

DrugPhenotypeQueryResult = make_linked_list_result_type("DrugPhenotypeQueryResult", "DrugPhenotypeLinkDetails", DrugPhenotypicFeatureEdge, PhenotypicFeature)
PhenotypeDrugQueryResult = make_linked_list_result_type("PhenotypeDrugQueryResult", "PhenotypeDrugLinkDetails", DrugPhenotypicFeatureEdge, Drug)

GeneDrugQueryResult = make_linked_list_result_type("GeneDrugQueryResult", "GeneDrugLinkDetails", GeneDiseaseEdge, Drug)
DrugGeneQueryResult = make_linked_list_result_type("DrugGeneQueryResult", "DrugGeneLinkDetails", GeneDiseaseEdge, Gene)

ENDPOINTS: Dict[type, Dict[str, str]] = {
    Drug: {
        "list": "drugs",
        "details": "resolve_drug"
    },
    Gene: {
        "list": "genes",
        "details": "resolve_gene"
    },
    Disease: {
        "list": "diseases",
        "details": "resolve_disease"
    },
    SequenceVariant: {
        "list": "sequence_variants",
        "details": "resolve_sequence_variants"
    },
    AdverseEvent: {
        "list": "adverse_events",
        "details": "resolve_adverse_event"
    },
    PhenotypicFeature: {
        "list": "phenotypic_features",
        "details": "resolve_phenotypic_feature"
    }
}

EDGES : Dict[type, str] = {
    DiseasePhenotypicFeatureEdge: "disease_phenotypic_feature_edges",
    GeneDiseaseEdge: "gene_disease_edges",
    DrugAdverseEventEdge: "drug_adverse_event_edges",
    GeneSequenceVariantEdge: "gene_sequence_variant_edges",
    DrugPhenotypicFeatureEdge: "drug_phenotypic_feature_edges",
    DrugDiseaseEdge: "drug_disease_edges",
    SequenceVariantDiseaseEdge: "sequence_variant_disease_edges"
}

def Query(url):
    resolvers = generate_resolvers(ENDPOINTS, EDGES, url)
    return strawberry.type(type("Query", (), resolvers))
