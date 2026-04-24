from typing import Dict, List

from src.constants import DataSourceName
from src.interfaces.id_resolver import IdResolver, IdMatch, NoMatchBehavior, MultiMatchBehavior
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, GeneDiseaseEdge, ProteinDiseaseEdge
from src.models.expression import GeneTissueExpressionEdge, ProteinTissueExpressionEdge
from src.models.gene import Gene, MeasuredGene, MeasuredGeneEdge
from src.models.pathway import GenePathwayEdge, Pathway, ProteinPathwayEdge
from src.models.protein import Protein, MeasuredProtein, MeasuredProteinEdge
from src.models.tissue import Tissue
from src.models.alliance_genome import AllianceGeneDiseaseEdge


class _RetypeGeneResolver(IdResolver):
    def resolve_internal(self, input_nodes: List[Gene]) -> Dict[str, List[IdMatch]]:
        return {
            node.id: [IdMatch(node.id, f"IFXProtein:{node.id}", equivalent_ids=[f"NCBIGene:{node.id}"])]
            for node in input_nodes
        }


class _IdentityResolver(IdResolver):
    def resolve_internal(self, input_nodes):
        return {
            node.id: [IdMatch(node.id, node.id, equivalent_ids=[node.id])]
            for node in input_nodes
        }


class _SingleBatchAdapter(InputAdapter):
    def __init__(self, entries):
        self._entries = entries

    def get_all(self):
        yield self._entries

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TargetGraph

    def get_version(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(version="test", version_date=None, download_date=None)


def _resolver_map():
    return {
        "Gene": _RetypeGeneResolver(
            types=["Gene"],
            no_match_behavior=NoMatchBehavior.Skip,
            multi_match_behavior=MultiMatchBehavior.All,
            canonical_class=Protein,
        )
    }


def test_input_adapter_remaps_gene_tissue_expression_edge_to_protein_edge():
    edge = GeneTissueExpressionEdge(start_node=Gene(id="G1"), end_node=Tissue(id="T1"), details=[])
    adapter = _SingleBatchAdapter([edge])

    batches = list(adapter.get_resolved_and_provenanced_list(_resolver_map()))
    rel = batches[-1][0]

    assert isinstance(rel, ProteinTissueExpressionEdge)
    assert rel.start_node.id == "IFXProtein:G1"


def test_input_adapter_remaps_gene_disease_edge_to_protein_edge():
    edge = GeneDiseaseEdge(start_node=Gene(id="G1"), end_node=Disease(id="D1"), details=[])
    adapter = _SingleBatchAdapter([edge])

    batches = list(adapter.get_resolved_and_provenanced_list(_resolver_map()))
    rel = batches[-1][0]

    assert isinstance(rel, ProteinDiseaseEdge)
    assert rel.start_node.id == "IFXProtein:G1"


def test_input_adapter_remaps_gene_pathway_edge_to_protein_edge():
    edge = GenePathwayEdge(start_node=Gene(id="G1"), end_node=Pathway(id="PW1"), source="x")
    adapter = _SingleBatchAdapter([edge])

    batches = list(adapter.get_resolved_and_provenanced_list(_resolver_map()))
    rel = batches[-1][0]

    assert isinstance(rel, ProteinPathwayEdge)
    assert rel.start_node.id == "IFXProtein:G1"
    assert rel.source == "x"


def test_input_adapter_leaves_unmapped_gene_edge_classes_unchanged():
    edge = AllianceGeneDiseaseEdge(start_node=Gene(id="G1"), end_node=Disease(id="D1"), evidence_terms=["ECO:1"])
    adapter = _SingleBatchAdapter([edge])

    batches = list(adapter.get_resolved_and_provenanced_list(_resolver_map()))
    rel = batches[-1][0]

    assert isinstance(rel, AllianceGeneDiseaseEdge)
    assert isinstance(rel.start_node, Protein)
    assert rel.start_node.id == "IFXProtein:G1"
    assert rel.evidence_terms == ["ECO:1"]


def test_input_adapter_keeps_measured_gene_edge_start_node_typed_as_measured_gene():
    edge = MeasuredGeneEdge(
        start_node=MeasuredGene(id="Ensembl:G1", symbol="GENE1"),
        end_node=Gene(id="Ensembl:G1"),
    )
    adapter = _SingleBatchAdapter([edge])

    batches = list(adapter.get_resolved_and_provenanced_list({
        "Gene": _IdentityResolver(
            types=["Gene"],
            no_match_behavior=NoMatchBehavior.Skip,
            multi_match_behavior=MultiMatchBehavior.All,
        )
    }))
    rel = batches[-1][0]

    assert isinstance(rel, MeasuredGeneEdge)
    assert isinstance(rel.start_node, MeasuredGene)
    assert rel.start_node.id == "Ensembl:G1"
    assert isinstance(rel.end_node, Gene)
    assert rel.end_node.id == "Ensembl:G1"


def test_input_adapter_keeps_measured_protein_edge_start_node_typed_as_measured_protein():
    edge = MeasuredProteinEdge(
        start_node=MeasuredProtein(id="UniProtKB:P12345", name="Protein 1"),
        end_node=Protein(id="UniProtKB:P12345"),
    )
    adapter = _SingleBatchAdapter([edge])

    batches = list(adapter.get_resolved_and_provenanced_list({
        "Protein": _IdentityResolver(
            types=["Protein"],
            no_match_behavior=NoMatchBehavior.Skip,
            multi_match_behavior=MultiMatchBehavior.All,
        )
    }))
    rel = batches[-1][0]

    assert isinstance(rel, MeasuredProteinEdge)
    assert isinstance(rel.start_node, MeasuredProtein)
    assert rel.start_node.id == "UniProtKB:P12345"
    assert isinstance(rel.end_node, Protein)
    assert rel.end_node.id == "UniProtKB:P12345"


def test_input_adapter_preserves_existing_edge_sources_and_provenance():
    edge = ProteinPathwayEdge(
        start_node=Protein(id="UniProtKB:P1"),
        end_node=Pathway(id="Reactome:R-HSA-1"),
        source="Reactome",
    )
    edge.sources = ["BioPlex\t3.0 (293T)\t2024-01-19\t2026-04-24", "Reactome\t96\t2026-03-24\t2026-04-24"]
    edge.provenance = "BioPlex\t3.0 (293T)\t2024-01-19\t2026-04-24"
    adapter = _SingleBatchAdapter([edge])

    batches = list(adapter.get_resolved_and_provenanced_list({
        "Protein": _IdentityResolver(
            types=["Protein"],
            no_match_behavior=NoMatchBehavior.Skip,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
        "Pathway": _IdentityResolver(
            types=["Pathway"],
            no_match_behavior=NoMatchBehavior.Skip,
            multi_match_behavior=MultiMatchBehavior.All,
        ),
    }))
    rel = batches[-1][0]

    assert rel.sources == ["BioPlex\t3.0 (293T)\t2024-01-19\t2026-04-24", "Reactome\t96\t2026-03-24\t2026-04-24"]
    assert rel.provenance == "BioPlex\t3.0 (293T)\t2024-01-19\t2026-04-24"
