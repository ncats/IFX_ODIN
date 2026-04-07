from typing import Dict, List

from src.constants import DataSourceName
from src.interfaces.id_resolver import IdResolver, IdMatch, NoMatchBehavior, MultiMatchBehavior
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, GeneDiseaseEdge, ProteinDiseaseEdge
from src.models.expression import GeneTissueExpressionEdge, ProteinTissueExpressionEdge
from src.models.gene import Gene
from src.models.pathway import GenePathwayEdge, Pathway, ProteinPathwayEdge
from src.models.protein import Protein
from src.models.tissue import Tissue
from src.models.alliance_genome import AllianceGeneDiseaseEdge


class _RetypeGeneResolver(IdResolver):
    def resolve_internal(self, input_nodes: List[Gene]) -> Dict[str, List[IdMatch]]:
        return {
            node.id: [IdMatch(node.id, f"IFXProtein:{node.id}", equivalent_ids=[f"NCBIGene:{node.id}"])]
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
