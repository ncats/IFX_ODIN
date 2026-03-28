from pathlib import Path

from src.input_adapters.wikipathways.wikipathways_pathways import (
    WikiPathwaysGenePathwayEdgeAdapter,
    WikiPathwaysPathwayAdapter,
)
from src.models.gene import Gene
from src.models.pathway import GenePathwayEdge, Pathway


def _write_wikipathways_fixture(path: Path) -> None:
    path.write_text(
        "Pathway A%WikiPathways_20260310%WP1%Homo sapiens\thttps://www.wikipathways.org/instance/WP1\t1017\t1018\tABC\n"
        "Pathway B%WikiPathways_20260310%WP2%Homo sapiens\thttps://www.wikipathways.org/instance/WP2\t7157\n",
        encoding="utf-8",
    )


def test_wikipathways_pathway_adapter_emits_pathway_nodes(tmp_path):
    fixture_path = tmp_path / "wikipathways.gmt"
    _write_wikipathways_fixture(fixture_path)

    adapter = WikiPathwaysPathwayAdapter(file_path=str(fixture_path))
    batches = list(adapter.get_all())

    assert len(batches) == 1
    pathways = batches[0]
    assert {type(obj) for obj in pathways} == {Pathway}
    assert [p.id for p in pathways] == ["WP1", "WP2"]


def test_wikipathways_gene_pathway_adapter_emits_gene_edges(tmp_path):
    fixture_path = tmp_path / "wikipathways.gmt"
    _write_wikipathways_fixture(fixture_path)

    adapter = WikiPathwaysGenePathwayEdgeAdapter(file_path=str(fixture_path))
    batches = list(adapter.get_all())

    assert len(batches) == 1
    edges = batches[0]
    assert len(edges) == 3
    assert all(isinstance(edge, GenePathwayEdge) for edge in edges)
    assert all(isinstance(edge.start_node, Gene) for edge in edges)
    assert edges[0].start_node.id == "NCBIGene:1017"
    assert edges[0].end_node.id == "WP1"
    assert edges[0].source == "WikiPathways"


def test_wikipathways_adapter_honors_max_rows(tmp_path):
    fixture_path = tmp_path / "wikipathways.gmt"
    _write_wikipathways_fixture(fixture_path)

    adapter = WikiPathwaysGenePathwayEdgeAdapter(file_path=str(fixture_path), max_rows=1)
    batches = list(adapter.get_all())

    edges = batches[0]
    assert len(edges) == 2
    assert {edge.end_node.id for edge in edges} == {"WP1"}
