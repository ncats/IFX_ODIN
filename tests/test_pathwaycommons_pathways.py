import gzip
from pathlib import Path

from src.input_adapters.pathwaycommons.pathwaycommons_pathways import (
    PathwayCommonsGenePathwayEdgeAdapter,
    PathwayCommonsPathwayAdapter,
)
from src.models.gene import Gene
from src.models.pathway import GenePathwayEdge, Pathway


def _write_pathwaycommons_fixture(path: Path) -> None:
    content = (
        "biofactoid:abc\tname: Example Pathway; datasource: biofactoid; organism: 9606; idtype: hgnc.symbol\tAPP\tNLRP3\n"
        "https://identifiers.org/pathbank:PWB123\tname: PathBank Example; datasource: pathbank; organism: 9606; idtype: hgnc.symbol\tEIF4A3\n"
        "reactome:R-HSA-1\tname: Skip Reactome; datasource: reactome; organism: 9606; idtype: hgnc.symbol\tSHOULD_SKIP\n"
    )
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(content)


def test_pathwaycommons_pathway_adapter_emits_filtered_pathway_nodes(tmp_path):
    fixture_path = tmp_path / "pathwaycommons.gmt.gz"
    _write_pathwaycommons_fixture(fixture_path)

    adapter = PathwayCommonsPathwayAdapter(file_path=str(fixture_path))
    batches = list(adapter.get_all())

    assert len(batches) == 1
    pathways = batches[0]
    assert {type(obj) for obj in pathways} == {Pathway}
    assert len(pathways) == 2
    assert pathways[0].id == "biofactoid:abc"
    assert pathways[1].id == "pathbank:PWB123"
    assert pathways[1].source_id == "PWB123"


def test_pathwaycommons_gene_pathway_adapter_emits_gene_edges(tmp_path):
    fixture_path = tmp_path / "pathwaycommons.gmt.gz"
    _write_pathwaycommons_fixture(fixture_path)

    adapter = PathwayCommonsGenePathwayEdgeAdapter(file_path=str(fixture_path))
    batches = list(adapter.get_all())

    assert len(batches) == 1
    edges = batches[0]
    assert len(edges) == 3
    assert all(isinstance(edge, GenePathwayEdge) for edge in edges)
    assert all(isinstance(edge.start_node, Gene) for edge in edges)
    assert edges[0].start_node.id == "Symbol:APP"
    assert edges[0].end_node.id == "biofactoid:abc"
    assert edges[0].source == "PathwayCommons"


def test_pathwaycommons_adapter_honors_max_rows(tmp_path):
    fixture_path = tmp_path / "pathwaycommons.gmt.gz"
    _write_pathwaycommons_fixture(fixture_path)

    adapter = PathwayCommonsGenePathwayEdgeAdapter(file_path=str(fixture_path), max_rows=1)
    batches = list(adapter.get_all())

    edges = batches[0]
    assert len(edges) == 2
    assert {edge.end_node.id for edge in edges} == {"biofactoid:abc"}
