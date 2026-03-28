import gzip
from pathlib import Path

from src.input_adapters.ctd.ctd_gene_disease import CTDGeneDiseaseAdapter
from src.models.disease import Disease, GeneDiseaseEdge
from src.models.gene import Gene


def _write_ctd_fixture(path: Path) -> None:
    content = """# header
ABC\t1\tDisease One\tMESH:D000001\tmarker/mechanism\t\t12345|67890
XYZ\t2\tDisease Two\tOMIM:123456\ttherapeutic\t\t11111
"""
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        handle.write(content)


def test_ctd_adapter_emits_gene_disease_edges(tmp_path):
    fixture_path = tmp_path / "ctd.tsv.gz"
    _write_ctd_fixture(fixture_path)

    adapter = CTDGeneDiseaseAdapter(file_path=str(fixture_path))
    batches = list(adapter.get_all())

    assert len(batches) == 2

    diseases = batches[0]
    edges = batches[1]

    assert {type(obj) for obj in diseases} == {Disease}
    assert {d.id for d in diseases} == {"MESH:D000001", "OMIM:123456"}

    assert len(edges) == 2
    assert all(isinstance(edge, GeneDiseaseEdge) for edge in edges)
    assert all(isinstance(edge.start_node, Gene) for edge in edges)
    assert edges[0].start_node.id == "NCBIGene:1"
    assert edges[0].end_node.id == "MESH:D000001"
    assert len(edges[0].details) == 1
    detail = edges[0].details[0]
    assert detail.source == "CTD"
    assert detail.source_id == "MESH:D000001"
    assert detail.evidence_terms == ["marker/mechanism"]
    assert detail.pmids == ["12345", "67890"]


def test_ctd_adapter_honors_max_rows(tmp_path):
    fixture_path = tmp_path / "ctd.tsv.gz"
    _write_ctd_fixture(fixture_path)

    adapter = CTDGeneDiseaseAdapter(file_path=str(fixture_path), max_rows=1)
    batches = list(adapter.get_all())

    diseases = batches[0]
    edges = batches[1]

    assert len(diseases) == 1
    assert len(edges) == 1
    assert edges[0].start_node.id == "NCBIGene:1"
