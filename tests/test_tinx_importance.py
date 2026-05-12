from pathlib import Path

from src.input_adapters.jensenlab.tinx import TINXImportanceFileAdapter
from src.models.disease import TINXImportanceEdge


def _write_fixture(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")


def _flatten_edge_batches(batches):
    return [edge for batch in batches for edge in batch if isinstance(edge, TINXImportanceEdge)]


def test_tinx_importance_adapter_emits_expected_scores(tmp_path):
    protein_path = tmp_path / "human_textmining_mentions.tsv"
    disease_path = tmp_path / "disease_textmining_mentions.tsv"

    _write_fixture(
        protein_path,
        (
            "ENSP0001\t1 2\n"
            "ENSP0002\t2 3\n"
        ),
    )
    _write_fixture(
        disease_path,
        (
            "DOID:1\t1 2\n"
            "DOID:2\t2 3\n"
        ),
    )

    adapter = TINXImportanceFileAdapter(
        protein_mentions_file_path=str(protein_path),
        disease_mentions_file_path=str(disease_path),
    )

    edges = _flatten_edge_batches(list(adapter.get_all()))

    assert len(edges) == 4

    scores = {
        (edge.end_node.id, edge.start_node.id): edge.details[0].importance[0]
        for edge in edges
    }
    assert scores == {
        ("DOID:1", "ENSEMBL:ENSP0001"): 1.25,
        ("DOID:1", "ENSEMBL:ENSP0002"): 0.25,
        ("DOID:2", "ENSEMBL:ENSP0001"): 0.25,
        ("DOID:2", "ENSEMBL:ENSP0002"): 1.25,
    }


def test_tinx_importance_adapter_honors_max_diseases_and_max_pairs(tmp_path):
    protein_path = tmp_path / "human_textmining_mentions.tsv"
    disease_path = tmp_path / "disease_textmining_mentions.tsv"

    _write_fixture(
        protein_path,
        (
            "ENSP0001\t1 2\n"
            "ENSP0002\t2 3\n"
        ),
    )
    _write_fixture(
        disease_path,
        (
            "DOID:1\t1 2\n"
            "DOID:2\t2 3\n"
        ),
    )

    adapter = TINXImportanceFileAdapter(
        protein_mentions_file_path=str(protein_path),
        disease_mentions_file_path=str(disease_path),
        max_diseases=1,
        max_pairs=1,
    )

    edges = _flatten_edge_batches(list(adapter.get_all()))

    assert len(edges) == 1
    edge = edges[0]
    assert edge.end_node.id == "DOID:1"
    assert edge.start_node.id == "ENSEMBL:ENSP0001"
    assert edge.details[0].importance == [1.5]
