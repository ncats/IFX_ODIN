import csv
from pathlib import Path

from src.input_adapters.jensenlab.tissues_expression import JensenLabTissuesExpressionAdapter
from src.models.expression import ProteinTissueExpressionEdge
from src.models.tissue import Tissue


def _write_jensenlab_fixture(tmp_path: Path):
    data_path = tmp_path / "human_tissue_integrated_full.tsv"
    rows = [
        ["ENSP00000000001", "PROT1", "BTO:0000107", "liver", "3.5"],
        ["ENSP00000000001", "PROT1", "BTO:0000089", "heart", "4.0"],
        ["ENSP00000000002", "PROT2", "BTO:0000107", "liver", "1.2"],
        ["SYMBOL_ONLY", "BAD", "BTO:0000107", "liver", "9.9"],
    ]
    with open(data_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerows(rows)

    version_path = tmp_path / "tissues_version.tsv"
    with open(version_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["version", "version_date"], delimiter="\t")
        writer.writeheader()
        writer.writerow({"version": "v1", "version_date": "2026-01-01"})

    return data_path, version_path


def test_jensenlab_adapter_emits_protein_tissue_edges(tmp_path):
    data_path, version_path = _write_jensenlab_fixture(tmp_path)

    adapter = JensenLabTissuesExpressionAdapter(
        data_file_path=str(data_path),
        version_file_path=str(version_path),
        max_rows=3,
    )

    batches = list(adapter.get_all())
    assert len(batches) == 2

    tissues = batches[0]
    edges = batches[1]

    assert all(isinstance(t, Tissue) for t in tissues)
    assert {t.id for t in tissues} == {"BTO:0000107", "BTO:0000089"}

    assert len(edges) == 3
    assert all(isinstance(edge, ProteinTissueExpressionEdge) for edge in edges)
    assert {edge.start_node.id for edge in edges} == {"ENSEMBL:ENSP00000000001", "ENSEMBL:ENSP00000000002"}
    assert {edge.end_node.id for edge in edges} == {"BTO:0000107", "BTO:0000089"}
    assert all(detail.source == "JensenLab" for edge in edges for detail in edge.details)


def test_jensenlab_adapter_honors_max_rows(tmp_path):
    data_path, version_path = _write_jensenlab_fixture(tmp_path)

    adapter = JensenLabTissuesExpressionAdapter(
        data_file_path=str(data_path),
        version_file_path=str(version_path),
        max_rows=2,
    )

    batches = list(adapter.get_all())
    edges = batches[1]

    assert len(edges) == 2
    assert {edge.start_node.id for edge in edges} == {"ENSEMBL:ENSP00000000001"}


def test_jensenlab_adapter_max_rows_counts_kept_rows_only(tmp_path):
    data_path = tmp_path / "human_tissue_integrated_full.tsv"
    rows = [
        ["SYMBOL_ONLY", "BAD", "BTO:0000107", "liver", "9.9"],
        ["ENSP00000000001", "PROT1", "BTO:0000107", "liver", "3.5"],
        ["ENSP00000000001", "PROT1", "BTO:0000089", "heart", "4.0"],
        ["ENSP00000000002", "PROT2", "BTO:0000107", "liver", "1.2"],
    ]
    with open(data_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle, delimiter="\t")
        writer.writerows(rows)

    version_path = tmp_path / "tissues_version.tsv"
    with open(version_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["version", "version_date"], delimiter="\t")
        writer.writeheader()
        writer.writerow({"version": "v1", "version_date": "2026-01-01"})

    adapter = JensenLabTissuesExpressionAdapter(
        data_file_path=str(data_path),
        version_file_path=str(version_path),
        max_rows=2,
    )

    batches = list(adapter.get_all())
    edges = batches[1]

    assert len(edges) == 2
    assert {edge.start_node.id for edge in edges} == {"ENSEMBL:ENSP00000000001"}
