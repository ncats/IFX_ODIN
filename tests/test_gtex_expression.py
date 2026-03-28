import csv
import gzip
from pathlib import Path

from src.input_adapters.gtex.gtex_expression import GTExExpressionAdapter
from src.models.expression import GeneTissueExpressionEdge
from src.models.gene import Gene
from src.models.tissue import Tissue


def _write_gtex_fixture(tmp_path: Path):
    matrix_path = tmp_path / "gtex.gct.gz"
    with gzip.open(matrix_path, "wt", encoding="utf-8") as handle:
        handle.write("#1.2\n")
        handle.write("2\t2\n")
        writer = csv.writer(handle, delimiter="\t")
        writer.writerow(["Name", "Description", "GTEX-1117F-0001-SM-A", "GTEX-1117F-0002-SM-B"])
        writer.writerow(["ENSG00000000003.1", "TSPAN6", "1.0", "2.0"])
        writer.writerow(["ENSG00000000005.1", "TNMD", "0.0", "5.0"])

    sample_attr_path = tmp_path / "sample_attributes.txt"
    with open(sample_attr_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["SAMPID", "SMTSD", "SMTS", "SMUBRID", "SMATSSCR"], delimiter="\t")
        writer.writeheader()
        writer.writerow({"SAMPID": "GTEX-1117F-0001-SM-A", "SMTSD": "liver", "SMTS": "liver", "SMUBRID": "UBERON:0002107", "SMATSSCR": "1"})
        writer.writerow({"SAMPID": "GTEX-1117F-0002-SM-B", "SMTSD": "heart", "SMTS": "heart", "SMUBRID": "UBERON:0000948", "SMATSSCR": "1"})

    subject_path = tmp_path / "subject_phenotypes.txt"
    with open(subject_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["SUBJID", "SEX", "DTHHRDY"], delimiter="\t")
        writer.writeheader()
        writer.writerow({"SUBJID": "GTEX-1117F", "SEX": "1", "DTHHRDY": "1"})

    version_path = tmp_path / "gtex_version.tsv"
    with open(version_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["version", "version_date"], delimiter="\t")
        writer.writeheader()
        writer.writerow({"version": "v11", "version_date": "2025-08-22"})

    return matrix_path, sample_attr_path, subject_path, version_path


def test_gtex_adapter_emits_gene_expression_edges(tmp_path):
    matrix_path, sample_attr_path, subject_path, version_path = _write_gtex_fixture(tmp_path)

    adapter = GTExExpressionAdapter(
        matrix_file_path=str(matrix_path),
        sample_attributes_file_path=str(sample_attr_path),
        subject_phenotypes_file_path=str(subject_path),
        version_file_path=str(version_path),
        max_genes=1,
    )

    batches = list(adapter.get_all())
    assert len(batches) == 2

    tissues = batches[0]
    expression_batch = batches[1]

    assert all(isinstance(t, Tissue) for t in tissues)
    assert any(t.id == "UBERON:0002107" for t in tissues)

    genes = [obj for obj in expression_batch if isinstance(obj, Gene)]
    edges = [obj for obj in expression_batch if isinstance(obj, GeneTissueExpressionEdge)]

    assert len(genes) == 1
    assert genes[0].id == "ENSEMBL:ENSG00000000003"
    assert genes[0].calculated_properties["gtex_tau"] >= 0
    assert "gtex_tau_male" in genes[0].calculated_properties
    assert "gtex_tau_female" in genes[0].calculated_properties
    assert len(edges) == 2
    assert all(isinstance(edge.start_node, Gene) for edge in edges)
    assert {edge.start_node.id for edge in edges} == {"ENSEMBL:ENSG00000000003"}
    assert {edge.end_node.id for edge in edges} == {"UBERON:0002107", "UBERON:0000948"}
    assert all(detail.source == "GTEx" for edge in edges for detail in edge.details)
