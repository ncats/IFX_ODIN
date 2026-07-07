import csv
import io
import zipfile
from datetime import date
from pathlib import Path

from src.input_adapters.hpa.hpa_expression import HPAProteinExpressionAdapter, HPARnaExpressionAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.expression import GeneTissueExpressionEdge
from src.models.gene import Gene
from src.models.tissue import Tissue


class FakeDataSource:
    def __init__(self, root: Path):
        self.root = root

    def file(self, file_name: str):
        return self.root / file_name

    def version_info(self):
        return DatasourceVersionInfo(
            version="24.0",
            version_date=date(2026, 1, 1),
        )


def _write_hpa_protein_fixture(tmp_path: Path):
    data_path = tmp_path / "normal_ihc_data.tsv.zip"
    with zipfile.ZipFile(data_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        payload = io.StringIO()
        writer = csv.DictWriter(
            payload,
            fieldnames=["Gene", "Gene name", "Tissue", "IHC tissue name", "Cell type", "Level", "Reliability"],
            delimiter="\t",
        )
        writer.writeheader()
        writer.writerow(
            {
                "Gene": "ENSG00000000003",
                "Gene name": "TSPAN6",
                "Tissue": "Liver",
                "IHC tissue name": "Liver",
                "Cell type": "hepatocytes",
                "Level": "High",
                "Reliability": "Approved",
            }
        )
        writer.writerow(
            {
                "Gene": "ENSG00000000003",
                "Gene name": "TSPAN6",
                "Tissue": "Liver",
                "IHC tissue name": "Liver",
                "Cell type": "kupffer cells",
                "Level": "Low",
                "Reliability": "Supported",
            }
        )
        writer.writerow(
            {
                "Gene": "ENSG00000000003",
                "Gene name": "TSPAN6",
                "Tissue": "Heart muscle",
                "IHC tissue name": "Heart muscle",
                "Cell type": "cardiomyocytes",
                "Level": "Not detected",
                "Reliability": "Approved",
            }
        )
        writer.writerow(
            {
                "Gene": "ENSG00000000005",
                "Gene name": "TNMD",
                "Tissue": "Liver",
                "IHC tissue name": "Liver",
                "Cell type": "hepatocytes",
                "Level": "Medium",
                "Reliability": "Approved",
            }
        )
        zf.writestr("normal_ihc_data.tsv", payload.getvalue())

    uberon_map_path = tmp_path / "manual_uberon_map.tsv"
    uberon_map_path.write_text(
        "liver\tUBERON:0002107\nliver - hepatocytes\tUBERON:0002107\nheart muscle\tUBERON:0000948\n",
        encoding="utf-8",
    )

    return data_path, uberon_map_path


def _write_hpa_rna_fixture(tmp_path: Path):
    data_path = tmp_path / "rna_tissue_hpa.tsv.zip"
    with zipfile.ZipFile(data_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        payload = io.StringIO()
        writer = csv.DictWriter(payload, fieldnames=["Gene", "Gene name", "Tissue", "TPM", "pTPM", "nTPM"], delimiter="\t")
        writer.writeheader()
        writer.writerow({"Gene": "ENSG00000000003", "Gene name": "TSPAN6", "Tissue": "liver", "TPM": "20.8", "pTPM": "25.9", "nTPM": "19.3"})
        writer.writerow({"Gene": "ENSG00000000003", "Gene name": "TSPAN6", "Tissue": "heart muscle", "TPM": "17.7", "pTPM": "22.4", "nTPM": "13.3"})
        writer.writerow({"Gene": "ENSG00000000005", "Gene name": "TNMD", "Tissue": "liver", "TPM": "0.0", "pTPM": "0.0", "nTPM": "0.0"})
        zf.writestr("rna_tissue_hpa.tsv", payload.getvalue())

    uberon_map_path = tmp_path / "manual_uberon_map.tsv"
    uberon_map_path.write_text("liver\tUBERON:0002107\nheart muscle\tUBERON:0000948\n", encoding="utf-8")

    return data_path, uberon_map_path


def test_hpa_protein_adapter_emits_gene_expression_edges(tmp_path):
    _data_path, uberon_map_path = _write_hpa_protein_fixture(tmp_path)

    adapter = HPAProteinExpressionAdapter(
        uberon_map_file_path=str(uberon_map_path),
        data_source=FakeDataSource(tmp_path),
        max_genes=1,
    )

    batches = list(adapter.get_all())
    assert len(batches) == 2

    tissues = batches[0]
    expression_batch = batches[1]

    assert all(isinstance(t, Tissue) for t in tissues)
    assert {t.id for t in tissues} == {"UBERON:0002107", "UBERON:0000948"}

    genes = [obj for obj in expression_batch if isinstance(obj, Gene)]
    edges = [obj for obj in expression_batch if isinstance(obj, GeneTissueExpressionEdge)]

    assert len(genes) == 1
    assert genes[0].id == "ENSEMBL:ENSG00000000003"
    assert genes[0].calculated_properties["hpa_ihc_tau"] >= 0
    assert len(edges) == 2
    assert all(isinstance(edge.start_node, Gene) for edge in edges)
    assert {edge.start_node.id for edge in edges} == {"ENSEMBL:ENSG00000000003"}
    assert {edge.end_node.id for edge in edges} == {"UBERON:0002107", "UBERON:0000948"}
    assert all(detail.source == "HPA Protein" for edge in edges for detail in edge.details)
    assert {detail.cell_type for edge in edges for detail in edge.details} == {"hepatocytes", "kupffer cells", "cardiomyocytes"}


def test_hpa_rna_adapter_emits_gene_expression_edges(tmp_path):
    _data_path, uberon_map_path = _write_hpa_rna_fixture(tmp_path)

    adapter = HPARnaExpressionAdapter(
        uberon_map_file_path=str(uberon_map_path),
        data_source=FakeDataSource(tmp_path),
        max_genes=1,
    )

    batches = list(adapter.get_all())
    assert len(batches) == 2

    tissues = batches[0]
    expression_batch = batches[1]

    assert all(isinstance(t, Tissue) for t in tissues)
    assert {t.id for t in tissues} == {"UBERON:0002107", "UBERON:0000948"}

    genes = [obj for obj in expression_batch if isinstance(obj, Gene)]
    edges = [obj for obj in expression_batch if isinstance(obj, GeneTissueExpressionEdge)]

    assert len(genes) == 1
    assert genes[0].id == "ENSEMBL:ENSG00000000003"
    assert genes[0].calculated_properties["hpa_rna_tau"] >= 0
    assert len(edges) == 2
    assert all(isinstance(edge.start_node, Gene) for edge in edges)
    assert {edge.start_node.id for edge in edges} == {"ENSEMBL:ENSG00000000003"}
    assert {edge.end_node.id for edge in edges} == {"UBERON:0002107", "UBERON:0000948"}
    assert all(detail.source == "HPA RNA" for edge in edges for detail in edge.details)
