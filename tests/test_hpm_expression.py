import csv
from datetime import date
from pathlib import Path

from src.input_adapters.hpm.hpm_expression import HPMExpressionAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.expression import ProteinTissueExpressionEdge
from src.models.protein import Protein
from src.models.tissue import Tissue


class FakeDataSource:
    def __init__(self, root: Path):
        self.root = root

    def file(self, file_name: str):
        return self.root / file_name

    def version_info(self):
        return DatasourceVersionInfo(
            version="A draft map of the human proteome (2014)",
            version_date=date(2014, 5, 29),
        )


def _write_hpm_fixture(tmp_path: Path):
    data_path = tmp_path / "HPM_protein_level_expression_matrix_Kim_et_al_052914.csv"
    with open(data_path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["Accession", "RefSeq Accession", "Adult Liver", "Adult Heart"],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Accession": "1",
                "RefSeq Accession": "NP_000445.1",
                "Adult Liver": "10.0",
                "Adult Heart": "5.0",
            }
        )
        writer.writerow(
            {
                "Accession": "2",
                "RefSeq Accession": "NP_006348.1",
                "Adult Liver": "1.0",
                "Adult Heart": "0.0",
            }
        )

    uberon_map_path = tmp_path / "manual_uberon_map.tsv"
    uberon_map_path.write_text("adult liver\tUBERON:0002107\nadult heart\tUBERON:0000948\n", encoding="utf-8")

    return data_path, uberon_map_path


def test_hpm_adapter_emits_protein_expression_edges(tmp_path):
    _data_path, uberon_map_path = _write_hpm_fixture(tmp_path)

    adapter = HPMExpressionAdapter(
        uberon_map_file_path=str(uberon_map_path),
        data_source=FakeDataSource(tmp_path),
        max_rows=1,
    )

    batches = list(adapter.get_all())
    assert len(batches) == 2

    tissues = batches[0]
    expression_batch = batches[1]

    assert all(isinstance(t, Tissue) for t in tissues)
    assert {t.id for t in tissues} == {"UBERON:0002107", "UBERON:0000948"}

    proteins = [obj for obj in expression_batch if isinstance(obj, Protein)]
    edges = [obj for obj in expression_batch if isinstance(obj, ProteinTissueExpressionEdge)]

    assert len(proteins) == 1
    assert proteins[0].id == "RefSeq:NP_000445"
    assert proteins[0].calculated_properties["hpm_tau"] > 0

    assert len(edges) == 2
    assert all(edge.start_node.id == "RefSeq:NP_000445" for edge in edges)
    assert {edge.end_node.id for edge in edges} == {"UBERON:0002107", "UBERON:0000948"}
    assert all(detail.source == "HPM Protein" for edge in edges for detail in edge.details)
