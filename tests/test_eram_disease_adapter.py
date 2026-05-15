from src.constants import DataSourceName
from src.input_adapters.pharos_mysql.eram_disease_adapter import ERAMDiseaseAdapter
from src.models.disease import Disease, ProteinDiseaseEdge


class FakeQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def join(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def order_by(self, *args, **kwargs):
        return self

    def __iter__(self):
        return iter(self.rows)


class FakeSession:
    def __init__(self, rows):
        self.rows = list(rows)

    def query(self, *args, **kwargs):
        return FakeQuery(self.rows)


def test_eram_adapter_reports_legacy_pharos_version():
    adapter = object.__new__(ERAMDiseaseAdapter)

    assert adapter.get_datasource_name() == DataSourceName.OldPharos
    assert adapter.get_version().version == "3.19"
    assert adapter.get_version().version_date.isoformat() == "2024-02-15"


def test_eram_adapter_emits_doid_diseases_and_collapsed_edges():
    adapter = object.__new__(ERAMDiseaseAdapter)
    adapter.get_session = lambda: FakeSession([
        ("DOID:1", "Disease One", "CTD_human|ORPHANET", "P11111"),
        ("DOID:1", "Disease One", "CLINVAR|ORPHANET", "P11111"),
        ("DOID:2", "Disease Two", "UniProtKB-KW", "Q22222"),
        ("", "Missing Disease Id", "GHR", "Q33333"),
        ("DOID:3", "", "GHR", "Q33333"),
        ("DOID:4", "Missing UniProt", "GHR", ""),
    ])

    batches = list(adapter.get_all())

    assert len(batches) == 2

    diseases = batches[0]
    edges = batches[1]

    assert [type(obj) for obj in diseases] == [Disease, Disease]
    assert [d.id for d in diseases] == ["DOID:1", "DOID:2"]
    assert [d.name for d in diseases] == ["Disease One", "Disease Two"]

    assert len(edges) == 2
    assert all(isinstance(edge, ProteinDiseaseEdge) for edge in edges)

    first_edge = edges[0]
    assert first_edge.start_node.id == "UniProtKB:P11111"
    assert first_edge.end_node.id == "DOID:1"
    assert len(first_edge.details) == 1
    assert first_edge.details[0].source == "eRAM"
    assert first_edge.details[0].source_id == "DOID:1"
    assert first_edge.details[0].original_sources == ["CTD_human", "ORPHANET", "CLINVAR"]

    second_edge = edges[1]
    assert second_edge.start_node.id == "UniProtKB:Q22222"
    assert second_edge.end_node.id == "DOID:2"
    assert second_edge.details[0].original_sources == ["UniProtKB-KW"]
