from src.constants import DataSourceName
from src.input_adapters.pharos_mysql.phipster_adapter import PHIPSTERLegacyLiftAdapter, PHIPSTER_VERSION


class FakeQuery:
    def __init__(self, rows):
        self.rows = list(rows)

    def order_by(self, *args, **kwargs):
        return self

    def filter(self, *args, **kwargs):
        return self

    def join(self, *args, **kwargs):
        return self

    def limit(self, n):
        return FakeQuery(self.rows[:n])

    def __iter__(self):
        return iter(self.rows)


class FakeSession:
    def __init__(self, query_rows):
        self.query_rows = list(query_rows)
        self.idx = 0

    def query(self, *args, **kwargs):
        rows = self.query_rows[self.idx]
        self.idx += 1
        return FakeQuery(rows)


def test_phipster_adapter_reports_publication_version():
    adapter = object.__new__(PHIPSTERLegacyLiftAdapter)

    assert adapter.get_datasource_name() == DataSourceName.PHIPSTER
    assert adapter.get_version() == PHIPSTER_VERSION
    assert adapter.get_version().version_date.isoformat() == "2019-09-05"


def test_phipster_adapter_emits_grouped_legacy_lift_objects():
    adapter = object.__new__(PHIPSTERLegacyLiftAdapter)
    adapter.max_rows = 1
    session = FakeSession([
        [
            ("1006008", "RNA", "ssRNA(+)", "Picornavirales", "Picornaviridae", None, "Enterovirus", "Enterovirus A", "Human enterovirus 71 HZ08/Hangzhou/2008"),
        ],
        [
            (15, "full_polyprotein 1..2193", "AEB71507.1", "1006008"),
        ],
        [
            ("1006008", 15),
        ],
        [
            (15, "P19320", "P-HIPSTer", 128.345190881009, "1z7z:3I(0.09,0.19,19/20,12,14)", 0),
            (15, "P61981", "P-HIPSTer", 408.948397259193, None, 1),
        ],
    ])
    adapter.get_session = lambda: session

    batches = list(adapter.get_all())

    assert len(batches) == 4

    virus_nodes = batches[0]
    viral_protein_nodes = batches[1]
    parent_edges = batches[2]
    ppi_edges = batches[3]

    assert virus_nodes[0].id == "NCBITaxon:1006008"
    assert virus_nodes[0].family == "Picornaviridae"
    assert virus_nodes[0].name == "Human enterovirus 71 HZ08/Hangzhou/2008"

    assert viral_protein_nodes[0].id == "PHIPSTER.ViralProtein:15"
    assert viral_protein_nodes[0].ncbi == "AEB71507.1"

    assert parent_edges[0].start_node.id == "PHIPSTER.ViralProtein:15"
    assert parent_edges[0].end_node.id == "NCBITaxon:1006008"

    assert len(ppi_edges) == 1
    assert ppi_edges[0].start_node.id == "UniProtKB:P19320"
    assert ppi_edges[0].end_node.id == "PHIPSTER.ViralProtein:15"
    assert len(ppi_edges[0].details) == 1
    assert ppi_edges[0].details[0].source == "P-HIPSTer"
    assert ppi_edges[0].details[0].source_protein_id == "UniProtKB:P19320"
    assert ppi_edges[0].details[0].final_lr == 128.345190881009
    assert ppi_edges[0].details[0].pdb_ids == ["1z7z:3I(0.09,0.19,19/20,12,14)"]
    assert ppi_edges[0].details[0].high_confidence is False
