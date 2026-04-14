from src.input_adapters.pharos_arango.tcrd.panther import (
    PantherClassAdapter,
    ProteinPantherClassAdapter,
    panther_class_query,
    panther_class_parent_query
)


def test_panther_class_adapter_hydrates_parent_pcids():
    adapter = PantherClassAdapter.__new__(PantherClassAdapter)

    class_rows = [
        {
            "id": "PANTHER.CLASS:PC00000",
            "source_id": "PC00000",
            "name": "protein class",
            "description": None,
            "hierarchy_code": "1.00.00.00.00",
            "source": "PANTHER",
            "provenance": "PANTHER Protein Classes\t19.0\t2026-04-14\t2026-04-14",
            "sources": ["PANTHER Protein Classes\t19.0\t2026-04-14\t2026-04-14"],
        },
        {
            "id": "PANTHER.CLASS:PC00021",
            "source_id": "PC00021",
            "name": "G-protein coupled receptor",
            "description": "GPCR",
            "hierarchy_code": "1.01.01.00.00",
            "source": "PANTHER",
            "provenance": "PANTHER Protein Classes\t19.0\t2026-04-14\t2026-04-14",
            "sources": ["PANTHER Protein Classes\t19.0\t2026-04-14\t2026-04-14"],
        },
    ]
    parent_rows = [
        {"child": "PANTHER.CLASS:PC00021", "parent": "PANTHER.CLASS:PC00000"},
    ]

    class FakeDb:
        @staticmethod
        def has_collection(name):
            return name == "PantherClassParentEdge"

    def fake_run_query(query):
        if query == panther_class_query():
            return class_rows
        if query == panther_class_parent_query():
            return parent_rows
        return []

    adapter.get_db = lambda: FakeDb()
    adapter.runQuery = fake_run_query

    batches = list(adapter.get_all())
    assert len(batches) == 1
    nodes = {node.id: node for node in batches[0]}

    assert nodes["PANTHER.CLASS:PC00000"].parent_pcids is None
    assert nodes["PANTHER.CLASS:PC00021"].parent_pcids == "PC00000"


def test_protein_panther_class_adapter_emits_edges():
    adapter = ProteinPantherClassAdapter.__new__(ProteinPantherClassAdapter)
    adapter.batch_size = 10_000

    edge_rows = [
        {
            "_key": "1",
            "start_id": "IFXProtein:ABC123",
            "end_id": "PANTHER.CLASS:PC00021",
            "source": "PANTHER",
        }
    ]

    def fake_run_query(query):
        if "FOR rel IN `ProteinPantherClassEdge`" in query:
            rows = edge_rows.copy()
            edge_rows.clear()
            return rows
        return []

    adapter.runQuery = fake_run_query

    batches = list(adapter.get_all())
    assert len(batches) == 1
    edge = batches[0][0]
    assert edge.start_node.id == "IFXProtein:ABC123"
    assert edge.end_node.id == "PANTHER.CLASS:PC00021"
    assert edge.source == "PANTHER"
