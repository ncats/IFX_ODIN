from src.models.protein import Protein
from src.output_adapters.arango_output_adapter import ArangoOutputAdapter


class FakeCollection:
    def __init__(self):
        self.insert_calls = []
        self.update_calls = []

    def insert_many(self, docs, overwrite=False):
        self.insert_calls.append({
            "docs": docs,
            "overwrite": overwrite,
        })
        return []

    def update_many(self, docs, merge=True, keep_none=False, check_rev=False):
        self.update_calls.append({
            "docs": docs,
            "merge": merge,
            "keep_none": keep_none,
            "check_rev": check_rev,
        })
        return []


class FakeGraph:
    def has_edge_collection(self, label):
        return False


def make_protein(protein_id: str, name: str, entity_resolution: str, provenance: str) -> Protein:
    protein = Protein(id=protein_id, name=name)
    protein.entity_resolution = entity_resolution
    protein.provenance = provenance
    return protein


def build_adapter(existing_nodes, collection):
    adapter = ArangoOutputAdapter.__new__(ArangoOutputAdapter)
    adapter._collection_schemas = {}
    adapter._graph_views = []
    adapter._graph_view_source_yaml = None
    adapter.minio_creds = None
    adapter.database_name = "test_db"
    adapter._handle_dataset_nodes = lambda objects: None
    adapter._handle_pounce_workbook_nodes = lambda objects: None
    adapter.get_db = lambda: object()
    adapter.get_graph = lambda: FakeGraph()
    adapter.create_indexes = lambda obj_cls, coll: None
    adapter.get_existing_nodes = lambda db, label, obj_list, skip_merge=False: (collection, existing_nodes)
    return adapter


def test_get_node_merge_fetch_fields_keeps_only_merge_relevant_fields():
    adapter = ArangoOutputAdapter.__new__(ArangoOutputAdapter)
    obj_list = [{
        "id": "IFXProtein:P1",
        "name": "Alpha",
        "pm_score": [1.0],
        "entity_resolution": "resolver-1",
        "provenance": "source-1",
        "_internal": "ignored",
    }]

    fields = adapter.get_node_merge_fetch_fields(obj_list)

    assert fields == [
        "_key",
        "creation",
        "id",
        "name",
        "pm_score",
        "resolved_ids",
        "updates",
    ]


def test_store_uses_update_many_for_existing_nodes_and_insert_many_for_new_nodes():
    existing_nodes = [{
        "_key": "IFXProtein:P1",
        "id": "IFXProtein:P1",
        "name": "Old name",
        "creation": "creation-source",
        "updates": ["existing-update"],
        "resolved_ids": ["resolver-old"],
    }]
    collection = FakeCollection()
    adapter = build_adapter(existing_nodes, collection)

    existing = make_protein("IFXProtein:P1", "New name", "resolver-new", "source-new")
    new = make_protein("IFXProtein:P2", "Brand new", "resolver-brand-new", "source-brand-new")

    adapter.store([existing, new], single_source=False)

    assert len(collection.update_calls) == 1
    assert len(collection.insert_calls) == 1

    updated_docs = collection.update_calls[0]["docs"]
    inserted_docs = collection.insert_calls[0]["docs"]

    assert len(updated_docs) == 1
    assert updated_docs[0]["_key"] == "IFXProtein:P1"
    assert updated_docs[0]["creation"] == "creation-source"
    assert sorted(updated_docs[0]["resolved_ids"]) == ["resolver-new", "resolver-old"]
    assert "existing-update" in updated_docs[0]["updates"]
    assert any("name\tOld name\tNew name\tsource-new\tKeepLast" == update for update in updated_docs[0]["updates"])

    assert len(inserted_docs) == 1
    assert inserted_docs[0]["_key"] == "IFXProtein:P2"
    assert inserted_docs[0]["creation"] == "source-brand-new"
    assert inserted_docs[0]["resolved_ids"] == ["resolver-brand-new"]
    assert collection.insert_calls[0]["overwrite"] is False
    assert collection.update_calls[0]["merge"] is True
    assert collection.update_calls[0]["keep_none"] is False
    assert collection.update_calls[0]["check_rev"] is False
