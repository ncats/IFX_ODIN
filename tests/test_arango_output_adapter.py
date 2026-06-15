import json
from pathlib import Path

from src.models.protein import Protein
from src.interfaces.resolver_metadata import resolver_fingerprints_by_type
from src.output_adapters.arango_output_adapter import ArangoOutputAdapter
from arango.exceptions import DocumentUpdateError
from src.shared.record_merger import FieldConflictBehavior
from src.registry.fetchers import MaterializedDataset


class FakeDocumentUpdateError(DocumentUpdateError):
    def __init__(self):
        Exception.__init__(self, "fake 413")


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


class FailingUpdateCollection(FakeCollection):
    def update_many(self, docs, merge=True, keep_none=False, check_rev=False):
        self.update_calls.append({
            "docs": docs,
            "merge": merge,
            "keep_none": keep_none,
            "check_rev": check_rev,
        })
        if len(docs) > 1:
            raise FakeDocumentUpdateError()
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


def test_arango_output_adapter_etl_metadata_includes_readable_resolver_metadata():
    resolver_snapshot = MaterializedDataset(
        source="target_graph",
        dataset="disease_ids",
        version="deps-test",
        version_date=None,
        download_date="2026-06-12",
        snapshot_id="target_graph:disease_ids:deps-test",
        manifest_uri="s3://ifx-registry/resolvers/target_graph/disease_ids/deps-test/manifest.yaml",
        manifest={"kind": "resolver_snapshot", "definition": {}, "files": []},
        local_dir=Path("/tmp/ifx-registry-cache/target_graph/disease_ids/deps-test"),
    )
    adapter = ArangoOutputAdapter.__new__(ArangoOutputAdapter)
    resolver_metadata = resolver_fingerprints_by_type([{
        "label": "disease_ids",
        "import": "./src/id_resolvers/disease_resolver.py",
        "class": "DiseaseIdResolver",
        "kwargs": {
            "resolver_snapshot": resolver_snapshot,
            "types": ["Disease"],
            "multi_match_behavior": "All",
        },
    }])
    adapter.set_resolver_metadata(
        resolver_fingerprints_by_type=resolver_metadata,
        source_yaml="./src/use_cases/pharos/pharos.yaml",
    )

    metadata = adapter.get_etl_metadata()

    disease_metadata = metadata["resolver_metadata"]["by_type"]["Disease"]
    assert metadata["resolver_metadata"]["source_yaml"] == "./src/use_cases/pharos/pharos.yaml"
    assert disease_metadata["class"] == "DiseaseIdResolver"
    assert disease_metadata["resolver_snapshot"]["snapshot_id"] == "target_graph:disease_ids:deps-test"
    assert disease_metadata["kwargs"]["resolver_snapshot"]["snapshot_id"] == "target_graph:disease_ids:deps-test"
    assert "local_dir" not in disease_metadata["resolver_snapshot"]
    assert disease_metadata["fingerprint"]


def test_arango_output_adapter_resolver_metadata_serializes_registry_resolver_snapshot(tmp_path):
    resolver_snapshot = MaterializedDataset(
        source="cure",
        dataset="cure_id_labels",
        version="deps-test",
        version_date=None,
        download_date="2026-06-12",
        snapshot_id="cure:cure_id_labels:deps-test",
        manifest_uri="s3://ifx-registry/resolvers/cure/cure_id_labels/deps-test/manifest.yaml",
        manifest={"kind": "resolver_snapshot", "definition": {}, "files": []},
        local_dir=tmp_path,
    )
    adapter = ArangoOutputAdapter.__new__(ArangoOutputAdapter)
    resolver_metadata = resolver_fingerprints_by_type([{
        "label": "cure_id_labels",
        "import": "./src/id_resolvers/cure_id_label_resolver.py",
        "class": "CureIdLabelResolver",
        "kwargs": {
            "resolver_snapshot": resolver_snapshot,
            "types": ["Gene"],
        },
    }])
    adapter.set_resolver_metadata(
        resolver_fingerprints_by_type=resolver_metadata,
        source_yaml="./src/use_cases/cure/cure_rasopathies.yaml",
    )

    metadata = adapter.get_etl_metadata()

    json.dumps(metadata)
    snapshot_metadata = metadata["resolver_metadata"]["by_type"]["Gene"]["resolver_snapshot"]
    assert snapshot_metadata["snapshot_id"] == "cure:cure_id_labels:deps-test"
    assert snapshot_metadata["kind"] == "resolver_snapshot"


def test_arango_output_adapter_etl_metadata_includes_registry_datasets():
    adapter = ArangoOutputAdapter.__new__(ArangoOutputAdapter)
    adapter.set_registry_dataset_metadata([{
        "source": "cure",
        "dataset": "case_reports",
        "version": "reports_20260612T182139Z",
        "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
        "usages": ["adapter:CUREAdapter"],
    }])

    metadata = adapter.get_etl_metadata()

    assert metadata["registry_datasets"] == [{
        "source": "cure",
        "dataset": "case_reports",
        "version": "reports_20260612T182139Z",
        "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
        "usages": ["adapter:CUREAdapter"],
    }]


def test_arango_output_adapter_merges_existing_resolver_metadata_for_post_processing():
    existing = resolver_fingerprints_by_type([{
        "label": "disease_ids",
        "import": "./src/id_resolvers/disease_resolver.py",
        "class": "DiseaseIdResolver",
        "kwargs": {
            "file_path": "./input_files/manual/target_graph/disease_ids.tsv",
            "multi_match_behavior": "All",
            "types": ["Disease"],
        },
    }])
    current = resolver_fingerprints_by_type([{
        "label": "tcrd_targets",
        "import": "./src/id_resolvers/target_graph_resolver.py",
        "class": "TCRDTargetResolver",
        "kwargs": {
            "canonical_type": "Protein",
            "collapse_to_canonical": True,
            "types": ["Protein", "Gene", "Transcript"],
        },
    }])

    merged = ArangoOutputAdapter._merge_resolver_metadata(
        {
            "source_yaml": "./src/use_cases/pharos/pharos.yaml",
            "by_type": existing,
        },
        {
            "source_yaml": "./src/use_cases/pharos/pharos_aql_post.yaml",
            "by_type": current,
        },
    )

    assert sorted(merged["by_type"]) == ["Disease", "Gene", "Protein", "Transcript"]
    assert merged["source_yamls"] == [
        "./src/use_cases/pharos/pharos.yaml",
        "./src/use_cases/pharos/pharos_aql_post.yaml",
    ]
    assert merged["summary"]["Disease"]["class"] == "DiseaseIdResolver"
    assert merged["summary"]["Protein"]["class"] == "TCRDTargetResolver"


def test_arango_output_adapter_merges_existing_registry_datasets_for_post_processing():
    merged = ArangoOutputAdapter._merge_registry_datasets(
        [{
            "source": "cure",
            "dataset": "case_reports",
            "version": "reports_20260612T182139Z",
            "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
            "usages": ["adapter:CUREAdapter"],
        }],
        [{
            "source": "cure",
            "dataset": "case_reports",
            "version": "reports_20260612T182139Z",
            "snapshot_id": "cure:case_reports:reports_20260612T182139Z",
            "manifest_uri": "s3://ifx-registry/sources/cure/case_reports/reports_20260612T182139Z/manifest.yaml",
            "usages": ["adapter:RasopathiesAdapter"],
        }, {
            "source": "cure",
            "dataset": "curated_concepts",
            "version": "2026-05-14",
            "snapshot_id": "cure:curated_concepts:2026-05-14",
            "usages": ["resolver:cure_id_labels"],
        }],
    )

    assert [dataset["snapshot_id"] for dataset in merged] == [
        "cure:case_reports:reports_20260612T182139Z",
        "cure:curated_concepts:2026-05-14",
    ]
    assert merged[0]["manifest_uri"] == "s3://ifx-registry/sources/cure/case_reports/reports_20260612T182139Z/manifest.yaml"
    assert merged[0]["usages"] == ["adapter:CUREAdapter", "adapter:RasopathiesAdapter"]


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
    assert any("name\tOld name\tNew name\tsource-new\tKeepFirst" == update for update in updated_docs[0]["updates"])
    assert updated_docs[0]["name"] == "Old name"

    assert len(inserted_docs) == 1
    assert inserted_docs[0]["_key"] == "IFXProtein:P2"
    assert inserted_docs[0]["creation"] == "source-brand-new"
    assert inserted_docs[0]["resolved_ids"] == ["resolver-brand-new"]
    assert collection.insert_calls[0]["overwrite"] is False
    assert collection.update_calls[0]["merge"] is True
    assert collection.update_calls[0]["keep_none"] is False
    assert collection.update_calls[0]["check_rev"] is False


def test_store_uses_requested_field_conflict_behavior_for_existing_nodes():
    existing_nodes = [{
        "_key": "IFXProtein:P1",
        "id": "IFXProtein:P1",
        "name": "Old name",
        "creation": "creation-source",
        "updates": [],
        "resolved_ids": ["resolver-old"],
    }]
    collection = FakeCollection()
    adapter = build_adapter(existing_nodes, collection)

    incoming = make_protein("IFXProtein:P1", "New name", "resolver-new", "source-new")

    adapter.store(
        [incoming],
        single_source=False,
        field_conflict_behavior=FieldConflictBehavior.KeepLast,
    )

    updated_docs = collection.update_calls[0]["docs"]

    assert updated_docs[0]["name"] == "New name"
    assert any("name\tOld name\tNew name\tsource-new\tKeepLast" == update for update in updated_docs[0]["updates"])


def test_update_many_with_backoff_splits_on_document_update_error():
    adapter = ArangoOutputAdapter.__new__(ArangoOutputAdapter)
    collection = FailingUpdateCollection()
    records = [
        {"_key": "a", "id": "a"},
        {"_key": "b", "id": "b"},
        {"_key": "c", "id": "c"},
        {"_key": "d", "id": "d"},
    ]

    adapter.update_many_with_backoff(collection, records, label="Protein", kind="node")

    assert [len(call["docs"]) for call in collection.update_calls] == [4, 2, 1, 1, 2, 1, 1]
