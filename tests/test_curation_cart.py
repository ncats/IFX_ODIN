import json

from src.core.curations import (
    METABOLITE_ANNOTATIONS,
    METABOLITE_EQUIVALENCE_EDGES,
    METABOLITE_EXPECTED_CLIQUES,
)
from src.qa_browser.curation_cart import (
    add_cart_operation,
    draft_key,
    load_cart,
    publish_cart,
    remove_cart_operation,
)


class FakeStorage:
    bucket = "test-curations"

    def __init__(self):
        self.objects = {}

    def list_keys(self, prefix=""):
        return sorted(key for key in self.objects if key.startswith(prefix))

    def read_text(self, key):
        return self.objects[key]

    def write_text(self, key, text, content_type="text/plain"):
        self.objects[key] = text
        return f"s3://{self.bucket}/{key}"

    def delete_file(self, key):
        self.objects.pop(key, None)


class PreconditionFailed(Exception):
    response = {"Error": {"Code": "PreconditionFailed"}}


class ConditionalStorage(FakeStorage):
    def __init__(self):
        super().__init__()
        self.versions = {}
        self.concurrent_operation = None
        self.concurrent_delete_operation = None

    def read_text_with_etag(self, key):
        return self.objects[key], f'"v{self.versions[key]}"'

    def write_text(
        self,
        key,
        text,
        content_type="text/plain",
        *,
        if_match=None,
        if_none_match=None,
    ):
        current_etag = f'"v{self.versions[key]}"' if key in self.objects else None
        if self.concurrent_operation is not None and key in self.objects:
            cart = json.loads(self.objects[key])
            cart["operations"].append(self.concurrent_operation)
            self.versions[key] += 1
            self.objects[key] = json.dumps(cart)
            self.concurrent_operation = None
            raise PreconditionFailed()
        if if_none_match == "*" and key in self.objects:
            raise PreconditionFailed()
        if if_match is not None and if_match != current_etag:
            raise PreconditionFailed()
        self.versions[key] = self.versions.get(key, 0) + 1
        self.objects[key] = text
        return f"s3://{self.bucket}/{key}"

    def delete_file(self, key, *, if_match=None):
        if self.concurrent_delete_operation is not None and key in self.objects:
            cart = json.loads(self.objects[key])
            cart["operations"].append(self.concurrent_delete_operation)
            self.versions[key] += 1
            self.objects[key] = json.dumps(cart)
            self.concurrent_delete_operation = None
            raise PreconditionFailed()
        current_etag = f'"v{self.versions[key]}"' if key in self.objects else None
        if if_match is not None and if_match != current_etag:
            raise PreconditionFailed()
        self.objects.pop(key, None)
        self.versions.pop(key, None)


def edge_removal(left="CHEBI:1", right="HMDB:1"):
    return {
        "action": "remove_edge",
        "edge_type": "MetaboliteIdentifierMappingEdge",
        "start_id": left,
        "end_id": right,
        "symmetric": True,
    }


def edge_retention(left="CHEBI:1", right="HMDB:1"):
    return {
        "action": "retain_edge",
        "edge_type": "MetaboliteIdentifierMappingEdge",
        "start_id": left,
        "end_id": right,
        "symmetric": True,
    }


def expected_clique(name="Glucose anomers", rationale="Expected together"):
    return {
        "action": "assert_same_clique",
        "assertion_id": "same-clique-1234567890abcdef12345678",
        "assertion_type": "expected_same_clique",
        "member_ids": ["CHEBI:15903", "CHEBI:17925"],
        "name": name,
        "rationale": rationale,
    }


def property_changes(values=None, remove_overrides=None):
    return {
        "action": "set_properties",
        "target": {
            "kind": "node",
            "model_type": "MetaboliteIdentifier",
            "id": "KEGG.COMPOUND:C00001",
        },
        "values": values or {},
        "remove_overrides": remove_overrides or [],
    }


def test_cart_autosaves_and_reloads_one_draft_per_curator_and_type():
    storage = FakeStorage()

    first = add_cart_operation(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "haley@example.org",
        "Haley",
        edge_removal(),
    )
    duplicate = add_cart_operation(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "haley@example.org",
        "Haley",
        edge_removal(),
    )
    reloaded = load_cart(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "haley@example.org",
        "Haley",
    )

    assert first["draft_id"] == duplicate["draft_id"] == reloaded["draft_id"]
    assert reloaded["operation_count"] == 1
    assert len(storage.objects) == 1
    assert "haley@example.org" not in next(iter(storage.objects))


def test_different_curators_get_different_typed_carts():
    storage = FakeStorage()
    add_cart_operation(storage, METABOLITE_EQUIVALENCE_EDGES, "haley", "Haley", edge_removal())
    add_cart_operation(storage, METABOLITE_EQUIVALENCE_EDGES, "keith", "Keith", edge_removal("CHEBI:2", "HMDB:2"))

    assert draft_key(METABOLITE_EQUIVALENCE_EDGES, "haley") in storage.objects
    assert draft_key(METABOLITE_EQUIVALENCE_EDGES, "keith") in storage.objects
    assert len(storage.objects) == 2


def test_new_edge_decision_replaces_opposite_decision_in_draft_cart():
    storage = FakeStorage()
    add_cart_operation(storage, METABOLITE_EQUIVALENCE_EDGES, "keith", "Keith", edge_removal())

    cart = add_cart_operation(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "keith",
        "Keith",
        edge_retention("HMDB:1", "CHEBI:1"),
    )

    assert cart["operation_count"] == 1
    assert cart["operations"][0]["action"] == "retain_edge"


def test_new_assertion_decision_replaces_same_assertion_in_draft_cart():
    storage = FakeStorage()
    add_cart_operation(storage, METABOLITE_EXPECTED_CLIQUES, "keith", "Keith", expected_clique())

    cart = add_cart_operation(
        storage,
        METABOLITE_EXPECTED_CLIQUES,
        "keith",
        "Keith",
        expected_clique(name="Glucose forms", rationale="Updated rationale"),
    )

    assert cart["operation_count"] == 1
    assert cart["operations"][0]["name"] == "Glucose forms"


def test_property_changes_merge_per_target_and_keep_one_cart_item():
    storage = FakeStorage()
    add_cart_operation(
        storage,
        METABOLITE_ANNOTATIONS,
        "keith",
        "Keith",
        property_changes({"is_generic_structure": True}),
    )

    cart = add_cart_operation(
        storage,
        METABOLITE_ANNOTATIONS,
        "keith",
        "Keith",
        property_changes({}, ["is_generic_structure"]),
    )

    assert cart["operation_count"] == 1
    assert cart["operations"][0]["values"] == {}
    assert cart["operations"][0]["remove_overrides"] == ["is_generic_structure"]


def test_replace_target_replaces_old_style_property_draft():
    storage = FakeStorage()
    old_operation = {
        "action": "set_property",
        "target": property_changes()["target"],
        "property": "is_generic_structure",
        "value": True,
    }
    add_cart_operation(storage, METABOLITE_ANNOTATIONS, "keith", "Keith", old_operation)

    cart = add_cart_operation(
        storage,
        METABOLITE_ANNOTATIONS,
        "keith",
        "Keith",
        property_changes({"is_generic_structure": None}),
        replace_target=True,
    )

    assert cart["operation_count"] == 1
    assert cart["operations"][0]["action"] == "set_properties"
    assert cart["operations"][0]["values"] == {"is_generic_structure": None}


def test_removing_last_item_deletes_persisted_draft():
    storage = FakeStorage()
    cart = add_cart_operation(storage, METABOLITE_EQUIVALENCE_EDGES, "keith", "Keith", edge_removal())

    emptied = remove_cart_operation(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "keith",
        "Keith",
        cart["operations"][0]["operation_id"],
    )

    assert emptied["operation_count"] == 0
    assert storage.objects == {}


def test_publish_writes_immutable_active_batch_and_clears_draft():
    storage = FakeStorage()
    cart = add_cart_operation(storage, METABOLITE_EQUIVALENCE_EDGES, "keith", "Keith", edge_removal())

    published = publish_cart(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "keith",
        "Keith",
        "Keith's batch 2026-08-24",
        "Reviewed molecular-weight conflict.",
    )

    assert draft_key(METABOLITE_EQUIVALENCE_EDGES, "keith") not in storage.objects
    assert published["storage_uri"].startswith(
        "s3://test-curations/curations/v2/metabolite_equivalence_edges/batches/qa-browser-"
    )
    published_key = published["storage_uri"].removeprefix("s3://test-curations/")
    batch = json.loads(storage.objects[published_key])
    assert batch["curation_batch_id"] == f"qa-browser-{cart['draft_id']}"
    assert batch["published_at"] == batch["created_at"]
    assert batch["created_by"] == {"id": "keith", "name": "Keith"}
    assert batch["name"] == "Keith's batch 2026-08-24"
    assert batch["operations"][0]["start_id"] == "CHEBI:1"
    manifest = json.loads(storage.objects["curations/v2/metabolite_equivalence_edges/manifest.json"])
    assert manifest["revision"] == 1
    assert manifest["batches"][0]["batch_id"] == batch["curation_batch_id"]


def test_concurrent_draft_edit_is_merged_after_etag_conflict():
    storage = ConditionalStorage()
    add_cart_operation(
        storage, METABOLITE_EQUIVALENCE_EDGES, "keith", "Keith", edge_removal()
    )
    storage.concurrent_operation = {
        **edge_removal("CHEBI:2", "HMDB:2"),
        "operation_id": "concurrent-operation",
    }

    cart = add_cart_operation(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "keith",
        "Keith",
        edge_removal("CHEBI:3", "HMDB:3"),
    )

    assert cart["operation_count"] == 3
    assert {operation["start_id"] for operation in cart["operations"]} == {
        "CHEBI:1", "CHEBI:2", "CHEBI:3",
    }


def test_publish_preserves_a_concurrent_new_draft_operation():
    storage = ConditionalStorage()
    original = add_cart_operation(
        storage, METABOLITE_EQUIVALENCE_EDGES, "keith", "Keith", edge_removal()
    )
    storage.concurrent_delete_operation = {
        **edge_removal("CHEBI:2", "HMDB:2"),
        "operation_id": "concurrent-operation",
    }

    publish_cart(
        storage,
        METABOLITE_EQUIVALENCE_EDGES,
        "keith",
        "Keith",
        "Concurrent publish",
    )
    remaining = load_cart(
        storage, METABOLITE_EQUIVALENCE_EDGES, "keith", "Keith"
    )

    assert remaining["operation_count"] == 1
    assert remaining["operations"][0]["start_id"] == "CHEBI:2"
    assert remaining["draft_id"] != original["draft_id"]
