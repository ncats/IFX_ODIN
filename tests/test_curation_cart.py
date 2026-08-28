import json

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


def test_cart_autosaves_and_reloads_one_draft_per_curator_and_graph():
    storage = FakeStorage()

    first = add_cart_operation(
        storage,
        "metabolite_harmonization",
        "haley@example.org",
        "Haley",
        edge_removal(),
    )
    duplicate = add_cart_operation(
        storage,
        "metabolite_harmonization",
        "haley@example.org",
        "Haley",
        edge_removal(),
    )
    reloaded = load_cart(
        storage,
        "metabolite_harmonization",
        "haley@example.org",
        "Haley",
    )

    assert first["draft_id"] == duplicate["draft_id"] == reloaded["draft_id"]
    assert reloaded["operation_count"] == 1
    assert len(storage.objects) == 1
    assert "haley@example.org" not in next(iter(storage.objects))


def test_different_curators_get_different_graph_carts():
    storage = FakeStorage()
    add_cart_operation(storage, "metabolite_harmonization", "haley", "Haley", edge_removal())
    add_cart_operation(storage, "metabolite_harmonization", "keith", "Keith", edge_removal("CHEBI:2", "HMDB:2"))

    assert draft_key("metabolite_harmonization", "haley") in storage.objects
    assert draft_key("metabolite_harmonization", "keith") in storage.objects
    assert len(storage.objects) == 2


def test_new_edge_decision_replaces_opposite_decision_in_draft_cart():
    storage = FakeStorage()
    add_cart_operation(storage, "metabolite_harmonization", "keith", "Keith", edge_removal())

    cart = add_cart_operation(
        storage,
        "metabolite_harmonization",
        "keith",
        "Keith",
        edge_retention("HMDB:1", "CHEBI:1"),
    )

    assert cart["operation_count"] == 1
    assert cart["operations"][0]["action"] == "retain_edge"


def test_new_assertion_decision_replaces_same_assertion_in_draft_cart():
    storage = FakeStorage()
    add_cart_operation(storage, "metabolite_harmonization", "keith", "Keith", expected_clique())

    cart = add_cart_operation(
        storage,
        "metabolite_harmonization",
        "keith",
        "Keith",
        expected_clique(name="Glucose forms", rationale="Updated rationale"),
    )

    assert cart["operation_count"] == 1
    assert cart["operations"][0]["name"] == "Glucose forms"


def test_removing_last_item_deletes_persisted_draft():
    storage = FakeStorage()
    cart = add_cart_operation(storage, "metabolite_harmonization", "keith", "Keith", edge_removal())

    emptied = remove_cart_operation(
        storage,
        "metabolite_harmonization",
        "keith",
        "Keith",
        cart["operations"][0]["operation_id"],
    )

    assert emptied["operation_count"] == 0
    assert storage.objects == {}


def test_publish_writes_immutable_active_batch_and_clears_draft():
    storage = FakeStorage()
    cart = add_cart_operation(storage, "metabolite_harmonization", "keith", "Keith", edge_removal())

    published = publish_cart(
        storage,
        "metabolite_harmonization",
        "keith",
        "Keith",
        "Keith's batch 2026-08-24",
        "Reviewed molecular-weight conflict.",
    )

    assert draft_key("metabolite_harmonization", "keith") not in storage.objects
    assert published["storage_uri"].startswith(
        "s3://test-curations/curations/v1/metabolite_harmonization/qa-browser-"
    )
    published_key = published["storage_uri"].removeprefix("s3://test-curations/")
    batch = json.loads(storage.objects[published_key])
    assert batch["curation_batch_id"] == f"qa-browser-{cart['draft_id']}"
    assert batch["published_at"] == batch["created_at"]
    assert batch["created_by"] == {"id": "keith", "name": "Keith"}
    assert batch["name"] == "Keith's batch 2026-08-24"
    assert batch["operations"][0]["start_id"] == "CHEBI:1"
