import json

import pytest

from src.core.curations import (
    METABOLITE_RECORD_PROPERTIES,
    METABOLITE_EQUIVALENCE_EDGES,
    METABOLITE_EXPECTED_CLIQUES,
    METABOLITE_MW_ADJUDICATIONS,
    validate_operation,
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


def record_property_changes(path, value, observed="old"):
    return {
        "action": "set_properties",
        "target": {
            "kind": "node",
            "curation_set": "metabolite_harmonization",
            "model_type": "MetaboliteIdentifier",
            "id": "REFMET:1",
        },
        "decisions": [{
            "path": path, "mode": "set", "value": value,
            "observed_value": observed,
        }],
        "note": "Correct source chemistry",
    }


def mw_adjudication(action="accept_mw_discrepancy", fingerprint="a" * 64):
    operation = {
        "action": action,
        "target": {
            "kind": "validation_finding",
            "curation_set": "metabolite_harmonization",
            "check": "mw_spread",
            "finding_id": "mw-1234567890abcdef12345678",
            "anchor_id": "CHEBI:1",
        },
        "observed_evidence_fingerprint": fingerprint,
        "observed_member_ids": ["CHEBI:1", "HMDB:1"],
        "note": "Reviewed expected chemistry.",
    }
    if action == "accept_mw_discrepancy":
        operation.update({
            "reason": "protonation_or_charge_state",
            "supporting_ids": ["CHEBI:1"],
        })
    return operation


def test_mw_adjudication_contract_supports_acceptance_and_reopening():
    accepted = validate_operation(
        METABOLITE_MW_ADJUDICATIONS, mw_adjudication()
    )
    reopened = validate_operation(
        METABOLITE_MW_ADJUDICATIONS, mw_adjudication("reopen_mw_discrepancy")
    )

    assert accepted["reason"] == "protonation_or_charge_state"
    assert reopened["action"] == "reopen_mw_discrepancy"


def test_mw_adjudication_contract_accepts_optional_snapshot_and_rejects_bad_rows():
    operation = mw_adjudication()
    operation["observed_evidence_snapshot"] = {
        "version": "mw-review-evidence-v1",
        "validator_version": "component-aware-v1",
        "threshold": "0.1",
        "mass_observations": [
            {"member_id": "CHEBI:1", "channel": "average", "value": "100"},
        ],
        "component_matches": [],
    }

    assert validate_operation(METABOLITE_MW_ADJUDICATIONS, operation) is operation

    malformed = json.loads(json.dumps(operation))
    malformed["observed_evidence_snapshot"]["mass_observations"][0]["member_id"] = "CHEBI:999"
    with pytest.raises(ValueError, match="must belong to the finding"):
        validate_operation(METABOLITE_MW_ADJUDICATIONS, malformed)


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


def test_generic_record_property_changes_merge_by_semantic_path():
    storage = FakeStorage()
    add_cart_operation(
        storage, METABOLITE_RECORD_PROPERTIES, "keith", "Keith",
        record_property_changes(["formula"], "C6H10O5"),
    )

    cart = add_cart_operation(
        storage, METABOLITE_RECORD_PROPERTIES, "keith", "Keith",
        record_property_changes(["mw"], "162.14"),
    )

    assert cart["operation_count"] == 1
    assert [decision["path"] for decision in cart["operations"][0]["decisions"]] == [
        ["formula"], ["mw"],
    ]


def test_generic_classification_change_preserves_pending_sibling_fields():
    storage = FakeStorage()
    add_cart_operation(
        storage, METABOLITE_RECORD_PROPERTIES, "keith", "Keith",
        record_property_changes(["formula"], "C6H10O5"),
    )
    classification = record_property_changes(
        ["is_generic_structure"], True, observed=None
    )

    cart = add_cart_operation(
        storage, METABOLITE_RECORD_PROPERTIES, "keith", "Keith", classification
    )

    assert [decision["path"] for decision in cart["operations"][0]["decisions"]] == [
        ["formula"], ["is_generic_structure"],
    ]


def test_record_property_publish_rejects_multiple_graphs_in_one_draft():
    storage = FakeStorage()
    add_cart_operation(
        storage, METABOLITE_RECORD_PROPERTIES, "keith", "Keith",
        record_property_changes(["formula"], "C6H10O5"),
    )
    other_graph = record_property_changes(["name"], "Corrected")
    other_graph["target"] = {
        **other_graph["target"],
        "curation_set": "pharos",
        "model_type": "Drug",
        "id": "CHEMBL:1",
    }
    with pytest.raises(ValueError, match="not registered"):
        add_cart_operation(
            storage, METABOLITE_RECORD_PROPERTIES, "keith", "Keith", other_graph
        )



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
