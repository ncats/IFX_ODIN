import json
import asyncio

import pytest
import src.qa_browser.app as qa_app

from src.core.curations import (
    METABOLITE_ANNOTATIONS,
    batch_key,
    curatable_property_definitions,
    manifest_key,
    payload_sha256,
    resolve_curation_type,
    validate_operation,
)
from src.qa_browser.app import (
    _curation_annotations_apply_to_generic_rule,
    _metabolite_curatable_properties,
    _metabolite_generic_structure_classification,
    _metabolite_generic_structure_operation,
    _normalize_metabolite_rule_ids,
)


class FakeStorage:
    bucket = "test-curations"

    def __init__(self, objects=None):
        self.objects = objects or {}

    def read_text(self, key):
        return self.objects[key]


def annotation_operation(identifier, value):
    return {
        "action": "set_property",
        "target": {
            "kind": "node",
            "model_type": "MetaboliteIdentifier",
            "id": identifier,
        },
        "property": "is_generic_structure",
        "value": value,
    }


def annotation_storage(batches):
    objects = {}
    entries = []
    for batch_id, operations in batches:
        batch = {
            "format_version": 2,
            "curation_batch_id": batch_id,
            "curation_type": METABOLITE_ANNOTATIONS,
            "published_at": f"2026-09-{len(entries) + 1:02d}T12:00:00Z",
            "operations": operations,
        }
        key = batch_key(METABOLITE_ANNOTATIONS, batch_id)
        objects[key] = json.dumps(batch)
        entries.append({
            "batch_id": batch_id,
            "object_key": key,
            "sha256": payload_sha256(batch),
            "published_at": batch["published_at"],
        })
    manifest = {
        "format_version": 2,
        "curation_type": METABOLITE_ANNOTATIONS,
        "revision": len(entries),
        "batches": entries,
    }
    objects[manifest_key(METABOLITE_ANNOTATIONS)] = json.dumps(manifest)
    return FakeStorage(objects)


def test_manifest_order_controls_latest_property_value():
    storage = annotation_storage([
        ("generic", [annotation_operation("KEGG.COMPOUND:C00001", True)]),
        ("not-generic", [annotation_operation("KEGG.COMPOUND:C00001", False)]),
    ])

    snapshot = resolve_curation_type(storage, METABOLITE_ANNOTATIONS)

    assert snapshot.manifest_revision == 2
    assert snapshot.batch_ids == ["generic", "not-generic"]
    assert snapshot.active_operations == []
    assert len(snapshot.active_property_decisions) == 1
    assert snapshot.active_property_decisions[0].value is False
    assert snapshot.active_property_decisions[0].mode == "set"
    assert len(snapshot.fingerprint) == 64


def test_batch_hash_mismatch_fails_before_resolution():
    storage = annotation_storage([
        ("generic", [annotation_operation("KEGG.COMPOUND:C00001", True)]),
    ])
    manifest = json.loads(storage.objects[manifest_key(METABOLITE_ANNOTATIONS)])
    manifest["batches"][0]["sha256"] = "0" * 64
    storage.objects[manifest_key(METABOLITE_ANNOTATIONS)] = json.dumps(manifest)

    with pytest.raises(ValueError, match="hash mismatch"):
        resolve_curation_type(storage, METABOLITE_ANNOTATIONS)


def test_annotation_registry_rejects_wrong_value_type_and_property():
    with pytest.raises(ValueError, match="requires bool"):
        validate_operation(
            METABOLITE_ANNOTATIONS,
            annotation_operation("KEGG.COMPOUND:C00001", "yes"),
        )
    invalid = annotation_operation("KEGG.COMPOUND:C00001", True)
    invalid["property"] = "id"
    with pytest.raises(ValueError, match="denied"):
        validate_operation(METABOLITE_ANNOTATIONS, invalid)


def test_curatable_properties_are_derived_from_scalar_model_fields_and_denylist():
    definitions = {
        definition.property_name: definition
        for definition in curatable_property_definitions("MetaboliteIdentifier")
    }

    assert set(definitions) == {"is_generic_structure"}
    assert definitions["is_generic_structure"].value_type_name == "boolean"
    assert definitions["is_generic_structure"].nullable is True


def test_multi_property_operation_distinguishes_null_from_remove_override():
    target = {
        "kind": "node",
        "model_type": "MetaboliteIdentifier",
        "id": "KEGG.COMPOUND:C00001",
    }
    set_no_value = {
        "action": "set_properties",
        "target": target,
        "values": {"is_generic_structure": None},
        "remove_overrides": [],
    }
    restore_graph_value = {
        "action": "set_properties",
        "target": target,
        "values": {},
        "remove_overrides": ["is_generic_structure"],
    }

    validate_operation(METABOLITE_ANNOTATIONS, set_no_value)
    validate_operation(METABOLITE_ANNOTATIONS, restore_graph_value)

    overlap = {**set_no_value, "remove_overrides": ["is_generic_structure"]}
    with pytest.raises(ValueError, match="must be disjoint"):
        validate_operation(METABOLITE_ANNOTATIONS, overlap)

    empty = {**set_no_value, "values": {}}
    with pytest.raises(ValueError, match="at least one"):
        validate_operation(METABOLITE_ANNOTATIONS, empty)

    with pytest.raises(ValueError, match="values must be an object"):
        validate_operation(METABOLITE_ANNOTATIONS, {
            **set_no_value,
            "values": [],
            "remove_overrides": ["is_generic_structure"],
        })
    with pytest.raises(ValueError, match="remove_overrides must be a list"):
        validate_operation(METABOLITE_ANNOTATIONS, {
            **set_no_value,
            "remove_overrides": {},
        })


def test_new_property_operation_supersedes_old_style_per_property():
    restore = {
        "action": "set_properties",
        "target": annotation_operation("KEGG.COMPOUND:C00001", True)["target"],
        "values": {},
        "remove_overrides": ["is_generic_structure"],
    }
    storage = annotation_storage([
        ("old-style", [annotation_operation("KEGG.COMPOUND:C00001", True)]),
        ("new-style", [restore]),
    ])

    snapshot = resolve_curation_type(storage, METABOLITE_ANNOTATIONS)

    assert len(snapshot.active_property_decisions) == 1
    assert snapshot.active_property_decisions[0].mode == "remove_override"
    assert snapshot.active_property_decisions[0].batch_id == "new-style"


def test_equivalence_registry_rejects_unregistered_or_asymmetric_edge_targets():
    operation = {
        "action": "remove_edge",
        "edge_type": "OtherEdge",
        "start_id": "CHEBI:1",
        "end_id": "HMDB:1",
        "symmetric": True,
    }
    with pytest.raises(ValueError, match="not registered"):
        validate_operation("metabolite_equivalence_edges", operation)
    operation["edge_type"] = "MetaboliteIdentifierMappingEdge"
    operation["symmetric"] = False
    with pytest.raises(ValueError, match="symmetric=true"):
        validate_operation("metabolite_equivalence_edges", operation)


def test_generic_structure_classification_keeps_unknown_distinct_from_false():
    unknown = _metabolite_generic_structure_classification({
        "id": "KEGG.COMPOUND:C00001",
        "chem_props": [],
        "chemical_entity": None,
    })
    detected_generic = _metabolite_generic_structure_classification({
        "id": "HMDB:1",
        "editable_properties": {"is_generic_structure": True},
        "chem_props": [{"source": "HMDB", "source_id": "HMDB:1", "smiles": "C(*)O"}],
        "chemical_entity": None,
    })
    overridden = _metabolite_generic_structure_classification(
        {
            "id": "HMDB:1",
            "editable_properties": {"is_generic_structure": True},
            "chem_props": [{"source": "HMDB", "source_id": "HMDB:1", "smiles": "C(*)O"}],
            "chemical_entity": None,
        },
        {"annotation_overrides": {"HMDB:1": False}},
    )

    assert unknown["detected"] is None and unknown["effective"] is None
    assert detected_generic["detected"] is True
    assert overridden["detected"] is True and overridden["effective"] is False


def test_generic_structure_classification_uses_persisted_field_and_all_evidence_for_explanation():
    classification = _metabolite_generic_structure_classification({
        "id": "HMDB:1",
        "editable_properties": {"is_generic_structure": True},
        "chem_props": [{"smiles": "CCO"}] * 8,
        "generic_structure_evidence": [{"smiles": "CCO"}] * 8 + [{"smiles": "C(*)O"}],
    })

    assert classification["detected"] is True
    assert classification["evidence"][-1]["value"] == "C(*)O"


def test_curatable_property_metadata_keeps_explicit_null_override_distinct():
    node = {
        "id": "KEGG.COMPOUND:C00001",
        "editable_properties": {},
        "generic_structure": {"detected": False},
    }
    state = {
        "annotation_decisions": {
            node["id"]: {
                "is_generic_structure": {
                    "mode": "set",
                    "value": None,
                    "curation_batch_id": "batch-1",
                }
            }
        }
    }

    properties = _metabolite_curatable_properties(node, state)

    assert properties == [{
        "model_type": "MetaboliteIdentifier",
        "name": "is_generic_structure",
        "label": "Is Generic Structure",
        "value_type": "boolean",
        "nullable": True,
        "graph_value": False,
        "graph_has_value": True,
        "has_published_override": True,
        "published_override": None,
        "effective_value": None,
        "effective_has_value": True,
        "editing_available": True,
        "state_error": None,
        "last_published_decision": {
            "mode": "set",
            "value": None,
            "curation_batch_id": "batch-1",
        },
    }]


def test_curatable_property_metadata_disables_editing_when_published_state_is_unavailable():
    properties = _metabolite_curatable_properties(
        {
            "id": "KEGG.COMPOUND:C00001",
            "editable_properties": {},
            "generic_structure": {"detected": None},
        },
        {
            "annotation_state_available": False,
            "annotation_state_error": "Published curation state is unavailable.",
        },
    )

    assert properties[0]["editing_available"] is False
    assert properties[0]["state_error"] == "Published curation state is unavailable."


def test_generic_structure_operation_supports_explicit_clear():
    set_generic = _metabolite_generic_structure_operation("kegg:C00001", True)
    clear = _metabolite_generic_structure_operation("kegg:C00001", "detected")

    assert set_generic["target"]["id"] == "KEGG.COMPOUND:C00001"
    assert set_generic["values"] == {"is_generic_structure": True}
    assert clear["action"] == "set_properties"
    assert clear["remove_overrides"] == ["is_generic_structure"]


def test_apply_curations_is_orderable_and_legacy_rule_is_renamed_in_place():
    assert _normalize_metabolite_rule_ids([
        "ignore_generic_structure_mismatch",
        "ignore_ramp_mapping_denylist",
    ]) == ["ignore_generic_structure_mismatch", "apply_curations"]
    assert _curation_annotations_apply_to_generic_rule([
        "apply_curations", "ignore_generic_structure_mismatch",
    ]) is True
    assert _curation_annotations_apply_to_generic_rule([
        "ignore_generic_structure_mismatch", "apply_curations",
    ]) is False


def test_multi_type_publish_reports_partial_success_and_remaining_types(monkeypatch):
    edge_operation = {
        "action": "remove_edge",
        "edge_type": "MetaboliteIdentifierMappingEdge",
        "start_id": "CHEBI:1",
        "end_id": "HMDB:1",
        "symmetric": True,
    }
    annotation = annotation_operation("KEGG.COMPOUND:C00001", True)
    carts = {
        "metabolite_equivalence_edges": {"operations": [edge_operation]},
        METABOLITE_ANNOTATIONS: {"operations": [annotation]},
        "metabolite_expected_cliques": {"operations": []},
    }

    monkeypatch.setattr(qa_app, "_curator_identity", lambda request, payload: ("keith", "Keith"))
    monkeypatch.setattr(qa_app, "_curation_cart_storage", lambda: object())
    monkeypatch.setattr(
        qa_app,
        "load_cart",
        lambda storage, curation_type, curator_id, curator_name: carts[curation_type],
    )

    def publish(storage, curation_type, curator_id, curator_name, batch_name, description):
        if curation_type == "metabolite_equivalence_edges":
            raise RuntimeError("simulated S3 failure")
        return {"operation_count": 1, "batch": {"curation_batch_id": "edge-batch"}}

    monkeypatch.setattr(qa_app, "publish_cart", publish)
    monkeypatch.setattr(
        qa_app,
        "_combined_metabolite_curation_cart",
        lambda *args: {
            "operations": [{"curation_type": "metabolite_equivalence_edges", **edge_operation}],
            "operation_count": 1,
        },
    )

    class FakeRequest:
        async def json(self):
            return {"batch_name": "Mixed review"}

    response = asyncio.run(qa_app.ramp_id_qa_publish_curation_cart(FakeRequest()))
    payload = json.loads(response.body)

    assert response.status_code == 207
    assert payload["partial"] is True
    assert payload["published"][0]["curation_type"] == METABOLITE_ANNOTATIONS
    assert payload["remaining_types"] == ["metabolite_equivalence_edges"]
