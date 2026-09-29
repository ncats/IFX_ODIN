import json
import asyncio

import pytest
import src.qa_browser.app as qa_app

from src.core.curations import (
    METABOLITE_RECORD_PROPERTIES,
    METABOLITE_RECORD_SUPPRESSIONS,
    batch_key,
    curatable_property_definitions,
    manifest_key,
    payload_sha256,
    resolve_curation_type,
    validate_operation,
)
from src.qa_browser.app import (
    _metabolite_generic_structure_classification,
    _normalize_metabolite_rule_ids,
    _record_properties_apply_to_generic_rule,
)


class FakeStorage:
    bucket = "test-curations"

    def __init__(self, objects=None):
        self.objects = objects or {}

    def read_text(self, key):
        return self.objects[key]


def annotation_operation(identifier, value):
    return {
        "action": "set_properties",
        "target": {
            "kind": "node",
            "curation_set": "metabolite_harmonization",
            "model_type": "MetaboliteIdentifier",
            "id": identifier,
        },
        "decisions": [{
            "path": ["is_generic_structure"],
            "mode": "set",
            "value": value,
            "observed_exists": False,
            "observed_value": None,
        }],
        "note": "Classify the structure.",
    }


def record_property_operation(identifier="REFMET:RM0006550", value="C6H10O5"):
    return {
        "action": "set_properties",
        "target": {
            "kind": "node",
            "curation_set": "metabolite_harmonization",
            "model_type": "MetaboliteIdentifier",
            "id": identifier,
        },
        "decisions": [{
            "path": ["formula"],
            "mode": "set",
            "value": value,
            "observed_value": "C6H1005",
        }],
        "note": "Correct a source typo.",
    }


def test_record_property_operation_requires_rationale_and_protects_identity():
    operation = record_property_operation()
    assert validate_operation(METABOLITE_RECORD_PROPERTIES, operation) is operation

    without_note = {**operation, "note": ""}
    with pytest.raises(ValueError, match="requires a rationale"):
        validate_operation(METABOLITE_RECORD_PROPERTIES, without_note)

    protected = {
        **operation,
        "decisions": [{
            "path": ["id"], "mode": "set", "value": "other", "observed_value": "old",
        }],
    }
    with pytest.raises(ValueError, match="managed by the graph build"):
        validate_operation(METABOLITE_RECORD_PROPERTIES, protected)


def test_record_property_resolution_is_latest_wins_per_set_and_path():
    batches = [
        ("batch-1", [record_property_operation(value="first")]),
        ("batch-2", [record_property_operation(value="second")]),
    ]
    objects = {}
    entries = []
    for index, (batch_id, operations) in enumerate(batches, start=1):
        batch = {
            "format_version": 2,
            "curation_batch_id": batch_id,
            "curation_type": METABOLITE_RECORD_PROPERTIES,
            "published_at": f"2026-09-{index:02d}T12:00:00Z",
            "operations": operations,
        }
        key = batch_key(METABOLITE_RECORD_PROPERTIES, batch_id)
        objects[key] = json.dumps(batch)
        entries.append({"batch_id": batch_id, "object_key": key, "sha256": payload_sha256(batch)})
    manifest = {
        "format_version": 2,
        "curation_type": METABOLITE_RECORD_PROPERTIES,
        "revision": 2,
        "batches": entries,
    }
    objects[manifest_key(METABOLITE_RECORD_PROPERTIES)] = json.dumps(manifest)

    snapshot = resolve_curation_type(FakeStorage(objects), METABOLITE_RECORD_PROPERTIES)

    decisions = snapshot.record_property_decisions_for_set("metabolite_harmonization")
    assert len(decisions) == 1
    assert decisions[0].value == "second"


def test_record_property_editor_uses_stable_selectors_for_nested_source_records():
    class EmptySnapshot:
        @staticmethod
        def record_property_decisions_for_set(_curation_set):
            return []

    document = {
        "_curation_set": "metabolite_harmonization",
        "_curation_model_type": "MetaboliteIdentifier",
        "id": "REFMET:1",
        "chem_props": [{
            "source": "RefMet", "source_id": "RM1",
            "molecular_formula": "C6H1005", "calculated_mw": "162.14",
        }],
    }
    schema = {"fields": {"chem_props": {
        "type": "list", "item_type": "object", "fields": {
            "source": "str", "source_id": "str", "molecular_formula": "str",
            "calculated_mw": "str",
        },
    }}}

    fields = qa_app._record_property_fields(document, schema, EmptySnapshot())

    assert [field["path"] for field in fields] == [[
        "chem_props",
        {"match": {"source": "RefMet", "source_id": "RM1"}},
        "molecular_formula",
    ]]


def test_record_property_editor_can_add_an_absent_top_level_scalar():
    class EmptySnapshot:
        @staticmethod
        def record_property_decisions_for_set(_curation_set):
            return []

    sparse_identifier = {
        "_curation_set": "metabolite_harmonization",
        "_curation_model_type": "MetaboliteIdentifier",
        "id": "BiGG:1315507",
        "prefix": "BiGG",
        "sources": ["RaMP"],
    }
    schema = {"fields": {
        "id": "str",
        "prefix": "str",
        "is_generic_structure": "bool",
        "chem_props": {
            "type": "list", "item_type": "object",
            "fields": {"source": "str", "mw": "str"},
        },
    }}

    fields = qa_app._record_property_fields(
        sparse_identifier, schema, EmptySnapshot()
    )

    assert [field["path"] for field in fields] == [["is_generic_structure"]]
    assert fields[0]["baseline"] is None
    assert fields[0]["baseline_present"] is False
    assert fields[0]["effective"] is None


def test_published_record_curations_are_annotated_at_exact_display_paths():
    document = {
        "id": "REFMET:RM0006550",
        "formula": "C6H1005",
        "chem_props": [
            {"source": "RefMet", "source_id": "RM1", "monoisotopic_mass": 166.047740},
            {"source": "HMDB", "source_id": "HMDB1", "monoisotopic_mass": 166.047740},
        ],
    }
    fields = [{
        "path": [
            "chem_props",
            {"match": {"source": "RefMet", "source_id": "RM1"}},
            "monoisotopic_mass",
        ],
        "effective": 150.13,
        "baseline": 166.047740,
        "baseline_present": True,
    }]

    displayed = qa_app._document_with_inline_curations(document, fields)

    marker = displayed["chem_props"][0]["monoisotopic_mass"]
    assert marker == {
        "_qa_inline_curation": True,
        "curated_display": "150.13",
        "original_display": "166.04774",
        "original_present": True,
        "original_missing_display": "not present",
    }
    assert displayed["chem_props"][1]["monoisotopic_mass"] == 166.047740
    assert document["chem_props"][0]["monoisotopic_mass"] == 166.047740


def test_inline_curation_marker_stays_in_top_level_properties():
    displayed = qa_app._document_with_inline_curations(
        {"id": "REFMET:1", "formula": "C6H1005"},
        [{
            "path": ["formula"],
            "effective": "C6H10O5",
            "baseline": "C6H1005",
            "baseline_present": True,
        }],
    )

    scalar_fields, list_fields, nested_fields = qa_app._categorize_document_fields(displayed)

    scalar_by_name = dict(scalar_fields)
    assert scalar_by_name["formula"]["curated_display"] == "C6H10O5"
    assert list_fields == []
    assert nested_fields == []


def test_inline_curation_projection_is_reserved_for_generic_document_template():
    assert qa_app._template_supports_inline_curations("document.html") is True
    assert qa_app._template_supports_inline_curations(
        "cure_case_report_document.html"
    ) is False


def test_record_curation_summary_deduplicates_curators_and_ranges_dates():
    summary = qa_app._record_curation_summary([
        {
            "published_by": {"id": "keith", "name": "Keith Kelleher"},
            "published_at": "2026-09-24T12:00:00Z",
        },
        {
            "published_by": {"id": "keith", "name": "Keith Kelleher"},
            "published_at": "2026-09-29T15:30:00+00:00",
        },
        {
            "published_by": {"id": "jess", "name": "Jessica Maine"},
            "published_at": "2026-09-29T16:00:00Z",
        },
    ])

    assert summary == {
        "field_count": 3,
        "curator_text": "Curated by Keith Kelleher, Jessica Maine",
        "date_text": "Published Sep 24, 2026–Sep 29, 2026",
    }


def test_record_curation_summary_calls_out_missing_provenance():
    summary = qa_app._record_curation_summary([
        {"published_by": None, "published_at": ""},
    ])

    assert summary["curator_text"] == "Curator not recorded"
    assert summary["date_text"] == "Publication date not recorded"


def test_curatable_properties_are_derived_from_scalar_model_fields_and_denylist():
    definitions = {
        definition.property_name: definition
        for definition in curatable_property_definitions("MetaboliteIdentifier")
    }

    assert set(definitions) == {"is_generic_structure"}
    assert definitions["is_generic_structure"].value_type_name == "boolean"
    assert definitions["is_generic_structure"].nullable is True


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


def test_record_suppression_requires_rationale_and_accepts_restore_inverse():
    target = {
        "kind": "node",
        "model_type": "MetaboliteIdentifier",
        "id": "REFMET:RM0233954",
    }
    suppress = {
        "action": "suppress_record",
        "target": target,
        "note": "Formula and mass conflict with the reported InChIKey and xrefs.",
    }
    restore = {
        "action": "restore_record",
        "target": target,
    }

    validate_operation(METABOLITE_RECORD_SUPPRESSIONS, suppress)
    validate_operation(METABOLITE_RECORD_SUPPRESSIONS, restore)

    missing_rationale = {**suppress}
    missing_rationale.pop("note")
    with pytest.raises(ValueError, match="requires a rationale"):
        validate_operation(METABOLITE_RECORD_SUPPRESSIONS, missing_rationale)


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
        {"record_property_decisions": {
            "HMDB:1": {
                json.dumps(["is_generic_structure"], separators=(",", ":")): {
                    "mode": "set", "value": False,
                },
            },
        }},
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


def test_generic_structure_classification_exposes_unavailable_published_state():
    classification = _metabolite_generic_structure_classification(
        {"id": "HMDB:1", "is_generic_structure": False},
        {
            "record_property_decisions": {},
            "record_property_state_available": False,
            "record_property_state_error": "Published curations could not be loaded.",
        },
    )

    assert classification["state_available"] is False
    assert classification["state_error"] == "Published curations could not be loaded."


def test_apply_curations_is_orderable_and_legacy_rule_is_renamed_in_place():
    assert _normalize_metabolite_rule_ids([
        "ignore_generic_structure_mismatch",
        "ignore_ramp_mapping_denylist",
    ]) == ["ignore_generic_structure_mismatch", "apply_curations"]
    assert _record_properties_apply_to_generic_rule([
        "apply_curations", "ignore_generic_structure_mismatch",
    ]) is True
    assert _record_properties_apply_to_generic_rule([
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
        METABOLITE_RECORD_PROPERTIES: {"operations": [annotation]},
        METABOLITE_RECORD_SUPPRESSIONS: {"operations": []},
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
        if curation_type == METABOLITE_RECORD_PROPERTIES:
            raise RuntimeError("simulated S3 failure")
        return {"operation_count": 1, "batch": {"curation_batch_id": "edge-batch"}}

    monkeypatch.setattr(qa_app, "publish_cart", publish)
    monkeypatch.setattr(
        qa_app,
        "_combined_curation_cart",
        lambda *args: {
            "operations": [{"curation_type": METABOLITE_RECORD_PROPERTIES, **annotation}],
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
    assert payload["published"][0]["curation_type"] == "metabolite_equivalence_edges"
    assert payload["remaining_types"] == [METABOLITE_RECORD_PROPERTIES]


def test_combined_curation_cart_includes_record_and_metabolite_changes(monkeypatch):
    record_change = {
        "action": "set_properties",
        "target": {
            "kind": "node",
            "curation_set": "metabolite_harmonization",
            "model_type": "MetaboliteIdentifier",
            "id": "BiGG:1315507",
        },
        "decisions": [{
            "path": ["formula"],
            "mode": "set",
            "value": "C6H12O6",
            "observed_value": "C6H12O5",
        }],
        "operation_id": "record-op",
    }
    edge_change = {
        "action": "remove_edge",
        "start_id": "CHEBI:1",
        "end_id": "HMDB:1",
        "operation_id": "edge-op",
    }
    carts = {
        curation_type: {"operations": []}
        for curation_type in qa_app._QA_CURATION_TYPES
    }
    carts[METABOLITE_RECORD_PROPERTIES] = {"operations": [record_change]}
    carts["metabolite_equivalence_edges"] = {"operations": [edge_change]}
    monkeypatch.setattr(
        qa_app,
        "load_cart",
        lambda _storage, curation_type, _curator_id, _curator_name: carts[curation_type],
    )

    combined = qa_app._combined_curation_cart(object(), "keith", "Keith")

    assert {item["curation_type"] for item in combined["operations"]} == {
        METABOLITE_RECORD_PROPERTIES,
        "metabolite_equivalence_edges",
    }
    presented = next(
        item for item in combined["operations"]
        if item["curation_type"] == METABOLITE_RECORD_PROPERTIES
    )
    assert presented["target_label"] == "BiGG:1315507"
    assert presented["decision_rows"][0]["path_label"] == "formula"


def test_record_curation_editor_is_collapsed_by_default_and_explains_unavailable_state():
    template = qa_app.templates.env.get_template("record_property_curation.html")
    available = template.render(
        root_path="",
        record_curation={
            "available": True,
            "curation_set": "metabolite_harmonization",
            "model_type": "MetaboliteIdentifier",
            "target_id": "BiGG:1315507",
            "doc_key": "BiGG:1315507",
            "fields": [],
        },
    )
    unavailable = template.render(
        root_path="",
        record_curation={
            "available": False,
            "error": "This record has no editable field values.",
            "fields": [],
        },
    )

    assert "Curate record" in available
    assert 'id="recordPropertyCuration" hidden' in available
    assert "Curation unavailable" in unavailable
    assert "This record has no editable field values." in unavailable
    assert "Review changes" in unavailable


def test_both_curation_drawers_use_the_shared_cart_and_curator_identity():
    metabolite_source = (
        qa_app.STATIC_DIR / "metabolite_curation_cart.js"
    ).read_text()
    record_source = (
        qa_app.STATIC_DIR / "record_property_curation.js"
    ).read_text()

    for source in (metabolite_source, record_source):
        assert '"odinCurationCurator"' in source
        assert '"metaboliteHarmonizationCurator"' in source
        assert "/api/curation-cart?" in source
        assert "/api/curation-cart/items/" in source
        assert "/api/curation-cart/publish" in source
    assert 'operation.curation_type === "metabolite_record_properties"' in metabolite_source
    property_request_source = metabolite_source.split(
        "async function addPropertyDecisions", 1
    )[1].split("window.addMetabolitePropertyCurations", 1)[0]
    assert "replace_target: true" not in property_request_source


def test_generic_structure_editor_disables_itself_when_published_state_is_unavailable():
    source = (
        qa_app.STATIC_DIR / "metabolite_harmonization_visuals.js"
    ).read_text()

    assert "classification.state_available !== false" in source
    assert "Published record-property curation state is unavailable" in source
    assert 'role="alert"' in source
