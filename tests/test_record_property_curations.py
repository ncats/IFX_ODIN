from types import SimpleNamespace

import pytest

from src.core.curations import METABOLITE_RECORD_PROPERTIES
from src.core.record_property_curations import (
    CURATION_ORIGINAL_FIELD,
    apply_record_property_decision,
    recalculate_curated_structure_derivatives,
    resolve_parent_and_field,
    schema_for_path,
    validate_value_for_schema,
)


def decision(path, value=None, observed=None, mode="set", observed_exists=True):
    return SimpleNamespace(
        curation_type=METABOLITE_RECORD_PROPERTIES,
        target={"model_type": "MetaboliteIdentifier", "id": "REFMET:1"},
        path=path,
        mode=mode,
        value=value,
        observed_value=observed,
        observed_exists=observed_exists,
        batch_id="batch-1",
        published_at="2026-09-29T12:00:00Z",
        published_by={"name": "Keith"},
        source_operation={"operation_id": "op-1", "note": "Fix the source value"},
    )


def test_applies_effective_value_and_preserves_original_with_provenance():
    document = {"id": "REFMET:1", "formula": "C6H1005", "sources": ["RefMet"]}
    projected, report = apply_record_property_decision(
        document, decision(["formula"], "C6H10O5", "C6H1005")
    )

    assert document["formula"] == "C6H1005"
    assert projected["formula"] == "C6H10O5"
    assert projected[CURATION_ORIGINAL_FIELD] == {"formula": "C6H1005"}
    assert projected["sources"][-1].startswith("Manual Curation\tbatch-1\t")
    assert "formula\t" in projected["updates"][0]
    assert report["status"] == "applied"


def test_nested_selector_preserves_original_at_the_same_object_level():
    document = {
        "id": "REFMET:1",
        "chem_props": [
            {"source": "RefMet", "source_id": "RM1", "mw": 100.0},
            {"source": "HMDB", "source_id": "HMDB1", "mw": 101.0},
        ],
    }
    path = ["chem_props", {"match": {"source": "RefMet", "source_id": "RM1"}}, "mw"]
    projected, _ = apply_record_property_decision(document, decision(path, 102.0, 100.0))

    corrected = projected["chem_props"][0]
    assert corrected["mw"] == 102.0
    assert corrected[CURATION_ORIGINAL_FIELD] == {"mw": 100.0}


def test_structure_recalculation_touches_only_selected_chem_props_once(monkeypatch):
    selected_path = [
        "chem_props",
        {"match": {"source": "ChEBI", "source_id": "CHEBI:1"}},
        "iso_smiles",
    ]
    document = {
        "chem_props": [
            {"source": "ChEBI", "source_id": "CHEBI:1", "iso_smiles": "[2H]O[2H]"},
            {"source": "HMDB", "source_id": "HMDB1", "iso_smiles": "C", "calculated_mw": "16"},
        ],
    }
    calls = []
    monkeypatch.setattr(
        "src.core.record_property_curations.calculate_metabolite_chem_props_derivatives",
        lambda properties: calls.append(properties["source_id"]) or {"calculated_mw": "20"},
    )

    projected = recalculate_curated_structure_derivatives(
        document,
        model_type="MetaboliteIdentifier",
        decisions=[
            decision(selected_path, "[2H]O[2H]", "CC"),
            decision(selected_path, "[2H]O[2H]", "CC"),
        ],
    )

    assert calls == ["CHEBI:1"]
    assert projected["chem_props"][0]["calculated_mw"] == "20"
    assert projected["chem_props"][1]["calculated_mw"] == "16"


def test_non_structure_curation_does_not_recalculate_chem_props(monkeypatch):
    document = {
        "chem_props": [{
            "source": "ChEBI",
            "source_id": "CHEBI:1",
            "mw": "10",
            "calculated_mw": "11",
        }],
    }
    monkeypatch.setattr(
        "src.core.record_property_curations.calculate_metabolite_chem_props_derivatives",
        lambda _properties: pytest.fail("non-structure curation triggered recalculation"),
    )

    projected = recalculate_curated_structure_derivatives(
        document,
        model_type="MetaboliteIdentifier",
        decisions=[decision([
            "chem_props",
            {"match": {"source": "ChEBI", "source_id": "CHEBI:1"}},
            "mw",
        ], "12", "10")],
    )

    assert projected["chem_props"][0]["calculated_mw"] == "11"


def test_selector_must_match_exactly_one_object():
    document = {"chem_props": [{"source": "x"}, {"source": "x"}]}
    with pytest.raises(ValueError, match="matched 2 records"):
        resolve_parent_and_field(
            document, ["chem_props", {"match": {"source": "x"}}, "mw"]
        )


def test_stale_decision_fails_but_already_curated_baseline_is_idempotent():
    with pytest.raises(ValueError, match="Stale curation"):
        apply_record_property_decision(
            {"id": "REFMET:1", "formula": "changed"},
            decision(["formula"], "curated", "old"),
        )

    projected, report = apply_record_property_decision(
        {"id": "REFMET:1", "formula": "curated"},
        decision(["formula"], "curated", "old"),
    )
    assert projected["formula"] == "curated"
    assert report["status"] == "redundant"


def test_absent_scalar_can_be_added_and_its_absence_is_preserved():
    projected, report = apply_record_property_decision(
        {"id": "BiGG:1315507"},
        decision(
            ["is_generic_structure"], True, None, observed_exists=False
        ),
    )

    assert projected["is_generic_structure"] is True
    assert projected[CURATION_ORIGINAL_FIELD]["is_generic_structure"] == {
        "_odin_field_was_missing": True
    }
    assert report["status"] == "applied"

    with pytest.raises(ValueError, match="Stale curation"):
        apply_record_property_decision(
            {"id": "BiGG:1315507", "is_generic_structure": None},
            decision(
                ["is_generic_structure"], True, None, observed_exists=False
            ),
        )


def test_remove_override_restores_persisted_original_on_resume():
    persisted = {
        "id": "REFMET:1",
        "formula": "C6H10O5",
        CURATION_ORIGINAL_FIELD: {"formula": "C6H1005"},
    }

    restored, report = apply_record_property_decision(
        persisted,
        decision(["formula"], mode="remove_override"),
    )

    assert restored["formula"] == "C6H1005"
    assert CURATION_ORIGINAL_FIELD not in restored
    assert report["previous"] == "C6H10O5"
    assert report["result"] == "C6H1005"


def test_remove_override_deletes_field_when_original_was_absent():
    persisted = {
        "id": "BiGG:1315507",
        "is_generic_structure": True,
        CURATION_ORIGINAL_FIELD: {
            "is_generic_structure": {"_odin_field_was_missing": True},
        },
    }

    restored, _ = apply_record_property_decision(
        persisted,
        decision(["is_generic_structure"], mode="remove_override"),
    )

    assert "is_generic_structure" not in restored
    assert CURATION_ORIGINAL_FIELD not in restored


def test_schema_validation_supports_nested_list_object_paths():
    schema = {
        "chem_props": {
            "type": "list", "item_type": "object",
            "fields": {"source": "str", "mw": "float"},
        },
    }
    path = ["chem_props", {"match": {"source": "RefMet"}}, "mw"]
    descriptor = schema_for_path(schema, path)
    validate_value_for_schema(12.5, descriptor, path)
    with pytest.raises(ValueError, match="does not match schema type float"):
        validate_value_for_schema("12.5", descriptor, path)


def test_aggregate_edit_cannot_replace_protected_nested_locator_fields():
    schema = {
        "chem_props": {
            "type": "list", "item_type": "object",
            "fields": {"source": "str", "source_id": "str", "mw": "float"},
        },
    }

    with pytest.raises(ValueError, match="protected nested fields"):
        schema_for_path(schema, ["chem_props"])

    descriptor = schema_for_path(
        schema,
        ["chem_props", {"match": {"source": "RefMet", "source_id": "RM1"}}, "mw"],
    )
    assert descriptor == "float"


def test_aggregate_object_edit_cannot_inject_undeclared_protected_fields():
    schema = {
        "profile": {"type": "object", "fields": {"name": "str"}},
    }

    with pytest.raises(ValueError, match="protected nested fields"):
        descriptor = schema_for_path(schema, ["profile"])
        validate_value_for_schema(
            {"name": "valid", "id": "injected", "sources": []},
            descriptor,
            ["profile"],
        )

    assert schema_for_path(schema, ["profile", "name"]) == "str"


def test_scalar_list_validates_each_item_type():
    descriptor = {"type": "list", "item_type": "str"}
    validate_value_for_schema(["a", "b"], descriptor, ["aliases"])

    with pytest.raises(ValueError, match="does not match schema type str"):
        validate_value_for_schema(
            ["valid", {"id": "injected", "sources": []}],
            descriptor,
            ["aliases"],
        )


def test_list_of_dict_schema_is_not_treated_as_a_scalar_list():
    schema = {
        "profiles": {
            "type": "list",
            "item_type": {"type": "dict", "key_type": "str", "value_type": "str"},
        },
    }

    with pytest.raises(ValueError, match="protected nested fields"):
        schema_for_path(schema, ["profiles"])
