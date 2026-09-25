from src.input_adapters.metabolite_harmonization.generic_structure import (
    MetaboliteGenericStructureAdapter,
)
from src.shared.metabolite_generic_structure import classify_generic_structure
from src.shared.record_merger import FieldConflictBehavior
import yaml


class _FakeAql:
    def execute(self, query, **_kwargs):
        if "FOR d IN MetaboliteIdentifier" in query:
            return [
                {"id": "HMDB:generic", "chem_props": [{"canonical_smiles": "C(*)O"}]},
                {"id": "HMDB:specific", "chem_props": [{"canonical_smiles": "CCO"}]},
                {"id": "CHEBI:generic", "chem_props": []},
                {"id": "HMDB:unknown", "chem_props": []},
            ]
        if "FOR d IN ChemicalEntity" in query:
            return [
                {"id": "CHEBI:generic", "smiles": "C([R])O", "formula": None, "inchi": None},
                {"id": "CHEBI:not-an-identifier", "smiles": "C(*)O", "formula": None, "inchi": None},
            ]
        raise AssertionError(query)


class _FakeDb:
    aql = _FakeAql()


def test_generic_structure_post_adapter_persists_true_false_and_skips_unknown(monkeypatch):
    adapter = object.__new__(MetaboliteGenericStructureAdapter)
    monkeypatch.setattr(adapter, "get_db", lambda: _FakeDb())

    rows = [row for batch in adapter.get_all() for row in batch]

    assert [(row.id, row.is_generic_structure) for row in rows] == [
        ("CHEBI:generic", True),
        ("HMDB:generic", True),
        ("HMDB:specific", False),
    ]
    assert adapter.get_field_conflict_behavior() == FieldConflictBehavior.KeepLast


def test_generic_structure_classifier_is_tri_state():
    assert classify_generic_structure([]) is None
    assert classify_generic_structure([{"smiles": "CCO"}]) is False
    assert classify_generic_structure([{"smiles": "C(*)O"}]) is True
    assert classify_generic_structure([{"smiles": "[Ru](Cl)(Cl)"}]) is False


def test_ramp_build_declares_generic_classifier_as_post_adapter():
    with open("src/use_cases/ramp/ramp.yaml", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)

    assert [entry["class"] for entry in config["post_adapters"]] == [
        "MetaboliteGenericStructureAdapter"
    ]
