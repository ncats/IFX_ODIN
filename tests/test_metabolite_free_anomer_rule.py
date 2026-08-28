from src.qa_browser.app import _free_anomer_rule_edges
from src.qa_browser.metabolite_anomer_rule import (
    CARBOHYDRATE_FAMILY_ALGORITHM_VERSION,
    CHEBI_POLYMER_ROOT_IDS,
    FREE_ANOMER_ALGORITHM_VERSION,
    FREE_ANOMER_RULE_ID,
    evaluate_free_anomer_candidate,
    generated_carbohydrate_structure,
    normalized_free_anomer_structure,
)


ALPHA_D_GLUCOSE = "OC[C@H]1O[C@H](O)[C@H](O)[C@@H](O)[C@@H]1O"
BETA_D_GLUCOSE = "OC[C@H]1O[C@@H](O)[C@H](O)[C@@H](O)[C@@H]1O"
D_GLUCOPYRANOSE = "OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@@H]1O"
D_MANNOPYRANOSE = "OC[C@H]1OC(O)[C@@H](O)[C@@H](O)[C@@H]1O"
METHYL_ALPHA_D_GLUCOPYRANOSIDE = "CO[C@H]1O[C@H](CO)[C@@H](O)[C@H](O)[C@H]1O"
D_GALACTOPYRANOSE = "OC[C@H]1OC(O)[C@H](O)[C@@H](O)[C@H]1O"
ALPHA_D_GALACTOSE = "OC[C@H]1O[C@H](O)[C@H](O)[C@@H](O)[C@H]1O"
BETA_D_GALACTOSE = "OC[C@H]1O[C@@H](O)[C@H](O)[C@@H](O)[C@H]1O"
L_GALACTOPYRANOSE = "OC[C@@H]1OC(O)[C@@H](O)[C@H](O)[C@@H]1O"
ALPHA_L_GALACTOSE = "OC[C@@H]1O[C@@H](O)[C@@H](O)[C@H](O)[C@@H]1O"
BETA_L_GALACTOSE = "OC[C@@H]1O[C@H](O)[C@@H](O)[C@H](O)[C@@H]1O"
GENERIC_HEXOPYRANOSE = "OCC1OC(O)C(O)C(O)C1O"


def candidate(child_smiles=ALPHA_D_GLUCOSE, parent_smiles=D_GLUCOPYRANOSE, **overrides):
    row = {
        "relationship_id": "IsAEdge:glucose",
        "relationship_predicate": "is_a",
        "child": {
            "id": "CHEBI:17925",
            "name": "alpha-D-glucose",
            "formula": "C6H12O6",
            "charge": "0",
            "smiles": child_smiles,
        },
        "parent": {
            "id": "CHEBI:4167",
            "name": "D-glucopyranose",
            "formula": "C6H12O6",
            "charge": "0",
            "smiles": parent_smiles,
        },
    }
    for path, value in overrides.items():
        side, field = path.split("__", 1)
        row[side][field] = value
    return row


def test_free_anomer_normalization_converges_alpha_beta_and_unspecified_glucose():
    results = [
        normalized_free_anomer_structure(smiles)[0]
        for smiles in (ALPHA_D_GLUCOSE, BETA_D_GLUCOSE, D_GLUCOPYRANOSE)
    ]

    assert {result["normalized_inchi_key"] for result in results} == {
        "WQZGKKKJIJFFOK-GASJEMHNSA-N"
    }
    assert len({result["source_inchi_key"] for result in results}) == 3


def test_generated_carbohydrate_family_key_uses_anomer_normalization():
    results = [
        generated_carbohydrate_structure(smiles)[0]
        for smiles in (ALPHA_D_GLUCOSE, BETA_D_GLUCOSE, D_GLUCOPYRANOSE)
    ]

    assert {result["family_inchi_key"] for result in results} == {
        "WQZGKKKJIJFFOK-GASJEMHNSA-N"
    }
    assert all(result["comparable"] for result in results)
    assert all(result["algorithm_version"] == CARBOHYDRATE_FAMILY_ALGORITHM_VERSION for result in results)


def test_generated_carbohydrate_family_preserves_non_anomeric_stereo_and_classifies_locked_forms():
    glucose = generated_carbohydrate_structure(D_GLUCOPYRANOSE)[0]
    mannose = generated_carbohydrate_structure(D_MANNOPYRANOSE)[0]
    locked = generated_carbohydrate_structure(METHYL_ALPHA_D_GLUCOPYRANOSIDE)[0]

    assert glucose["family_inchi_key"] != mannose["family_inchi_key"]
    assert locked["comparable"] is True
    assert locked["free_anomeric_center_count"] == 0
    assert locked["family_inchi_key"] == locked["source_inchi_key"]


def test_generated_carbohydrate_family_separates_d_and_l_galactose_and_ignores_generic_form():
    d_family = {
        generated_carbohydrate_structure(smiles)[0]["family_inchi_key"]
        for smiles in (D_GALACTOPYRANOSE, ALPHA_D_GALACTOSE, BETA_D_GALACTOSE)
    }
    l_family = {
        generated_carbohydrate_structure(smiles)[0]["family_inchi_key"]
        for smiles in (L_GALACTOPYRANOSE, ALPHA_L_GALACTOSE, BETA_L_GALACTOSE)
    }
    generic = generated_carbohydrate_structure(GENERIC_HEXOPYRANOSE)[0]

    assert len(d_family) == 1
    assert len(l_family) == 1
    assert d_family != l_family
    assert generic["comparable"] is False
    assert generic["classification_reason"] == "under_specified_stereochemistry"


def test_free_anomer_candidate_accepts_direct_glucose_anomer_pair():
    decision = evaluate_free_anomer_candidate(candidate())

    assert decision["accepted"] is True
    assert decision["reason"] == "free_anomer_match"
    assert decision["normalized_inchi_key"] == "WQZGKKKJIJFFOK-GASJEMHNSA-N"


def test_free_anomer_candidate_preserves_other_stereocenters():
    decision = evaluate_free_anomer_candidate(candidate(parent_smiles=D_MANNOPYRANOSE))

    assert decision["accepted"] is False
    assert decision["reason"] == "normalized_inchi_key_mismatch"


def test_free_anomer_candidate_rejects_locked_glycoside_and_repeat_formula():
    locked = evaluate_free_anomer_candidate(candidate(child_smiles=METHYL_ALPHA_D_GLUCOPYRANOSIDE))
    repeated = evaluate_free_anomer_candidate(candidate(child__formula="(C6H10O5)n.H2O"))

    assert locked["reason"] == "child_free_anomeric_center_count_not_one"
    assert repeated["reason"] == "repeat_formula"


def test_free_anomer_rule_builds_auditable_synthetic_stage_edge():
    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            if "INBOUND root IsAEdge" in query:
                return []
            assert bind_vars == {
                "polymer_ids": [],
            }
            return [candidate()]

    class FakeDb:
        aql = FakeAql()

    edges, summary = _free_anomer_rule_edges(
        FakeDb(),
        {"CHEBI:17925", "CHEBI:4167", "HMDB:HMDB0000122"},
    )

    assert len(edges) == 1
    assert edges[0]["start_id"] == "CHEBI:17925"
    assert edges[0]["end_id"] == "CHEBI:4167"
    assert edges[0]["synthetic"] is True
    assert edges[0]["fallback"] is False
    assert edges[0]["rule_id"] == FREE_ANOMER_RULE_ID
    assert edges[0]["algorithm_version"] == "free-anomer-v2"
    assert edges[0]["algorithm_version"] == FREE_ANOMER_ALGORITHM_VERSION
    assert "carbohydrate_gate_enabled" not in edges[0]["details"][0]
    assert edges[0]["details"][0]["normalized_inchi_key"] == "WQZGKKKJIJFFOK-GASJEMHNSA-N"
    assert summary["free_anomer_candidate_pair_count"] == 1
    assert summary["free_anomer_accepted_pair_count"] == 1
    assert "free_anomer_carbohydrate_gate_enabled" not in summary
    assert summary["free_anomer_candidate_identifier_count"] == 2
    assert summary["free_anomer_synthetic_edge_count"] == 1


def test_free_anomer_rule_checks_all_non_polymer_chebi_pairs_without_ancestry_gate():
    class FakeAql:
        def __init__(self):
            self.descendant_roots = []

        def execute(self, query, bind_vars=None, **_kwargs):
            if "INBOUND root IsAEdge" in query:
                self.descendant_roots.append(bind_vars["root_id"])
                return []
            assert bind_vars == {
                "polymer_ids": [],
            }
            return [candidate()]

    class FakeDb:
        def __init__(self):
            self.aql = FakeAql()

    db = FakeDb()
    edges, summary = _free_anomer_rule_edges(
        db,
        {"CHEBI:17925", "CHEBI:4167"},
    )

    assert len(edges) == 1
    assert db.aql.descendant_roots == list(CHEBI_POLYMER_ROOT_IDS)
    assert summary["free_anomer_polymer_excluded_identifier_count"] == 0
