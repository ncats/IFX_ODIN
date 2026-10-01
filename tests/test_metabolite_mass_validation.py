from src.shared.metabolite_mass_validation import (
    assess_mass_profiles,
    cluster_mass_observations,
    compare_mw_review_evidence,
    mass_validation_finding_metadata,
    mw_review_evidence_snapshot,
)


def _profile(*, average=(), monoisotopic=(), components=()):
    return {
        "whole": {
            "average": list(average),
            "monoisotopic": list(monoisotopic),
        },
        "components": list(components),
    }


def test_mass_observation_clustering_uses_validator_tolerances():
    observations = [
        {"member_id": "A", "value": 100.0},
        {"member_id": "B", "value": 100.05},
        {"member_id": "C", "value": 150.0},
    ]

    clusters = cluster_mass_observations(observations)

    assert [[item["member_id"] for item in cluster] for cluster in clusters] == [
        ["A", "B"],
        ["C"],
    ]
    assert [item["cluster_index"] for item in observations] == [0, 0, 1]


def test_mass_validation_ignores_small_within_channel_differences():
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"],
        {
            "CHEBI:1": _profile(average=[100.0], monoisotopic=[99.95]),
            "HMDB:1": _profile(average=[100.05], monoisotopic=[100.0]),
        },
        spread_threshold=0.10,
    )

    assert assessment is None


def test_mass_validation_marks_unexplained_difference_as_error():
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"],
        {
            "CHEBI:1": _profile(average=[100.0]),
            "HMDB:1": _profile(average=[150.0]),
        },
        spread_threshold=0.10,
    )

    assert assessment["severity"] == "error"
    assert assessment["reason"] == "unexplained_mass_difference"
    assert assessment["component_matches"] == []


def test_mw_finding_fingerprint_is_order_independent_but_membership_sensitive():
    profiles = {
        "CHEBI:1": _profile(average=[100.0]),
        "HMDB:1": _profile(average=[150.0]),
    }
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"], profiles, spread_threshold=0.10
    )
    first = mass_validation_finding_metadata(
        ["CHEBI:1", "HMDB:1"], profiles, assessment, spread_threshold=0.10
    )
    reordered = mass_validation_finding_metadata(
        ["HMDB:1", "CHEBI:1"], profiles, assessment, spread_threshold=0.10
    )
    with_massless_member = mass_validation_finding_metadata(
        ["HMDB:1", "CHEBI:1", "CAS:1"], profiles, assessment, spread_threshold=0.10
    )

    assert first == reordered
    assert first["anchor_id"] == "CHEBI:1"
    assert with_massless_member["finding_id"] != first["finding_id"]
    assert with_massless_member["evidence_fingerprint"] != first["evidence_fingerprint"]


def test_mw_finding_fingerprint_ignores_observation_display_routing_metadata():
    profiles = {
        "CHEBI:1": {
            **_profile(average=[100.0]),
            "whole_observations": [{
                "source": "ChEBI",
                "source_id": "CHEBI:1",
                "channel": "average",
                "value": 100.0,
                "molecular_formula": "C1",
            }],
        },
        "HMDB:1": _profile(average=[150.0]),
    }
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"], profiles, spread_threshold=0.10
    )
    before = mass_validation_finding_metadata(
        ["CHEBI:1", "HMDB:1"], profiles, assessment, spread_threshold=0.10
    )
    profiles["CHEBI:1"]["whole_observations"][0]["model_type"] = "ChemicalEntity"
    after = mass_validation_finding_metadata(
        ["CHEBI:1", "HMDB:1"], profiles, assessment, spread_threshold=0.10
    )

    assert after == before


def test_mass_validation_downgrades_salt_difference_to_warning():
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"],
        {
            "CHEBI:1": _profile(average=[100.0]),
            "HMDB:1": _profile(
                average=[135.45],
                components=[
                    {
                        "component_index": 0,
                        "average": 100.0,
                        "monoisotopic": 99.9,
                        "molecular_formula": "C5H8O2",
                        "smiles": "CCCCC(=O)O",
                    },
                    {
                        "component_index": 1,
                        "average": 35.45,
                        "monoisotopic": 34.97,
                        "molecular_formula": "Cl-",
                        "smiles": "[Cl-]",
                    },
                ],
            ),
        },
        spread_threshold=0.10,
    )

    assert assessment["severity"] == "warning"
    assert assessment["reason"] == "component_mass_match"
    assert assessment["component_matches"][0]["whole_member_id"] == "CHEBI:1"
    assert assessment["component_matches"][0]["component_member_id"] == "HMDB:1"


def test_mass_validation_handles_repeated_components_without_collapsing_them():
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"],
        {
            "CHEBI:1": _profile(average=[100.0]),
            "HMDB:1": _profile(
                average=[200.0],
                components=[
                    {"component_index": 0, "average": 100.0},
                    {"component_index": 1, "average": 100.0},
                ],
            ),
        },
        spread_threshold=0.10,
    )

    assert assessment["severity"] == "warning"
    assert {match["component_index"] for match in assessment["component_matches"]} == {0, 1}


def test_mass_validation_never_matches_average_to_monoisotopic_mass():
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"],
        {
            "CHEBI:1": _profile(average=[100.0]),
            "HMDB:1": _profile(
                average=[150.0],
                components=[{"component_index": 0, "monoisotopic": 100.0}],
            ),
        },
        spread_threshold=0.10,
    )

    assert assessment["severity"] == "error"
    assert assessment["component_matches"] == []
    assert assessment["mass_summary"] is None
    assert assessment["mass_summaries"]["average"] == {
        "min": 100.0, "max": 150.0, "median": 125.0, "count": 2,
    }
    assert assessment["mass_summaries"]["monoisotopic"] is None


def test_all_problematic_channels_must_have_component_explanation():
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1"],
        {
            "CHEBI:1": _profile(average=[100.0], monoisotopic=[90.0]),
            "HMDB:1": _profile(
                average=[150.0],
                monoisotopic=[140.0],
                components=[{"component_index": 0, "average": 100.0}],
            ),
        },
        spread_threshold=0.10,
    )

    assert assessment["severity"] == "error"
    assert [match["channel"] for match in assessment["component_matches"]] == ["average"]


def test_partial_component_match_does_not_hide_unexplained_mass_cluster():
    assessment = assess_mass_profiles(
        ["CHEBI:1", "HMDB:1", "REFMET:1"],
        {
            "CHEBI:1": _profile(average=[100.0]),
            "HMDB:1": _profile(average=[130.0]),
            "REFMET:1": _profile(
                average=[200.0],
                components=[{"component_index": 0, "average": 100.0}],
            ),
        },
        spread_threshold=0.10,
    )

    assert assessment["severity"] == "error"
    assert assessment["reason"] == "unexplained_mass_difference"
    assert assessment["channel_results"]["average"]["mass_cluster_count"] == 3
    assert assessment["channel_results"]["average"]["explained_mass_cluster_count"] == 2
    assert len(assessment["component_matches"]) == 1


def test_mw_review_snapshot_is_deterministic_and_uses_decimal_strings():
    first = mw_review_evidence_snapshot(
        {
            "member_mass_examples": [
                {"member_id": "HMDB:1", "average_masses": [150.0]},
                {"member_id": "CHEBI:1", "average_masses": [100.00]},
            ],
            "component_matches": [{
                "channel": "average",
                "whole_member_id": "CHEBI:1",
                "whole_mass": 100.0,
                "component_member_id": "HMDB:1",
                "component_mass": 150.00,
                "component_formula": "Na",
            }],
        },
        validator_version="component-aware-v1",
        threshold=0.10,
    )
    reordered = mw_review_evidence_snapshot(
        {
            "member_mass_examples": [
                {"member_id": "CHEBI:1", "average_masses": [100]},
                {"member_id": "HMDB:1", "average_masses": [150]},
            ],
            "component_matches": list(reversed(first["component_matches"])),
        },
        validator_version="component-aware-v1",
        threshold=0.1,
    )

    assert first == reordered
    assert first["threshold"] == "0.1"
    assert [item["value"] for item in first["mass_observations"]] == ["100", "150"]


def test_mw_review_comparison_reports_legacy_and_precise_changes():
    current = {
        "version": "mw-review-evidence-v1",
        "validator_version": "component-aware-v1",
        "threshold": "0.1",
        "mass_observations": [
            {"member_id": "CHEBI:1", "channel": "average", "value": "100"},
            {"member_id": "HMDB:1", "channel": "average", "value": "151"},
        ],
        "component_matches": [],
    }
    previous = {
        **current,
        "mass_observations": [
            {"member_id": "CHEBI:1", "channel": "average", "value": "100"},
            {"member_id": "HMDB:1", "channel": "average", "value": "150"},
        ],
    }

    legacy = compare_mw_review_evidence(
        current_member_ids=["CHEBI:1", "HMDB:1"],
        current_snapshot=current,
        current_fingerprint="b" * 64,
        previous_member_ids=["CHEBI:1", "HMDB:1"],
        previous_snapshot=None,
        previous_fingerprint="a" * 64,
    )
    precise = compare_mw_review_evidence(
        current_member_ids=["CHEBI:1", "HMDB:1"],
        current_snapshot=current,
        current_fingerprint="b" * 64,
        previous_member_ids=["CHEBI:1", "HMDB:1"],
        previous_snapshot=previous,
        previous_fingerprint="a" * 64,
    )

    assert legacy["membership_changed"] is False
    assert legacy["previous_evidence_available"] is False
    assert precise["added_mass_observations"][0]["value"] == "151"
    assert precise["removed_mass_observations"][0]["value"] == "150"


def test_compare_mw_review_evidence_separates_membership_only_changes():
    snapshot = {
        "version": "mw-review-evidence-v1",
        "validator_version": "component-aware-v1",
        "threshold": "1",
        "mass_observations": [
            {"member_id": "CHEBI:1", "channel": "monoisotopic", "value": "10"},
        ],
        "component_matches": [],
    }

    precise = compare_mw_review_evidence(
        current_member_ids=["CHEBI:1", "CHEBI:MASSLESS"],
        current_snapshot=snapshot,
        current_fingerprint="new-fingerprint",
        previous_member_ids=["CHEBI:1"],
        previous_snapshot=snapshot,
        previous_fingerprint="old-fingerprint",
    )
    legacy = compare_mw_review_evidence(
        current_member_ids=["CHEBI:1", "CHEBI:MASSLESS"],
        current_snapshot=snapshot,
        current_fingerprint="new-fingerprint",
        previous_member_ids=["CHEBI:1"],
        previous_snapshot=None,
        previous_fingerprint="old-fingerprint",
    )

    assert precise["membership_changed"] is True
    assert precise["evidence_changed"] is False
    assert legacy["membership_changed"] is True
    assert legacy["evidence_changed"] is None
