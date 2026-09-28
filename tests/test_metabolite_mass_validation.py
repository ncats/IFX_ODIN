from src.shared.metabolite_mass_validation import assess_mass_profiles


def _profile(*, average=(), monoisotopic=(), components=()):
    return {
        "whole": {
            "average": list(average),
            "monoisotopic": list(monoisotopic),
        },
        "components": list(components),
    }


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
