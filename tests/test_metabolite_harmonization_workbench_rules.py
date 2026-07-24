from src.qa_browser.app import (
    _build_harmonization_stage_mw_validation,
    _metabolite_mass_summary,
    _metabolite_member_mass_examples,
    _metabolite_member_mass_values,
    _metabolite_mw_spread_percent,
    _remove_refmet_only_metabolites,
)


def test_remove_refmet_only_metabolites_uses_node_level_support():
    active_ids = {
        "REFMET:RM1",
        "CHEBI:1",
        "PUBCHEM.COMPOUND:1",
        "REFMET:RM2",
        "CHEBI:2",
        "HMDB:HMDB1",
        "CHEBI:3",
        "REFMET:RM3",
    }
    active_edges = [
        {
            "start_id": "REFMET:RM1",
            "end_id": "CHEBI:1",
            "sources": ["RefMet"],
        },
        {
            "start_id": "CHEBI:1",
            "end_id": "PUBCHEM.COMPOUND:1",
            "sources": ["RefMet"],
        },
        {
            "start_id": "REFMET:RM2",
            "end_id": "CHEBI:2",
            "sources": ["RefMet"],
        },
        {
            "start_id": "CHEBI:2",
            "end_id": "HMDB:HMDB1",
            "sources": ["HMDB"],
        },
    ]
    groups = [
        ["CHEBI:1", "PUBCHEM.COMPOUND:1", "REFMET:RM1"],
        ["CHEBI:2", "HMDB:HMDB1", "REFMET:RM2"],
    ]
    support_by_id = {
        "REFMET:RM1": {"RefMet"},
        "CHEBI:1": {"RefMet", "Reactome"},
        "PUBCHEM.COMPOUND:1": {"RefMet", "PubChem"},
        "REFMET:RM2": {"RefMet"},
        "CHEBI:2": {"RefMet", "HMDB"},
        "HMDB:HMDB1": {"HMDB"},
        "CHEBI:3": {"RefMet"},
        "REFMET:RM3": {"RefMet"},
    }

    filtered_ids, filtered_edges, filtered_groups, summary = _remove_refmet_only_metabolites(
        active_ids,
        active_edges,
        groups,
        support_by_id,
    )

    assert filtered_ids == active_ids - {"CHEBI:3", "REFMET:RM3"}
    assert filtered_edges == active_edges
    assert filtered_groups == groups
    assert summary == {
        "refmet_only_removed_metabolite_count": 2,
        "refmet_only_removed_group_count": 0,
        "refmet_only_removed_singleton_count": 2,
        "refmet_only_removed_identifier_count": 2,
        "refmet_only_removed_edge_count": 0,
    }


def test_remove_refmet_only_metabolites_removes_node_level_refmet_only_groups():
    active_ids = {"REFMET:RM1", "CHEBI:1", "PUBCHEM.COMPOUND:1"}
    active_edges = [
        {"start_id": "REFMET:RM1", "end_id": "CHEBI:1", "sources": ["RefMet"]},
        {"start_id": "CHEBI:1", "end_id": "PUBCHEM.COMPOUND:1", "sources": ["RefMet"]},
    ]
    groups = [["CHEBI:1", "PUBCHEM.COMPOUND:1", "REFMET:RM1"]]
    support_by_id = {
        "REFMET:RM1": {"RefMet\tsha256-example\t2026-06-30\t2026-06-30"},
        "CHEBI:1": {"RefMet\tsha256-example\t2026-06-30\t2026-06-30"},
        "PUBCHEM.COMPOUND:1": {"RefMet\tsha256-example\t2026-06-30\t2026-06-30"},
    }

    filtered_ids, filtered_edges, filtered_groups, summary = _remove_refmet_only_metabolites(
        active_ids,
        active_edges,
        groups,
        support_by_id,
    )

    assert filtered_ids == set()
    assert filtered_edges == []
    assert filtered_groups == []
    assert summary == {
        "refmet_only_removed_metabolite_count": 1,
        "refmet_only_removed_group_count": 1,
        "refmet_only_removed_singleton_count": 0,
        "refmet_only_removed_identifier_count": 3,
        "refmet_only_removed_edge_count": 2,
    }


def test_metabolite_mw_spread_uses_parseable_member_masses():
    member_rows = [
        {"member_id": "CHEBI:1", "raw_masses": ["100", None, "bad"]},
        {"member_id": "HMDB:1", "raw_masses": ["111.1"]},
        {"member_id": "CAS:1", "raw_masses": []},
    ]

    masses = _metabolite_member_mass_values(member_rows)
    summary = _metabolite_mass_summary(masses)

    assert masses == [100.0, 111.1]
    assert summary == {
        "count": 2,
        "min": 100.0,
        "median": 105.55,
        "max": 111.1,
    }
    assert round(_metabolite_mw_spread_percent(summary), 3) == 0.111
    assert _metabolite_member_mass_examples(member_rows) == [
        {"member_id": "CHEBI:1", "masses": [100.0]},
        {"member_id": "HMDB:1", "masses": [111.1]},
    ]


def test_build_harmonization_stage_mw_validation_flags_large_spreads():
    validation = _build_harmonization_stage_mw_validation(
        groups=[
            ["CHEBI:1", "HMDB:1"],
            ["CHEBI:2", "HMDB:2"],
        ],
        mass_values_by_id={
            "CHEBI:1": [100.0],
            "HMDB:1": [111.0],
            "CHEBI:2": [200.0],
            "HMDB:2": [201.0],
        },
        threshold=0.10,
        limit=10,
    )

    assert validation["computed"] is True
    assert validation["warning_count"] == 1
    assert validation["warnings"][0]["representative_id"] == "CHEBI:1"
    assert validation["warnings"][0]["spread_percent"] == 11.0
    assert validation["warnings"][0]["comparison_ids"] == "CHEBI:1 HMDB:1"
