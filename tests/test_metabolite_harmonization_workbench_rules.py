from src.qa_browser.app import (
    _build_harmonization_stage_denylist_validation,
    _build_harmonization_stage_mw_validation,
    _filter_identifier_support_for_rules,
    _metabolite_mass_summary,
    _metabolite_member_mass_examples,
    _metabolite_member_mass_values,
    _metabolite_mw_spread_percent,
    _normalize_metabolite_rule_parameters,
    _remove_refmet_only_metabolites,
    _wikipathways_xref_only_ids_from_rows,
)


def test_normalize_metabolite_rule_parameters_parses_default_and_submitted_textareas():
    default_parameters = _normalize_metabolite_rule_parameters(
        ["ignore_wikipathways_prefixes"],
        {},
    )
    submitted_parameters = _normalize_metabolite_rule_parameters(
        ["ignore_wikipathways_prefixes"],
        {
            "ignore_wikipathways_prefixes": {
                "prefixes": "CHEBI\r\nHMDB\r\nchebi",
                "source_fields": "bdbKeggCompound\r\nbdbWikidata\r\nbdbKeggCompound",
            }
        },
    )

    assert default_parameters["ignore_wikipathways_prefixes"]["source_fields"] == [
        "bdbKeggCompound",
    ]
    assert submitted_parameters["ignore_wikipathways_prefixes"] == {
        "prefixes": ["CHEBI", "HMDB"],
        "source_fields": ["bdbKeggCompound", "bdbWikidata"],
    }


def test_wikipathways_ignored_source_fields_identify_only_xref_only_identifiers():
    mapping_rows = [
        {
            "start_id": "KEGG.COMPOUND:C1",
            "end_id": "Wikidata:Q1",
            "details": [{
                "source": "WikiPathways",
                "source_field": "bdbWikidata",
                "source_id": "KEGG.COMPOUND:C1",
            }],
        },
        {
            "start_id": "CHEBI:2",
            "end_id": "KEGG.COMPOUND:C2",
            "details": [{
                "source": "WikiPathways",
                "source_field": "bdbKeggCompound",
                "source_id": "CHEBI:2",
            }],
        },
        {
            "start_id": "HMDB:3",
            "end_id": "KEGG.COMPOUND:C2",
            "details": [{
                "source": "WikiPathways",
                "source_field": "bdbHmdb",
                "source_id": "HMDB:3",
            }],
        },
        {
            "start_id": "Wikidata:Q2",
            "end_id": "CHEBI:4",
            "details": [{
                "source": "WikiPathways",
                "source_field": "bdbChEBI",
                "source_id": "Wikidata:Q2",
            }],
        },
        {
            "start_id": "CHEBI:5",
            "end_id": "Wikidata:Q3",
            "details": [{
                "source": "WikiPathways",
                "source_field": "bdbWikidata",
                "source_id": "CHEBI:5",
            }],
        },
    ]

    ignored_ids = _wikipathways_xref_only_ids_from_rows(
        mapping_rows,
        pathway_primary_ids={"Wikidata:Q1"},
        ignored_source_fields={"bdbKeggCompound", "bdbWikidata"},
    )

    # Q1 is protected because it is also pathway-native; C2 is protected because
    # it is introduced through both an ignored and an allowed source field.
    assert ignored_ids == {"Wikidata:Q3"}


def test_filter_identifier_support_removes_only_wikipathways_from_ignored_xref_only_ids():
    support_by_id = {
        "KEGG.COMPOUND:C1": {"WikiPathways"},
        "Wikidata:Q1": {"WikiPathways"},
        "Wikidata:Q2": {"WikiPathways", "HMDB"},
        "CHEBI:1": {"WikiPathways"},
    }

    filtered = _filter_identifier_support_for_rules(
        support_by_id,
        ["ignore_wikipathways_prefixes"],
        {
            "ignore_wikipathways_prefixes": {
                "prefixes": [],
                "source_fields": ["bdbWikidata"],
            }
        },
        wikipathways_ignored_source_field_ids={"Wikidata:Q1", "Wikidata:Q2"},
    )

    assert filtered == {
        "KEGG.COMPOUND:C1": {"WikiPathways"},
        "Wikidata:Q1": set(),
        "Wikidata:Q2": {"HMDB"},
        "CHEBI:1": {"WikiPathways"},
    }


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


def test_build_harmonization_stage_denylist_validation_flags_still_merged_pairs():
    validation = _build_harmonization_stage_denylist_validation(
        groups=[
            ["CHEBI:1", "HMDB:1", "PUBCHEM.COMPOUND:1"],
            ["CHEBI:2", "HMDB:2"],
        ],
        denylist_pairs={
            ("CHEBI:1", "HMDB:1"),
            ("CHEBI:2", "HMDB:2"),
            ("CHEBI:9", "HMDB:9"),
        },
        rule_enabled=True,
        limit=10,
    )

    assert validation["computed"] is True
    assert validation["rule_enabled"] is True
    assert validation["denylist_pair_count"] == 3
    assert validation["warning_count"] == 2
    assert validation["affected_clique_count"] == 2
    warning_clique_pairs = {
        w["rank_by_size"]: {(p["left_id"], p["right_id"]) for p in w["pairs"]}
        for w in validation["warnings"]
    }
    assert warning_clique_pairs == {1: {("CHEBI:1", "HMDB:1")}, 2: {("CHEBI:2", "HMDB:2")}}
    first = next(w for w in validation["warnings"] if w["rank_by_size"] == 1)
    assert first["size"] == 3
    assert first["comparison_ids"] == "CHEBI:1 HMDB:1 PUBCHEM.COMPOUND:1"


def test_build_harmonization_stage_denylist_validation_groups_multiple_pairs_in_one_clique():
    validation = _build_harmonization_stage_denylist_validation(
        groups=[
            ["CAS:1", "CHEBI:1", "HMDB:1", "HMDB:2", "KEGG.COMPOUND:1"],
        ],
        denylist_pairs={
            ("HMDB:1", "KEGG.COMPOUND:1"),
            ("HMDB:2", "KEGG.COMPOUND:1"),
        },
        rule_enabled=True,
        limit=10,
    )

    # Two denylisted pairs both land in the same clique -- one row, not two.
    assert validation["warning_count"] == 2
    assert validation["affected_clique_count"] == 1
    assert len(validation["warnings"]) == 1
    warning = validation["warnings"][0]
    assert warning["pair_count"] == 2
    assert warning["size"] == 5
    warning_pairs = {(p["left_id"], p["right_id"]) for p in warning["pairs"]}
    assert warning_pairs == {("HMDB:1", "KEGG.COMPOUND:1"), ("HMDB:2", "KEGG.COMPOUND:1")}


def test_build_harmonization_stage_denylist_validation_ignores_separated_or_missing_pairs():
    validation = _build_harmonization_stage_denylist_validation(
        groups=[
            ["CHEBI:1", "PUBCHEM.COMPOUND:1"],
        ],
        denylist_pairs={
            # HMDB:1 is an active singleton (no group entry) after the denylist edge was ignored.
            ("CHEBI:1", "HMDB:1"),
            ("CHEBI:9", "HMDB:9"),
        },
        rule_enabled=True,
        limit=10,
    )

    assert validation["warning_count"] == 0
    assert validation["denylist_pair_count"] == 2
    assert validation["affected_clique_count"] == 0
    assert validation["warnings"] == []
