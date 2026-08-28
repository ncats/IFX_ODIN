import hashlib
import json

import src.qa_browser.app as qa_app
from src.qa_browser.app import (
    _build_harmonization_pipeline_tree,
    _build_harmonization_stage_denylist_validation,
    _build_harmonization_stage_carbohydrate_family_validation,
    _build_harmonization_stage_mw_validation,
    _build_harmonization_stage_cart_flags,
    _filter_identifier_support_for_rules,
    _harmonization_denylist_review_from_stage,
    _harmonization_stage_cart_warning_ranks,
    _harmonization_jobs_by_pipeline_key,
    _load_metabolite_edge_removal_curations,
    _materialize_harmonization_stage,
    _metabolite_edge_removal_pairs_from_curation_batch,
    _annotate_harmonization_pipeline_curation_status,
    _delete_previous_harmonization_pipeline_runs,
    _metabolite_mass_summary,
    _metabolite_member_mass_examples,
    _metabolite_member_mass_values,
    _metabolite_identifier_source_linkout,
    _metabolite_edge_decision_operation,
    _metabolite_expected_clique_assertion_operation,
    _evaluate_expected_clique_assertions,
    _expected_clique_assertion_matrix,
    _expected_clique_assertion_edges,
    _newly_failing_expected_clique_assertions,
    _metabolite_mw_spread_percent,
    _annotate_harmonization_pipeline_run_progress,
    _normalize_metabolite_rule_parameters,
    _wikipathways_xref_only_ids_from_rows,
)


class _FakeCurationStorage:
    bucket = "test-curations"

    def __init__(self, objects):
        self.objects = objects

    def list_keys(self, prefix):
        return [key for key in self.objects if key.startswith(prefix)]

    def read_text(self, key):
        return self.objects[key]


def test_harmonization_rules_have_expected_workbench_groups():
    groups_by_rule = {
        rule["id"]: rule["group"]
        for rule in qa_app._METABOLITE_HARMONIZATION_RULES
    }

    assert groups_by_rule["ignore_generic_structure_mismatch"] == "Pruning"
    assert groups_by_rule["merge_inchikey_by_mw_cutoff"] == "Merging"
    assert groups_by_rule["merge_derived_inchikey_by_mw_cutoff"] == "Chemistry-based merging"
    assert groups_by_rule["merge_free_anomeric_forms"] == "Chemistry-based merging"
    assert groups_by_rule["ignore_ramp_mapping_denylist"] == "Cleanup"
    assert groups_by_rule["force_expected_clique_assertions"] == "Cleanup"


def test_metabolite_identifier_summaries_batch_ids_without_legacy_edge_queries(monkeypatch):
    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            assert "FOR identifier IN @identifiers" in query
            assert "MetaboliteIdentifierMappingEdge" not in query
            assert "ChebiChemicalEntityMetaboliteIdentifierEdge" not in query
            assert bind_vars == {"identifiers": ["CHEBI:1", "HMDB:1"]}
            return [
                {"query_id": "CHEBI:1", "found": True, "metabolite": {"id": "CHEBI:1"}},
                {"query_id": "HMDB:1", "found": False, "metabolite": None},
            ]

    class FakeDb:
        aql = FakeAql()

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())

    rows = qa_app._load_metabolite_identifier_summaries(["CHEBI:1", "HMDB:1"])

    assert [row["query_id"] for row in rows] == ["CHEBI:1", "HMDB:1"]


def test_harmonization_pipeline_tree_shares_prefix_and_branches_at_first_different_stage():
    pipelines = [
        {
            "_key": "pipeline-a",
            "name": "Anomer merge with cleanup A",
            "runs": [{
                "_key": "run-a",
                "status": "complete",
                "stages": [
                    {"_key": "baseline", "stage_index": 0, "rule_ids": [], "display_label": "Baseline"},
                    {"_key": "shared-1", "stage_index": 1, "rule_ids": ["shared"], "display_label": "Shared rule"},
                    {"_key": "branch-a-2", "stage_index": 2, "rule_ids": ["cleanup-a"], "display_label": "Cleanup A"},
                ],
            }],
        },
        {
            "_key": "pipeline-b",
            "name": "Anomer merge with cleanup B",
            "runs": [{
                "_key": "run-b",
                "status": "complete",
                "stages": [
                    {"_key": "baseline", "stage_index": 0, "rule_ids": [], "display_label": "Baseline"},
                    {"_key": "shared-1", "stage_index": 1, "rule_ids": ["shared"], "display_label": "Shared rule"},
                    {"_key": "branch-b-2", "stage_index": 2, "rule_ids": ["cleanup-b"], "display_label": "Cleanup B"},
                ],
            }],
        },
        {"_key": "pipeline-c", "name": "Not run", "runs": []},
    ]

    tree = _build_harmonization_pipeline_tree(pipelines, [{
        "pipeline_key": "pipeline-a",
        "stages": [{
            "_key": "shared-1",
            "overview_stats": {
                "clique_count": 123,
                "mw_warning_count": 2,
                "denylist_warning_count": 1,
                "assertion_count": 4,
            },
        }],
    }])

    assert tree["pipeline_count"] == 3
    assert tree["represented_pipeline_count"] == 2
    assert tree["not_run_count"] == 1
    assert len(tree["roots"]) == 1
    baseline = tree["roots"][0]
    shared = baseline["children"][0]
    assert baseline["pipeline_count"] == 2
    assert baseline["is_shared"] is True
    assert shared["stage_key"] == "shared-1"
    assert shared["pipeline_count"] == 2
    assert shared["stats"]["clique_count"] == 123
    assert [child["stage_key"] for child in shared["children"]] == ["branch-a-2", "branch-b-2"]
    assert [
        child["terminals"][0]["pipeline_name"] for child in shared["children"]
    ] == ["Anomer merge with cleanup A", "Anomer merge with cleanup B"]


def test_harmonization_pipeline_page_loads_only_requested_pipeline(monkeypatch):
    pipeline = {"_key": "pipeline-a", "name": "Pipeline A", "runs": []}
    calls = {}

    monkeypatch.setattr(qa_app, "get_db", lambda name: calls.setdefault("db_name", name) or object())
    monkeypatch.setattr(qa_app, "_ensure_harmonization_pipeline_collections", lambda _db: None)

    def fake_list(limit=25, pipeline_key=None):
        calls["list"] = (limit, pipeline_key)
        return [pipeline]

    monkeypatch.setattr(qa_app, "_list_harmonization_pipelines", fake_list)
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_edge_removal_curations",
        lambda: {"assertions": []},
    )
    monkeypatch.setattr(
        qa_app,
        "_list_harmonization_pipeline_stage_overview_stats",
        lambda pipelines, _curation_state: [{"pipeline_key": pipelines[0]["_key"], "stages": []}],
    )

    page = qa_app._load_harmonization_pipeline_page("pipeline-a")

    assert calls["db_name"] == "metabolite_harmonization"
    assert calls["list"] == (1, "pipeline-a")
    assert page["pipeline"] == pipeline
    assert page["overview"]["pipeline_stage_stats"][0]["pipeline_key"] == "pipeline-a"


def test_derived_inchikey_iterator_selects_derived_fields():
    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            assert "prop[@inchi_key_field]" in query
            assert "prop[@inchi_key_prefix_field]" in query
            assert bind_vars == {
                "inchi_key_field": "derived_inchi_key",
                "inchi_key_prefix_field": "derived_inchi_key_prefix",
            }
            return [{
                "id": "CHEBI:1",
                "inchi_keys": ["ABCDEFGHIJKLMN-ABCDEFGHIJ-N"],
                "prefixes": ["ABCDEFGHIJKLMN"],
                "masses": ["100"],
            }]

    class FakeDb:
        aql = FakeAql()

    assert list(qa_app._iter_metabolite_identifier_inchi_key_matches(
        FakeDb(),
        "mw_cutoff",
        500,
        "derived",
    )) == [
        ("CHEBI:1", ["ABCDEFGHIJKLMN-ABCDEFGHIJ"], "duplex", 100.0),
    ]


def test_derived_inchikey_rule_merges_without_changing_reported_rule(monkeypatch):
    def fake_matches(_db, mode, mw_cutoff=None, key_kind="reported"):
        assert mode == "mw_cutoff"
        assert mw_cutoff == 500
        if key_kind == "derived":
            yield "CHEBI:1", ["ABCDEFGHIJKLMN-ABCDEFGHIJ"], "duplex", 100.0
            yield "HMDB:1", ["ABCDEFGHIJKLMN-ABCDEFGHIJ"], "duplex", 100.0

    monkeypatch.setattr(
        qa_app,
        "_iter_metabolite_identifier_inchi_key_matches",
        fake_matches,
    )

    reported_groups, _ = qa_app._build_harmonized_groups(
        object(),
        {"CHEBI:1", "HMDB:1"},
        [],
        ["merge_inchikey_by_mw_cutoff"],
        {"merge_inchikey_by_mw_cutoff": {"mw_cutoff": 500}},
    )
    derived_groups, summary = qa_app._build_harmonized_groups(
        object(),
        {"CHEBI:1", "HMDB:1"},
        [],
        ["merge_derived_inchikey_by_mw_cutoff"],
        {"merge_derived_inchikey_by_mw_cutoff": {"mw_cutoff": 500}},
    )

    assert reported_groups == []
    assert derived_groups == [["CHEBI:1", "HMDB:1"]]
    assert summary["derived_inchi_key_mw_cutoff_identifier_count"] == 2
    assert summary["derived_inchi_key_mw_cutoff_merge_count"] == 1


def test_metabolite_curation_batch_extracts_symmetric_mapping_edge_removals():
    pairs = _metabolite_edge_removal_pairs_from_curation_batch({
        "format_version": 1,
        "graph": "metabolite_harmonization",
        "operations": [
            {
                "action": "remove_edge",
                "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": "hmdb:HMDB00001",
                "end_id": "kegg:C00001",
                "symmetric": True,
            },
            {
                "action": "set_field",
                "node_id": "HMDB:HMDB00001",
                "field": "name",
                "value": "ignored by this loader",
            },
        ],
    })

    assert pairs == {("HMDB:HMDB00001", "KEGG.COMPOUND:C00001")}


def test_metabolite_edge_decision_operation_builds_normalized_retention():
    assert _metabolite_edge_decision_operation(
        "retain_edge",
        "refmet:RM0039120",
        "chebi:73585",
        "Current records agree.",
    ) == {
        "action": "retain_edge",
        "edge_type": "MetaboliteIdentifierMappingEdge",
        "start_id": "CHEBI:73585",
        "end_id": "REFMET:RM0039120",
        "symmetric": True,
        "note": "Current records agree.",
    }


def test_expected_clique_assertion_operation_normalizes_order_and_has_stable_id():
    first = _metabolite_expected_clique_assertion_operation(
        "chebi:17925 CHEBI:15903 CHEBI:17925",
        "Glucose anomers",
        "Expected to resolve together.",
    )
    second = _metabolite_expected_clique_assertion_operation(
        ["CHEBI:15903", "CHEBI:17925"],
        "Renamed assertion",
    )

    assert first["member_ids"] == ["CHEBI:15903", "CHEBI:17925"]
    assert first["assertion_id"] == second["assertion_id"]
    assert first["rationale"] == "Expected to resolve together."


def test_metabolite_identifier_source_linkouts_cover_common_databases():
    assert _metabolite_identifier_source_linkout("HMDB:HMDB0000122") == {
        "label": "HMDB",
        "url": "https://hmdb.ca/metabolites/HMDB0000122",
    }
    assert _metabolite_identifier_source_linkout("KEGG.COMPOUND:C00031") == {
        "label": "KEGG Compound",
        "url": "https://www.kegg.jp/entry/C00031",
    }
    assert _metabolite_identifier_source_linkout("LIPIDMAPS:LMFA01010001") == {
        "label": "LIPID MAPS",
        "url": "https://www.lipidmaps.org/databases/lmsd/LMFA01010001",
    }
    assert _metabolite_identifier_source_linkout("REFMET:RM0108637") == {
        "label": "RefMet",
        "url": "https://www.metabolomicsworkbench.org/databases/refmet/refmet_details.php?REFMET_ID=RM0108637",
    }
    assert _metabolite_identifier_source_linkout("CHEBI:15956") == {
        "label": "ChEBI",
        "url": "https://www.ebi.ac.uk/chebi/searchId.do?chebiId=CHEBI%3A15956",
    }
    assert _metabolite_identifier_source_linkout(
        "InChIKey:BSYNRYMUTXBXSQ-UHFFFAOYSA-N"
    ) == {
        "label": "PubChem InChIKey lookup",
        "url": "https://pubchem.ncbi.nlm.nih.gov/compound/BSYNRYMUTXBXSQ-UHFFFAOYSA-N",
    }
    assert _metabolite_identifier_source_linkout("Unknown:123") is None


def test_load_metabolite_curations_reads_json_batches_and_fingerprints_content():
    prefix = "curations/v1/metabolite_harmonization/"
    storage = _FakeCurationStorage({
        f"{prefix}batch-a.json": json.dumps({
            "format_version": 1,
            "curation_batch_id": "batch-a",
            "graph": "metabolite_harmonization",
            "operations": [{
                "action": "remove_edge",
                "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": "CHEBI:1",
                "end_id": "HMDB:1",
                "symmetric": True,
            }],
        }),
        f"{prefix}README.txt": "not a batch",
    })

    loaded = _load_metabolite_edge_removal_curations(storage, prefix)

    assert loaded["pairs"] == {("CHEBI:1", "HMDB:1")}
    assert loaded["batch_ids"] == ["batch-a"]
    assert len(loaded["fingerprint"]) == 64
    assert loaded["prefix"] == f"s3://test-curations/{prefix}"


def test_assertion_only_batch_does_not_change_edge_curation_fingerprint():
    prefix = "curations/v1/metabolite_harmonization/"
    assertion = _metabolite_expected_clique_assertion_operation(
        ["CHEBI:15903", "CHEBI:17925"],
        "Glucose anomers",
    )
    storage = _FakeCurationStorage({
        f"{prefix}assertions.json": json.dumps({
            "format_version": 1,
            "curation_batch_id": "assertions",
            "graph": "metabolite_harmonization",
            "published_at": "2026-08-25T12:00:00Z",
            "created_by": {"id": "keith", "name": "Keith"},
            "operations": [assertion],
        }),
    })

    loaded = _load_metabolite_edge_removal_curations(storage, prefix)

    assert loaded["fingerprint"] == hashlib.sha256().hexdigest()
    assert loaded["batch_ids"] == []
    assert loaded["assertion_batch_ids"] == ["assertions"]
    assert loaded["assertions"][0]["assertion_id"] == assertion["assertion_id"]


def test_published_assertion_can_be_retired_chronologically():
    prefix = "curations/v1/metabolite_harmonization/"
    assertion = _metabolite_expected_clique_assertion_operation(
        ["CHEBI:15903", "CHEBI:17925"],
        "Glucose anomers",
    )
    storage = _FakeCurationStorage({
        f"{prefix}assert.json": json.dumps({
            "format_version": 1,
            "curation_batch_id": "assert",
            "graph": "metabolite_harmonization",
            "published_at": "2026-08-25T12:00:00Z",
            "operations": [assertion],
        }),
        f"{prefix}retire.json": json.dumps({
            "format_version": 1,
            "curation_batch_id": "retire",
            "graph": "metabolite_harmonization",
            "published_at": "2026-08-25T13:00:00Z",
            "operations": [{"action": "retire_assertion", "assertion_id": assertion["assertion_id"]}],
        }),
    })

    loaded = _load_metabolite_edge_removal_curations(storage, prefix)

    assert loaded["assertions"] == []
    assert loaded["assertion_batch_ids"] == ["assert", "retire"]


def test_expected_clique_evaluation_reports_pass_split_and_missing_and_first_failure():
    assertion = {
        "assertion_id": "same-clique-test",
        "name": "Glucose anomers",
        "member_ids": ["CHEBI:15903", "CHEBI:17925"],
    }
    stages = [
        {"_key": "baseline", "display_label": "Baseline"},
        {"_key": "step-2", "display_label": "Ignore HMDB prefixes"},
        {"_key": "cleanup", "display_label": "Cleanup"},
    ]
    active = [
        {"stage_key": stage, "member_id": member}
        for stage, members in {
            "baseline": assertion["member_ids"],
            "step-2": assertion["member_ids"],
            "cleanup": ["CHEBI:15903"],
        }.items()
        for member in members
    ]
    memberships = [
        {"stage_key": "baseline", "member_id": member, "clique_id": "clique/glucose", "clique_size": 47}
        for member in assertion["member_ids"]
    ] + [
        {"stage_key": "step-2", "member_id": "CHEBI:15903", "clique_id": "clique/beta", "clique_size": 16},
        {"stage_key": "step-2", "member_id": "CHEBI:17925", "clique_id": "clique/alpha", "clique_size": 10},
    ]

    evaluated = _evaluate_expected_clique_assertions(
        [assertion],
        [stage["_key"] for stage in stages],
        active,
        memberships,
    )
    matrix = _expected_clique_assertion_matrix(evaluated, stages)

    assert [result["status"] for result in matrix["rows"][0]["stage_results"]] == ["pass", "split", "missing"]
    assert matrix["rows"][0]["first_failure"]["stage_key"] == "step-2"
    assert matrix["rows"][0]["first_failure"]["display_label"] == "Ignore HMDB prefixes"
    assert matrix["rows"][0]["final_status"] == "missing"


def test_stage_comparison_only_reports_assertions_that_newly_fail():
    assertion_results = {
        "by_stage": {
            "left": {
                "results": [
                    {"assertion_id": "new-failure", "name": "Glucose forms", "member_ids": ["CHEBI:1", "CHEBI:2"], "status": "pass"},
                    {"assertion_id": "already-failing", "name": "Existing failure", "member_ids": ["CHEBI:3", "CHEBI:4"], "status": "split"},
                    {"assertion_id": "still-passing", "name": "Stable assertion", "member_ids": ["CHEBI:5", "CHEBI:6"], "status": "pass"},
                ],
            },
            "right": {
                "results": [
                    {"assertion_id": "new-failure", "name": "Glucose forms", "member_ids": ["CHEBI:1", "CHEBI:2"], "status": "split"},
                    {"assertion_id": "already-failing", "name": "Existing failure", "member_ids": ["CHEBI:3", "CHEBI:4"], "status": "missing"},
                    {"assertion_id": "still-passing", "name": "Stable assertion", "member_ids": ["CHEBI:5", "CHEBI:6"], "status": "pass"},
                ],
            },
        },
    }

    failures = _newly_failing_expected_clique_assertions(assertion_results, "left", "right")

    assert [failure["assertion_id"] for failure in failures] == ["new-failure"]
    assert failures[0]["left_status"] == "pass"
    assert failures[0]["right_status"] == "split"
    assert failures[0]["inspection_query"] == "id=CHEBI%3A1+CHEBI%3A2&stages=left%2Cright"


def test_stage_comparison_involved_preview_deduplicates_names_and_ids():
    preview = qa_app._snapshot_compare_involved_preview(
        {"CHEBI:3", "CHEBI:1", "CHEBI:2"},
        {
            "CHEBI:1": {"label": "D-glucose"},
            "CHEBI:2": {"label": "d-Glucose"},
            "CHEBI:3": {"label": "D-mannose"},
        },
        limit=2,
    )

    assert preview == {
        "count": 3,
        "ids": ["CHEBI:1", "CHEBI:2"],
        "names": ["D-glucose"],
        "remaining_id_count": 1,
    }


def test_stage_comparison_pipeline_context_shows_overlap_and_selected_stages():
    shared_stages = [
        {"_key": "baseline", "stage_index": 0, "rule_ids": [], "display_label": "Baseline"},
        {"_key": "shared-1", "stage_index": 1, "rule_ids": ["ignore_generic_structure_mismatch"], "display_label": "Ignore Generic Structure Mismatch"},
    ]
    pipelines = [
        {
            "_key": "pipeline-a",
            "name": "Anomer merge with cleanup A",
            "runs": [{
                "_key": "run-a",
                "stages": [*shared_stages, {"_key": "left", "stage_index": 2, "rule_ids": ["a", "left"], "display_label": "Cleanup A"}],
            }],
        },
        {
            "_key": "pipeline-b",
            "name": "Anomer merge with cleanup B",
            "runs": [{
                "_key": "run-b",
                "stages": [*shared_stages, {"_key": "right", "stage_index": 2, "rule_ids": ["b", "right"], "display_label": "Cleanup B"}],
            }],
        },
    ]

    context = qa_app._snapshot_comparison_pipeline_context(
        {"_key": "left", "stage_index": 2, "rule_ids": ["a", "left"]},
        {"_key": "right", "stage_index": 2, "rule_ids": ["b", "right"]},
        pipelines,
    )

    assert context["same_pipeline"] is False
    assert context["shared_stage_count"] == 2
    assert context["common_prefix_count"] == 2
    assert context["divergence_after_label"] == "Ignore Generic Structure Mismatch"
    assert [lane["pipeline_name"] for lane in context["lanes"]] == [
        "Anomer merge with cleanup A",
        "Anomer merge with cleanup B",
    ]
    assert context["lanes"][0]["stages"][-1]["is_left"] is True
    assert context["lanes"][1]["stages"][-1]["is_right"] is True
    assert [stage["key"] for stage in context["trunk_stages"]] == ["baseline", "shared-1"]
    assert [[stage["key"] for stage in branch["stages"]] for branch in context["branches"]] == [
        ["left"],
        ["right"],
    ]
    assert context["branch_start_column"] == 2


def test_stage_comparison_same_pipeline_marks_context_stages_shared():
    pipeline = {
        "_key": "pipeline",
        "name": "One pipeline",
        "runs": [{
            "_key": "run",
            "stages": [
                {"_key": "baseline", "stage_index": 0, "rule_ids": [], "display_label": "Baseline"},
                {"_key": "left", "stage_index": 1, "rule_ids": ["left"], "display_label": "Left"},
                {"_key": "right", "stage_index": 2, "rule_ids": ["left", "right"], "display_label": "Right"},
            ],
        }],
    }

    context = qa_app._snapshot_comparison_pipeline_context(
        {"_key": "left", "stage_index": 1, "rule_ids": ["left"]},
        {"_key": "right", "stage_index": 2, "rule_ids": ["left", "right"]},
        [pipeline],
    )

    assert context["same_pipeline"] is True
    assert context["branches"] == []
    assert context["shared_stage_count"] == 3
    assert all(stage["is_shared"] for stage in context["lanes"][0]["stages"])


def test_stage_comparison_visualization_reuses_snapshot_graph_and_sankey_builders(monkeypatch):
    snapshot_graphs = [
        {"snapshot_key": "left", "cliques": []},
        {"snapshot_key": "right", "cliques": []},
    ]
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_snapshot_union",
        lambda ids, stages: {
            "member_ids": ids,
            "snapshot_graphs": snapshot_graphs,
            "stages": stages,
        },
    )
    monkeypatch.setattr(
        qa_app,
        "_build_metabolite_snapshot_sankey",
        lambda sections, order: {"sections": sections, "order": order},
    )

    result = qa_app._load_metabolite_snapshot_comparison_visualization(
        ["CHEBI:47935", "CHEBI:47936"],
        "left",
        "right",
    )

    assert result["query_ids"] == ["CHEBI:47935", "CHEBI:47936"]
    assert result["snapshot_graphs"] == snapshot_graphs
    assert result["sankey"] == {"sections": snapshot_graphs, "order": ["left", "right"]}


def test_explicit_stage_selection_builds_one_sankey_across_pipeline_boundaries(monkeypatch):
    monkeypatch.setattr(
        qa_app,
        "_list_harmonization_pipelines",
        lambda limit=100: (_ for _ in ()).throw(AssertionError("pipeline grouping should be bypassed")),
    )
    sections = [
        {
            "snapshot_key": "pipeline-a-cleanup",
            "snapshot_name": "Pipeline A cleanup",
            "snapshot_created_at": "2026-08-27T10:00:00Z",
            "cliques": [{
                "clique_key": "a",
                "clique_size": 2,
                "member_ids": ["CHEBI:1", "CHEBI:2"],
                "member_name_by_id": {},
            }],
        },
        {
            "snapshot_key": "pipeline-b-cleanup",
            "snapshot_name": "Pipeline B cleanup",
            "snapshot_created_at": "2026-08-27T11:00:00Z",
            "cliques": [{
                "clique_key": "b",
                "clique_size": 2,
                "member_ids": ["CHEBI:1", "CHEBI:2"],
                "member_name_by_id": {},
            }],
        },
    ]

    plots = qa_app._build_metabolite_snapshot_sankeys(
        sections,
        ["pipeline-a-cleanup", "pipeline-b-cleanup"],
    )

    assert len(plots) == 1
    assert plots[0]["pipeline_key"] == "selected-stage-comparison"
    assert plots[0]["stage_count"] == 2
    assert [node["snapshot_key"] for node in plots[0]["sankey"]["nodes"]] == [
        "pipeline-a-cleanup",
        "pipeline-b-cleanup",
    ]
    assert plots[0]["sankey"]["links"][0]["value"] == 2


def test_expected_clique_assertion_edges_use_a_provenanced_star_and_skip_missing_ids():
    edges, summary = _expected_clique_assertion_edges(
        {"CHEBI:1", "CHEBI:2", "CHEBI:3"},
        [
            {
                "assertion_id": "same-clique-present",
                "name": "Three forms",
                "rationale": "Curated expectation",
                "member_ids": ["CHEBI:1", "CHEBI:2", "CHEBI:3"],
                "curation_batch_id": "batch-1",
                "published_by": {"id": "curator@example.org"},
            },
            {
                "assertion_id": "same-clique-missing",
                "name": "Missing form",
                "member_ids": ["CHEBI:1", "CHEBI:4"],
            },
        ],
    )

    assert [(edge["start_id"], edge["end_id"]) for edge in edges] == [
        ("CHEBI:1", "CHEBI:2"),
        ("CHEBI:1", "CHEBI:3"),
    ]
    assert all(edge["synthetic"] and edge["fallback"] for edge in edges)
    assert edges[0]["details"][0]["assertion_id"] == "same-clique-present"
    assert edges[0]["details"][0]["curation_batch_id"] == "batch-1"
    assert summary["expected_clique_assertion_count"] == 2
    assert summary["expected_clique_assertion_applied_count"] == 1
    assert summary["expected_clique_assertion_skipped_count"] == 1
    assert summary["expected_clique_synthetic_edge_count"] == 2
    assert summary["expected_clique_assertion_skipped_samples"][0]["missing_member_ids"] == ["CHEBI:4"]


def test_load_metabolite_curations_applies_edge_decisions_in_publication_order():
    prefix = "curations/v1/metabolite_harmonization/"

    def batch(batch_id, published_at, operations):
        return json.dumps({
            "format_version": 1,
            "curation_batch_id": batch_id,
            "graph": "metabolite_harmonization",
            "published_at": published_at,
            "operations": operations,
        })

    def decision(action, left, right):
        return {
            "action": action,
            "edge_type": "MetaboliteIdentifierMappingEdge",
            "start_id": left,
            "end_id": right,
            "symmetric": True,
        }

    storage = _FakeCurationStorage({
        f"{prefix}z-base.json": batch("base", "2026-08-24T13:00:00Z", [
            decision("remove_edge", "CHEBI:1", "HMDB:1"),
            decision("remove_edge", "CHEBI:2", "HMDB:2"),
        ]),
        f"{prefix}a-retain.json": batch("retain", "2026-08-24T14:00:00Z", [
            decision("retain_edge", "CHEBI:1", "HMDB:1"),
            decision("retain_edge", "CHEBI:2", "HMDB:2"),
        ]),
        f"{prefix}m-remove-again.json": batch("remove-again", "2026-08-24T15:00:00Z", [
            decision("remove_edge", "HMDB:2", "CHEBI:2"),
        ]),
    })

    loaded = _load_metabolite_edge_removal_curations(storage, prefix)

    assert loaded["pairs"] == {("CHEBI:2", "HMDB:2")}
    assert loaded["pair_states"] == {
        ("CHEBI:1", "HMDB:1"): "retain_edge",
        ("CHEBI:2", "HMDB:2"): "remove_edge",
    }
    assert loaded["batch_ids"] == ["base", "retain", "remove-again"]


def test_pipeline_curation_status_uses_explicit_run_button_states():
    current_engine = qa_app._HARMONIZATION_ENGINE_VERSION
    pipelines = [
        {"_key": "new", "rule_ids": ["ignore_ramp_mapping_denylist"], "runs": []},
        {
            "_key": "stale",
            "engine_version": current_engine,
            "rule_ids": ["ignore_ramp_mapping_denylist"],
            "runs": [{"_key": "old-run", "status": "complete", "curation_fingerprint": "old"}],
        },
        {
            "_key": "current",
            "engine_version": current_engine,
            "rule_ids": ["ignore_ramp_mapping_denylist"],
            "runs": [{"_key": "new-run", "status": "complete", "curation_fingerprint": "current"}],
        },
        {
            "_key": "unaffected",
            "engine_version": current_engine,
            "rule_ids": ["merge_shared_inchikey_duplex"],
            "runs": [{"_key": "base-run", "status": "complete"}],
        },
        {
            "_key": "assertions-stale",
            "engine_version": current_engine,
            "rule_ids": [
                "force_expected_clique_assertions",
                "remove_enrichment_only_metabolites",
            ],
            "runs": [{"_key": "assertion-run", "status": "complete", "assertion_fingerprint": "old"}],
        },
        {
            "_key": "failed",
            "engine_version": current_engine,
            "rule_ids": ["force_expected_clique_assertions"],
            "runs": [{"_key": "failed-run", "status": "failed", "assertion_fingerprint": "current-assertions"}],
        },
        {
            "_key": "running",
            "engine_version": current_engine,
            "rule_ids": [],
            "runs": [{"_key": "running-run", "status": "running"}],
        },
        {
            "_key": "cleaning",
            "engine_version": current_engine,
            "rule_ids": [],
            "runs": [{"_key": "cleaning-run", "status": "cleaning_up"}],
        },
    ]

    _annotate_harmonization_pipeline_curation_status(pipelines, "current", "current-assertions")

    assert [pipeline["run_button_label"] for pipeline in pipelines] == [
        "Run pipeline",
        "Sync curations",
        "Up to date",
        "Up to date",
        "Sync curations",
        "Retry failed pipeline",
        "Running...",
        "Running...",
    ]
    assert pipelines[1]["needs_curation_sync"] is True
    assert pipelines[1]["edge_curations_changed"] is True
    assert pipelines[1]["assertion_curations_changed"] is False
    assert pipelines[2]["needs_curation_sync"] is False
    assert pipelines[4]["needs_curation_sync"] is True
    assert pipelines[4]["edge_curations_changed"] is False
    assert pipelines[4]["assertion_curations_changed"] is True
    assert pipelines[1]["sync_from_stage_index"] == 1
    assert pipelines[4]["sync_from_stage_index"] == 1
    assert pipelines[2]["sync_from_stage_index"] is None
    assert [pipeline["run_action_enabled"] for pipeline in pipelines] == [
        True,
        True,
        False,
        False,
        True,
        True,
        False,
        False,
    ]


def test_pipeline_engine_change_enables_validation_rebuild():
    pipelines = [{
        "_key": "old-engine",
        "engine_version": "staged-pipeline-v4",
        "rule_ids": [],
        "runs": [{"_key": "old-run", "status": "complete"}],
    }]

    _annotate_harmonization_pipeline_curation_status(pipelines, None, None)

    assert pipelines[0]["engine_version_changed"] is True
    assert pipelines[0]["run_action_enabled"] is True
    assert pipelines[0]["run_action_kind"] == "rebuild"
    assert pipelines[0]["run_button_label"] == "Re-run pipeline"


def test_pipeline_job_history_hides_failure_superseded_by_successful_retry():
    jobs = [
        {
            "id": "successful-retry",
            "pipeline_key": "pipeline-a",
            "action": "run_pipeline",
            "status": "complete",
            "created_at": "2026-08-27T14:00:00Z",
        },
        {
            "id": "failed-attempt",
            "pipeline_key": "pipeline-a",
            "action": "run_pipeline",
            "status": "failed",
            "created_at": "2026-08-27T13:00:00Z",
            "error": "[HTTP 404][ERR 1600] cursor not found",
        },
        {
            "id": "unresolved-failure",
            "pipeline_key": "pipeline-b",
            "action": "run_pipeline",
            "status": "failed",
            "created_at": "2026-08-27T13:30:00Z",
        },
    ]

    jobs_by_pipeline = _harmonization_jobs_by_pipeline_key(jobs)

    assert [job["id"] for job in jobs_by_pipeline["pipeline-a"]] == ["successful-retry"]
    assert [job["id"] for job in jobs_by_pipeline["pipeline-b"]] == ["unresolved-failure"]


def test_completed_run_engine_version_takes_precedence_over_pipeline_version():
    pipelines = [{
        "_key": "current-run",
        "engine_version": "staged-pipeline-v4",
        "rule_ids": [],
        "runs": [{
            "_key": "current-run",
            "status": "complete",
            "engine_version": qa_app._HARMONIZATION_ENGINE_VERSION,
        }],
    }]

    _annotate_harmonization_pipeline_curation_status(pipelines, None, None)

    assert pipelines[0]["engine_version_changed"] is False
    assert pipelines[0]["run_action_enabled"] is False
    assert pipelines[0]["run_button_label"] == "Up to date"


def test_pipeline_run_progress_marks_the_next_unfinished_step_and_active_job():
    pipelines = [{
        "_key": "pipeline-a",
        "rules": [
            {"id": "rule-1", "label": "First rule"},
            {"id": "rule-2", "label": "Second rule"},
            {"id": "rule-3", "label": "Third rule"},
        ],
        "runs": [{
            "status": "running",
            "stage_keys": ["baseline", "stage-1", "stage-2"],
        }],
    }]
    jobs = [{
        "action": "run_pipeline",
        "pipeline_key": "pipeline-a",
        "status": "running",
    }]

    _annotate_harmonization_pipeline_run_progress(pipelines, jobs)

    assert pipelines[0]["active_stage_index"] == 3
    assert pipelines[0]["active_stage_text"] == "Step 3: Third rule"
    assert jobs[0]["active_stage_text"] == "Step 3: Third rule"


def test_pipeline_run_progress_distinguishes_baseline_and_finalization():
    pipelines = [
        {
            "_key": "baseline",
            "rules": [{"id": "rule-1", "label": "First rule"}],
            "runs": [{"status": "running", "stage_keys": []}],
        },
        {
            "_key": "finalizing",
            "rules": [{"id": "rule-1", "label": "First rule"}],
            "runs": [{"status": "cleaning_up", "stage_keys": ["baseline", "stage-1"]}],
        },
    ]

    _annotate_harmonization_pipeline_run_progress(pipelines, [])

    assert pipelines[0]["active_stage_text"] == "Baseline"
    assert pipelines[1]["active_stage_index"] is None
    assert pipelines[1]["active_stage_text"] == "Cleaning up old snapshots"


def test_interrupted_stage_materialization_is_not_marked_complete(monkeypatch):
    class FakeCollection:
        def __init__(self, fail_on_insert_many=False):
            self.fail_on_insert_many = fail_on_insert_many
            self.inserted = []
            self.updates = []
            self.insert_many_kwargs = []

        def insert(self, document, **_kwargs):
            self.inserted.append(dict(document))

        def insert_many(self, documents, **kwargs):
            self.insert_many_kwargs.append(kwargs)
            if self.fail_on_insert_many:
                raise ConnectionError("response interrupted")

        def update(self, document):
            self.updates.append(dict(document))

    class FakeDb:
        def __init__(self):
            self.collections = {
                qa_app._HARMONIZATION_STAGE_COLLECTION: FakeCollection(),
                qa_app._HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION: FakeCollection(),
                qa_app._HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION: FakeCollection(),
                qa_app._HARMONIZED_METABOLITE_COLLECTION: FakeCollection(),
                qa_app._HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION: FakeCollection(fail_on_insert_many=True),
            }

        def collection(self, name):
            return self.collections[name]

    db = FakeDb()
    stage_doc = {
        "_key": "stage-test",
        "id": "HarmonizationStage:stage-test",
        "name": "Test stage",
        "status": "complete",
    }
    monkeypatch.setattr(qa_app, "_delete_harmonization_stage_artifacts", lambda *_args: None)
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_identifier_handles",
        lambda _db, _ids: {"CHEBI:1": "MetaboliteIdentifier/chebi-1"},
    )

    try:
        _materialize_harmonization_stage(
            db,
            "stage-test",
            stage_doc,
            {"CHEBI:1"},
            [],
            [["CHEBI:1"]],
        )
        assert False, "Expected the simulated interrupted response to propagate"
    except ConnectionError as exc:
        assert str(exc) == "response interrupted"

    stage_collection = db.collection(qa_app._HARMONIZATION_STAGE_COLLECTION)
    assert stage_collection.inserted[0]["status"] == "materializing"
    assert stage_collection.updates[-1]["status"] == "failed"
    assert stage_doc["status"] == "failed"
    member_collection = db.collection(qa_app._HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION)
    assert member_collection.insert_many_kwargs == [{"overwrite": True, "silent": True}]


def test_successful_stage_materialization_marks_complete_only_after_writes(monkeypatch):
    class FakeCollection:
        def __init__(self):
            self.inserted = []
            self.updates = []

        def insert(self, document, **_kwargs):
            self.inserted.append(dict(document))

        def insert_many(self, _documents, **_kwargs):
            return None

        def update(self, document):
            self.updates.append(dict(document))

    class FakeDb:
        def __init__(self):
            self.collections = {
                name: FakeCollection()
                for name in (
                    qa_app._HARMONIZATION_STAGE_COLLECTION,
                    qa_app._HARMONIZATION_STAGE_ACTIVE_IDENTIFIER_CHUNK_COLLECTION,
                    qa_app._HARMONIZATION_STAGE_EVIDENCE_EDGE_COLLECTION,
                    qa_app._HARMONIZED_METABOLITE_COLLECTION,
                    qa_app._HARMONIZED_METABOLITE_MEMBER_EDGE_COLLECTION,
                )
            }

        def collection(self, name):
            return self.collections[name]

    db = FakeDb()
    stage_doc = {
        "_key": "stage-test",
        "id": "HarmonizationStage:stage-test",
        "name": "Test stage",
        "status": "complete",
    }
    monkeypatch.setattr(qa_app, "_delete_harmonization_stage_artifacts", lambda *_args: None)
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_identifier_handles",
        lambda _db, _ids: {"CHEBI:1": "MetaboliteIdentifier/chebi-1"},
    )

    _materialize_harmonization_stage(
        db,
        "stage-test",
        stage_doc,
        {"CHEBI:1"},
        [],
        [["CHEBI:1"]],
    )

    stage_collection = db.collection(qa_app._HARMONIZATION_STAGE_COLLECTION)
    assert stage_collection.inserted[0]["status"] == "materializing"
    assert stage_collection.updates == [{
        "_key": "stage-test",
        "status": "complete",
        "updated_at": stage_doc["updated_at"],
        "materialization_completed_at": stage_doc["materialization_completed_at"],
    }]
    assert stage_doc["status"] == "complete"


def test_replacing_pipeline_runs_deletes_only_unreferenced_stage_artifacts(monkeypatch):
    class FakeAql:
        def __init__(self):
            self.deleted_run_keys = []

        def execute(self, query, bind_vars=None, **_kwargs):
            if "RETURN KEEP" in query:
                return [
                    {"_key": "old-run-a", "stage_keys": ["baseline", "old-curated"]},
                    {"_key": "old-run-b", "stage_keys": ["baseline", "old-curated"]},
                ]
            self.deleted_run_keys = list((bind_vars or {}).get("run_keys") or [])
            return []

    class FakeStageCollection:
        def __init__(self):
            self.deleted = []

        def has(self, _key):
            return True

        def delete(self, key):
            self.deleted.append(key)

    class FakeDb:
        def __init__(self):
            self.aql = FakeAql()
            self.stages = FakeStageCollection()

        def collection(self, _name):
            return self.stages

    db = FakeDb()
    deleted_artifacts = []
    monkeypatch.setattr(
        qa_app,
        "_stage_is_referenced_by_any_run",
        lambda _db, stage_key: stage_key == "baseline",
    )
    monkeypatch.setattr(
        qa_app,
        "_delete_harmonization_stage_artifacts",
        lambda _db, stage_key: deleted_artifacts.append(stage_key),
    )
    monkeypatch.setattr(
        qa_app,
        "_delete_unreferenced_harmonization_stages",
        lambda _db: ["preexisting-zombie"],
    )

    result = _delete_previous_harmonization_pipeline_runs(db, "pipeline", "current-run")

    assert result == {
        "deleted_run_keys": ["old-run-a", "old-run-b"],
        "deleted_stage_keys": ["old-curated", "preexisting-zombie"],
        "deleted_superseded_stage_keys": ["old-curated"],
        "deleted_orphan_stage_keys": ["preexisting-zombie"],
    }
    assert db.aql.deleted_run_keys == ["old-run-a", "old-run-b"]
    assert deleted_artifacts == ["old-curated"]
    assert db.stages.deleted == ["old-curated"]


def test_delete_unreferenced_harmonization_stages_rechecks_before_deleting(monkeypatch):
    class FakeAql:
        def __init__(self):
            self.bind_vars = None

        def execute(self, _query, bind_vars=None, **_kwargs):
            self.bind_vars = bind_vars
            return ["old-zombie", "reused-during-scan"]

    class FakeStageCollection:
        def __init__(self):
            self.deleted = []

        def has(self, _key):
            return True

        def delete(self, key):
            self.deleted.append(key)

    class FakeDb:
        def __init__(self):
            self.aql = FakeAql()
            self.stages = FakeStageCollection()

        def has_collection(self, _name):
            return True

        def collection(self, _name):
            return self.stages

    db = FakeDb()
    deleted_artifacts = []
    monkeypatch.setattr(
        qa_app,
        "_stage_is_referenced_by_any_run",
        lambda _db, stage_key: stage_key == "reused-during-scan",
    )
    monkeypatch.setattr(
        qa_app,
        "_delete_harmonization_stage_artifacts",
        lambda _db, stage_key: deleted_artifacts.append(stage_key),
    )

    deleted = qa_app._delete_unreferenced_harmonization_stages(
        db,
        orphaned_before="2026-08-25T00:00:00+00:00",
    )

    assert deleted == ["old-zombie"]
    assert db.aql.bind_vars == {"orphaned_before": "2026-08-25T00:00:00+00:00"}
    assert deleted_artifacts == ["old-zombie"]
    assert db.stages.deleted == ["old-zombie"]


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


def test_free_anomer_rule_has_no_parameters():
    rule = next(
        rule
        for rule in qa_app._METABOLITE_HARMONIZATION_RULES
        if rule["id"] == "merge_free_anomeric_forms"
    )
    normalized_parameters = _normalize_metabolite_rule_parameters(
        ["merge_free_anomeric_forms"],
        {"merge_free_anomeric_forms": {"obsolete_parameter": "ignored"}},
    )

    assert "parameters" not in rule
    assert normalized_parameters == {}


def test_harmonization_evidence_display_identifies_and_labels_ifx_rule_edges():
    display = qa_app._metabolite_harmonization_evidence_display({
        "sources": ["IFX Harmonization Rule"],
        "rule_id": "merge_free_anomeric_forms",
        "details": [],
    })

    assert display == {
        "label": "IFX: Merge Free Anomeric Forms",
        "class_name": "metabolite-equivalence-edge ifx-harmonization-rule-edge",
        "rule_id": "merge_free_anomeric_forms",
        "rule_label": "Merge Free Anomeric Forms",
    }


def test_harmonization_evidence_display_falls_back_to_detail_rule_id():
    display = qa_app._metabolite_harmonization_evidence_display({
        "sources": ["IFX Harmonization Rule"],
        "details": [{"rule_id": "future_rule"}],
    })

    assert display["label"] == "IFX: future_rule"
    assert display["rule_id"] == "future_rule"
    assert "ifx-harmonization-rule-edge" in display["class_name"]


def test_harmonization_evidence_display_preserves_source_labels_for_regular_edges():
    display = qa_app._metabolite_harmonization_evidence_display({
        "sources": ["ChEBI", "HMDB"],
        "details": [],
    })

    assert display["label"] == "ChEBI, HMDB"
    assert display["class_name"] == "metabolite-equivalence-edge"


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


def test_build_carbohydrate_family_validation_flags_distinct_generated_families():
    validation = _build_harmonization_stage_carbohydrate_family_validation(
        groups=[
            ["CHEBI:1", "CHEBI:2", "CHEBI:3", "HMDB:1"],
            ["CHEBI:4", "CHEBI:5"],
        ],
        carbohydrate_structures_by_id={
            "CHEBI:1": {
                "id": "CHEBI:1",
                "name": "alpha-D-example",
                "structure": {
                    "comparable": True,
                    "family_inchi_key": "PREFIX-DFAMILYSA-N",
                    "source_inchi_key": "PREFIX-ALPHADSA-N",
                    "classification_reason": "free_anomer_normalized",
                },
                "error": None,
            },
            "CHEBI:2": {
                "id": "CHEBI:2",
                "name": "beta-D-example",
                "structure": {
                    "comparable": True,
                    "family_inchi_key": "PREFIX-DFAMILYSA-N",
                    "source_inchi_key": "PREFIX-BETADSA-N",
                    "classification_reason": "free_anomer_normalized",
                },
                "error": None,
            },
            "CHEBI:3": {
                "id": "CHEBI:3",
                "name": "L-example",
                "structure": {
                    "comparable": True,
                    "family_inchi_key": "PREFIX-LFAMILYSA-N",
                    "source_inchi_key": "PREFIX-ALPHALSA-N",
                    "classification_reason": "free_anomer_normalized",
                },
                "error": None,
            },
            "CHEBI:4": {
                "id": "CHEBI:4",
                "name": "generic",
                "structure": {
                    "comparable": False,
                    "family_inchi_key": None,
                    "classification_reason": "under_specified_stereochemistry",
                },
                "error": None,
            },
            "CHEBI:5": {
                "id": "CHEBI:5",
                "name": "specific",
                "structure": {
                    "comparable": True,
                    "family_inchi_key": "PREFIX-SPECIFICSA-N",
                    "source_inchi_key": "PREFIX-SPECIFICSA-N",
                    "classification_reason": "fully_specified_structure",
                },
                "error": None,
            },
        },
    )

    assert validation["warning_count"] == 1
    warning = validation["warnings"][0]
    assert warning["family_count"] == 2
    assert warning["classified_carbohydrate_count"] == 3
    assert warning["comparison_ids"].startswith("CHEBI:1 CHEBI:2 CHEBI:3")


def test_build_harmonization_stage_cart_flags_marks_only_active_edges_in_mw_warning_cliques():
    flags = _build_harmonization_stage_cart_flags(
        operations=[
            {
                "action": "remove_edge",
                "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": "HMDB:1",
                "end_id": "CHEBI:1",
            },
            {
                "action": "remove_edge",
                "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": "CHEBI:2",
                "end_id": "HMDB:2",
            },
            {
                "action": "remove_edge",
                "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": "CHEBI:3",
                "end_id": "HMDB:3",
            },
        ],
        member_rank_by_id={
            "CHEBI:1": 4,
            "HMDB:1": 4,
            "CHEBI:2": 7,
            "HMDB:2": 7,
            "CHEBI:3": 9,
            "HMDB:3": 10,
        },
        active_edge_pairs={
            ("CHEBI:1", "HMDB:1"),
            ("CHEBI:3", "HMDB:3"),
        },
        warning_ranks={4, 7, 9},
    )

    assert flags == [{
        "rank_by_size": 4,
        "queued_edge_count": 1,
        "edges": [{"action": "remove_edge", "start_id": "CHEBI:1", "end_id": "HMDB:1"}],
    }]


def test_build_harmonization_stage_cart_flags_marks_pending_denylist_retention():
    flags = _build_harmonization_stage_cart_flags(
        operations=[{
            "action": "retain_edge",
            "edge_type": "MetaboliteIdentifierMappingEdge",
            "start_id": "REFMET:1",
            "end_id": "CHEBI:1",
        }],
        member_rank_by_id={"CHEBI:1": 8, "REFMET:1": 8},
        active_edge_pairs=set(),
        warning_ranks={8},
        denylist_warning_pairs={("CHEBI:1", "REFMET:1")},
    )

    assert flags == [{
        "rank_by_size": 8,
        "queued_edge_count": 1,
        "edges": [{"action": "retain_edge", "start_id": "CHEBI:1", "end_id": "REFMET:1"}],
    }]


def test_harmonization_stage_cart_warning_ranks_include_all_validation_cliques():
    stage = {
        "validation": {
            "mw_spread": {"computed": True, "warnings": [{"rank_by_size": 4}]},
            "denylist_still_merged": {
                "computed": True,
                "rule_enabled": True,
                "warnings": [{"rank_by_size": 9}, {"rank_by_size": 4}],
            },
            "carbohydrate_family_conflicts": {
                "computed": True,
                "warnings": [{"rank_by_size": 11}],
            },
        },
    }

    assert _harmonization_stage_cart_warning_ranks(stage) == {4, 9, 11}


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


def test_denylist_review_selects_all_pairs_for_the_affected_clique():
    stage = {
        "_key": "stage-6",
        "validation": {
            "denylist_still_merged": {
                "computed": True,
                "warnings": [
                    {
                        "rank_by_size": 2,
                        "pairs": [
                            {"left_id": "HMDB:1", "right_id": "KEGG.COMPOUND:1"},
                            {"left_id": "HMDB:2", "right_id": "KEGG.COMPOUND:1"},
                        ],
                    },
                ],
            },
        },
    }

    review = _harmonization_denylist_review_from_stage(stage, 2)

    assert review == {
        "stage_key": "stage-6",
        "rank_by_size": 2,
        "pairs": [
            {"left_id": "HMDB:1", "right_id": "KEGG.COMPOUND:1"},
            {"left_id": "HMDB:2", "right_id": "KEGG.COMPOUND:1"},
        ],
    }
    assert _harmonization_denylist_review_from_stage(stage, 3) is None


def test_snapshot_union_resolves_members_by_public_id_instead_of_arango_key(monkeypatch):
    member_id = "CAS:62-31-7"
    stage_key = "stage-cas"
    clique_key = "stage-cas-000001-clique"

    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            bind_vars = bind_vars or {}
            if "FILTER node.id IN @ids" in query:
                return [member_id]
            if "FILTER e._from IN @clique_vertex_ids" in query:
                assert "DOCUMENT(e._to)" in query
                return [member_id]
            if "FOR e IN HarmonizedMetaboliteMemberEdge" in query:
                assert "FILTER e.member_id IN @member_ids" in query
                assert bind_vars["member_ids"] == [member_id]
                return [{
                    "member_id": member_id,
                    "member_label": member_id,
                    "member_prefix": "CAS",
                    "name_count": 0,
                    "synonym_count": 0,
                    "chem_prop_count": 0,
                    "snapshot_key": stage_key,
                    "snapshot_name": "CAS stage",
                    "snapshot_created_at": "2026-08-25T00:00:00+00:00",
                    "rules": [],
                    "clique_id": f"HarmonizedMetabolite:{clique_key}",
                    "clique_key": clique_key,
                    "clique_size": 1,
                    "clique_rank_by_size": 1,
                }]
            if "LET chemical_entity" in query:
                return [{
                    "id": member_id,
                    "label": member_id,
                    "names": [],
                    "prefix": "CAS",
                    "name_count": 0,
                    "synonym_count": 0,
                    "chem_prop_count": 0,
                    "raw_masses": [],
                    "chem_props": [],
                    "formulas": [],
                    "smiles": [],
                    "inchi_keys": [],
                    "chemical_entity": None,
                }]
            if "FOR e IN HarmonizationStageEvidenceEdge" in query:
                assert "FILTER e.start_id IN @member_ids" in query
                assert "FILTER e.end_id IN @member_ids" in query
                return []
            raise AssertionError(query)

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, _name):
            return True

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())
    monkeypatch.setattr(qa_app, "_load_metabolite_snapshot_memberships", lambda _ids: [{
        "member_id": member_id,
        "clique_key": clique_key,
        "snapshot_key": stage_key,
    }])
    monkeypatch.setattr(qa_app, "_list_harmonization_stages", lambda: [{
        "_key": stage_key,
        "name": "CAS stage",
        "created_at": "2026-08-25T00:00:00+00:00",
        "rules": [],
    }])
    monkeypatch.setattr(qa_app, "_list_harmonization_pipelines", lambda limit=100: [])

    result = qa_app._load_metabolite_snapshot_union([member_id], [stage_key])

    clique = result["snapshot_graphs"][0]["cliques"][0]
    assert clique["member_ids"] == [member_id]
    assert clique["elements"][0]["data"]["id"] == member_id


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
