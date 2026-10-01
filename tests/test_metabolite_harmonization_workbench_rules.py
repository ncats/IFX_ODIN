import asyncio
import hashlib
import json
import threading
import time
from pathlib import Path

import pytest
import yaml
from fastapi import HTTPException

import src.qa_browser.app as qa_app
from src.core.curations import (
    CHEBI_RECORD_PROPERTIES,
    METABOLITE_EQUIVALENCE_EDGES,
    METABOLITE_EXPECTED_CLIQUES,
    METABOLITE_MW_ADJUDICATIONS,
    METABOLITE_RECORD_SUPPRESSIONS,
    METABOLITE_RECORD_PROPERTIES,
    batch_key,
    manifest_key,
    payload_sha256,
    validate_operation,
)
from src.qa_browser.app import (
    _build_harmonization_pipeline_tree,
    _build_harmonization_stage_denylist_validation,
    _build_harmonization_stage_generic_structure_validation,
    _build_harmonization_stage_mw_validation,
    _build_harmonization_stage_cart_flags,
    _filter_identifier_support_for_rules,
    _harmonization_denylist_review_from_stage,
    _build_harmonization_denylist_pair_review,
    _harmonization_stage_cart_warning_ranks,
    _harmonization_jobs_by_pipeline_key,
    _load_metabolite_edge_removal_curations,
    _load_metabolite_identifier_mass_values,
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
    _effective_inchi_key_matches,
    _expected_clique_assertion_matrix,
    _expected_clique_assertion_edges,
    _newly_failing_expected_clique_assertions,
    _metabolite_mw_spread_percent,
    _prioritize_kegg_validation_samples,
    _with_validation_review_ids,
    _with_mw_adjudications,
    _annotate_harmonization_pipeline_run_progress,
    _normalize_metabolite_rule_parameters,
    _normalize_harmonization_denylist_pair,
    _default_metabolite_denylist_pair_stage_filter,
    _wikipathways_xref_only_ids_from_rows,
)


def test_mw_acceptance_becomes_warning_and_membership_change_makes_it_stale():
    warning = {
        "finding_id": "mw-1234567890abcdef12345678",
        "anchor_id": "CHEBI:1",
        "member_ids": ["CHEBI:1", "HMDB:1"],
        "evidence_fingerprint": "a" * 64,
        "evidence_fingerprint_version": "mw-finding-v1",
        "severity": "error",
    }
    validation = {"computed": True, "warnings": [warning]}
    decision = {
        "action": "accept_mw_discrepancy",
        "target": {"anchor_id": "CHEBI:1"},
        "observed_evidence_fingerprint": "a" * 64,
        "observed_member_ids": ["CHEBI:1", "HMDB:1"],
    }

    accepted = _with_mw_adjudications(validation, {"decisions": [decision]})
    assert accepted["warnings"][0]["review_status"] == "accepted"
    assert accepted["warnings"][0]["effective_severity"] == "warning"
    assert accepted["error_count"] == 0
    assert accepted["accepted_count"] == 1

    changed = {
        **warning,
        "member_ids": ["CAS:1", "CHEBI:1", "HMDB:1"],
        "finding_id": "mw-abcdef1234567890abcdef12",
        "evidence_fingerprint": "b" * 64,
    }
    stale = _with_mw_adjudications(
        {"computed": True, "warnings": [changed]}, {"decisions": [decision]}
    )
    assert stale["warnings"][0]["review_status"] == "stale"
    assert stale["error_count"] == 1
    assert stale["stale_acceptance_count"] == 1

    anchor_removed = {
        **changed,
        "member_ids": ["CAS:1", "HMDB:1"],
    }
    stale_without_anchor = _with_mw_adjudications(
        {"computed": True, "warnings": [anchor_removed]},
        {"decisions": [decision]},
    )
    assert stale_without_anchor["warnings"][0]["review_status"] == "stale"


def test_mw_acceptance_survives_legacy_fingerprint_migration_only_when_evidence_matches():
    finding = {
        "finding_id": "mw-current",
        "anchor_id": "CHEBI:1",
        "member_ids": ["CHEBI:1", "HMDB:1"],
        "evidence_fingerprint": "a" * 64,
        "evidence_fingerprint_version": "mw-finding-v1",
        "severity": "error",
        "mass_summaries": {"average": {"min": 100.0, "max": 150.0}},
        "channel_results": {"average": {"mass_cluster_count": 2}},
        "component_matches": [],
        "member_mass_examples": [
            {"member_id": "CHEBI:1", "average_masses": [100.0]},
            {"member_id": "HMDB:1", "average_masses": [150.0]},
        ],
    }
    validation = {
        "computed": True,
        "version": "mw-validator-v1",
        "threshold": 0.10,
        "finding_index": [{
            key: finding[key]
            for key in (
                "finding_id", "anchor_id", "member_ids",
                "evidence_fingerprint", "evidence_fingerprint_version",
                "severity",
            )
        }],
        "warnings": [finding],
    }
    legacy_fingerprint = qa_app._legacy_mw_finding_metadata(
        validation, finding
    )["evidence_fingerprint"]
    decision = {
        "action": "accept_mw_discrepancy",
        "target": {"anchor_id": "CHEBI:1"},
        "observed_member_ids": finding["member_ids"],
        "observed_evidence_fingerprint": legacy_fingerprint,
    }

    accepted = _with_mw_adjudications(validation, {"decisions": [decision]})

    assert accepted["warnings"][0]["review_status"] == "accepted"
    assert accepted["error_count"] == 0

    changed_validation = {
        **validation,
        "warnings": [{
            **finding,
            "evidence_fingerprint": "b" * 64,
            "member_mass_examples": [
                {"member_id": "CHEBI:1", "average_masses": [100.0]},
                {"member_id": "HMDB:1", "average_masses": [175.0]},
            ],
        }],
    }
    stale = _with_mw_adjudications(
        changed_validation, {"decisions": [decision]}
    )

    assert stale["warnings"][0]["review_status"] == "stale"
    assert stale["error_count"] == 1


def test_mw_legacy_compatibility_fingerprint_counts_acceptance_outside_display_sample():
    decision = {
        "action": "accept_mw_discrepancy",
        "target": {"anchor_id": "CHEBI:1"},
        "observed_member_ids": ["CHEBI:1", "HMDB:1"],
        "observed_evidence_fingerprint": "legacy-fingerprint",
    }
    result = _with_mw_adjudications(
        {
            "computed": True,
            "finding_index": [{
                "finding_id": "mw-current",
                "anchor_id": "CHEBI:1",
                "member_ids": ["CHEBI:1", "HMDB:1"],
                "evidence_fingerprint": "current-fingerprint",
                "evidence_fingerprint_version": "mw-finding-v1",
                "legacy_evidence_fingerprint": "legacy-fingerprint",
                "severity": "error",
            }],
            "warnings": [],
        },
        {"decisions": [decision]},
    )

    assert result["error_count"] == 0
    assert result["accepted_count"] == 1
    assert result["stale_acceptance_count"] == 0


def test_mw_adjudication_preserves_totals_beyond_display_sample():
    validation = {
        "computed": True,
        "error_count": 75,
        "review_warning_count": 12,
        "warnings": [{
            "finding_id": "mw-1234567890abcdef12345678",
            "anchor_id": "CHEBI:1",
            "member_ids": ["CHEBI:1", "HMDB:1"],
            "evidence_fingerprint": "a" * 64,
            "evidence_fingerprint_version": "mw-finding-v1",
            "severity": "error",
        }],
    }

    result = _with_mw_adjudications(validation, {"decisions": []})

    assert result["detected_error_count"] == 75
    assert result["error_count"] == 75
    assert result["review_warning_count"] == 12


def test_mw_adjudication_counts_accepted_finding_outside_display_sample():
    finding = {
        "finding_id": "mw-1234567890abcdef12345678",
        "anchor_id": "CHEBI:1",
        "member_ids": ["CHEBI:1", "HMDB:1"],
        "evidence_fingerprint": "a" * 64,
        "evidence_fingerprint_version": "mw-finding-v1",
        "severity": "error",
    }
    decision = {
        "action": "accept_mw_discrepancy",
        "target": {"anchor_id": "CHEBI:1"},
        "observed_evidence_fingerprint": "a" * 64,
        "observed_member_ids": ["CHEBI:1", "HMDB:1"],
    }

    result = _with_mw_adjudications(
        {
            "computed": True,
            "error_count": 1,
            "review_warning_count": 0,
            "finding_index": [finding],
            "warnings": [],
        },
        {"decisions": [decision]},
    )

    assert result["error_count"] == 0
    assert result["accepted_count"] == 1
    assert result["warnings"] == []


def test_mw_overview_counts_from_lightweight_finding_index():
    findings = [
        {
            "finding_id": "mw-accepted",
            "member_ids": ["CHEBI:1", "HMDB:1"],
            "evidence_fingerprint": "current-accepted",
            "legacy_evidence_fingerprint": "legacy-accepted",
            "severity": "error",
        },
        {
            "finding_id": "mw-stale",
            "member_ids": ["CHEBI:2", "HMDB:2"],
            "evidence_fingerprint": "current-stale",
            "severity": "error",
        },
        {
            "finding_id": "mw-warning",
            "member_ids": ["CHEBI:3", "HMDB:3"],
            "evidence_fingerprint": "current-warning",
            "severity": "warning",
        },
    ]
    decisions = [
        {
            "action": "accept_mw_discrepancy",
            "target": {"anchor_id": "CHEBI:1"},
            "observed_member_ids": findings[0]["member_ids"],
            "observed_evidence_fingerprint": "legacy-accepted",
        },
        {
            "action": "accept_mw_discrepancy",
            "target": {"anchor_id": "CHEBI:2"},
            "observed_member_ids": findings[1]["member_ids"],
            "observed_evidence_fingerprint": "old-stale",
        },
    ]

    result = qa_app._mw_validation_overview_from_doc(
        {"validation": {"mw_spread": {
            "computed": True,
            "finding_index": findings,
        }}},
        {"decisions": decisions},
    )

    assert result == {
        "computed": True,
        "warning_count": 3,
        "error_count": 1,
        "review_warning_count": 1,
        "accepted_count": 1,
        "stale_acceptance_count": 1,
    }


def test_metabolite_curation_cache_can_be_invalidated_and_forced(monkeypatch):
    qa_app._invalidate_harmonization_curation_cache()
    resolved = []

    def fake_resolve(**_kwargs):
        value = {"revision": len(resolved) + 1}
        resolved.append(value)
        return value

    monkeypatch.setattr(qa_app, "_resolve_metabolite_curations", fake_resolve)

    first = qa_app._load_metabolite_curations()
    assert qa_app._load_metabolite_curations() is first
    assert len(resolved) == 1

    forced = qa_app._load_metabolite_curations(force_refresh=True)
    assert forced["revision"] == 2
    assert len(resolved) == 2

    qa_app._invalidate_harmonization_curation_cache()
    assert qa_app._load_metabolite_curations()["revision"] == 3
    qa_app._invalidate_harmonization_curation_cache()


def test_reaccepting_after_anchor_removal_uses_current_anchor(monkeypatch):
    finding = {
        "finding_id": "mw-abcdef1234567890abcdef12",
        "anchor_id": "HMDB:1",
        "member_ids": ["CAS:1", "HMDB:1"],
        "evidence_fingerprint": "b" * 64,
        "evidence_fingerprint_version": "mw-finding-v1",
        "severity": "error",
    }
    monkeypatch.setattr(
        qa_app,
        "_get_harmonization_stage",
        lambda _stage_key: {"validation": {"mw_spread": {"warnings": [finding]}}},
    )
    operation = qa_app._metabolite_mw_adjudication_operation(
        "accept_mw_discrepancy",
        "stage-1",
        finding["finding_id"],
        "salt_or_counterion",
        [],
        "Reviewed again after membership changed.",
    )

    assert operation["target"]["anchor_id"] == "HMDB:1"
    assert "superseded_anchor_ids" not in operation
    assert operation["observed_evidence_snapshot"] == {
        "version": "mw-review-evidence-v1",
        "validator_version": None,
        "threshold": None,
        "mass_observations": [],
        "component_matches": [],
    }
    validate_operation(METABOLITE_MW_ADJUDICATIONS, operation)


def test_mw_findings_sort_by_review_priority_then_spread_size_and_id():
    warnings = [
        {
            "finding_id": "accepted",
            "anchor_id": "CHEBI:4",
            "member_ids": ["CHEBI:4"],
            "evidence_fingerprint": "d" * 64,
            "severity": "error",
            "spread_percent": 90,
            "size": 9,
        },
        {
            "finding_id": "warning",
            "anchor_id": "CHEBI:3",
            "member_ids": ["CHEBI:3"],
            "evidence_fingerprint": "c" * 64,
            "severity": "warning",
            "spread_percent": 80,
            "size": 8,
        },
        {
            "finding_id": "stale",
            "anchor_id": "CHEBI:2",
            "member_ids": ["CHEBI:2"],
            "evidence_fingerprint": "b" * 64,
            "severity": "error",
            "spread_percent": 70,
            "size": 7,
        },
        {
            "finding_id": "error-low",
            "anchor_id": "CHEBI:1",
            "member_ids": ["CHEBI:1"],
            "evidence_fingerprint": "a" * 64,
            "severity": "error",
            "spread_percent": 20,
            "size": 2,
        },
        {
            "finding_id": "error-high",
            "anchor_id": "CHEBI:5",
            "member_ids": ["CHEBI:5"],
            "evidence_fingerprint": "e" * 64,
            "severity": "error",
            "spread_percent": 30,
            "size": 1,
        },
    ]
    decisions = [
        {
            "action": "accept_mw_discrepancy",
            "target": {"anchor_id": "CHEBI:4"},
            "observed_member_ids": ["CHEBI:4"],
            "observed_evidence_fingerprint": "d" * 64,
        },
        {
            "action": "accept_mw_discrepancy",
            "target": {"anchor_id": "CHEBI:2"},
            "observed_member_ids": ["CHEBI:2"],
            "observed_evidence_fingerprint": "f" * 64,
        },
    ]

    result = _with_mw_adjudications(
        {"computed": True, "warnings": warnings},
        {"decisions": decisions},
    )

    assert [finding["finding_id"] for finding in result["warnings"]] == [
        "error-high", "error-low", "stale", "warning", "accepted",
    ]


def test_mw_history_preserves_stale_sibling_after_split_branch_is_accepted():
    original = {
        "action": "accept_mw_discrepancy",
        "target": {"anchor_id": "CHEBI:A"},
        "observed_member_ids": ["CHEBI:A", "HMDB:B"],
        "observed_evidence_fingerprint": "a" * 64,
    }
    accepted_branch = {
        "action": "accept_mw_discrepancy",
        "target": {"anchor_id": "CHEBI:A"},
        "observed_member_ids": ["CHEBI:A", "HMDB:X"],
        "observed_evidence_fingerprint": "b" * 64,
    }
    validation = {
        "computed": True,
        "finding_index": [
            {
                "member_ids": ["CHEBI:A", "HMDB:X"],
                "evidence_fingerprint": "b" * 64,
                "severity": "error",
            },
            {
                "member_ids": ["HMDB:B", "CAS:Y"],
                "evidence_fingerprint": "c" * 64,
                "severity": "error",
            },
        ],
        "warnings": [],
    }

    result = _with_mw_adjudications(
        validation,
        {"decisions": [accepted_branch, original]},
    )

    assert result["finding_index"][0]["review_status"] == "accepted"
    assert result["finding_index"][1]["review_status"] == "stale"


class _FakeCurationStorage:
    bucket = "test-curations"

    def __init__(self, objects):
        self.objects = objects

    def list_keys(self, prefix):
        return [key for key in self.objects if key.startswith(prefix)]

    def read_text(self, key):
        return self.objects[key]


def _typed_curation_objects(curation_type, batches):
    objects = {}
    manifest_batches = []
    for batch in batches:
        key = batch_key(curation_type, batch["curation_batch_id"])
        objects[key] = json.dumps(batch)
        manifest_batches.append({
            "batch_id": batch["curation_batch_id"],
            "object_key": key,
            "sha256": payload_sha256(batch),
            "published_at": batch.get("published_at"),
        })
    objects[manifest_key(curation_type)] = json.dumps({
        "format_version": 2,
        "curation_type": curation_type,
        "revision": len(batches),
        "batches": manifest_batches,
    })
    return objects


def test_harmonization_rules_have_expected_workbench_groups():
    groups_by_rule = {
        rule["id"]: rule["group"]
        for rule in qa_app._METABOLITE_HARMONIZATION_RULES
    }

    assert groups_by_rule["ignore_generic_structure_mismatch"] == "Pruning"
    assert groups_by_rule["merge_inchikey_by_mw_cutoff"] == "Merging"
    assert "merge_derived_inchikey_by_mw_cutoff" not in groups_by_rule
    assert groups_by_rule["merge_free_anomeric_forms"] == "Chemistry-based merging"
    assert groups_by_rule["apply_curations"] == "Curation"
    assert groups_by_rule["force_expected_clique_assertions"] == "Cleanup"
    apply_rule = next(
        rule for rule in qa_app._METABOLITE_HARMONIZATION_RULES
        if rule["id"] == "apply_curations"
    )
    assert METABOLITE_RECORD_PROPERTIES in apply_rule["parameters"][0]["default"]


def test_ramp_evidence_graph_does_not_materialize_curations_during_etl():
    config = yaml.safe_load(
        Path("src/use_cases/ramp/ramp.yaml").read_text(encoding="utf-8")
    )

    assert "curations" not in config


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
    assert shared["is_missing"] is False
    assert baseline["is_missing"] is True
    assert [child["stage_key"] for child in shared["children"]] == ["branch-a-2", "branch-b-2"]
    assert [
        child["terminals"][0]["pipeline_name"] for child in shared["children"]
    ] == ["Anomer merge with cleanup A", "Anomer merge with cleanup B"]


def test_graph_fingerprint_uses_content_checksums_not_only_counts():
    class FakeAql:
        def execute(self, _query, **_kwargs):
            return [{
                "metabolite_identifier_count": 10,
                "equivalence_edge_count": 9,
            }]

    class FakeCollection:
        def __init__(self, revision):
            self._revision = revision

        def revision(self):
            return self._revision

    class FakeDb:
        aql = FakeAql()

        def __init__(self, node_checksum, edge_checksum):
            self.checksums = {
                "MetaboliteIdentifier": node_checksum,
                "MetaboliteIdentifierMappingEdge": edge_checksum,
            }

        def collection(self, name):
            return FakeCollection(self.checksums[name])

        def has_collection(self, _name):
            return _name in self.checksums

    first = qa_app._harmonization_graph_fingerprint(FakeDb("nodes-a", "edges-a"))
    second = qa_app._harmonization_graph_fingerprint(FakeDb("nodes-b", "edges-a"))

    assert first["metabolite_identifier_count"] == second["metabolite_identifier_count"]
    assert (
        first["source_collection_revisions"]["MetaboliteIdentifier"]
        != second["source_collection_revisions"]["MetaboliteIdentifier"]
    )
    assert qa_app._harmonization_stage_key([], {}, first) != qa_app._harmonization_stage_key([], {}, second)


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


def test_inchikey_iterator_uses_derived_only_when_reported_is_unavailable():
    class FakeAql:
        def execute(self, query, **_kwargs):
            assert '"derived_inchi_key"' in query
            return [{
                "id": "CHEBI:1",
                "chem_props": [{
                    "derived_inchi_key": "ABCDEFGHIJKLMN-ABCDEFGHIJ-N",
                }],
                "masses": ["100"],
            }]

    class FakeDb:
        aql = FakeAql()

    assert list(qa_app._iter_metabolite_identifier_inchi_key_matches(
        FakeDb(),
        "mw_cutoff",
        500,
    )) == [
        ("CHEBI:1", ["ABCDEFGHIJKLMN-ABCDEFGHIJ"], "duplex", 100.0, True),
    ]


def test_effective_inchikey_prefers_reported_key_over_conflicting_derived_key():
    matches, used_fallback = _effective_inchi_key_matches(
        [{
            "inchi_key": "AAAAAAAAAAAAAA-BBBBBBBBBB-N",
            "derived_inchi_key": "CCCCCCCCCCCCCC-DDDDDDDDDD-N",
        }],
        "duplex",
    )

    assert matches == ["AAAAAAAAAAAAAA-BBBBBBBBBB"]
    assert used_fallback is False


def test_effective_inchikey_fallback_is_evaluated_per_chemistry_record():
    matches, used_fallback = _effective_inchi_key_matches(
        [
            {"inchi_key": "AAAAAAAAAAAAAA-BBBBBBBBBB-N"},
            {"derived_inchi_key": "CCCCCCCCCCCCCC-DDDDDDDDDD-N"},
        ],
        "duplex",
    )

    assert matches == [
        "AAAAAAAAAAAAAA-BBBBBBBBBB",
        "CCCCCCCCCCCCCC-DDDDDDDDDD",
    ]
    assert used_fallback is True


def test_inchikey_iterator_uses_stage_record_overlay():
    class FakeAql:
        def execute(self, _query, bind_vars=None, **_kwargs):
            assert bind_vars == {"overlay_ids": ["REFMET:RM1"]}
            return [{
                "id": "REFMET:RM1",
                "chem_props": [{
                    "inchi_key": "AAAAAAAAAAAAAA-BBBBBBBBBB-N",
                }],
                "masses": ["100"],
            }]

    class FakeDb:
        aql = FakeAql()

    rows = list(qa_app._iter_metabolite_identifier_inchi_key_matches(
        FakeDb(),
        "duplex",
        record_overlays={
            "REFMET:RM1": {
                "chem_props": [{
                    "inchi_key": "CCCCCCCCCCCCCC-DDDDDDDDDD-N",
                    "mw": "200",
                }],
            },
        },
    ))

    assert rows == [(
        "REFMET:RM1",
        ["CCCCCCCCCCCCCC-DDDDDDDDDD"],
        "duplex",
        200.0,
        False,
    )]


def test_three_selectable_inchikey_rules_share_effective_key_policy(monkeypatch):
    calls = []

    def fake_matches(_db, mode, mw_cutoff=None, key_kind="effective"):
        calls.append((mode, mw_cutoff, key_kind))
        return iter(())

    monkeypatch.setattr(qa_app, "_iter_metabolite_identifier_inchi_key_matches", fake_matches)

    qa_app._build_harmonized_groups(
        object(),
        {"CHEBI:1"},
        [],
        [
            "merge_shared_inchikey_prefix",
            "merge_shared_inchikey_duplex",
            "merge_inchikey_by_mw_cutoff",
        ],
        {"merge_inchikey_by_mw_cutoff": {"mw_cutoff": 600}},
    )

    assert calls == [
        ("prefix", None, "effective"),
        ("duplex", None, "effective"),
        ("mw_cutoff", 600, "effective"),
    ]


def test_record_property_overlay_only_affects_merge_rules_after_apply_curations(monkeypatch):
    calls = []

    def fake_matches(_db, mode, mw_cutoff=None, key_kind="effective", record_overlays=None):
        calls.append((mode, record_overlays))
        return iter(())

    monkeypatch.setattr(
        qa_app, "_iter_metabolite_identifier_inchi_key_matches", fake_matches
    )
    overlays = {"CHEBI:1": {"id": "CHEBI:1", "chem_props": []}}

    qa_app._build_harmonized_groups(
        object(),
        {"CHEBI:1"},
        [],
        ["merge_shared_inchikey_duplex", "apply_curations"],
        {},
        overlays,
    )
    qa_app._build_harmonized_groups(
        object(),
        {"CHEBI:1"},
        [],
        ["apply_curations", "merge_shared_inchikey_duplex"],
        {},
        overlays,
    )

    assert calls == [("duplex", None), ("duplex", overlays)]


def test_mw_validation_loader_uses_stage_record_overlay():
    class FakeAql:
        def execute(self, query, **_kwargs):
            assert "FOR d IN MetaboliteIdentifier" in query
            return [{
                "id": "REFMET:RM1",
                "chemistry": [{
                    "source": "RefMet",
                    "source_id": "RM1",
                    "average": ["999", "150.13"],
                    "monoisotopic": ["999", "150.0528"],
                    "components": [],
                }],
            }]

    class FakeDb:
        aql = FakeAql()

        @staticmethod
        def has_collection(name):
            return name == "MetaboliteIdentifier"

    overlays = {
        "REFMET:RM1": {
            "id": "REFMET:RM1",
            "chem_props": [{
                "source": "RefMet",
                "source_id": "RM1",
                "mw": "150.13",
                "calculated_mw": "150.13",
                "monoisotopic_mass": "150.0528",
                "calculated_monoisotopic_mass": "150.0528",
                "structure_components": [],
            }],
        },
    }

    masses = _load_metabolite_identifier_mass_values(FakeDb(), overlays)

    assert masses["REFMET:RM1"]["whole"] == {
        "average": [150.13],
        "monoisotopic": [150.0528],
    }


def test_overlay_mass_profiles_are_loaded_once_per_pipeline_run(monkeypatch):
    calls = []
    expected = {"REFMET:RM1": {"whole": {"average": [150.13]}}}

    def load(_db, overlays, chemical_entity_overlays=None):
        calls.append((overlays, chemical_entity_overlays))
        return expected

    monkeypatch.setattr(qa_app, "_load_metabolite_identifier_mass_values", load)
    provider = qa_app._cached_record_overlay_mass_values_provider(object())
    overlays = {"REFMET:RM1": {"id": "REFMET:RM1"}}

    assert provider(overlays) is expected
    assert provider(overlays) is expected
    assert calls == [(overlays, None)]


def test_chebi_record_overlay_recalculates_and_replaces_mass_evidence(monkeypatch):
    operation = {
        "action": "set_properties",
        "target": {
            "kind": "node",
            "curation_set": "chebi",
            "model_type": "ChemicalEntity",
            "id": "CHEBI:137735",
        },
        "decisions": [
            {
                "path": ["smiles"],
                "mode": "set",
                "value": "corrected-isotopic-smiles",
                "observed_value": "bad-smiles",
                "observed_exists": True,
            },
            {
                "path": ["mass"],
                "mode": "set",
                "value": "284.31",
                "observed_value": "336.40",
                "observed_exists": True,
            },
            {
                "path": ["monoisotopic_mass"],
                "mode": "set",
                "value": "284.265337",
                "observed_value": "336.30283",
                "observed_exists": True,
            },
        ],
        "note": "Correct deuterium atoms misread as methyl groups.",
    }
    batch = {
        "format_version": 2,
        "curation_batch_id": "chebi-deuterium-fix",
        "curation_type": CHEBI_RECORD_PROPERTIES,
        "published_at": "2026-09-30T12:00:00Z",
        "operations": [operation],
    }
    state = qa_app._load_metabolite_curations(_FakeCurationStorage(
        _typed_curation_objects(CHEBI_RECORD_PROPERTIES, [batch])
    ))
    source_document = {
        "id": "CHEBI:137735",
        "smiles": "bad-smiles",
        "mass": "336.40",
        "monoisotopic_mass": "336.30283",
        "calculated_mw": "336.40",
        "calculated_monoisotopic_mass": "336.30283",
        "structure_components": [],
    }

    class OverlayAql:
        def execute(self, _query, bind_vars=None, **_kwargs):
            assert bind_vars == {
                "@collection": "ChemicalEntity",
                "target_ids": ["CHEBI:137735"],
            }
            return [source_document]

    class OverlayDb:
        aql = OverlayAql()

    monkeypatch.setattr(
        qa_app,
        "_get_collection_schema_entry",
        lambda _db, _name: {"fields": {
            "smiles": "str",
            "mass": "str",
            "monoisotopic_mass": "str",
        }},
    )
    monkeypatch.setattr(
        qa_app,
        "calculate_smiles_chemistry",
        lambda smiles, input_field: {
            "calculated_mw": "284.31",
            "calculated_monoisotopic_mass": "284.265337",
            "structure_components": [],
            "structure_calculation_input_field": input_field,
        },
    )

    overlays, reports = qa_app._load_record_overlays(
        OverlayDb(),
        state["chebi_record_property_snapshot"],
        curation_set="chebi",
        model_type="ChemicalEntity",
    )

    assert len(reports) == 3
    assert overlays["CHEBI:137735"]["calculated_monoisotopic_mass"] == "284.265337"

    class MassAql:
        def execute(self, _query, **_kwargs):
            return [{
                "id": "CHEBI:137735",
                "chemistry": {
                    "source": "ChEBI",
                    "source_id": "CHEBI:137735",
                    "molecular_formula": "C22H40O2",
                    "average": ["336.40", "336.40"],
                    "monoisotopic": ["336.30283", "336.30283"],
                    "components": [],
                },
            }]

    class MassDb:
        aql = MassAql()

        def has_collection(self, name):
            return name == "ChemicalEntity"

    masses = _load_metabolite_identifier_mass_values(
        MassDb(), chemical_entity_overlays=overlays
    )

    assert masses["CHEBI:137735"]["whole"] == {
        "average": [284.31],
        "monoisotopic": [284.265337],
    }


def test_chebi_overlay_can_add_first_mass_evidence_to_empty_source_record():
    overlay = {
        "id": "CHEBI:1",
        "formula": "H2O",
        "mass": None,
        "monoisotopic_mass": None,
        "calculated_mw": "18.015",
        "calculated_monoisotopic_mass": "18.010565",
        "structure_components": [],
    }

    class EmptyMassAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            assert "OR d.id IN @overlay_ids" in query
            assert bind_vars == {"overlay_ids": ["CHEBI:1"]}
            return [{
                "id": "CHEBI:1",
                "chemistry": {
                    "source": "ChEBI",
                    "source_id": "CHEBI:1",
                    "molecular_formula": None,
                    "average": [None, None],
                    "monoisotopic": [None, None],
                    "components": [],
                },
            }]

    class EmptyMassDb:
        aql = EmptyMassAql()

        def has_collection(self, name):
            return name == "ChemicalEntity"

    masses = _load_metabolite_identifier_mass_values(
        EmptyMassDb(), chemical_entity_overlays={"CHEBI:1": overlay}
    )

    assert masses["CHEBI:1"]["whole"] == {
        "average": [18.015],
        "monoisotopic": [18.010565],
    }


def test_retired_derived_cutoff_rule_remains_executable_for_historical_pipelines(monkeypatch):
    def fake_matches(_db, mode, mw_cutoff=None, key_kind="effective"):
        assert (mode, mw_cutoff, key_kind) == ("mw_cutoff", 500, "derived")
        yield "CHEBI:1", ["AAAAAAAAAAAAAA-BBBBBBBBBB"], "duplex", 100.0, False
        yield "HMDB:1", ["AAAAAAAAAAAAAA-BBBBBBBBBB"], "duplex", 100.0, False

    monkeypatch.setattr(qa_app, "_iter_metabolite_identifier_inchi_key_matches", fake_matches)

    groups, summary = qa_app._build_harmonized_groups(
        object(),
        {"CHEBI:1", "HMDB:1"},
        [],
        ["merge_derived_inchikey_by_mw_cutoff"],
        {"merge_derived_inchikey_by_mw_cutoff": {"mw_cutoff": 500}},
    )

    assert groups == [["CHEBI:1", "HMDB:1"]]
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
    batch = {
            "format_version": 2,
            "curation_batch_id": "batch-a",
            "curation_type": METABOLITE_EQUIVALENCE_EDGES,
            "operations": [{
                "action": "remove_edge",
                "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": "CHEBI:1",
                "end_id": "HMDB:1",
                "symmetric": True,
            }],
        }
    storage = _FakeCurationStorage(_typed_curation_objects(
        METABOLITE_EQUIVALENCE_EDGES, [batch]
    ))

    loaded = _load_metabolite_edge_removal_curations(storage)

    assert loaded["pairs"] == {("CHEBI:1", "HMDB:1")}
    assert loaded["batch_ids"] == ["batch-a"]
    assert len(loaded["fingerprint"]) == 64
    assert loaded["prefix"] == "s3://test-curations/curations/v2/"


def test_record_property_curations_project_stage_values_without_writing_graph(monkeypatch):
    operation = {
        "action": "set_properties",
        "target": {
            "kind": "node",
            "curation_set": "metabolite_harmonization",
            "model_type": "MetaboliteIdentifier",
            "id": "KEGG.COMPOUND:C00626",
        },
        "decisions": [{
            "path": ["is_generic_structure"],
            "mode": "set",
            "value": True,
            "observed_value": False,
            "observed_exists": True,
        }],
        "note": "KEGG represents a generic structure.",
    }
    batch = {
        "format_version": 2,
        "curation_batch_id": "generic-kegg",
        "curation_type": METABOLITE_RECORD_PROPERTIES,
        "published_at": "2026-09-29T12:00:00Z",
        "operations": [operation],
    }
    state = qa_app._load_metabolite_curations(_FakeCurationStorage(
        _typed_curation_objects(METABOLITE_RECORD_PROPERTIES, [batch])
    ))
    other_operation = {
        **operation,
        "target": {
            **operation["target"],
            "curation_set": "pharos",
            "id": "UniProtKB:P12345",
        },
    }
    other_batch = {
        **batch,
        "curation_batch_id": "unrelated-pharos-correction",
        "operations": [other_operation],
    }
    state_with_unrelated = qa_app._load_metabolite_curations(
        _FakeCurationStorage(
            _typed_curation_objects(METABOLITE_RECORD_PROPERTIES, [batch, other_batch])
        )
    )
    evidence = {
        "id": "KEGG.COMPOUND:C00626",
        "is_generic_structure": False,
        "sources": ["KEGG"],
    }

    class FakeAql:
        def execute(self, _query, bind_vars=None, **_kwargs):
            assert bind_vars == {
                "@collection": "MetaboliteIdentifier",
                "target_ids": ["KEGG.COMPOUND:C00626"],
            }
            return [evidence]

    class FakeDb:
        aql = FakeAql()

    monkeypatch.setattr(
        qa_app,
        "_get_collection_schema_entry",
        lambda _db, _name: {"fields": {"is_generic_structure": "bool"}},
    )

    overlays, reports = qa_app._load_metabolite_identifier_record_overlays(
        FakeDb(), state["record_property_snapshot"]
    )

    assert evidence["is_generic_structure"] is False
    assert "_curation_original" not in evidence
    assert overlays["KEGG.COMPOUND:C00626"]["is_generic_structure"] is True
    assert reports[0]["status"] == "applied"
    assert state["snapshots"][METABOLITE_RECORD_PROPERTIES]["manifest_revision"] == 1
    assert (
        state_with_unrelated["record_property_fingerprint"]
        == state["record_property_fingerprint"]
    )
    assert state_with_unrelated["record_property_batch_ids"] == ["generic-kegg"]


def test_assertion_only_batch_does_not_change_edge_curation_fingerprint():
    assertion = _metabolite_expected_clique_assertion_operation(
        ["CHEBI:15903", "CHEBI:17925"],
        "Glucose anomers",
    )
    batch = {
            "format_version": 2,
            "curation_batch_id": "assertions",
            "curation_type": METABOLITE_EXPECTED_CLIQUES,
            "published_at": "2026-08-25T12:00:00Z",
            "created_by": {"id": "keith", "name": "Keith"},
            "operations": [assertion],
        }
    storage = _FakeCurationStorage(_typed_curation_objects(
        METABOLITE_EXPECTED_CLIQUES, [batch]
    ))

    loaded = _load_metabolite_edge_removal_curations(storage)

    assert len(loaded["fingerprint"]) == 64
    assert loaded["batch_ids"] == []
    assert loaded["assertion_batch_ids"] == ["assertions"]
    assert loaded["assertions"][0]["assertion_id"] == assertion["assertion_id"]


def test_load_metabolite_curations_resolves_record_suppression_and_restore():
    target = {
        "kind": "node",
        "model_type": "MetaboliteIdentifier",
        "id": "REFMET:RM0233954",
    }
    batches = [
        {
            "format_version": 2,
            "curation_batch_id": "suppress",
            "curation_type": METABOLITE_RECORD_SUPPRESSIONS,
            "published_at": "2026-09-29T12:00:00Z",
            "operations": [{
                "action": "suppress_record",
                "target": target,
                "note": "Internally inconsistent identity fields.",
            }],
        },
        {
            "format_version": 2,
            "curation_batch_id": "restore",
            "curation_type": METABOLITE_RECORD_SUPPRESSIONS,
            "published_at": "2026-09-29T13:00:00Z",
            "operations": [{
                "action": "restore_record",
                "target": target,
            }],
        },
    ]
    storage = _FakeCurationStorage(_typed_curation_objects(
        METABOLITE_RECORD_SUPPRESSIONS, batches
    ))

    loaded = _load_metabolite_edge_removal_curations(storage)

    assert loaded["suppressed_identifier_ids"] == set()
    assert loaded["record_states"] == {"REFMET:RM0233954": "restore_record"}
    assert loaded["suppression_batch_ids"] == ["suppress", "restore"]
    assert loaded["record_decisions"]["REFMET:RM0233954"]["batch_id"] == "restore"


def test_pipeline_curation_fingerprint_only_uses_selected_streams():
    curation_state = {
        "edge_fingerprint": "edges-v1",
        "annotation_fingerprint": "annotations-v2",
        "suppression_fingerprint": "suppressions-v3",
        "record_property_fingerprint": "properties-v4",
    }
    edge_only = {
        "rule_ids": ["apply_curations"],
        "rule_parameters": {"apply_curations": {
            "curation_types": [METABOLITE_EQUIVALENCE_EDGES],
        }},
    }
    edge_and_suppression = {
        "rule_ids": ["apply_curations"],
        "rule_parameters": {"apply_curations": {
            "curation_types": [
                METABOLITE_EQUIVALENCE_EDGES,
                METABOLITE_RECORD_SUPPRESSIONS,
            ],
        }},
    }
    property_only = {
        "rule_ids": ["apply_curations"],
        "rule_parameters": {"apply_curations": {
            "curation_types": [METABOLITE_RECORD_PROPERTIES],
        }},
    }

    edge_fingerprint = qa_app._pipeline_curation_fingerprint(edge_only, curation_state)
    combined_fingerprint = qa_app._pipeline_curation_fingerprint(
        edge_and_suppression, curation_state
    )
    property_fingerprint = qa_app._pipeline_curation_fingerprint(
        property_only, curation_state
    )

    assert edge_fingerprint == payload_sha256({
        METABOLITE_EQUIVALENCE_EDGES: "edges-v1",
    })
    assert combined_fingerprint == payload_sha256({
        METABOLITE_EQUIVALENCE_EDGES: "edges-v1",
        METABOLITE_RECORD_SUPPRESSIONS: "suppressions-v3",
    })
    assert edge_fingerprint != combined_fingerprint
    assert property_fingerprint == payload_sha256({
        METABOLITE_RECORD_PROPERTIES: "properties-v4",
    })


def test_record_suppression_removes_identifier_before_stage_rules(monkeypatch):
    captured = {}

    class FakeStageCollection:
        def get(self, _key):
            return None

    class FakeDb:
        def collection(self, name):
            assert name == qa_app._HARMONIZATION_STAGE_COLLECTION
            return FakeStageCollection()

    support = {
        "CHEBI:1": {"CHEBI"},
        "REFMET:RM0233954": {"REFMET"},
    }
    monkeypatch.setattr(
        qa_app, "_load_metabolite_identifier_source_support", lambda _db: support
    )
    monkeypatch.setattr(
        qa_app,
        "_filter_identifier_support_for_rules",
        lambda support_by_id, *_args: support_by_id,
    )

    def active_edges(_db, active_ids, *_args):
        captured["rule_active_ids"] = set(active_ids)
        return [], {"mapping_edge_count": 0}

    monkeypatch.setattr(
        qa_app, "_active_metabolite_identifier_mapping_edges_for_rules", active_edges
    )
    monkeypatch.setattr(
        qa_app,
        "_build_harmonized_groups",
        lambda _db, active_ids, *_args: (
            captured.update({"group_active_ids": set(active_ids)}) or [],
            {},
        ),
    )
    monkeypatch.setattr(
        qa_app,
        "_materialize_harmonization_stage",
        lambda _db, _key, _doc, active_ids, *_args, **_kwargs: captured.update(
            {"materialized_active_ids": set(active_ids)}
        ),
    )
    curation_state = {
        "pairs": set(),
        "batch_ids": [],
        "edge_fingerprint": "edges",
        "annotation_overrides": {},
        "annotation_batch_ids": [],
        "annotation_fingerprint": "annotations",
        "assertions": [],
        "assertion_batch_ids": [],
        "assertion_fingerprint": "assertions",
        "suppressed_identifier_ids": {"REFMET:RM0233954"},
        "record_states": {"REFMET:RM0233954": "suppress_record"},
        "record_decisions": {},
        "suppression_batch_ids": ["quarantine-1"],
        "suppression_fingerprint": "suppressions",
        "prefix": "s3://test-curations/curations/v2/",
    }

    stage = qa_app._ensure_harmonization_stage(
        FakeDb(),
        ["apply_curations"],
        {"apply_curations": {
            "curation_types": [METABOLITE_RECORD_SUPPRESSIONS],
        }},
        {"database": "metabolite_harmonization"},
        "After quarantine",
        1,
        mass_values_provider=lambda: {},
        generic_structure_classifications_provider=lambda: {},
        curation_state=curation_state,
    )

    assert captured == {
        "rule_active_ids": {"CHEBI:1"},
        "group_active_ids": {"CHEBI:1"},
        "materialized_active_ids": {"CHEBI:1"},
    }
    assert stage["summary"]["suppressed_identifier_count"] == 1
    assert stage["summary"]["active_identifier_count"] == 1
    assert stage["suppression_curation_batch_ids"] == ["quarantine-1"]


def test_published_curation_index_lists_denied_pairs_and_quarantined_records(monkeypatch):
    pair = ("CHEBI:1", "HMDB:1")
    captured = {}
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_edge_removal_curations",
        lambda: {
            "pair_states": {pair: "remove_edge"},
            "pair_decisions": {pair: {"note": "Different structures"}},
            "suppressed_identifier_ids": {"REFMET:RM0233954"},
            "record_decisions": {
                "REFMET:RM0233954": {"note": "Internally inconsistent fields"},
            },
        },
    )
    monkeypatch.setattr(
        qa_app.templates,
        "TemplateResponse",
        lambda request, name, context: captured.update({
            "template": name,
            "context": context,
        }) or context,
    )

    context = qa_app.ramp_id_qa_curations(request=object())

    assert captured["template"] == "ramp_id_curations.html"
    assert context["index"]["denylist_pairs"] == [{
        "left_id": "CHEBI:1",
        "right_id": "HMDB:1",
        "decision": {"note": "Different structures"},
        "review_query": "denylist_pair=CHEBI%3A1%7CHMDB%3A1",
    }]
    assert context["index"]["suppressed_records"] == [{
        "identifier": "REFMET:RM0233954",
        "decision": {"note": "Internally inconsistent fields"},
        "review_query": "id=REFMET%3ARM0233954",
    }]


def test_published_assertion_can_be_retired_chronologically():
    assertion = _metabolite_expected_clique_assertion_operation(
        ["CHEBI:15903", "CHEBI:17925"],
        "Glucose anomers",
    )
    batches = [{
            "format_version": 2,
            "curation_batch_id": "assert",
            "curation_type": METABOLITE_EXPECTED_CLIQUES,
            "published_at": "2026-08-25T12:00:00Z",
            "operations": [assertion],
        }, {
            "format_version": 2,
            "curation_batch_id": "retire",
            "curation_type": METABOLITE_EXPECTED_CLIQUES,
            "published_at": "2026-08-25T13:00:00Z",
            "operations": [{"action": "retire_assertion", "assertion_id": assertion["assertion_id"]}],
        }]
    storage = _FakeCurationStorage(_typed_curation_objects(
        METABOLITE_EXPECTED_CLIQUES, batches
    ))

    loaded = _load_metabolite_edge_removal_curations(storage)

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
    def batch(batch_id, published_at, operations):
        return {
            "format_version": 2,
            "curation_batch_id": batch_id,
            "curation_type": METABOLITE_EQUIVALENCE_EDGES,
            "published_at": published_at,
            "operations": operations,
        }

    def decision(action, left, right):
        return {
            "action": action,
            "edge_type": "MetaboliteIdentifierMappingEdge",
            "start_id": left,
            "end_id": right,
            "symmetric": True,
        }

    batches = [
        batch("base", "2026-08-24T13:00:00Z", [
            decision("remove_edge", "CHEBI:1", "HMDB:1"),
            decision("remove_edge", "CHEBI:2", "HMDB:2"),
        ]),
        batch("retain", "2026-08-24T14:00:00Z", [
            decision("retain_edge", "CHEBI:1", "HMDB:1"),
            decision("retain_edge", "CHEBI:2", "HMDB:2"),
        ]),
        batch("remove-again", "2026-08-24T15:00:00Z", [
            decision("remove_edge", "HMDB:2", "CHEBI:2"),
        ]),
    ]
    storage = _FakeCurationStorage(_typed_curation_objects(
        METABOLITE_EQUIVALENCE_EDGES, batches
    ))

    loaded = _load_metabolite_edge_removal_curations(storage)

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


def test_missing_completed_stage_requires_pipeline_repair():
    pipelines = [{
        "_key": "broken",
        "engine_version": qa_app._HARMONIZATION_ENGINE_VERSION,
        "rule_ids": ["apply_curations"],
        "rule_parameters": {"apply_curations": {
            "curation_types": [METABOLITE_EQUIVALENCE_EDGES],
        }},
        "runs": [{
            "_key": "complete-run",
            "status": "complete",
            "engine_version": qa_app._HARMONIZATION_ENGINE_VERSION,
            "curation_fingerprint": payload_sha256({
                METABOLITE_EQUIVALENCE_EDGES: "edges-v1",
            }),
            "stage_keys": ["baseline", "missing-stage"],
        }],
    }]
    curation_state = {"edge_fingerprint": "edges-v1"}

    _annotate_harmonization_pipeline_curation_status(
        pipelines,
        curation_state,
        available_stage_keys={"baseline"},
    )

    assert pipelines[0]["missing_stage_keys"] == ["missing-stage"]
    assert pipelines[0]["has_stage_integrity_failure"] is True
    assert pipelines[0]["run_action_kind"] == "repair"
    assert pipelines[0]["run_button_label"] == "Repair pipeline"
    assert pipelines[0]["run_action_enabled"] is True


def test_changed_source_graph_requires_rebuild_even_when_counts_might_match():
    pipelines = [{
        "_key": "source-stale",
        "engine_version": qa_app._HARMONIZATION_ENGINE_VERSION,
        "rule_ids": [],
        "runs": [{
            "_key": "complete-run",
            "status": "complete",
            "engine_version": qa_app._HARMONIZATION_ENGINE_VERSION,
            "source_graph_content_fingerprint": "old-source-build",
            "stage_keys": ["baseline"],
        }],
    }]

    _annotate_harmonization_pipeline_curation_status(
        pipelines,
        None,
        available_stage_keys={"baseline"},
        current_source_graph_fingerprint="new-source-build",
    )

    assert pipelines[0]["source_graph_changed"] is True
    assert pipelines[0]["run_action_kind"] == "rebuild"
    assert pipelines[0]["run_button_label"] == "Re-run pipeline"
    assert pipelines[0]["run_action_enabled"] is True


def test_unchanged_source_graph_content_remains_current():
    pipelines = [{
        "_key": "source-current",
        "engine_version": qa_app._HARMONIZATION_ENGINE_VERSION,
        "rule_ids": [],
        "runs": [{
            "_key": "complete-run",
            "status": "complete",
            "engine_version": qa_app._HARMONIZATION_ENGINE_VERSION,
            "source_graph_content_fingerprint": "same-source-content",
            "stage_keys": ["baseline"],
        }],
    }]

    _annotate_harmonization_pipeline_curation_status(
        pipelines,
        None,
        available_stage_keys={"baseline"},
        current_source_graph_fingerprint="same-source-content",
    )

    assert pipelines[0]["source_graph_changed"] is False
    assert pipelines[0]["run_action_kind"] == "current"
    assert pipelines[0]["run_button_label"] == "Up to date"
    assert pipelines[0]["run_action_enabled"] is False


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


def test_pipeline_jobs_serialize_shared_stage_mutations(monkeypatch):
    active_count = 0
    max_active_count = 0
    count_lock = threading.Lock()

    def fake_run(pipeline_key):
        nonlocal active_count, max_active_count
        with count_lock:
            active_count += 1
            max_active_count = max(max_active_count, active_count)
        time.sleep(0.02)
        with count_lock:
            active_count -= 1
        return {"_key": f"run-{pipeline_key}", "pipeline_name": pipeline_key}

    monkeypatch.setattr(qa_app, "_run_harmonization_pipeline", fake_run)
    monkeypatch.setattr(qa_app, "_update_metabolite_snapshot_job", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(qa_app, "_harmonization_pipeline_mutation_lock", threading.Lock())

    threads = [
        threading.Thread(
            target=qa_app._run_metabolite_snapshot_job,
            args=(f"job-{index}", "run_pipeline", {"pipeline_key": f"pipeline-{index}"}),
        )
        for index in range(2)
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert max_active_count == 1


def test_reconcile_interrupted_runs_only_targets_prior_boot_for_same_instance():
    class FakeAql:
        def __init__(self):
            self.parent_update_bind_vars = None

        def execute(self, query, bind_vars=None, **_kwargs):
            if "FOR run IN HarmonizationPipelineRun" in query:
                assert 'run.status IN ["running", "cleaning_up"]' in query
                assert "run.runner_instance_id == @runner_instance_id" in query
                assert "run.runner_boot_id != @runner_boot_id" in query
                assert 'failure_kind: "interrupted"' in query
                assert bind_vars == {
                    "runner_instance_id": "qa-local-keith",
                    "runner_boot_id": "new-boot",
                    "interrupted_at": "2026-09-24T14:00:00+00:00",
                }
                return [
                    {"run_key": "old-run", "pipeline_key": "pipeline-a"},
                    {"run_key": "superseded-run", "pipeline_key": "pipeline-b"},
                ]
            assert "pipeline.latest_run_key == interrupted_run.run_key" in query
            assert 'pipeline.status IN ["running", "cleaning_up"]' in query
            self.parent_update_bind_vars = bind_vars
            return ["pipeline-a"]

    fake_aql = FakeAql()

    class FakeDb:
        aql = fake_aql

    interrupted = qa_app._reconcile_interrupted_harmonization_runs(
        FakeDb(),
        runner_instance_id="qa-local-keith",
        runner_boot_id="new-boot",
        interrupted_at="2026-09-24T14:00:00+00:00",
    )

    assert interrupted == ["old-run", "superseded-run"]
    assert fake_aql.parent_update_bind_vars == {
        "interrupted_runs": [
            {"run_key": "old-run", "pipeline_key": "pipeline-a"},
            {"run_key": "superseded-run", "pipeline_key": "pipeline-b"},
        ],
        "interrupted_at": "2026-09-24T14:00:00+00:00",
    }


def test_reconciliation_retries_after_transient_startup_failure(monkeypatch):
    attempts = []

    def fake_reconcile(_db):
        attempts.append(True)
        if len(attempts) == 1:
            raise ConnectionError("temporary Arango outage")
        return ["recovered-run"]

    monotonic_values = iter([100.0, 131.0])
    monkeypatch.setattr(qa_app, "_harmonization_reconciliation_last_attempt", 0.0)
    monkeypatch.setattr(qa_app.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(qa_app, "get_db", lambda _name: object())
    monkeypatch.setattr(qa_app, "_reconcile_interrupted_harmonization_runs", fake_reconcile)

    assert qa_app._try_reconcile_interrupted_harmonization_runs(force=True) == []
    assert qa_app._try_reconcile_interrupted_harmonization_runs() == ["recovered-run"]
    assert len(attempts) == 2


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


def test_replacing_pipeline_runs_preserves_immutable_stage_artifacts(monkeypatch):
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
        "deleted_stage_keys": [],
        "deleted_superseded_stage_keys": [],
        "deleted_orphan_stage_keys": [],
    }
    assert db.aql.deleted_run_keys == ["old-run-a", "old-run-b"]
    assert deleted_artifacts == []
    assert db.stages.deleted == []


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


def test_mass_profile_loader_keeps_channels_and_component_metadata_separate():
    class FakeAql:
        def execute(self, query, **_kwargs):
            if "FOR d IN MetaboliteIdentifier" in query:
                return [{
                    "id": "HMDB:1",
                    "chemistry": [{
                        "source": "HMDB",
                        "source_id": "HMDB:1",
                        "average": [135.45, 135.46],
                        "monoisotopic": [134.94, None],
                        "components": [{
                            "mw": "100.0",
                            "monoisotopic_mass": "99.9",
                            "molecular_formula": "C5H8O2",
                            "smiles": "CCCCC(=O)O",
                        }],
                    }],
                }]
            if "FOR d IN ChemicalEntity" in query:
                return [{
                    "id": "CHEBI:1",
                    "chemistry": {
                        "average": [135.45, 135.46],
                        "monoisotopic": [134.94, 134.95],
                        "components": [{
                            "mw": "100.0",
                            "monoisotopic_mass": "99.9",
                            "molecular_formula": "C5H8O2",
                            "smiles": "CCCCC(=O)O",
                        }],
                    },
                }]
            raise AssertionError(query)

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, name):
            return name in {"MetaboliteIdentifier", "ChemicalEntity"}

    profiles = _load_metabolite_identifier_mass_values(FakeDb())

    assert profiles["HMDB:1"]["whole"] == {
        "average": [135.45, 135.46],
        "monoisotopic": [134.94],
    }
    assert profiles["HMDB:1"]["components"] == [{
        "source": "HMDB",
        "source_id": "HMDB:1",
        "component_index": 0,
        "average": 100.0,
        "monoisotopic": 99.9,
        "molecular_formula": "C5H8O2",
        "smiles": "CCCCC(=O)O",
    }]
    assert profiles["CHEBI:1"]["whole"] == {
        "average": [135.45, 135.46],
        "monoisotopic": [134.94, 134.95],
    }
    assert profiles["CHEBI:1"]["components"] == [{
        "source": "ChEBI",
        "source_id": "CHEBI:1",
        "component_index": 0,
        "average": 100.0,
        "monoisotopic": 99.9,
        "molecular_formula": "C5H8O2",
        "smiles": "CCCCC(=O)O",
    }]


def test_validation_samples_put_kegg_identifiers_first_without_reordering_others():
    validation = _prioritize_kegg_validation_samples({
        "warnings": [{
            "sample_member_ids": ["CHEBI:1", "KEGG.COMPOUND:C00001", "HMDB:1"],
            "unknown_member_samples": ["CAS:1", "KEGG.DRUG:D00001", "BioCyc:1"],
            "member_mass_examples": [
                {"member_id": "CHEBI:1", "masses": [10.0]},
                {"member_id": "KEGG.COMPOUND:C00001", "masses": [20.0]},
                {"member_id": "HMDB:1", "masses": [30.0]},
            ],
            "comparison_ids": "CHEBI:1 KEGG.COMPOUND:C00001 HMDB:1",
        }],
    })

    warning = validation["warnings"][0]
    assert warning["sample_member_ids"] == [
        "KEGG.COMPOUND:C00001", "CHEBI:1", "HMDB:1",
    ]
    assert warning["unknown_member_samples"] == [
        "KEGG.DRUG:D00001", "CAS:1", "BioCyc:1",
    ]
    assert [item["member_id"] for item in warning["member_mass_examples"]] == [
        "KEGG.COMPOUND:C00001", "CHEBI:1", "HMDB:1",
    ]
    assert warning["comparison_ids"] == "KEGG.COMPOUND:C00001 CHEBI:1 HMDB:1"


def test_validation_review_id_prefers_kegg_and_falls_back_to_representative():
    validation = _with_validation_review_ids(
        {
            "warnings": [
                {"rank_by_size": 1, "representative_id": "CHEBI:1"},
                {"rank_by_size": 2, "representative_id": "HMDB:2"},
            ]
        },
        {1: "KEGG.COMPOUND:C00001"},
    )

    assert validation["warnings"][0]["review_id"] == "KEGG.COMPOUND:C00001"
    assert validation["warnings"][0]["representative_id"] == "CHEBI:1"
    assert validation["warnings"][1]["review_id"] == "HMDB:2"


def test_mw_validation_selects_kegg_examples_before_truncating():
    member_rows = [
        {"member_id": f"CHEBI:{index}", "raw_masses": [index]}
        for index in range(1, 10)
    ] + [{"member_id": "KEGG.COMPOUND:C00001", "raw_masses": [20]}]

    examples = _metabolite_member_mass_examples(member_rows, limit=8)

    assert examples[0]["member_id"] == "KEGG.COMPOUND:C00001"
    assert len(examples) == 8


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
        "updated_identifier_count": 0,
        "edges": [{"action": "remove_edge", "start_id": "CHEBI:1", "end_id": "HMDB:1"}],
        "identifier_updates": [],
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
        "updated_identifier_count": 0,
        "edges": [{"action": "retain_edge", "start_id": "CHEBI:1", "end_id": "REFMET:1"}],
        "identifier_updates": [],
    }]


def test_build_harmonization_stage_cart_flags_includes_node_updates_and_edges():
    flags = _build_harmonization_stage_cart_flags(
        operations=[
            {
                "action": "set_properties",
                "target": {
                    "kind": "node",
                    "curation_set": "metabolite_harmonization",
                    "model_type": "MetaboliteIdentifier",
                    "id": "KEGG.COMPOUND:C00001",
                },
                "decisions": [
                    {"path": ["is_generic_structure"], "mode": "set", "value": True},
                    {"path": ["name"], "mode": "set", "value": "Water class"},
                    {"path": ["description"], "mode": "remove_override"},
                ],
            },
            {
                "action": "remove_edge",
                "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": "CHEBI:1",
                "end_id": "KEGG.COMPOUND:C00001",
            },
        ],
        member_rank_by_id={"CHEBI:1": 4, "KEGG.COMPOUND:C00001": 4},
        active_edge_pairs={("CHEBI:1", "KEGG.COMPOUND:C00001")},
        warning_ranks={4},
    )

    assert flags == [{
        "rank_by_size": 4,
        "queued_edge_count": 1,
        "updated_identifier_count": 1,
        "edges": [{
            "action": "remove_edge",
            "start_id": "CHEBI:1",
            "end_id": "KEGG.COMPOUND:C00001",
        }],
        "identifier_updates": [{
            "target_id": "KEGG.COMPOUND:C00001",
            "changes": [
                {"property": "is_generic_structure", "mode": "set", "value": True},
                {"property": "name", "mode": "set", "value": "Water class"},
                {"property": "description", "mode": "remove_override", "value": None},
            ],
        }],
    }]


def test_harmonization_stage_cart_warning_ranks_include_remaining_validation_cliques():
    stage = {
        "validation": {
            "mw_spread": {"computed": True, "warnings": [{"rank_by_size": 4}]},
            "denylist_still_merged": {
                "computed": True,
                "rule_enabled": True,
                "warnings": [{"rank_by_size": 9}, {"rank_by_size": 4}],
            },
            "generic_structure_consistency": {
                "computed": True,
                "warnings": [{"rank_by_size": 12}],
            },
        },
    }

    assert _harmonization_stage_cart_warning_ranks(stage) == {4, 9, 12}


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


def test_denylist_validation_excludes_pairs_incident_to_suppressed_records():
    validation = _build_harmonization_stage_denylist_validation(
        [["CHEBI:1", "HMDB:1"], ["HMDB:2", "REFMET:1"]],
        {("CHEBI:1", "HMDB:1"), ("HMDB:2", "REFMET:1")},
        True,
        suppressed_identifier_ids={"REFMET:1"},
    )

    assert validation["denylist_pair_count"] == 2
    assert validation["evaluated_denylist_pair_count"] == 1
    assert validation["suppressed_incident_pair_count"] == 1
    assert validation["warning_count"] == 1


def test_denylist_pair_review_is_stable_and_focuses_first_shared_clique():
    pair = _normalize_harmonization_denylist_pair(
        "HMDB:HMDB0000001 | KEGG.COMPOUND:C00001"
    )
    result = {
        "snapshot_graphs": [
            {
                "snapshot_key": "baseline",
                "snapshot_name": "Baseline",
                "cliques": [{
                    "clique_key": "baseline-clique",
                    "clique_size": 5,
                    "member_ids": [*pair, "CHEBI:1"],
                }],
            },
            {
                "snapshot_key": "curated",
                "snapshot_name": "After curations",
                "cliques": [
                    {
                        "clique_key": "left-clique",
                        "clique_size": 2,
                        "member_ids": [pair[0], "CHEBI:1"],
                    },
                    {
                        "clique_key": "right-clique",
                        "clique_size": 1,
                        "is_singleton_bucket": True,
                        "member_ids": [pair[1]],
                    },
                ],
            },
        ],
    }
    curation_state = {
        "pair_states": {pair: "remove_edge"},
        "pair_decisions": {pair: {
            "action": "remove_edge",
            "batch_id": "batch-1",
            "published_at": "2026-09-01T00:00:00Z",
            "published_by": {"id": "keith", "name": "Keith"},
            "note": "MW mismatch",
        }},
    }

    review = _build_harmonization_denylist_pair_review(pair, result, curation_state)

    assert pair == ("HMDB:HMDB0000001", "KEGG.COMPOUND:C00001")
    assert review["retain_allowed"] is True
    assert review["decision"]["batch_id"] == "batch-1"
    assert review["focus"] == {
        "stage_key": "baseline",
        "clique_key": "baseline-clique",
    }
    assert review["stage_states"][0]["together"] is True
    assert review["stage_states"][1]["together"] is False


def test_denylist_pair_review_is_read_only_without_active_remove():
    pair = ("CHEBI:1", "HMDB:1")

    review = _build_harmonization_denylist_pair_review(
        pair,
        {"snapshot_graphs": []},
        {"pair_states": {pair: "retain_edge"}, "pair_decisions": {}},
    )

    assert review["active_action"] == "retain_edge"
    assert review["retain_allowed"] is False


def test_retain_cart_api_rejects_pair_without_active_published_removal(monkeypatch):
    monkeypatch.setattr(qa_app, "_curator_identity", lambda _request, _payload: ("keith", "Keith"))
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_edge_removal_curations",
        lambda: {"pair_states": {("CHEBI:1", "HMDB:1"): "retain_edge"}},
    )

    class FakeRequest:
        async def json(self):
            return {
                "action": "retain_edge",
                "start_id": "CHEBI:1",
                "end_id": "HMDB:1",
            }

    with pytest.raises(HTTPException) as exc:
        asyncio.run(qa_app.ramp_id_qa_add_curation_cart_item(FakeRequest()))

    assert exc.value.status_code == 409
    assert "no longer an active published removal" in exc.value.detail


def test_denylist_pair_review_template_shows_provenance_and_retain_action():
    html = qa_app.templates.env.get_template("ramp_id_qa.html").render(
        request={"scope": {"path": "/ramp-id-qa"}},
        root_path="",
        query_id="CHEBI:1 HMDB:1",
        selected_snapshot_keys=["baseline", "curated"],
        denylist_pair="CHEBI:1|HMDB:1",
        denylist_pair_review={
            "pair": {"left_id": "CHEBI:1", "right_id": "HMDB:1"},
            "active_action": "remove_edge",
            "is_active_removal": True,
            "retain_allowed": True,
            "decision": {
                "batch_id": "batch-1",
                "published_at": "2026-09-01T00:00:00Z",
                "published_by": {"id": "keith", "name": "Keith"},
                "note": "salt difference",
            },
            "stage_states": [{
                "stage_key": "baseline",
                "stage_name": "Baseline",
                "together": True,
                "found_count": 2,
                "memberships": [
                    {"identifier": "CHEBI:1", "clique_key": "c1", "clique_size": 2},
                    {"identifier": "HMDB:1", "clique_key": "c1", "clique_size": 2},
                ],
            }],
            "focus": {"stage_key": "baseline", "clique_key": "c1"},
        },
        denylist_review=None,
        result={"snapshot_graphs": [], "snapshot_sankeys": []},
        overview=None,
        error=None,
    )

    assert "Deny-list Pair Review" in html
    assert "batch-1" in html
    assert "by Keith" in html
    assert "salt difference" in html
    assert "Queue retain decision" in html
    assert 'data-curation-action="retain_edge"' in html


def test_full_page_mw_finding_panel_supports_acceptance_workflow():
    html = qa_app.templates.env.get_template("ramp_id_qa.html").render(
        request={"scope": {"path": "/ramp-id-qa"}},
        root_path="",
        query_id="CHEBI:1 HMDB:1",
        selected_snapshot_keys=["stage-1"],
        denylist_pair_review=None,
        denylist_review=None,
        result=None,
        overview=None,
        error=None,
        mw_validation_review={
            "stage_key": "stage-1",
            "stage_name": "Final",
            "validator_version": "component-aware-v1",
            "threshold_percent": 10,
            "reason_options": [{"value": "salt_or_counterion", "label": "Salt or counterion"}],
            "finding": {
                "finding_id": "mw-1234567890abcdef12345678",
                "review_status": "unresolved",
                "detected_severity": "error",
                "spread_percent": 50,
                "size": 2,
                "channel_results": {"average": {
                    "summary": {"min": 100, "max": 150, "median": 125, "count": 2},
                    "mass_cluster_count": 2,
                    "explained_mass_cluster_count": 0,
                }},
                "member_mass_examples": [],
                "mass_value_rows": [
                    {
                        "channel": "average",
                        "channel_label": "Average",
                        "mass": 150.0,
                        "mass_label": "150",
                        "identifiers": ["HMDB:1"],
                    },
                    {
                        "channel": "average",
                        "channel_label": "Average",
                        "mass": 100.0,
                        "mass_label": "100",
                        "identifiers": ["CHEBI:1"],
                    },
                ],
                "mass_cluster_rows": [{
                    "channel": "average",
                    "channel_label": "Average",
                    "cluster_count": 2,
                    "explained_cluster_count": 0,
                    "clusters": [
                        {"mass_label": "≈150 Da", "identifier_count": 1},
                        {"mass_label": "≈100 Da", "identifier_count": 1},
                    ],
                    "fallback_range_label": None,
                    "evidence_complete": True,
                }],
                "mass_evidence_id_count": 2,
                "component_matches": [],
                "source_mass_observations": [
                    {
                        "member_id": "CHEBI:1",
                        "model_type": "ChemicalEntity",
                        "source": "ChEBI",
                        "source_id": "CHEBI:1",
                        "channel": "monoisotopic",
                        "value": 100,
                    },
                    {
                        "member_id": "CHEBI:1",
                        "model_type": "MetaboliteIdentifier",
                        "source": "ChEBI",
                        "source_id": "CHEBI:1",
                        "channel": "monoisotopic",
                        "value": 150,
                    },
                ],
                "adjudication": None,
            },
        },
    )

    assert "MW Validation Finding" in html
    assert "Unresolved error" in html
    assert "Queue accepted discrepancy" in html
    assert 'data-action="accept_mw_discrepancy"' in html
    assert "does not change clique membership or source chemistry" in html
    assert "Component and source evidence" in html
    assert "Mass clusters" in html
    assert "2 clusters" in html
    assert "0 component-explained" in html
    assert "≈150 Da" in html
    assert "Exact values and identifiers (2 IDs)" in html
    assert "Mass (Da)" in html
    mass_table = html.split("metabolite-mw-value-table", 1)[1].split("</table>", 1)[0]
    assert mass_table.index("HMDB:1") < mass_table.index("CHEBI:1")
    assert "Reason &amp; curation" in html
    assert "Review ChEBI ontology record" in html
    assert "Review ChEBI SDF record" in html
    assert "/collection/ChemicalEntity/doc/CHEBI%3A1" in html
    assert "/collection/MetaboliteIdentifier/doc/CHEBI%3A1" in html
    assert "<details open>" not in html


def test_stale_mw_finding_shows_change_summary_and_keep_acceptance_queue():
    html = qa_app.templates.env.get_template("ramp_id_qa.html").render(
        request={"scope": {"path": "/ramp-id-qa"}},
        root_path="",
        query_id="CHEBI:1 HMDB:1",
        selected_snapshot_keys=["stage-1"],
        denylist_pair_review=None,
        denylist_review=None,
        result=None,
        overview=None,
        error=None,
        mw_validation_review={
            "stage_key": "stage-1",
            "stage_name": "Final",
            "validator_version": "component-aware-v1",
            "threshold_percent": 10,
            "reason_options": [
                {"value": "salt_or_counterion", "label": "Salt or counterion"},
            ],
            "finding": {
                "finding_id": "mw-1234567890abcdef12345678",
                "review_status": "stale",
                "detected_severity": "error",
                "spread_percent": 50,
                "size": 2,
                "mass_cluster_rows": [],
                "mass_value_rows": [],
                "mass_evidence_id_count": 0,
                "component_matches": [],
                "source_mass_observations": [],
                "adjudication": {
                    "reason": "salt_or_counterion",
                    "supporting_ids": ["HMDB:1"],
                    "note": "Reviewed salt form.",
                    "observed_member_ids": ["CHEBI:1", "HMDB:1"],
                    "published_at": "2026-09-30T13:56:19Z",
                    "curation_batch_id": "batch-1",
                },
                "reusable_supporting_ids": ["HMDB:1"],
                "removed_supporting_ids": [],
                "change_summary": {
                    "membership_changed": False,
                    "current_member_count": 2,
                    "added_member_ids": [],
                    "removed_member_ids": [],
                    "evidence_changed": True,
                    "previous_evidence_available": False,
                    "previous_evidence_fingerprint": "a" * 64,
                    "current_evidence_fingerprint": "b" * 64,
                },
            },
        },
    )

    assert "What changed" in html
    assert "Unchanged — 2 IDs" in html
    assert "previous exact MW evidence was not saved" in html
    assert "Keep acceptance" in html
    assert "data-mw-keep-acceptance" in html
    assert "Edit explanation" in html
    assert 'value="salt_or_counterion" selected' in html
    assert "Reviewed salt form." in html


def test_mw_finding_mass_rows_group_exact_values_and_sort_descending_by_channel():
    rows = qa_app._mw_finding_mass_value_rows({
        "member_mass_examples": [
            {
                "member_id": "CHEBI:1",
                "average_masses": [100.0, 150.0],
                "monoisotopic_masses": [99.9],
            },
            {
                "member_id": "HMDB:1",
                "average_masses": [150.0],
                "monoisotopic_masses": [149.8, 99.9],
            },
        ],
    })

    assert [(row["channel"], row["mass"]) for row in rows] == [
        ("average", 150.0),
        ("average", 100.0),
        ("monoisotopic", 149.8),
        ("monoisotopic", 99.9),
    ]
    assert rows[0]["identifiers"] == ["CHEBI:1", "HMDB:1"]


def test_mw_finding_mass_rows_preserve_legacy_untyped_mass_evidence():
    rows = qa_app._mw_finding_mass_value_rows({
        "member_mass_examples": [
            {"member_id": "CHEBI:1", "masses": [100.0, 150.0]},
            {"member_id": "HMDB:1", "masses": [150.0]},
        ],
    })

    assert [(row["channel"], row["mass"]) for row in rows] == [
        ("unspecified", 150.0),
        ("unspecified", 100.0),
    ]
    assert rows[0]["channel_label"] == "Unspecified (legacy)"
    assert rows[0]["identifiers"] == ["CHEBI:1", "HMDB:1"]


def test_mw_finding_cluster_rows_use_validator_tolerance_and_descending_order():
    finding = {
        "channel_results": {"average": {
            "summary": {"min": 100.0, "max": 150.0, "median": 149.95, "count": 3},
            "mass_cluster_count": 2,
            "explained_mass_cluster_count": 0,
        }},
        "member_mass_examples": [
            {"member_id": "CHEBI:1", "average_masses": [100.0, 150.0]},
            {"member_id": "HMDB:1", "average_masses": [149.95]},
        ],
    }

    rows = qa_app._mw_finding_mass_cluster_rows(finding)

    assert len(rows) == 1
    assert rows[0]["channel"] == "average"
    assert rows[0]["cluster_count"] == 2
    assert rows[0]["clusters"] == [
        {"mass_label": "149.95–150 Da", "identifier_count": 2},
        {"mass_label": "≈100 Da", "identifier_count": 1},
    ]


def test_mw_finding_cluster_rows_do_not_claim_incomplete_legacy_membership():
    finding = {
        "evidence_fingerprint_version": "legacy-saved-finding-v1",
        "channel_results": {"average": {
            "summary": {"min": 100.0, "max": 200.0, "median": 150.0, "count": 3},
            "mass_cluster_count": 2,
            "explained_mass_cluster_count": 0,
        }},
        "member_ids": [
            "CHEBI:1", "HMDB:1", "REFMET:1", "REFMET:2", "REFMET:3",
            "REFMET:4", "REFMET:5", "REFMET:6", "REFMET:7", "REFMET:8",
        ],
        "member_mass_examples": [
            {"member_id": "CHEBI:1", "average_masses": [100.0]},
            {"member_id": "HMDB:1", "average_masses": [200.0]},
            {"member_id": "REFMET:1", "average_masses": [100.0]},
            {"member_id": "REFMET:2", "average_masses": [200.0]},
            {"member_id": "REFMET:3", "average_masses": [100.0]},
            {"member_id": "REFMET:4", "average_masses": [200.0]},
            {"member_id": "REFMET:5", "average_masses": [100.0]},
            {"member_id": "REFMET:6", "average_masses": [200.0]},
        ],
    }

    row = qa_app._mw_finding_mass_cluster_rows(finding)[0]

    assert row["cluster_count"] == 2
    assert row["clusters"] == []
    assert row["fallback_range_label"] == "100–200 Da"
    assert row["evidence_complete"] is False


def test_full_clique_page_uses_compact_node_curation_layout_without_rule_pills():
    template_source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_qa.html"
    )[0]
    visuals_source = (
        qa_app.STATIC_DIR / "metabolite_harmonization_visuals.js"
    ).read_text()
    cart_source = (
        qa_app.STATIC_DIR / "metabolite_curation_cart.js"
    ).read_text()

    assert "data-selected-clique-rules" not in template_source
    assert "renderRuleBadges" not in template_source
    assert "Open MetaboliteIdentifier record" in visuals_source
    assert "Open ChemicalEntity record" in visuals_source
    assert 'detailRow("Raw"' in visuals_source
    assert 'detailRow("Curated"' in visuals_source
    assert 'detailRow("Effective"' in visuals_source
    assert 'data-generic-classification-choice="detected"' in visuals_source
    assert '[data-generic-classification-value]:checked' in cart_source
    assert "event.submitter?.dataset.genericClassificationChoice" in cart_source
    identifier_block = visuals_source.split("const identifierRows = [", 1)[1].split(
        "].filter(Boolean).join", 1
    )[0]
    assert 'detailRow("ID"' in identifier_block
    assert 'detailRow("Source"' in identifier_block
    assert 'detailRow("Names"' in identifier_block
    assert 'detailRow("Prefix"' not in identifier_block
    assert 'detailRow("Name count"' not in identifier_block


def test_full_page_keeps_saved_mw_finding_when_graph_query_fails(monkeypatch):
    review = {"finding": {"finding_id": "mw-1"}}
    monkeypatch.setattr(qa_app, "_build_mw_validation_review", lambda *_args: review)

    def fail_graph(*_args, **_kwargs):
        raise RuntimeError("graph memory limit exceeded")

    monkeypatch.setattr(qa_app, "_load_metabolite_identifier_qa_many", fail_graph)
    monkeypatch.setattr(
        qa_app.templates,
        "TemplateResponse",
        lambda _request, _name, context: context,
    )

    context = qa_app.ramp_id_qa(
        {},
        id="CHEBI:1 HMDB:1",
        stages="stage-1",
        mw_finding="mw-1",
    )

    assert context["mw_validation_review"] is review
    assert "graph memory limit exceeded" in context["error"]


def test_full_page_mw_history_displays_supporting_ids():
    source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_qa.html"
    )[0]

    assert "finding.adjudication.supporting_ids" in source


def test_free_anomer_panel_is_bounded_to_the_stage_that_applied_the_rule():
    source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_stage_stats.html"
    )[0]

    assert 'stats.stage.rule_ids[-1] == "merge_free_anomeric_forms"' in source


def test_stage_cards_color_mw_status_counts_independently():
    pipeline_source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_pipeline_table.html"
    )[0]
    stats_source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_stage_stats_table.html"
    )[0]

    for class_name in (
        "metabolite-mw-count-error",
        "metabolite-mw-count-warning",
        "metabolite-mw-count-accepted",
    ):
        assert class_name in pipeline_source
        assert class_name in stats_source


def test_metabolite_graph_node_highlights_use_neutral_elliptical_underlays():
    qa_source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_qa.html"
    )[0]
    shared_source = (
        qa_app.STATIC_DIR / "metabolite_harmonization_visuals.js"
    ).read_text()

    for source in (qa_source, shared_source):
        assert '"underlay-shape": "ellipse"' in source

    stats_source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_stage_stats.html"
    )[0]
    assert "cytoscape.min.js" not in stats_source
    assert "/ramp-id-qa/api/metabolite" not in stats_source
    assert "Review here" not in stats_source


def test_pending_record_participation_updates_visible_graph_nodes():
    qa_source = qa_app.templates.env.loader.get_source(
        qa_app.templates.env, "ramp_id_qa.html"
    )[0]
    shared_source = (
        qa_app.STATIC_DIR / "metabolite_harmonization_visuals.js"
    ).read_text()

    for source in (qa_source,):
        assert 'node.toggleClass("pending-record-suppression"' in source
        assert 'node.toggleClass("pending-record-restoration"' in source
        assert "Draft participation changes" in source
        assert "metabolite-curation-cart:changed" in source
    assert '{selector: ".pending-record-suppression"' in shared_source
    assert '{selector: ".pending-record-restoration"' in shared_source


def test_snapshot_union_resolves_members_by_public_id_instead_of_arango_key(monkeypatch):
    member_id = "CAS:62-31-7"
    stage_key = "stage-cas"
    clique_key = "stage-cas-000001-clique"

    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            bind_vars = bind_vars or {}
            if "FOR chunk IN HarmonizationStageActiveIdentifierChunk" in query:
                return [{"stage_key": stage_key, "member_id": member_id}]
            if "FILTER node.id IN @ids" in query:
                return [member_id]
            if (
                "FILTER e._from IN @clique_vertex_ids" in query
                and "RETURN member_id" in query
            ):
                assert "DOCUMENT(e._to)" in query
                return [member_id]
            if "LET node = DOCUMENT(e._to)" in query:
                assert "FILTER e._from IN @clique_vertex_ids" in query
                assert bind_vars["clique_vertex_ids"] == [
                    f"HarmonizedMetabolite/{clique_key}"
                ]
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
            if "LET query_nodes" in query:
                assert "FILTER e._from == handle" in query
                assert "FILTER e._to == handle" in query
                assert query.count("LIMIT @neighbor_limit") == 2
                assert bind_vars["neighbor_limit"] == 100
                return []
            if "RETURN {\n            id: node.id,\n            raw_masses:" in query:
                return [{"id": member_id, "raw_masses": []}]
            if "LET chemical_entity" in query:
                return [{
                    "id": member_id,
                    "label": member_id,
                    "names": [],
                    "prefix": "CAS",
                    "name_count": 0,
                    "synonym_count": 0,
                    "chem_prop_count": 0,
                    "editable_properties": {"is_generic_structure": True},
                    "raw_masses": [],
                    "chem_props": [],
                    "formulas": [],
                    "smiles": [],
                    "inchi_keys": [],
                    "chemical_entity": None,
                }]
            if "FOR e IN HarmonizationStageEvidenceEdge" in query:
                assert "FOR selection IN @display_selections" in query
                assert "FILTER membership._from == selection.clique_vertex_id" in query
                assert "FILTER membership.member_id IN selection.member_ids" in query
                assert "FILTER e._from == node.handle" in query
                assert "FILTER HAS(member_id_by_handle, e._to)" in query
                assert "FILTER e.stage_key == selection.stage_key" in query
                assert bind_vars["display_selections"] == [{
                    "stage_key": stage_key,
                    "clique_vertex_id": f"HarmonizedMetabolite/{clique_key}",
                    "member_ids": [member_id],
                }]
                return []
            raise AssertionError(query)

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, _name):
            return True

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())
    monkeypatch.setattr(qa_app, "_load_metabolite_snapshot_memberships", lambda _ids, _stages=None: [{
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
    assert clique["elements"][0]["data"]["generic_structure_state"] == "generic"
    assert "generic-structure-generic" in clique["elements"][0]["classes"]


def test_snapshot_union_keeps_raw_identifier_available_without_clique_membership(monkeypatch):
    member_id = "REFMET:RM0233954"
    stage_key = "stage-suppressed"

    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            if "COLLECT existing_id = node.id" in query:
                return [member_id]
            if "FOR e IN HarmonizedMetaboliteMemberEdge" in query:
                return []
            if "LET query_nodes" in query:
                return []
            if "RETURN {\n            id: node.id,\n            raw_masses:" in query:
                return [{"id": member_id, "raw_masses": []}]
            if "LET chemical_entity" in query:
                return [{
                    "id": member_id,
                    "label": "PS 18:2",
                    "names": ["PS 18:2"],
                    "prefix": "REFMET",
                    "name_count": 1,
                    "synonym_count": 0,
                    "chem_prop_count": 1,
                    "editable_properties": {},
                    "raw_masses": [],
                    "chem_props": [],
                    "formulas": ["C24H42NO10P"],
                    "smiles": [],
                    "inchi_keys": [],
                    "chemical_entity": None,
                }]
            if "FOR chunk IN HarmonizationStageActiveIdentifierChunk" in query:
                return []
            if "FOR selection IN @display_selections" in query:
                return []
            raise AssertionError(query)

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, _name):
            return True

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())
    monkeypatch.setattr(qa_app, "_load_metabolite_snapshot_memberships", lambda *_args: [])
    monkeypatch.setattr(qa_app, "_load_metabolite_edge_removal_curations", lambda: {
        "record_states": {member_id: "suppress_record"},
        "record_decisions": {member_id: {"note": "Inconsistent source record"}},
        "suppression_state_available": True,
        "annotation_state_available": True,
        "annotation_decisions": {},
        "annotation_overrides": {},
    })
    monkeypatch.setattr(qa_app, "_list_harmonization_stages", lambda: [{
        "_key": stage_key,
        "name": "After suppression",
        "created_at": "2026-09-29T00:00:00+00:00",
        "rules": [],
    }])
    monkeypatch.setattr(qa_app, "_list_harmonization_pipelines", lambda limit=100: [])

    result = qa_app._load_metabolite_snapshot_union([member_id], [stage_key])

    clique = result["snapshot_graphs"][0]["cliques"][0]
    node = clique["elements"][0]
    assert clique["is_inactive_bucket"] is True
    assert clique["member_ids"] == [member_id]
    assert node["data"]["id"] == member_id
    assert node["data"]["is_active_in_stage"] is False
    assert node["data"]["harmonization_participation"]["is_suppressed"] is True


def test_snapshot_memberships_use_edge_index_and_database_stage_filter(monkeypatch):
    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            assert "FOR node IN MetaboliteIdentifier" in query
            assert "FILTER e._to == node._id" in query
            assert "FILTER e.member_id IN" not in query
            assert "FILTER e.stage_key IN @snapshot_keys" in query
            assert bind_vars == {
                "identifier_ids": ["KEGG.COMPOUND:C00422"],
                "snapshot_keys": ["stage-08"],
            }
            return [{"member_id": "KEGG.COMPOUND:C00422", "snapshot_key": "stage-08"}]

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, _name):
            return True

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())

    rows = qa_app._load_metabolite_snapshot_memberships(
        ["KEGG.COMPOUND:C00422"],
        ["stage-08"],
    )

    assert rows == [{"member_id": "KEGG.COMPOUND:C00422", "snapshot_key": "stage-08"}]


def test_snapshot_memberships_omit_unused_stage_bind_parameter(monkeypatch):
    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            assert "FILTER e._to == node._id" in query
            assert "@snapshot_keys" not in query
            assert bind_vars == {"identifier_ids": ["KEGG.COMPOUND:C00422"]}
            return []

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, _name):
            return True

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())

    assert qa_app._load_metabolite_snapshot_memberships(
        ["KEGG.COMPOUND:C00422"]
    ) == []


def test_default_snapshot_filter_uses_latest_completed_stage(monkeypatch):
    monkeypatch.setattr(qa_app, "_list_harmonization_stages", lambda: [
        {"_key": "baseline", "created_at": "2026-01-01T00:00:00Z"},
        {"_key": "stage-08", "created_at": "2026-01-02T00:00:00Z"},
    ])

    assert qa_app._default_metabolite_snapshot_key_filter() == ["stage-08"]


def test_default_denylist_pair_filter_uses_matching_baseline_and_latest_stage(monkeypatch):
    monkeypatch.setattr(qa_app, "_list_harmonization_stages", lambda: [
        {
            "_key": "old-baseline", "stage_index": 0, "rule_ids": [],
            "engine_version": "v7", "summary": {
                "database": "metabolite_harmonization",
                "metabolite_identifier_count": 10, "equivalence_edge_count": 9,
            },
        },
        {
            "_key": "current-baseline", "stage_index": 0, "rule_ids": [],
            "engine_version": "v8", "summary": {
                "database": "metabolite_harmonization",
                "metabolite_identifier_count": 12, "equivalence_edge_count": 11,
            },
        },
        {
            "_key": "current-final", "stage_index": 7, "rule_ids": ["apply_curations"],
            "engine_version": "v8", "summary": {
                "database": "metabolite_harmonization",
                "metabolite_identifier_count": 12, "equivalence_edge_count": 11,
            },
        },
    ])

    assert _default_metabolite_denylist_pair_stage_filter() == [
        "current-baseline", "current-final",
    ]


def test_denylist_pair_route_defaults_to_before_and_after_stages(monkeypatch):
    captured = {}
    monkeypatch.setattr(
        qa_app,
        "_default_metabolite_denylist_pair_stage_filter",
        lambda: ["baseline", "final"],
    )
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_identifier_qa_many",
        lambda query, stages, include_identifier_details=False: (
            captured.update({"query": query, "stages": stages})
            or {"snapshot_graphs": []}
        ),
    )
    monkeypatch.setattr(
        qa_app,
        "_load_metabolite_edge_removal_curations",
        lambda: {"pair_states": {}, "pair_decisions": {}},
    )
    monkeypatch.setattr(
        qa_app.templates,
        "TemplateResponse",
        lambda request, name, context: context,
    )

    context = qa_app.ramp_id_qa(
        request=object(),
        denylist_pair="CHEBI:1|HMDB:1",
    )

    assert captured["query"] == "CHEBI:1 HMDB:1"
    assert captured["stages"] == ["baseline", "final"]
    assert context["selected_stage_keys"] == ["baseline", "final"]


def test_display_member_limit_is_applied_per_stage_clique():
    rows = []
    for stage_key, neighbor_id in (("stage-a", "A:neighbor"), ("stage-b", "B:neighbor")):
        for member_id in ["QUERY:1", neighbor_id, *[f"{stage_key}:{index}" for index in range(5)]]:
            rows.append({
                "snapshot_key": stage_key,
                "clique_key": f"{stage_key}-clique",
                "member_id": member_id,
            })

    selections = qa_app._metabolite_display_member_ids_by_clique(
        rows,
        ["QUERY:1"],
        [
            {"stage_key": "stage-a", "id": "A:neighbor"},
            {"stage_key": "stage-b", "id": "B:neighbor"},
        ],
        limit=3,
    )

    assert selections[("stage-a", "stage-a-clique")][:2] == ["QUERY:1", "A:neighbor"]
    assert selections[("stage-b", "stage-b-clique")][:2] == ["QUERY:1", "B:neighbor"]
    assert all(len(member_ids) == 3 for member_ids in selections.values())


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


def test_generic_structure_validation_distinguishes_failures_from_classification_gaps():
    validation = _build_harmonization_stage_generic_structure_validation(
        groups=[
            ["GENERIC:1", "SPECIFIC:1"],
            ["GENERIC:2", "UNKNOWN:1"],
            ["SPECIFIC:2", "UNKNOWN:2"],
        ],
        active_edges=[
            {"start_id": "GENERIC:1", "end_id": "SPECIFIC:1", "sources": ["test"]},
            {"start_id": "GENERIC:2", "end_id": "UNKNOWN:1", "synthetic": True, "rule_id": "test-rule"},
            {"start_id": "SPECIFIC:2", "end_id": "UNKNOWN:2", "sources": ["test"]},
        ],
        classifications={
            "GENERIC:1": True,
            "SPECIFIC:1": False,
            "GENERIC:2": True,
            "SPECIFIC:2": False,
        },
        rule_enabled=False,
    )

    assert validation["inconsistent_clique_count"] == 1
    assert validation["generic_unknown_clique_count"] == 1
    assert validation["warning_count"] == 2
    assert validation["actionable_raw_edge_count"] == 1
    assert validation["warnings"][0]["status"] == "inconsistent"
    assert validation["warnings"][1]["status"] == "generic_unknown"
    assert validation["warnings"][1]["boundary_edges"][0]["synthetic"] is True


def test_generic_structure_pruning_keeps_generic_unknown_edges_as_gaps():
    class FakeAql:
        def execute(self, _query, **_kwargs):
            return [
                {"key": "generic-specific", "id": "generic-specific", "start_id": "GENERIC:1", "end_id": "SPECIFIC:1", "details": []},
                {"key": "generic-unknown", "id": "generic-unknown", "start_id": "GENERIC:1", "end_id": "UNKNOWN:1", "details": []},
            ]

    class FakeDb:
        aql = FakeAql()

    edges, summary = qa_app._active_metabolite_identifier_mapping_edges_for_rules(
        FakeDb(),
        {"GENERIC:1", "SPECIFIC:1", "UNKNOWN:1"},
        ["ignore_generic_structure_mismatch"],
        {},
        generic_structure_classifications={"GENERIC:1": True, "SPECIFIC:1": False},
    )

    assert [(edge["start_id"], edge["end_id"]) for edge in edges] == [
        ("GENERIC:1", "UNKNOWN:1")
    ]
    assert summary["generic_structure_ignored_edge_count"] == 1


def test_old_stage_reports_generic_structure_validation_as_not_computed():
    validation = qa_app._harmonization_stage_generic_structure_validation_from_doc({
        "rule_ids": ["ignore_generic_structure_mismatch"],
    })

    assert validation["computed"] is False
    assert validation["rule_enabled"] is True


def test_stage_overview_exposes_generic_structure_validation_counts(monkeypatch):
    stage = {
        "_key": "stage-structure",
        "rule_ids": ["ignore_generic_structure_mismatch"],
        "summary": {"clique_count": 12},
        "validation": {
            "generic_structure_consistency": {
                "computed": True,
                "rule_enabled": True,
                "inconsistent_clique_count": 3,
                "generic_unknown_clique_count": 7,
                "warnings": [],
            },
        },
    }

    class FakeAql:
        def execute(self, query, **_kwargs):
            assert 'UNSET(\n                  s.validation.denylist_still_merged' in query
            assert '"legacy_evidence_fingerprint"' in query
            return [stage]

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, name):
            return name == qa_app._HARMONIZATION_STAGE_COLLECTION

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())

    result = qa_app._list_harmonization_stage_overview_stats(include_distribution=False)

    overview = result[0]["overview_stats"]
    assert overview["generic_structure_validation_computed"] is True
    assert overview["generic_structure_rule_enabled"] is True
    assert overview["generic_structure_failure_count"] == 3
    assert overview["generic_structure_gap_count"] == 7


def test_assertion_lookup_uses_target_edge_index_instead_of_member_edge_scan():
    queries = []

    class FakeAql:
        def execute(self, query, **_kwargs):
            queries.append(query)
            if "HarmonizationStageActiveIdentifierChunk" in query:
                return [
                    {"stage_key": "stage-1", "member_id": "CHEBI:1"},
                    {"stage_key": "stage-1", "member_id": "HMDB:1"},
                ]
            return [
                {
                    "stage_key": "stage-1",
                    "member_id": member_id,
                    "clique_id": "HarmonizedMetabolite/shared-clique",
                    "rank_by_size": 1,
                    "clique_size": 2,
                    "representative_id": "CHEBI:1",
                }
                for member_id in ("CHEBI:1", "HMDB:1")
            ]

    class FakeDb:
        aql = FakeAql()

    result = qa_app._load_expected_clique_assertion_results(
        FakeDb(),
        [{"assertion_id": "assertion-1", "member_ids": ["CHEBI:1", "HMDB:1"]}],
        ["stage-1"],
    )

    assert "FOR member_id IN @member_ids" in queries[0]
    assert (
        "FOR clique, edge IN INBOUND member HarmonizedMetaboliteMemberEdge"
        in queries[1]
    )
    assert "FOR edge IN HarmonizedMetaboliteMemberEdge" not in queries[1]
    assert result["by_stage"]["stage-1"]["pass_count"] == 1
    assert result["by_stage"]["stage-1"]["split_count"] == 0


def test_requested_stage_overview_requires_complete_stage(monkeypatch):
    class FakeAql:
        def execute(self, query, bind_vars=None, **_kwargs):
            assert 's._key IN @stage_keys AND s.status == "complete"' in query
            assert bind_vars["stage_keys"] == ["failed-stage"]
            return []

    class FakeDb:
        aql = FakeAql()

        def has_collection(self, name):
            return name == qa_app._HARMONIZATION_STAGE_COLLECTION

    monkeypatch.setattr(qa_app, "get_db", lambda _name: FakeDb())

    assert qa_app._list_harmonization_stage_overview_stats(
        stage_keys=["failed-stage"], include_distribution=False
    ) == []


def test_generic_structure_stage_stats_lists_affected_cliques():
    html = qa_app.templates.env.get_template(
        "ramp_id_generic_structure_validation.html"
    ).render(
        root_path="",
        stats={
            "stage": {"_key": "stage-structure"},
            "generic_structure_validation": {
                "computed": True,
                "rule_enabled": True,
                "inconsistent_clique_count": 1,
                "generic_unknown_clique_count": 1,
                    "warning_count": 1,
                "warnings": [{
                    "status": "inconsistent",
                    "rank_by_size": 4,
                    "representative_id": "KEGG:C00626",
                    "review_id": "KEGG:C00626",
                    "size": 3,
                    "generic_member_count": 1,
                    "specific_member_count": 1,
                    "unknown_member_count": 1,
                    "generic_member_samples": ["KEGG:C00626"],
                    "specific_member_samples": ["HMDB:1"],
                    "unknown_member_samples": ["CHEBI:2"],
                    "boundary_edge_count": 1,
                    "boundary_edges": [{
                        "start_id": "KEGG:C00626",
                        "end_id": "HMDB:1",
                    }],
                    "comparison_ids": "KEGG:C00626 HMDB:1 CHEBI:2",
                }],
            },
        },
    )

    assert "Generic-Structure Consistency" in html
    assert "1 failure" in html
    assert "classification gap" not in html.lower()
    assert "KEGG:C00626" in html
    assert "Review ID:" in html
    assert "Pending changes" in html
    assert "data-generic-warning-rank=\"4\"" in html
    assert "data-generic-cart-flag" in html
    assert "Review clique" in html
    assert "stages=stage-structure" in html
    assert "Showing the top" not in html


def test_generic_structure_status_renders_in_pipeline_cards_and_stage_listing():
    states = [
        ("pre-rule", False, True, 2, 5),
        ("post-rule", True, True, 3, 7),
        ("legacy", False, False, 0, 0),
    ]
    nodes = []
    stages = []
    for stage_key, rule_enabled, computed, failures, gaps in states:
        overview_stats = {
            "active_identifier_count": 10,
            "clique_count": 4,
            "non_singleton_clique_count": 2,
            "singleton_identifier_count": 2,
            "active_edge_count": 8,
            "ignored_edge_count": 0,
            "mw_validation_computed": False,
            "mw_warning_count": 0,
            "denylist_validation_computed": False,
            "denylist_rule_enabled": False,
            "denylist_warning_count": 0,
            "generic_structure_validation_computed": computed,
            "generic_structure_rule_enabled": rule_enabled,
            "generic_structure_failure_count": failures,
            "generic_structure_gap_count": gaps,
            "assertion_count": 0,
            "largest_clique_sizes": [],
            "max_size": 2,
            "bins": {},
        }
        nodes.append({
            "stage_key": stage_key,
            "stage_index": 0,
            "label": stage_key,
            "stats": overview_stats,
            "terminals": [],
            "children": [],
        })
        stages.append({
            "_key": stage_key,
            "display_label": stage_key,
            "rule_ids": ["ignore_generic_structure_mismatch"] if rule_enabled else [],
            "overview_stats": overview_stats,
        })

    pipeline_html = qa_app.templates.env.get_template(
        "ramp_id_pipeline_table.html"
    ).render(
        root_path="",
        pipeline_table_oob=False,
        overview={
            "active_job_count": 0,
            "pipelines": [],
            "pipeline_stage_stats": [],
            "pipeline_tree": {
                "roots": nodes,
                "represented_pipeline_count": 1,
                "not_run_count": 0,
            },
        },
    )
    stage_listing_html = qa_app.templates.env.get_template(
        "ramp_id_stage_stats_table.html"
    ).render(
        root_path="",
        stage_stats_oob=False,
        overview={
            "pipeline_stage_stats": [{
                "pipeline_name": "Structure validation",
                "pipeline_key": "structure-validation",
                "run": None,
                "stages": stages,
                "expected_clique_assertions": {
                    "assertion_count": 0,
                    "rows": [],
                    "stages": [],
                },
            }],
        },
    )

    for html in (pipeline_html, stage_listing_html):
        assert "2 mixed" in html
        assert "3 fail" in html
        assert " gaps" not in html
        assert "Not computed" in html
        assert "/ramp-id-qa/stages/pre-rule#genericStructureValidation" in html
        assert "/ramp-id-qa/stages/post-rule#genericStructureValidation" in html
        assert "/ramp-id-qa/stages/legacy#genericStructureValidation" in html


def test_stage_comparison_aggregates_and_prioritizes_generic_structure_failures(monkeypatch):
    destination_warning = {
        "status": "inconsistent",
        "size": 3,
        "generic_member_count": 1,
        "specific_member_count": 1,
        "unknown_member_count": 1,
        "generic_member_samples": ["GENERIC:2"],
        "specific_member_samples": ["SPECIFIC:2"],
        "unknown_member_samples": ["UNKNOWN:2"],
        "boundary_edges": [{
            "start_id": "GENERIC:2",
            "end_id": "SPECIFIC:2",
            "synthetic": False,
        }],
        "comparison_ids": "GENERIC:2 SPECIFIC:2 UNKNOWN:2",
    }
    stages = {
        "left": {
            "_key": "left", "name": "From", "created_at": "2026-01-01", "rule_ids": [],
            "validation": {"generic_structure_consistency": {
                "computed": True, "inconsistent_clique_count": 1,
                "generic_unknown_clique_count": 0, "warnings": [],
            }},
        },
        "right": {
            "_key": "right", "name": "To", "created_at": "2026-01-02", "rule_ids": [],
            "validation": {"generic_structure_consistency": {
                "computed": True, "inconsistent_clique_count": 2,
                "generic_unknown_clique_count": 0, "warnings": [destination_warning],
            }},
        },
    }
    unchanged = {
        "clique_key": "unchanged", "clique_id": "unchanged", "size": 2,
        "rank_by_size": 1, "representative_id": "GENERIC:1",
        "member_ids": ["GENERIC:1", "SPECIFIC:1"], "signature": "GENERIC:1\nSPECIFIC:1",
        "generic_structure_status": "inconsistent", "generic_member_count": 1,
        "specific_member_count": 1, "unknown_member_count": 0,
        "generic_member_samples": ["GENERIC:1"], "specific_member_samples": ["SPECIFIC:1"],
        "unknown_member_samples": [],
    }
    left_changed = {
        "clique_key": "left-changed", "clique_id": "left-changed", "size": 2,
        "rank_by_size": 2, "representative_id": "GENERIC:2",
        "member_ids": ["GENERIC:2", "UNKNOWN:2"], "signature": "GENERIC:2\nUNKNOWN:2",
        "generic_structure_status": "generic_unknown", "generic_member_count": 1,
        "specific_member_count": 0, "unknown_member_count": 1,
        "generic_member_samples": ["GENERIC:2"], "specific_member_samples": [],
        "unknown_member_samples": ["UNKNOWN:2"],
    }
    right_changed = {
        "clique_key": "right-changed", "clique_id": "right-changed", "size": 3,
        "rank_by_size": 2, "representative_id": "GENERIC:2",
        "member_ids": ["GENERIC:2", "SPECIFIC:2", "UNKNOWN:2"],
        "signature": "GENERIC:2\nSPECIFIC:2\nUNKNOWN:2",
        "generic_structure_status": "inconsistent", "generic_member_count": 1,
        "specific_member_count": 1, "unknown_member_count": 1,
        "generic_member_samples": ["GENERIC:2"], "specific_member_samples": ["SPECIFIC:2"],
        "unknown_member_samples": ["UNKNOWN:2"],
    }

    monkeypatch.setattr(qa_app, "_get_harmonization_stage", lambda key: stages[key])
    monkeypatch.setattr(
        qa_app,
        "_load_stage_harmonized_member_sets",
        lambda key: [dict(unchanged), dict(left_changed if key == "left" else right_changed)],
    )
    monkeypatch.setattr(qa_app, "_load_metabolite_edge_removal_curations", lambda: {"assertions": []})
    monkeypatch.setattr(qa_app, "_load_expected_clique_assertion_results", lambda *_args: {"by_stage": {}})
    monkeypatch.setattr(qa_app, "_load_metabolite_identifier_display_map", lambda ids: {
        identifier: {"id": identifier, "label": identifier, "names": [], "prefix": identifier.split(":")[0]}
        for identifier in ids
    })
    monkeypatch.setattr(qa_app, "_list_harmonization_pipelines", lambda limit=100: [])
    monkeypatch.setattr(qa_app, "_list_harmonization_stages", lambda: list(stages.values()))
    monkeypatch.setattr(qa_app, "get_db", lambda _name: object())

    comparison = qa_app._load_metabolite_snapshot_comparison("left", "right")

    generic = comparison["generic_structure_validation"]
    assert generic["new_failure_count"] == 1
    assert generic["persistent_failure_count"] == 1
    assert generic["resolved_failure_count"] == 0
    assert generic["destination_warnings"] == [destination_warning]
    assert comparison["components"][0]["generic_structure_new_failure_count"] == 1
    assert comparison["components"][0]["review_ids"][:3] == [
        "GENERIC:2", "SPECIFIC:2", "UNKNOWN:2"
    ]

    from types import SimpleNamespace
    html = qa_app.templates.env.get_template("ramp_id_snapshot_compare.html").render(
        request=SimpleNamespace(scope={"path": "/ramp-id-qa/stage-comparison"}),
        root_path="",
        comparison=comparison,
        error=None,
        left_snapshot_key="left",
        right_snapshot_key="right",
        limit=100,
        metabolite_visuals_version="test",
    )
    assert 'class="btn metabolite-mark-generic-add"' in html
    assert 'data-metabolite-id="UNKNOWN:2"' in html
    assert 'class="btn metabolite-curation-add"' in html
    assert 'data-curation-start-id="GENERIC:2"' in html
    assert 'id="metaboliteCurationCart"' in html
