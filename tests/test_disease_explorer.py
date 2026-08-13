"""Regression tests for disease harmonization data integrity and new features."""
from __future__ import annotations

import pytest
from pathlib import Path

from src.qa_browser.disease_id_graph import (
    DiseaseGraphData,
    REVIEW_INTAKE_COLUMNS,
    _concept_to_row,
    _is_flagged,
    _match_type_to_skos,
    _match_source_to_semapv,
    compute_dashboard_stats,
    compute_source_agreement_matrix,
    compute_source_flow_data,
    compute_version_diff,
    export_sssom,
    find_concept_neighbors,
    build_provenance_chain,
    build_review_queue,
    build_disease_graph_payload,
    export_review_intake_template,
    resolve_concept,
    resolve_name_candidates,
    search_concepts,
)


# ---------------------------------------------------------------------------
# Fixture: load bundled v2.0.1 data (skip if not available)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def disease_data():
    """Load bundled disease data for testing."""
    # Import separately to avoid loading data at import time
    from src.qa_browser.disease_id_graph import load_disease_graph_data

    data_dir = Path(__file__).parent.parent / "src" / "qa_browser" / "data" / "disease_app_graph" / "v2.0.1"
    if not data_dir.is_dir():
        pytest.skip("Bundled disease data not available")
    # Reset singleton to allow re-loading in tests
    import src.qa_browser.disease_id_graph as mod
    mod._singleton = None
    return load_disease_graph_data(data_dir)


# ---------------------------------------------------------------------------
# Unit tests for helper functions (no data needed)
# ---------------------------------------------------------------------------

class TestMatchTypeToSkos:
    """Verify match_type -> SKOS predicate mapping."""

    def test_exact(self):
        assert _match_type_to_skos("exact") == "skos:exactMatch"

    def test_broad(self):
        assert _match_type_to_skos("broad") == "skos:broadMatch"

    def test_narrow(self):
        assert _match_type_to_skos("narrow") == "skos:narrowMatch"

    def test_related(self):
        assert _match_type_to_skos("related") == "skos:relatedMatch"

    def test_unknown_fallback(self):
        assert _match_type_to_skos("unknown") == "skos:closeMatch"

    def test_unclassified_fallback(self):
        assert _match_type_to_skos("unclassified") == "skos:closeMatch"

    def test_empty_fallback(self):
        assert _match_type_to_skos("") == "skos:closeMatch"


class TestMatchSourceToSemapv:
    """Verify match_type_source -> semapv justification mapping."""

    def test_lexical(self):
        assert _match_source_to_semapv("lexical_matching") == "semapv:LexicalMatching"

    def test_manual(self):
        assert _match_source_to_semapv("manual_curation") == "semapv:ManualMappingCuration"

    def test_unknown(self):
        assert _match_source_to_semapv("some_other_thing") == "semapv:UnspecifiedMatching"


class TestNeedsReviewDecisionFlags:
    """Verify concept-level triage decisions are separate from unresolved flags."""

    def test_auto_cleared_needs_review_is_not_flagged(self):
        data = DiseaseGraphData()
        concept = {
            "primary_xref": "MONDO:AUTO",
            "ncats_disease_id": "IFXDisease:AUTO",
            "standard_name": "example disease",
            "confidence_tier": "multi_source_supported",
            "confidence_tier_original": "needs_review",
            "needs_review_auto_cleared": "true",
            "needs_review_triage_bucket": "auto_clear_umls_proxy",
            "needs_review_triage_action": "set_confidence_tier_multi_source_supported",
            "needs_review_problem_namespaces": "UMLS",
            "evidence_note": "Only UMLS proxy noise drove the original review tier.",
            "cardinality_issue_count": "0",
            "obsolete_xref_count": "0",
            "has_obsolete": "false",
        }

        assert _is_flagged(concept, []) is False

        row = _concept_to_row("MONDO:AUTO", concept, data)
        assert row["flags"] == ""
        assert row["needs_review_decision"].startswith("auto-cleared:")
        assert row["needs_review_problem_namespaces"] == "UMLS"

    def test_review_retained_needs_review_is_flagged(self):
        data = DiseaseGraphData()
        concept = {
            "primary_xref": "MONDO:REVIEW",
            "ncats_disease_id": "IFXDisease:REVIEW",
            "standard_name": "example disease",
            "confidence_tier": "needs_review",
            "confidence_tier_original": "needs_review",
            "needs_review_auto_cleared": "false",
            "needs_review_triage_bucket": "review_gard_grouping_or_conflict",
            "needs_review_triage_action": "manual_review",
            "needs_review_problem_namespaces": "GARD",
            "cardinality_issue_count": "0",
            "obsolete_xref_count": "0",
            "has_obsolete": "false",
        }

        assert _is_flagged(concept, []) is True

        row = _concept_to_row("MONDO:REVIEW", concept, data)
        assert "needs_review" in row["flags"]
        assert "review_retained(review_gard_grouping_or_conflict)" in row["flags"]
        assert row["needs_review_decision"].startswith("review-retained:")


class TestReviewQueue:
    """Verify app review queue clustering and TargetGraph intake export."""

    def test_review_queue_groups_open_registry_decisions(self):
        data = DiseaseGraphData()
        data.concepts_by_pxref["MONDO:REVIEW"] = {
            "primary_xref": "MONDO:REVIEW",
            "ncats_disease_id": "IFXDisease:REVIEW",
            "standard_name": "example review disease",
            "confidence_tier": "needs_review",
            "cardinality_issue_count": "0",
            "obsolete_xref_count": "0",
            "has_obsolete": "false",
        }
        data.decisions_by_pxref["MONDO:REVIEW"].append({
            "decision_id": "doid_GARD_MONDO:REVIEW_conflict",
            "decision_source": "divergence_registry",
            "primary_xref": "MONDO:REVIEW",
            "ncats_disease_id": "IFXDisease:REVIEW",
            "standard_name": "example review disease",
            "xref_id": "GARD:123",
            "xref_namespace": "GARD",
            "decision_type": "conflict",
            "scenario_id": "S20",
            "status": "open",
            "auto_decision": "needs_expert_review",
            "auto_rationale": "Requires needs_expert_review.",
            "source_value": "GARD:123",
            "consensus_value": "GARD:456",
        })

        queue = build_review_queue(data)

        assert queue["total"] == 1
        row = queue["rows"][0]
        assert row["category"] == "gard_conflict"
        assert row["registry_id"] == "doid_GARD_MONDO:REVIEW_conflict"
        assert row["saveable"] is True

    def test_review_intake_template_has_targetgraph_columns(self):
        data = DiseaseGraphData()
        data.concepts_by_pxref["MONDO:REVIEW"] = {
            "primary_xref": "MONDO:REVIEW",
            "ncats_disease_id": "IFXDisease:REVIEW",
            "standard_name": "example review disease",
            "confidence_tier": "needs_review",
        }
        data.decisions_by_pxref["MONDO:REVIEW"].append({
            "decision_id": "registry-row-1",
            "decision_source": "divergence_registry",
            "xref_id": "EFO:1",
            "xref_namespace": "EFO",
            "decision_type": "obsolete_ref",
            "status": "open",
            "auto_rationale": "Obsolete xref.",
        })

        template = export_review_intake_template(data)
        header = template.splitlines()[0].split("\t")

        assert header == REVIEW_INTAKE_COLUMNS
        assert "registry-row-1" in template
        assert "Obsolete xref." in template


class TestDiseaseResolver:
    """Verify resolver ranking behavior on small in-memory fixtures."""

    def test_exact_base_name_beats_numbered_subtype(self):
        data = DiseaseGraphData()
        data.concepts_by_pxref["MONDO:BASE"] = {
            "primary_xref": "MONDO:BASE",
            "ncats_disease_id": "IFXDisease:BASE",
            "standard_name": "Alzheimer disease",
            "confidence_tier": "needs_review",
            "n_sources": "5",
        }
        data.concepts_by_pxref["MONDO:SUB"] = {
            "primary_xref": "MONDO:SUB",
            "ncats_disease_id": "IFXDisease:SUB",
            "standard_name": "Alzheimer disease 10",
            "confidence_tier": "multi_source_supported",
            "n_sources": "5",
        }

        result = resolve_name_candidates(data, "Alzheimer disease", limit=2)

        assert result["best"]["primary_xref"] == "MONDO:BASE"
        assert result["best"]["match_status"] == "exact"

    def test_bare_umls_cui_is_exact_only(self):
        data = DiseaseGraphData()
        data.concepts_by_pxref["MONDO:TARGET"] = {
            "primary_xref": "MONDO:TARGET",
            "ncats_disease_id": "IFXDisease:TARGET",
            "standard_name": "striate palmoplantar keratoderma",
            "confidence_tier": "multi_source_supported",
            "n_sources": "5",
        }
        data.concepts_by_pxref["MONDO:OTHER"] = {
            "primary_xref": "MONDO:OTHER",
            "ncats_disease_id": "IFXDisease:OTHER",
            "standard_name": "other disease",
            "confidence_tier": "multi_source_supported",
            "n_sources": "5",
        }
        data.edges_by_pxref["MONDO:TARGET"].append({
            "xref_id": "UMLS:C4707237",
            "xref_namespace": "UMLS",
            "xref_label": "Striate palmoplantar keratoderma",
            "match_type": "exact",
            "xref_confidence": "1.0",
        })
        data.edges_by_pxref["MONDO:OTHER"].append({
            "xref_id": "UMLS:C4707238",
            "xref_namespace": "UMLS",
            "xref_label": "Other disease",
            "match_type": "exact",
            "xref_confidence": "1.0",
        })

        result = resolve_name_candidates(data, "C4707237", limit=5)

        assert len(result["candidates"]) == 1
        assert result["best"]["primary_xref"] == "MONDO:TARGET"
        assert result["best"]["matched_term"] == "UMLS:C4707237"


class TestDiseaseGraphPayload:
    """Verify graph payload metadata and classes."""

    def test_phenotype_concepts_get_graph_class(self):
        data = DiseaseGraphData()
        data.concepts_by_pxref["MONDO:PHENO"] = {
            "primary_xref": "MONDO:PHENO",
            "ncats_disease_id": "IFXDisease:PHENO",
            "standard_name": "phenotypic feature example",
            "disease_type": "biolink:PhenotypicFeature",
        }

        payload = build_disease_graph_payload(data, ["MONDO:PHENO"])
        concept_node = next(
            element for element in payload["elements"]
            if element["data"]["id"] == "concept::MONDO:PHENO"
        )

        assert "concept" in concept_node["classes"].split()
        assert "phenotype-concept" in concept_node["classes"].split()

    def test_duplicate_resolved_obsolete_decisions_are_collapsed(self):
        data = DiseaseGraphData()
        data.concepts_by_pxref["MONDO:DUP"] = {
            "primary_xref": "MONDO:DUP",
            "ncats_disease_id": "IFXDisease:DUP",
            "standard_name": "duplicate obsolete decision example",
        }
        data.edges_by_pxref["MONDO:DUP"].append({
            "primary_xref": "MONDO:DUP",
            "xref_id": "GARD:OLD",
            "xref_namespace": "GARD",
            "xref_label": "obsolete GARD example",
            "match_type": "unclassified",
            "xref_confidence": "0.2",
        })
        data.decisions_by_pxref["MONDO:DUP"].extend([
            {
                "decision_id": "row_majority",
                "primary_xref": "MONDO:DUP",
                "xref_id": "GARD:OLD",
                "decision_type": "majority_dissent",
                "status": "resolved",
                "auto_decision": "omit_obsolete_xref",
                "resolution": "Remove obsolete xref",
            },
            {
                "decision_id": "row_obsolete",
                "primary_xref": "MONDO:DUP",
                "xref_id": "GARD:OLD",
                "decision_type": "obsolete_ref",
                "status": "resolved",
                "auto_decision": "omit_obsolete_xref",
                "resolution": "Remove obsolete xref",
            },
        ])

        payload = build_disease_graph_payload(data, ["MONDO:DUP"])
        decision_nodes = [
            element["data"]
            for element in payload["elements"]
            if element["data"].get("kind") == "decision"
        ]
        decision_edges = [
            element["data"]
            for element in payload["elements"]
            if element["data"].get("kind") == "decision_edge"
        ]
        xref_edge = next(
            element["data"]
            for element in payload["elements"]
            if element["data"].get("kind") == "xref_edge"
        )

        assert len(decision_nodes) == 1
        assert len(decision_edges) == 1
        assert decision_nodes[0]["decision_type"] == "resolved_obsolete_xref"
        assert decision_nodes[0]["raw_decision_type"] == "majority_dissent|obsolete_ref"
        assert decision_nodes[0]["source_decision_ids"] == "row_majority|row_obsolete"
        assert xref_edge["conflict_type"] == "resolved_obsolete_xref"
        assert xref_edge["raw_conflict_type"] == "majority_dissent|obsolete_ref"


class TestVersionDiff:
    """Verify version diffs expose category and source namespace changes."""

    def test_diff_tracks_biolink_categories_and_source_namespaces(self):
        baseline = DiseaseGraphData()
        current = DiseaseGraphData()
        baseline.manifest = {"pipeline_version": "1.0.1"}
        current.manifest = {"pipeline_version": "2.0.1"}

        baseline.concepts_by_pxref["MONDO:BASE"] = {
            "primary_xref": "MONDO:BASE",
            "standard_name": "base disease",
            "confidence_tier": "multi_source_supported",
            "overall_quality": "1.0",
            "disease_type": "biolink:Disease",
        }
        baseline.edges_by_pxref["MONDO:BASE"].append({
            "xref_id": "DOID:1",
            "xref_namespace": "DOID",
        })

        current.concepts_by_pxref["MONDO:BASE"] = {
            "primary_xref": "MONDO:BASE",
            "standard_name": "base disease",
            "confidence_tier": "multi_source_supported",
            "overall_quality": "1.0",
            "disease_type": "biolink:PhenotypicFeature",
        }
        current.concepts_by_pxref["MONDO:NEW"] = {
            "primary_xref": "MONDO:NEW",
            "standard_name": "new phenotype",
            "confidence_tier": "single_authoritative_source",
            "overall_quality": "1.0",
            "disease_type": "biolink:PhenotypicFeature",
        }
        current.edges_by_pxref["MONDO:BASE"].extend([
            {"xref_id": "DOID:1", "xref_namespace": "DOID"},
            {"xref_id": "EFO:1", "xref_namespace": "EFO"},
        ])
        current.edges_by_pxref["MONDO:NEW"].append({
            "xref_id": "SNOMEDCT:1",
            "xref_namespace": "SNOMEDCT",
        })

        diff = compute_version_diff(current, baseline)

        assert diff["summary"]["concepts_added"] == 1
        assert diff["summary"]["disease_type_changes"] == 1
        category_delta = {
            row["category"]: row["delta"]
            for row in diff["biolink_category_distribution"]["delta"]
        }
        assert category_delta["biolink:Disease"] == -1
        assert category_delta["biolink:PhenotypicFeature"] == 2
        namespace_delta = {
            row["namespace"]: row["delta"]
            for row in diff["source_namespace_distribution"]["delta"]
        }
        assert namespace_delta["EFO"] == 1
        assert namespace_delta["SNOMEDCT"] == 1
        assert diff["disease_type_changes"][0]["new_category"] == "biolink:PhenotypicFeature"

    def test_source_only_rows_are_excluded_from_retired_concepts(self):
        baseline = DiseaseGraphData()
        current = DiseaseGraphData()
        baseline.manifest = {"pipeline_version": "1.0.1"}
        current.manifest = {"pipeline_version": "2.0.1"}
        baseline.concepts_by_pxref["ICD10CM:T30.3"] = {
            "primary_xref": "ICD10CM:T30.3",
            "standard_name": "Partial deep dermal and full thickness burns",
            "harmonized_to_ontology": "false",
            "primary_xref_grounding_type": "other_namespace_fallback",
            "confidence_tier": "single_authoritative_source",
            "overall_quality": "1.0",
        }

        diff = compute_version_diff(current, baseline)

        assert diff["removed_concepts"] == []
        assert diff["summary"]["concepts_removed"] == 0
        assert diff["summary"]["legacy_source_only_removed"] == 1
        assert diff["legacy_source_only_rows"][0]["reason"] == (
            "Older single-source export row; not a harmonized concept in the current release"
        )

    def test_merged_concepts_remain_in_retired_concepts(self):
        baseline = DiseaseGraphData()
        current = DiseaseGraphData()
        baseline.manifest = {"pipeline_version": "1.0.1"}
        current.manifest = {"pipeline_version": "2.0.1"}
        baseline.concepts_by_pxref["GARD:3096"] = {
            "primary_xref": "GARD:3096",
            "standard_name": "Erythrokeratoderma variabilis progressiva",
            "confidence_tier": "single_authoritative_source",
            "overall_quality": "1.0",
        }
        baseline.edges_by_pxref["GARD:3096"].append({
            "xref_id": "UMLS:C0000001",
            "xref_namespace": "UMLS",
        })
        current.concepts_by_pxref["MONDO:0018853"] = {
            "primary_xref": "MONDO:0018853",
            "standard_name": "Erythrokeratoderma variabilis progressiva",
            "confidence_tier": "multi_source_supported",
            "overall_quality": "1.0",
        }
        current.edges_by_pxref["MONDO:0018853"].append({
            "xref_id": "UMLS:C0000001",
            "xref_namespace": "UMLS",
        })

        diff = compute_version_diff(current, baseline)

        assert diff["summary"]["concepts_removed"] == 1
        assert diff["removed_concepts"][0]["reason"] == "Merged into MONDO:0018853"

    def test_rekeyed_concept_detected_by_ncats_id(self):
        """A concept with the same ncats_disease_id but different primary_xref
        should appear as rekeyed, not as added+removed."""
        baseline = DiseaseGraphData()
        current = DiseaseGraphData()
        baseline.manifest = {"pipeline_version": "2.0.1"}
        current.manifest = {"pipeline_version": "2.1.1"}

        # Same concept, different primary_xref, same ncats_disease_id
        baseline.concepts_by_pxref["OMIM:612110"] = {
            "primary_xref": "OMIM:612110",
            "ncats_disease_id": "IFXDisease:00001",
            "standard_name": "Epilepsy example",
            "confidence_tier": "multi_source_supported",
            "overall_quality": "1.0",
            "disease_type": "biolink:Disease",
        }
        current.concepts_by_pxref["MEDGEN:393757"] = {
            "primary_xref": "MEDGEN:393757",
            "ncats_disease_id": "IFXDisease:00001",
            "standard_name": "Epilepsy example",
            "confidence_tier": "multi_source_supported",
            "overall_quality": "1.0",
            "disease_type": "biolink:Disease",
        }
        # An actually new concept
        current.concepts_by_pxref["MONDO:NEW1"] = {
            "primary_xref": "MONDO:NEW1",
            "ncats_disease_id": "IFXDisease:NEW1",
            "standard_name": "Truly new disease",
            "confidence_tier": "single_authoritative_source",
            "overall_quality": "1.0",
            "disease_type": "biolink:Disease",
        }
        # An actually removed concept (no ncats_disease_id match)
        baseline.concepts_by_pxref["ORPHANET:OLD1"] = {
            "primary_xref": "ORPHANET:OLD1",
            "ncats_disease_id": "IFXDisease:OLD1",
            "standard_name": "Truly removed disease",
            "confidence_tier": "single_authoritative_source",
            "overall_quality": "1.0",
            "disease_type": "biolink:Disease",
        }

        diff = compute_version_diff(current, baseline)

        # The rekeyed concept should NOT be in added or removed
        assert diff["summary"]["concepts_added"] == 1
        added_pxrefs = {c["primary_xref"] for c in diff["added_concepts"]}
        assert "MEDGEN:393757" not in added_pxrefs
        assert "MONDO:NEW1" in added_pxrefs

        # removed_pxrefs won't include OMIM:612110 since it was matched by ncats_id
        # ORPHANET:OLD1 should still be in removed (as other_removed or legacy)
        all_removed_pxrefs = (
            {c["primary_xref"] for c in diff["removed_concepts"]}
            | {c["primary_xref"] for c in diff["legacy_source_only_rows"]}
            | {c["primary_xref"] for c in diff["other_removed_rows"]}
        )
        assert "OMIM:612110" not in all_removed_pxrefs
        assert "ORPHANET:OLD1" in all_removed_pxrefs

        # The rekeyed concept should appear in rekeyed_concepts
        assert diff["summary"]["concepts_rekeyed"] == 1
        assert len(diff["rekeyed_concepts"]) == 1
        rekeyed = diff["rekeyed_concepts"][0]
        assert rekeyed["ncats_disease_id"] == "IFXDisease:00001"
        assert rekeyed["old_primary_xref"] == "OMIM:612110"
        assert rekeyed["new_primary_xref"] == "MEDGEN:393757"
        assert rekeyed["standard_name"] == "Epilepsy example"

    def test_rekeyed_concept_tier_change_detected(self):
        """A rekeyed concept with a tier change should appear in both
        rekeyed_concepts and tier_changes."""
        baseline = DiseaseGraphData()
        current = DiseaseGraphData()
        baseline.manifest = {"pipeline_version": "2.0.1"}
        current.manifest = {"pipeline_version": "2.1.1"}

        baseline.concepts_by_pxref["OMIM:100"] = {
            "primary_xref": "OMIM:100",
            "ncats_disease_id": "IFXDisease:TIER",
            "standard_name": "Tier change disease",
            "confidence_tier": "needs_review",
            "overall_quality": "0.5",
            "disease_type": "biolink:Disease",
        }
        current.concepts_by_pxref["MEDGEN:100"] = {
            "primary_xref": "MEDGEN:100",
            "ncats_disease_id": "IFXDisease:TIER",
            "standard_name": "Tier change disease",
            "confidence_tier": "multi_source_supported",
            "overall_quality": "1.0",
            "disease_type": "biolink:Disease",
        }

        diff = compute_version_diff(current, baseline)

        assert diff["summary"]["concepts_rekeyed"] == 1
        assert diff["summary"]["tier_changes"] == 1
        assert diff["tier_changes"][0]["primary_xref"] == "MEDGEN:100"
        assert diff["tier_changes"][0]["old_tier"] == "needs_review"
        assert diff["tier_changes"][0]["new_tier"] == "multi_source_supported"

    def test_truncation_metadata_present(self):
        """The diff response should include truncation metadata."""
        baseline = DiseaseGraphData()
        current = DiseaseGraphData()
        baseline.manifest = {"pipeline_version": "1.0.1"}
        current.manifest = {"pipeline_version": "2.0.1"}

        current.concepts_by_pxref["MONDO:1"] = {
            "primary_xref": "MONDO:1",
            "standard_name": "test",
            "confidence_tier": "multi_source_supported",
            "overall_quality": "1.0",
        }

        diff = compute_version_diff(current, baseline)

        assert "truncation" in diff
        t = diff["truncation"]
        assert "added_concepts" in t
        assert t["added_concepts"]["total"] == 1
        assert t["added_concepts"]["shown"] == 1
        assert "removed_concepts" in t
        assert "rekeyed_concepts" in t
        assert "tier_changes" in t
        assert "quality_changes" in t
        assert "disease_type_changes" in t
        assert "legacy_source_only_rows" in t
        assert "other_removed_rows" in t


# ---------------------------------------------------------------------------
# Regression tests against known data (require v2.0.1 dataset)
# ---------------------------------------------------------------------------

def test_type2_diabetes_has_omim_xref(disease_data):
    """MONDO:0005148 (type 2 diabetes) must have OMIM xref."""
    edges = disease_data.edges_by_pxref.get("MONDO:0005148", [])
    omim_edges = [e for e in edges if e.get("xref_namespace") == "OMIM"]
    assert len(omim_edges) > 0


def test_type2_diabetes_confidence_tier(disease_data):
    """MONDO:0005148 should have a known confidence tier."""
    concept = disease_data.concepts_by_pxref.get("MONDO:0005148")
    assert concept is not None
    valid_tiers = {
        "single_authoritative_source", "multi_source_supported",
        "needs_review", "review_validator_disagreement", "review_obsolete_xref",
    }
    assert concept["confidence_tier"] in valid_tiers


def test_total_concepts_in_range(disease_data):
    """Total concept count should be within expected range."""
    stats = compute_dashboard_stats(disease_data)
    assert 100_000 < stats["total_concepts"] < 200_000


def test_all_sources_present(disease_data):
    """Key source namespaces should have coverage in xref edges."""
    stats = compute_dashboard_stats(disease_data)
    expected = {"DOID", "OMIM", "Orphanet", "GARD"}
    actual = set(stats["source_coverage"].keys())
    assert expected.issubset(actual)


def test_dashboard_stats_cached(disease_data):
    """Dashboard stats should be cached after first call."""
    stats1 = compute_dashboard_stats(disease_data)
    stats2 = compute_dashboard_stats(disease_data)
    assert stats1 is stats2


# ---------------------------------------------------------------------------
# SSSOM export tests
# ---------------------------------------------------------------------------

def test_sssom_export_valid(disease_data):
    """SSSOM export should produce valid TSV with required columns."""
    content = export_sssom(disease_data)
    lines = [l for l in content.split("\n") if not l.startswith("#") and l.strip()]
    assert len(lines) > 1  # header + at least one data row
    header = lines[0].split("\t")
    assert "subject_id" in header
    assert "predicate_id" in header
    assert "object_id" in header
    assert "mapping_justification" in header
    assert "confidence" in header
    assert "subject_source" in header
    assert "object_source" in header


def test_sssom_metadata_header(disease_data):
    """SSSOM export should include metadata comment block."""
    content = export_sssom(disease_data)
    lines = content.split("\n")
    comment_lines = [l for l in lines if l.startswith("#")]
    assert any("curie_map" in l for l in comment_lines)
    assert any("mapping_set_id" in l for l in comment_lines)
    assert any("creator_label" in l for l in comment_lines)


def test_sssom_predicates_are_skos(disease_data):
    """All predicate_id values should be valid SKOS URIs."""
    content = export_sssom(disease_data)
    lines = [l for l in content.split("\n") if not l.startswith("#") and l.strip()]
    header = lines[0].split("\t")
    pred_idx = header.index("predicate_id")
    valid_predicates = {
        "skos:exactMatch", "skos:broadMatch", "skos:narrowMatch",
        "skos:relatedMatch", "skos:closeMatch",
    }
    # Check first 100 data rows
    for line in lines[1:101]:
        fields = line.split("\t")
        if len(fields) > pred_idx:
            assert fields[pred_idx] in valid_predicates, f"Invalid predicate: {fields[pred_idx]}"


def test_sssom_source_filtering(disease_data):
    """SSSOM export with include_sources should filter correctly."""
    content = export_sssom(disease_data, include_sources={"OMIM"})
    lines = [l for l in content.split("\n") if not l.startswith("#") and l.strip()]
    header = lines[0].split("\t")
    obj_src_idx = header.index("object_source")
    for line in lines[1:101]:
        fields = line.split("\t")
        if len(fields) > obj_src_idx:
            assert fields[obj_src_idx] == "OMIM"


# ---------------------------------------------------------------------------
# Resolve concept tests
# ---------------------------------------------------------------------------

def test_resolve_returns_xrefs(disease_data):
    """resolve_concept should return concept with xrefs list."""
    result = resolve_concept(disease_data, "MONDO:0005148")
    assert result is not None
    assert len(result["xrefs"]) > 0
    assert result["primary_xref"] == "MONDO:0005148"
    assert result["standard_name"] != ""


def test_resolve_returns_nodenorm(disease_data):
    """resolve_concept should include nodenorm data."""
    result = resolve_concept(disease_data, "MONDO:0005148")
    assert "nodenorm" in result
    assert "canonical_curie" in result["nodenorm"]


def test_resolve_unknown_returns_none(disease_data):
    """resolve_concept should return None for unknown CURIEs."""
    result = resolve_concept(disease_data, "FAKE:99999999")
    assert result is None


def test_resolve_by_ncats_id(disease_data):
    """resolve_concept should work with ncats_disease_id."""
    # Get an ncats_id from the data
    some_concept = next(iter(disease_data.concepts_by_ncats_id.values()))
    ncats_id = some_concept.get("ncats_disease_id", "")
    if ncats_id:
        result = resolve_concept(disease_data, ncats_id)
        assert result is not None


# ---------------------------------------------------------------------------
# Agreement matrix tests
# ---------------------------------------------------------------------------

def test_agreement_matrix_symmetric(disease_data):
    """Agreement matrix should be symmetric."""
    matrix_data = compute_source_agreement_matrix(disease_data)
    matrix = matrix_data["matrix"]
    n = len(matrix)
    for i in range(n):
        for j in range(n):
            assert matrix[i][j] == matrix[j][i], (
                f"Asymmetric at [{i}][{j}]: {matrix[i][j]} != {matrix[j][i]}"
            )


def test_agreement_matrix_has_sources(disease_data):
    """Agreement matrix should include major sources."""
    matrix_data = compute_source_agreement_matrix(disease_data)
    sources = set(matrix_data["sources"])
    # At least some of the major namespaces should be present
    assert len(sources) >= 4


def test_agreement_matrix_values_in_range(disease_data):
    """Jaccard overlap rates should be between 0 and 1."""
    matrix_data = compute_source_agreement_matrix(disease_data)
    matrix = matrix_data["matrix"]
    for row in matrix:
        for val in row:
            assert 0.0 <= val <= 1.0


def test_agreement_matrix_diagonal_is_one(disease_data):
    """Diagonal of Jaccard matrix should be 1.0 (self-overlap)."""
    matrix_data = compute_source_agreement_matrix(disease_data)
    matrix = matrix_data["matrix"]
    for i in range(len(matrix)):
        assert matrix[i][i] == 1.0


def test_agreement_matrix_no_duplicate_snomed(disease_data):
    """Namespace normalization should merge snomedct → SNOMEDCT."""
    matrix_data = compute_source_agreement_matrix(disease_data)
    sources = matrix_data["sources"]
    assert "snomedct" not in sources


# ---------------------------------------------------------------------------
# Source flow tests
# ---------------------------------------------------------------------------

def test_source_flows_structure(disease_data):
    """Source flow data should have nodes and links."""
    flow_data = compute_source_flow_data(disease_data)
    assert "nodes" in flow_data
    assert "links" in flow_data
    assert len(flow_data["nodes"]) > 0
    assert len(flow_data["links"]) > 0


def test_source_flows_node_types(disease_data):
    """Source flow nodes should be either 'source' or 'tier'."""
    flow_data = compute_source_flow_data(disease_data)
    for node in flow_data["nodes"]:
        assert node["type"] in ("source", "tier")


def test_source_flows_links_reference_valid_nodes(disease_data):
    """Source flow links should reference existing node IDs."""
    flow_data = compute_source_flow_data(disease_data)
    node_ids = {n["id"] for n in flow_data["nodes"]}
    for link in flow_data["links"]:
        assert link["source"] in node_ids, f"Invalid source: {link['source']}"
        assert link["target"] in node_ids, f"Invalid target: {link['target']}"


# ---------------------------------------------------------------------------
# Neighborhood expansion tests
# ---------------------------------------------------------------------------

def test_find_neighbors_returns_list(disease_data):
    """find_concept_neighbors should return a list of primary_xrefs."""
    neighbors = find_concept_neighbors(disease_data, "MONDO:0005148", max_neighbors=5)
    assert isinstance(neighbors, list)
    # MONDO:0005148 (type 2 diabetes) should have xrefs that other concepts share
    # It may or may not have neighbors depending on data overlap


def test_find_neighbors_excludes_self(disease_data):
    """find_concept_neighbors should not include the query concept itself."""
    pxref = "MONDO:0005148"
    neighbors = find_concept_neighbors(disease_data, pxref, max_neighbors=10)
    assert pxref not in neighbors


def test_find_neighbors_respects_max(disease_data):
    """find_concept_neighbors should respect max_neighbors limit."""
    neighbors = find_concept_neighbors(disease_data, "MONDO:0005148", max_neighbors=3)
    assert len(neighbors) <= 3


# ---------------------------------------------------------------------------
# Provenance chain tests
# ---------------------------------------------------------------------------

def test_provenance_chain_not_empty(disease_data):
    """build_provenance_chain should return steps for a known concept."""
    chain = build_provenance_chain(disease_data, "MONDO:0005148")
    assert len(chain) > 0


def test_provenance_has_source_assertions(disease_data):
    """Provenance chain should include source_assertion steps."""
    chain = build_provenance_chain(disease_data, "MONDO:0005148")
    assertion_steps = [s for s in chain if s["step"] == "source_assertion"]
    assert len(assertion_steps) > 0


def test_provenance_has_resolution(disease_data):
    """Provenance chain should end with a resolution step."""
    chain = build_provenance_chain(disease_data, "MONDO:0005148")
    resolution_steps = [s for s in chain if s["step"] == "resolution"]
    assert len(resolution_steps) == 1


def test_provenance_unknown_concept_empty(disease_data):
    """Provenance for unknown concept should return empty list."""
    chain = build_provenance_chain(disease_data, "FAKE:99999999")
    assert chain == []


# ---------------------------------------------------------------------------
# Search tests
# ---------------------------------------------------------------------------

def test_search_returns_results(disease_data):
    """search_concepts should return results for 'diabetes'."""
    result = search_concepts(disease_data, q="diabetes", page=1, per_page=10)
    assert result["total"] > 0
    assert len(result["rows"]) > 0


def test_search_pagination(disease_data):
    """search_concepts pagination should work correctly."""
    result = search_concepts(disease_data, q="", page=1, per_page=5)
    assert result["per_page"] == 5
    assert len(result["rows"]) == 5
    assert result["total_pages"] > 1
