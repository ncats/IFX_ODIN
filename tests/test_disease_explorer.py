"""Regression tests for disease harmonization data integrity and new features."""
from __future__ import annotations

import pytest
from pathlib import Path

from src.qa_browser.disease_id_graph import (
    DiseaseGraphData,
    _match_type_to_skos,
    _match_source_to_semapv,
    compute_dashboard_stats,
    compute_source_agreement_matrix,
    compute_source_flow_data,
    export_sssom,
    find_concept_neighbors,
    build_provenance_chain,
    resolve_concept,
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
