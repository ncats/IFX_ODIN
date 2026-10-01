from src.core.curations import (
    CHEBI_RECORD_PROPERTIES,
    METABOLITE_EQUIVALENCE_EDGES,
    METABOLITE_MW_ADJUDICATIONS,
    CurationSnapshot,
    ResolvedCurationBatch,
    ResolvedCurationOperation,
    ResolvedRecordPropertyDecision,
)
from src.qa_browser.metabolite_curation_review import (
    ReviewFilters,
    active_curation_rows,
    build_active_curation_review,
)


def _batch(batch_id="batch-1"):
    return ResolvedCurationBatch(
        batch_id=batch_id,
        name="Legacy review",
        description="",
        created_at="2023-03-01T16:50:17-05:00",
        published_at="2026-10-01T12:00:00Z",
        created_by={"id": "john", "name": "John Braisted"},
        source={
            "type": "git_commit",
            "repository": "ncats/RaMP-backend-ncats",
            "path": "config/curation_mapping_issues_list.txt",
            "commit": "dc584fb19656642948a195d22eb7230503a2ab79",
            "authored_at": "2023-03-01T16:50:17-05:00",
            "raw_author": {"name": "johnbraisted", "email": "jb212828@gmail.com"},
        },
        object_key=f"curations/v2/test/batches/{batch_id}.json",
        sha256="a" * 64,
    )


def _snapshot(curation_type, *, operations=(), property_decisions=()):
    batch = _batch()
    return CurationSnapshot(
        curation_type=curation_type,
        manifest_revision=1,
        manifest_hash="manifest",
        batch_ids=[batch.batch_id],
        batch_hashes=[batch.sha256],
        operations=list(operations),
        fingerprint="fingerprint",
        source_uri="s3://test/manifest.json",
        batches_by_id={batch.batch_id: batch},
        _active_by_subject={
            ("row", str(index)): operation
            for index, operation in enumerate(operations)
        },
        _active_record_property_decisions={
            decision.subject: decision for decision in property_decisions
        },
    )


def test_active_review_includes_edge_and_one_row_per_property_path():
    edge = ResolvedCurationOperation(
        curation_type=METABOLITE_EQUIVALENCE_EDGES,
        operation={
            "action": "remove_edge",
            "edge_type": "MetaboliteIdentifierMappingEdge",
            "start_id": "CHEBI:1",
            "end_id": "HMDB:2",
            "symmetric": True,
            "note": "Different compounds",
        },
        batch_id="batch-1",
        published_at="2026-10-01T12:00:00Z",
        published_by={"id": "john", "name": "John Braisted"},
    )
    source_operation = {"note": "Fix the ChEBI structure"}
    decisions = [
        ResolvedRecordPropertyDecision(
            curation_type=CHEBI_RECORD_PROPERTIES,
            target={
                "kind": "node", "curation_set": "chebi",
                "model_type": "ChemicalEntity", "id": "CHEBI:139244",
            },
            path=[field], mode="set", value=value,
            observed_value=old, observed_exists=True,
            batch_id="batch-1", published_at="2026-10-01T12:00:00Z",
            published_by={"id": "keith", "name": "Keith Kelleher"},
            source_operation=source_operation,
        )
        for field, value, old in (
            ("formula", "C32H38D6F6O4", "C38H56F6O4"),
            ("mass", 612.7246, 696.9),
        )
    ]
    snapshots = {
        METABOLITE_EQUIVALENCE_EDGES: _snapshot(
            METABOLITE_EQUIVALENCE_EDGES, operations=[edge]
        ),
        CHEBI_RECORD_PROPERTIES: _snapshot(
            CHEBI_RECORD_PROPERTIES, property_decisions=decisions
        ),
    }

    rows = active_curation_rows(snapshots)

    assert len(rows) == 3
    edge_row = next(row for row in rows if row["kind"] == "edge")
    assert edge_row["sources"] == ["CHEBI", "HMDB"]
    assert edge_row["batch_name"] == "Legacy review"
    assert edge_row["origin"]["commit"].startswith("dc584fb")
    property_rows = [row for row in rows if row["kind"] == "property"]
    assert {row["property_path"] for row in property_rows} == {"formula", "mass"}


def test_filters_are_or_within_facets_and_and_across_facets():
    operations = [
        ResolvedCurationOperation(
            curation_type=METABOLITE_EQUIVALENCE_EDGES,
            operation={
                "action": "remove_edge", "edge_type": "MetaboliteIdentifierMappingEdge",
                "start_id": left, "end_id": right, "symmetric": True, "note": note,
            },
            batch_id="batch-1", published_at="2026-10-01T12:00:00Z",
            published_by={"id": "john", "name": "John Braisted"},
        )
        for left, right, note in (
            ("CHEBI:1", "HMDB:1", "alpha"),
            ("KEGG.COMPOUND:C1", "REFMET:RM1", "beta"),
        )
    ]
    snapshot = _snapshot(METABOLITE_EQUIVALENCE_EDGES, operations=operations)

    result = build_active_curation_review(
        {METABOLITE_EQUIVALENCE_EDGES: snapshot},
        ReviewFilters(sources=("CHEBI", "KEGG.COMPOUND"), batches=("batch-1",)),
    )

    assert result["filtered_count"] == 2
    assert {item["value"] for item in result["facets"]["sources"]} == {
        "CHEBI", "HMDB", "KEGG.COMPOUND", "REFMET",
    }


def test_neutral_active_commands_are_not_presented():
    retained = ResolvedCurationOperation(
        curation_type=METABOLITE_EQUIVALENCE_EDGES,
        operation={
            "action": "retain_edge", "edge_type": "MetaboliteIdentifierMappingEdge",
            "start_id": "CHEBI:1", "end_id": "HMDB:1", "symmetric": True,
        },
        batch_id="batch-1", published_at="2026-10-01T12:00:00Z", published_by=None,
    )
    snapshot = _snapshot(METABOLITE_EQUIVALENCE_EDGES, operations=[retained])

    assert active_curation_rows({METABOLITE_EQUIVALENCE_EDGES: snapshot}) == []


def test_mw_review_rows_resolve_each_exact_evidence_lineage():
    def resolved(action, fingerprint, members):
        return ResolvedCurationOperation(
            curation_type=METABOLITE_MW_ADJUDICATIONS,
            operation={
                "action": action,
                "target": {"anchor_id": "CHEBI:1", "finding_id": "mw-" + "1" * 24},
                "observed_evidence_fingerprint": fingerprint,
                "observed_member_ids": members,
                "note": "Reviewed",
                "reason": "salt_or_counterion",
            },
            batch_id="batch-1", published_at="2026-10-01T12:00:00Z",
            published_by=None,
        )

    first = resolved("accept_mw_discrepancy", "a" * 64, ["CHEBI:1", "HMDB:1"])
    descendant = resolved(
        "accept_mw_discrepancy", "b" * 64, ["CHEBI:1", "HMDB:1", "CAS:1"]
    )
    reopened = resolved("reopen_mw_discrepancy", "a" * 64, ["CHEBI:1", "HMDB:1"])
    snapshot = _snapshot(
        METABOLITE_MW_ADJUDICATIONS,
        operations=[first, descendant, reopened],
    )

    rows = active_curation_rows({METABOLITE_MW_ADJUDICATIONS: snapshot})

    assert len(rows) == 1
    assert rows[0]["sources"] == ["CAS", "CHEBI", "HMDB"]
    assert rows[0]["review_path"].startswith(
        "/ramp-id-qa/curations/mw-review?finding_id=mw-"
    )
