from datetime import datetime, timedelta, timezone

import pytest

from src.qa_browser import harmonization_stage_maintenance as maintenance


def test_stage_artifact_key_bounds_select_only_the_stage_prefix():
    assert maintenance.stage_artifact_key_bounds("stage-07-abc") == (
        "stage-07-abc-",
        "stage-07-abc.",
    )


def test_delete_stage_artifacts_uses_primary_key_ranges_and_small_batches(monkeypatch):
    calls = []
    responses = {
        "HarmonizedMetaboliteMemberEdge": [["a", "b"], ["c"]],
        "HarmonizedMetabolite": [[]],
        "HarmonizationStageEvidenceEdge": [[]],
        "HarmonizationStageActiveIdentifierChunk": [[]],
    }

    class FakeAql:
        def execute(self, query, bind_vars=None, **kwargs):
            calls.append((query, bind_vars, kwargs))
            return responses[bind_vars["@collection"]].pop(0)

    class FakeDb:
        aql = FakeAql()

        @staticmethod
        def has_collection(_name):
            return True

    pauses = []
    monkeypatch.setattr(maintenance.time, "sleep", pauses.append)

    counts = maintenance.delete_stage_artifacts(
        FakeDb(),
        "stage-07-abc",
        batch_size=2,
        batch_pause_seconds=0.05,
    )

    assert counts["HarmonizedMetaboliteMemberEdge"] == 3
    assert pauses == [0.05]
    assert all("FILTER d._key >= @lower AND d._key < @upper" in query for query, _, _ in calls)
    assert all(call[1]["lower"] == "stage-07-abc-" for call in calls)
    assert all(call[1]["upper"] == "stage-07-abc." for call in calls)
    assert all(call[1]["batch_size"] == 2 for call in calls)


def test_claim_orphan_stage_claims_complete_or_resumes_deleting_stage():
    captured = {}

    class FakeAql:
        @staticmethod
        def execute(query, bind_vars=None, **kwargs):
            captured.update(query=query, bind_vars=bind_vars, kwargs=kwargs)
            return [{"_key": "stage-old", "status": "deleting"}]

    class FakeDb:
        aql = FakeAql()

    claimed = maintenance.claim_orphan_stage(
        FakeDb(),
        grace_period=timedelta(hours=1),
        now=datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc),
    )

    assert claimed["_key"] == "stage-old"
    assert captured["bind_vars"] == {
        "orphaned_before": "2026-10-01T11:00:00+00:00",
        "claimed_at": "2026-10-01T12:00:00+00:00",
    }
    assert 's.status == "deleting"' in captured["query"]
    assert "FILTER referenced == null" in captured["query"]
    assert 'status: "deleting"' in captured["query"]


def test_delete_one_orphan_stage_deletes_stage_document_last(monkeypatch):
    events = []

    class FakeStages:
        @staticmethod
        def get(stage_key):
            events.append(("get", stage_key))
            return {"_key": stage_key, "status": "deleting"}

        @staticmethod
        def delete(stage_key):
            events.append(("delete-stage", stage_key))

    class FakeDb:
        @staticmethod
        def collection(name):
            assert name == maintenance.STAGE_COLLECTION
            return FakeStages()

    monkeypatch.setattr(
        maintenance,
        "claim_orphan_stage",
        lambda *_args, **_kwargs: {"_key": "stage-old", "status": "deleting"},
    )

    def fake_delete_artifacts(_db, stage_key, **_kwargs):
        events.append(("delete-artifacts", stage_key))
        return {"HarmonizedMetabolite": 12}

    monkeypatch.setattr(maintenance, "delete_stage_artifacts", fake_delete_artifacts)

    result = maintenance.delete_one_orphan_stage(
        FakeDb(), grace_period=timedelta(hours=1)
    )

    assert result == {
        "stage_key": "stage-old",
        "deleted_counts": {"HarmonizedMetabolite": 12},
    }
    assert events == [
        ("delete-artifacts", "stage-old"),
        ("get", "stage-old"),
        ("delete-stage", "stage-old"),
    ]


def test_delete_one_orphan_stage_leaves_retryable_tombstone_on_failure(monkeypatch):
    updates = []

    class FakeStages:
        @staticmethod
        def update(document):
            updates.append(document)

    class FakeDb:
        @staticmethod
        def collection(_name):
            return FakeStages()

    monkeypatch.setattr(
        maintenance,
        "claim_orphan_stage",
        lambda *_args, **_kwargs: {"_key": "stage-old", "status": "deleting"},
    )
    monkeypatch.setattr(
        maintenance,
        "delete_stage_artifacts",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("temporary failure")),
    )

    with pytest.raises(RuntimeError, match="temporary failure"):
        maintenance.delete_one_orphan_stage(
            FakeDb(), grace_period=timedelta(hours=1)
        )

    assert updates[0]["_key"] == "stage-old"
    assert updates[0]["status"] == "deleting"
    assert updates[0]["deletion_error"] == "temporary failure"
