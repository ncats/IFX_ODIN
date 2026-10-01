"""Low-impact reclamation of immutable metabolite harmonization stages."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Optional


STAGE_COLLECTION = "HarmonizationStage"
RUN_COLLECTION = "HarmonizationPipelineRun"
ARTIFACT_COLLECTIONS = (
    "HarmonizedMetaboliteMemberEdge",
    "HarmonizedMetabolite",
    "HarmonizationStageEvidenceEdge",
    "HarmonizationStageActiveIdentifierChunk",
)


def stage_artifact_key_bounds(stage_key: str) -> tuple[str, str]:
    """Return primary-key range bounds for every artifact owned by a stage."""
    return f"{stage_key}-", f"{stage_key}."


def delete_stage_artifacts(
    db,
    stage_key: str,
    *,
    batch_size: int = 2000,
    batch_pause_seconds: float = 0.02,
) -> dict[str, int]:
    """Delete one stage's artifacts in bounded primary-index range batches."""
    lower, upper = stage_artifact_key_bounds(stage_key)
    deleted_counts = {}
    for collection_name in ARTIFACT_COLLECTIONS:
        if not db.has_collection(collection_name):
            continue
        deleted_count = 0
        while True:
            deleted = list(db.aql.execute(
                """
                LET keys = (
                  FOR d IN @@collection
                    FILTER d._key >= @lower AND d._key < @upper
                    SORT d._key
                    LIMIT @batch_size
                    RETURN d._key
                )
                FOR key IN keys
                  REMOVE key IN @@collection
                  RETURN OLD._key
                """,
                bind_vars={
                    "@collection": collection_name,
                    "lower": lower,
                    "upper": upper,
                    "batch_size": batch_size,
                },
                max_runtime=120,
            ))
            deleted_count += len(deleted)
            if len(deleted) < batch_size:
                break
            if batch_pause_seconds > 0:
                time.sleep(batch_pause_seconds)
        deleted_counts[collection_name] = deleted_count
    return deleted_counts


def claim_orphan_stage(
    db,
    *,
    grace_period: timedelta,
    now: Optional[datetime] = None,
) -> Optional[dict]:
    """Resume a tombstone or atomically claim the oldest eligible orphan."""
    timestamp = now or datetime.now(timezone.utc)
    cutoff = (timestamp - grace_period).isoformat()
    claimed_at = timestamp.isoformat()
    rows = list(db.aql.execute(
        f"""
        LET candidate = FIRST(
          FOR s IN {STAGE_COLLECTION}
            FILTER s.status == "deleting" OR (
              s.status == "complete"
              AND (s.updated_at != null OR s.created_at != null)
              AND (s.updated_at != null ? s.updated_at : s.created_at) < @orphaned_before
            )
            LET referenced = FIRST(
              FOR r IN {RUN_COLLECTION}
                FILTER s._key IN (r.stage_keys || [])
                LIMIT 1
                RETURN true
            )
            FILTER referenced == null
            SORT s.status == "deleting" DESC,
                 NOT_NULL(s.deletion_started_at, s.updated_at, s.created_at, "") ASC,
                 s._key
            LIMIT 1
            RETURN s
        )
        FILTER candidate != null
        UPDATE candidate WITH {{
          status: "deleting",
          deletion_started_at: NOT_NULL(candidate.deletion_started_at, @claimed_at),
          deletion_last_attempt_at: @claimed_at,
          deletion_attempt_count: NOT_NULL(candidate.deletion_attempt_count, 0) + 1,
          deletion_error: null
        }} IN {STAGE_COLLECTION}
        RETURN NEW
        """,
        bind_vars={
            "orphaned_before": cutoff,
            "claimed_at": claimed_at,
        },
        max_runtime=120,
    ))
    return rows[0] if rows else None


def delete_one_orphan_stage(
    db,
    *,
    grace_period: timedelta,
    now: Optional[datetime] = None,
    batch_size: int = 2000,
    batch_pause_seconds: float = 0.02,
) -> Optional[dict]:
    """Claim and fully delete at most one orphan stage, resuming after failure."""
    stage = claim_orphan_stage(db, grace_period=grace_period, now=now)
    if stage is None:
        return None
    stage_key = stage["_key"]
    try:
        deleted_counts = delete_stage_artifacts(
            db,
            stage_key,
            batch_size=batch_size,
            batch_pause_seconds=batch_pause_seconds,
        )
        stage_collection = db.collection(STAGE_COLLECTION)
        current = stage_collection.get(stage_key)
        if current and current.get("status") == "deleting":
            stage_collection.delete(stage_key)
        return {"stage_key": stage_key, "deleted_counts": deleted_counts}
    except Exception as exc:
        failed_at = datetime.now(timezone.utc).isoformat()
        try:
            db.collection(STAGE_COLLECTION).update({
                "_key": stage_key,
                "status": "deleting",
                "deletion_failed_at": failed_at,
                "deletion_error": str(exc),
            })
        except Exception:
            pass
        raise
