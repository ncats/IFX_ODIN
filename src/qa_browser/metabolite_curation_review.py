"""Presentation read model for the active metabolite curation inventory."""

from __future__ import annotations

import json
import math
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlencode

from src.core.curations import (
    CHEBI_RECORD_PROPERTIES,
    METABOLITE_EQUIVALENCE_EDGES,
    METABOLITE_EXPECTED_CLIQUES,
    METABOLITE_MW_ADJUDICATIONS,
    METABOLITE_RECORD_PROPERTIES,
    METABOLITE_RECORD_SUPPRESSIONS,
    CurationSnapshot,
    ResolvedCurationBatch,
)
from src.core.record_property_curations import display_path


TYPE_LABELS = {
    CHEBI_RECORD_PROPERTIES: "ChEBI property correction",
    METABOLITE_EQUIVALENCE_EDGES: "Equivalence deny",
    METABOLITE_EXPECTED_CLIQUES: "Expected clique",
    METABOLITE_MW_ADJUDICATIONS: "Accepted MW discrepancy",
    METABOLITE_RECORD_PROPERTIES: "Metabolite property correction",
    METABOLITE_RECORD_SUPPRESSIONS: "Quarantined record",
}


@dataclass(frozen=True)
class ReviewFilters:
    query: str = ""
    curation_types: tuple[str, ...] = ()
    sources: tuple[str, ...] = ()
    batches: tuple[str, ...] = ()
    curators: tuple[str, ...] = ()
    page: int = 1
    page_size: int = 50


def _identifier_source(identifier: Any) -> Optional[str]:
    text = str(identifier or "").strip()
    if ":" not in text:
        return None
    return text.split(":", 1)[0].strip().upper() or None


def _sources(identifiers: Iterable[Any], path: Optional[list[Any]] = None) -> list[str]:
    values = {_identifier_source(identifier) for identifier in identifiers}
    for token in path or []:
        if not isinstance(token, dict):
            continue
        match = token.get("match") or {}
        if match.get("source"):
            values.add(str(match["source"]).strip().upper())
        if match.get("source_id"):
            values.add(_identifier_source(match["source_id"]))
    return sorted(value for value in values if value)


def _json_value(value: Any, *, missing: bool = False) -> str:
    if missing:
        return "Not present"
    if value is None:
        return "null"
    if isinstance(value, str):
        return value
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _compact_identifiers(identifiers: list[str], limit: int = 5) -> str:
    shown = identifiers[:limit]
    suffix = f" · +{len(identifiers) - limit} more" if len(identifiers) > limit else ""
    return " ".join(shown) + suffix


def _person_label(person: Optional[dict]) -> str:
    if not person:
        return "Not recorded"
    return str(person.get("name") or person.get("id") or "Not recorded")


def _batch_fields(snapshot: CurationSnapshot, batch_id: str) -> dict[str, Any]:
    batch: Optional[ResolvedCurationBatch] = snapshot.batch(batch_id)
    if batch is None:
        return {
            "batch_id": batch_id,
            "batch_name": batch_id,
            "curator": "Not recorded",
            "published_at": "",
            "origin": None,
        }
    source = batch.source or {}
    raw_author = source.get("raw_author") if isinstance(source.get("raw_author"), dict) else None
    return {
        "batch_id": batch.batch_id,
        "batch_name": batch.name or batch.batch_id,
        "batch_description": batch.description,
        "curator": _person_label(batch.created_by),
        "published_at": batch.published_at,
        "origin": {
            "type": source.get("type"),
            "repository": source.get("repository"),
            "path": source.get("path"),
            "commit": source.get("commit"),
            "authored_at": source.get("authored_at"),
            "attributed_curator": _person_label(source.get("attributed_curator")),
            "raw_author": _person_label(raw_author),
            "attribution_evidence": source.get("attribution_evidence"),
        } if source else None,
    }


def _base_row(snapshot: CurationSnapshot, batch_id: str, curation_type: str) -> dict:
    return {
        "curation_type": curation_type,
        "type_label": TYPE_LABELS[curation_type],
        **_batch_fields(snapshot, batch_id),
    }


def _review_operations(curation_type: str, snapshot: CurationSnapshot):
    if curation_type != METABOLITE_MW_ADJUDICATIONS:
        return snapshot.active_operations
    # MW review history can branch when a clique splits. Resolve the latest
    # command per exact reviewed evidence lineage instead of collapsing every
    # descendant onto the original anchor identifier.
    by_lineage = {}
    for resolved in snapshot.operations:
        operation = resolved.operation
        target = operation.get("target") or {}
        lineage = (
            target.get("anchor_id"),
            operation.get("observed_evidence_fingerprint"),
            tuple(operation.get("observed_member_ids") or []),
        )
        by_lineage[lineage] = resolved
    return list(by_lineage.values())


def _operation_rows(curation_type: str, snapshot: CurationSnapshot) -> list[dict]:
    rows = []
    for resolved in _review_operations(curation_type, snapshot):
        operation = resolved.operation
        action = operation.get("action")
        base = _base_row(snapshot, resolved.batch_id, curation_type)
        if curation_type == METABOLITE_EQUIVALENCE_EDGES and action == "remove_edge":
            identifiers = sorted((operation["start_id"], operation["end_id"]))
            rows.append({
                **base,
                "kind": "edge",
                "primary": " ↔ ".join(identifiers),
                "secondary": "Equivalence edge removed",
                "note": operation.get("note") or "No rationale recorded",
                "sources": _sources(identifiers),
                "review_query": urlencode({"denylist_pair": "|".join(identifiers)}),
            })
        elif curation_type == METABOLITE_RECORD_SUPPRESSIONS and action == "suppress_record":
            identifier = operation["target"]["id"]
            rows.append({
                **base,
                "kind": "record",
                "primary": identifier,
                "secondary": "Excluded from harmonization",
                "note": operation.get("note") or "No rationale recorded",
                "sources": _sources([identifier]),
                "review_query": urlencode({"id": identifier}),
            })
        elif curation_type == METABOLITE_EXPECTED_CLIQUES and action == "assert_same_clique":
            identifiers = operation.get("member_ids") or []
            rows.append({
                **base,
                "kind": "assertion",
                "primary": operation.get("name") or operation.get("assertion_id"),
                "secondary": _compact_identifiers(identifiers),
                "note": operation.get("rationale") or "No rationale recorded",
                "sources": _sources(identifiers),
                "review_query": urlencode({"id": " ".join(identifiers)}),
            })
        elif curation_type == METABOLITE_MW_ADJUDICATIONS and action == "accept_mw_discrepancy":
            identifiers = operation.get("observed_member_ids") or []
            target = operation.get("target") or {}
            rows.append({
                **base,
                "kind": "mw",
                "primary": target.get("finding_id") or target.get("anchor_id"),
                "secondary": _compact_identifiers(identifiers),
                "note": operation.get("note") or "No rationale recorded",
                "reason": operation.get("reason"),
                "sources": _sources(identifiers),
                "review_query": urlencode({"id": " ".join(identifiers)}),
                "review_path": "/ramp-id-qa/curations/mw-review?" + urlencode({
                    "finding_id": target.get("finding_id") or "",
                    "anchor_id": target.get("anchor_id") or "",
                    "id": " ".join(identifiers),
                }),
            })
    return rows


def _property_rows(curation_type: str, snapshot: CurationSnapshot) -> list[dict]:
    rows = []
    for decision in snapshot.active_record_property_decisions:
        if decision.mode != "set":
            continue
        identifier = decision.target["id"]
        base = _base_row(snapshot, decision.batch_id, curation_type)
        rows.append({
            **base,
            "kind": "property",
            "primary": identifier,
            "secondary": display_path(decision.path),
            "property_path": display_path(decision.path),
            "observed_value": _json_value(
                decision.observed_value,
                missing=not decision.observed_exists,
            ),
            "curated_value": _json_value(decision.value),
            "note": decision.source_operation.get("note") or "No rationale recorded",
            "sources": _sources([identifier], decision.path),
            "review_query": urlencode({"id": identifier}),
        })
    return rows


def active_curation_rows(
    snapshots: Mapping[str, CurationSnapshot],
) -> list[dict]:
    rows = []
    for curation_type in TYPE_LABELS:
        snapshot = snapshots.get(curation_type)
        if snapshot is None:
            continue
        if curation_type in {CHEBI_RECORD_PROPERTIES, METABOLITE_RECORD_PROPERTIES}:
            rows.extend(_property_rows(curation_type, snapshot))
        else:
            rows.extend(_operation_rows(curation_type, snapshot))
    return sorted(rows, key=lambda row: (
        row["type_label"].casefold(),
        row["primary"].casefold(),
        row.get("secondary", "").casefold(),
        row["batch_id"],
    ))


def _matches(row: dict, filters: ReviewFilters) -> bool:
    if filters.curation_types and row["curation_type"] not in filters.curation_types:
        return False
    if filters.sources and not set(filters.sources).intersection(row["sources"]):
        return False
    if filters.batches and row["batch_id"] not in filters.batches:
        return False
    if filters.curators and row["curator"] not in filters.curators:
        return False
    if filters.query:
        haystack = " ".join(str(value) for value in (
            row.get("primary"), row.get("secondary"), row.get("note"),
            row.get("batch_id"), row.get("batch_name"), row.get("curator"),
            " ".join(row.get("sources") or []),
        )).casefold()
        if filters.query.casefold() not in haystack:
            return False
    return True


def _facet(rows: list[dict], key: str, *, many: bool = False) -> list[dict]:
    counts: Counter[str] = Counter()
    for row in rows:
        values = row.get(key) or [] if many else [row.get(key)]
        counts.update(str(value) for value in values if value)
    return [{"value": value, "count": count} for value, count in sorted(
        counts.items(), key=lambda item: (-item[1], item[0].casefold())
    )]


def build_active_curation_review(
    snapshots: Mapping[str, CurationSnapshot],
    filters: ReviewFilters,
    *,
    stream_errors: Optional[Mapping[str, str]] = None,
) -> dict:
    all_rows = active_curation_rows(snapshots)
    filtered_rows = [row for row in all_rows if _matches(row, filters)]
    page_size = min(max(int(filters.page_size), 1), 200)
    page_count = max(1, math.ceil(len(filtered_rows) / page_size))
    page = min(max(int(filters.page), 1), page_count)
    start = (page - 1) * page_size
    query_pairs = []
    if filters.query:
        query_pairs.append(("q", filters.query))
    query_pairs.extend(("curation_type", value) for value in filters.curation_types)
    query_pairs.extend(("source", value) for value in filters.sources)
    query_pairs.extend(("batch", value) for value in filters.batches)
    query_pairs.extend(("curator", value) for value in filters.curators)
    page_query = lambda value: urlencode([*query_pairs, ("page", value)])
    batch_labels = {
        row["batch_id"]: row.get("batch_name") or row["batch_id"]
        for row in all_rows
    }
    return {
        "rows": filtered_rows[start:start + page_size],
        "total_count": len(all_rows),
        "filtered_count": len(filtered_rows),
        "page": page,
        "page_size": page_size,
        "page_count": page_count,
        "previous_query": page_query(page - 1) if page > 1 else None,
        "next_query": page_query(page + 1) if page < page_count else None,
        "filters": filters,
        "facets": {
            "curation_types": [
                {**item, "label": TYPE_LABELS.get(item["value"], item["value"])}
                for item in _facet(all_rows, "curation_type")
            ],
            "sources": _facet(all_rows, "sources", many=True),
            "batches": [
                {**item, "label": batch_labels.get(item["value"], item["value"])}
                for item in _facet(all_rows, "batch_id")
            ],
            "curators": _facet(all_rows, "curator"),
        },
        "type_counts": dict(Counter(row["type_label"] for row in all_rows)),
        "stream_errors": dict(stream_errors or {}),
    }
