"""Disease harmonizer QA graph — loads app_graph TSVs and builds Cytoscape payloads."""
from __future__ import annotations

import csv
import io
import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from fastapi import HTTPException


class DiseaseGraphData:
    """Indexes built from the four app_graph TSV files."""

    __slots__ = (
        "concepts_by_pxref",
        "concepts_by_ncats_id",
        "edges_by_pxref",
        "labels_by_xref_id",
        "decisions_by_pxref",
        "manifest",
        "_dashboard_stats",
        "_xref_to_pxrefs",
    )

    def __init__(self) -> None:
        self.concepts_by_pxref: dict[str, dict] = {}
        self.concepts_by_ncats_id: dict[str, dict] = {}
        self.edges_by_pxref: dict[str, list[dict]] = defaultdict(list)
        self.labels_by_xref_id: dict[str, dict] = {}
        self.decisions_by_pxref: dict[str, list[dict]] = defaultdict(list)
        self.manifest: dict = {}
        self._dashboard_stats: dict | None = None
        self._xref_to_pxrefs: dict[str, set[str]] | None = None


_singleton: DiseaseGraphData | None = None


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def load_disease_graph_data(data_dir: str | Path) -> DiseaseGraphData:
    """Load TSVs + manifest, build indexes, cache as module singleton."""
    global _singleton
    if _singleton is not None:
        return _singleton

    data_dir = Path(data_dir)
    if not data_dir.is_dir():
        raise HTTPException(status_code=500, detail=f"Disease app_graph dir not found: {data_dir}")

    data = DiseaseGraphData()

    manifest_path = data_dir / "manifest.json"
    if manifest_path.exists():
        with open(manifest_path, encoding="utf-8") as fh:
            data.manifest = json.load(fh)

    # --- concepts ---
    for row in _read_tsv(data_dir / "disease_concepts.tsv"):
        pxref = row.get("primary_xref", "")
        ncats_id = row.get("ncats_disease_id", "")
        if pxref:
            data.concepts_by_pxref[pxref] = row
        if ncats_id:
            data.concepts_by_ncats_id[ncats_id] = row

    # --- xref edges ---
    for row in _read_tsv(data_dir / "disease_xref_edges.tsv"):
        pxref = row.get("primary_xref", "")
        if pxref:
            data.edges_by_pxref[pxref].append(row)

    # --- xref labels ---
    for row in _read_tsv(data_dir / "xref_labels.tsv"):
        xref_id = row.get("xref_id", "")
        if xref_id:
            data.labels_by_xref_id[xref_id] = row

    # --- review decisions ---
    for row in _read_tsv(data_dir / "review_decisions.tsv"):
        pxref = row.get("primary_xref", "")
        if pxref:
            data.decisions_by_pxref[pxref].append(row)

    _singleton = data
    n_concepts = len(data.concepts_by_pxref)
    n_edges = sum(len(v) for v in data.edges_by_pxref.values())
    n_labels = len(data.labels_by_xref_id)
    n_decisions = sum(len(v) for v in data.decisions_by_pxref.values())
    print(
        f"Disease graph loaded: {n_concepts:,} concepts, {n_edges:,} edges, "
        f"{n_labels:,} labels, {n_decisions:,} decisions"
    )
    return data


def compute_dashboard_stats(data: DiseaseGraphData) -> dict[str, Any]:
    """Aggregate dashboard stats, computed once and cached on the singleton."""
    if data._dashboard_stats is not None:
        return data._dashboard_stats

    total_concepts = len(data.concepts_by_pxref)
    total_edges = sum(len(v) for v in data.edges_by_pxref.values())
    total_labels = len(data.labels_by_xref_id)
    total_decisions = sum(len(v) for v in data.decisions_by_pxref.values())

    confidence_dist: Counter[str] = Counter()
    quality_dist: Counter[str] = Counter()
    source_coverage: Counter[str] = Counter()
    nodenorm_dist: Counter[str] = Counter()
    rare_count = 0
    flagged_count = 0
    total_xrefs = 0

    for pxref, concept in data.concepts_by_pxref.items():
        tier = concept.get("confidence_tier", "").strip()
        confidence_dist[tier if tier else "unknown"] += 1

        quality = concept.get("overall_quality", "").strip()
        quality_dist[quality if quality else "unknown"] += 1

        nn_status = concept.get("nodenorm_validation_status", "").strip()
        nodenorm_dist[nn_status if nn_status else "unknown"] += 1

        if concept.get("is_rare", "").lower() == "true":
            rare_count += 1

        xc = int(concept.get("xref_count") or 0)
        total_xrefs += xc

        # Flagged?
        decisions = data.decisions_by_pxref.get(pxref, [])
        if _is_flagged(concept, decisions):
            flagged_count += 1

        # Source namespace coverage from edges
        seen_ns: set[str] = set()
        for edge in data.edges_by_pxref.get(pxref, []):
            ns = _normalize_ns(edge.get("xref_namespace", "").strip())
            if ns:
                seen_ns.add(ns)
        for ns in seen_ns:
            source_coverage[ns] += 1

    avg_xrefs = round(total_xrefs / total_concepts, 1) if total_concepts else 0

    stats = {
        "total_concepts": total_concepts,
        "total_edges": total_edges,
        "total_labels": total_labels,
        "total_decisions": total_decisions,
        "confidence_distribution": dict(confidence_dist.most_common()),
        "quality_distribution": dict(quality_dist.most_common()),
        "source_coverage": dict(source_coverage.most_common()),
        "nodenorm_distribution": dict(nodenorm_dist.most_common()),
        "rare_count": rare_count,
        "flagged_count": flagged_count,
        "avg_xrefs_per_concept": avg_xrefs,
    }
    data._dashboard_stats = stats
    return stats


def parse_disease_ids(raw: str) -> list[str]:
    """Parse user input into a list of primary_xref or ncats_disease_id strings."""
    ids: list[str] = []
    for token in (raw or "").replace("|", " ").replace(",", " ").split():
        token = token.strip()
        if token and token not in ids:
            ids.append(token)
    return ids


def _resolve_to_pxref(data: DiseaseGraphData, query_id: str) -> str | None:
    """Resolve a query ID to a primary_xref. Accepts primary_xref or ncats_disease_id."""
    if query_id in data.concepts_by_pxref:
        return query_id
    concept = data.concepts_by_ncats_id.get(query_id)
    if concept:
        return concept.get("primary_xref")
    return None


def build_disease_graph_payload(data: DiseaseGraphData, query_ids: list[str]) -> dict[str, Any]:
    """Build Cytoscape elements for the given disease IDs."""
    if not query_ids:
        raise HTTPException(status_code=400, detail="Provide at least one disease ID.")

    concept_nodes: dict[str, dict] = {}
    xref_nodes: dict[str, dict] = {}
    decision_nodes: dict[str, dict] = {}
    edges: dict[str, dict] = {}
    missing: list[str] = []
    namespaces: set[str] = set()

    for qid in query_ids:
        pxref = _resolve_to_pxref(data, qid)
        if pxref is None:
            missing.append(qid)
            continue

        concept = data.concepts_by_pxref[pxref]
        concept_node_id = f"concept::{pxref}"
        standard_name = concept.get("standard_name", "")
        concept_label = f"{pxref}\n{standard_name}" if standard_name else pxref
        concept_nodes[pxref] = {
            "data": {
                "id": concept_node_id,
                "label": concept_label,
                "kind": "concept",
                "standard_name": concept.get("standard_name", ""),
                "ncats_disease_id": concept.get("ncats_disease_id", ""),
                "confidence_tier": concept.get("confidence_tier", ""),
                "overall_quality": concept.get("overall_quality", ""),
                "n_sources": concept.get("n_sources", ""),
                "primary_sources": concept.get("primary_sources", ""),
                "disease_type": concept.get("disease_type", ""),
                "is_rare": concept.get("is_rare", ""),
                "xref_count": concept.get("xref_count", ""),
                "cardinality_issue_count": concept.get("cardinality_issue_count", ""),
                "obsolete_xref_count": concept.get("obsolete_xref_count", ""),
                "nodenorm_canonical_curie": concept.get("nodenorm_canonical_curie", ""),
            },
            "classes": "concept",
        }

        # --- build decision lookup keyed by xref_id for this concept ---
        _dec_by_xref: dict[str, list[dict]] = defaultdict(list)
        for dec in data.decisions_by_pxref.get(pxref, []):
            dxid = dec.get("xref_id", "")
            if dxid:
                _dec_by_xref[dxid].append(dec)

        # --- xref edges ---
        for edge_row in data.edges_by_pxref.get(pxref, []):
            xref_id = edge_row.get("xref_id", "")
            if not xref_id:
                continue
            ns = _normalize_ns(edge_row.get("xref_namespace", "")).lower()
            namespaces.add(ns)
            xref_node_id = f"xref::{xref_id}"
            label_row = data.labels_by_xref_id.get(xref_id, {})
            raw_label = edge_row.get("xref_label", "") or label_row.get("preferred_label", "")
            pref_label = "" if raw_label in ("nan", "NaN") else raw_label
            xref_display = f"{xref_id}\n{pref_label}" if pref_label else xref_id
            xref_nodes.setdefault(xref_id, {
                "data": {
                    "id": xref_node_id,
                    "label": xref_display,
                    "kind": "xref",
                    "xref_namespace": _normalize_ns(edge_row.get("xref_namespace", "")),
                    "xref_label": pref_label,
                    "is_obsolete": label_row.get("is_obsolete", ""),
                    "obsolete_detail": label_row.get("obsolete_detail", ""),
                    "label_source": label_row.get("label_source", ""),
                    "lookup_status": label_row.get("lookup_status", ""),
                },
                "classes": f"xref {ns}",
            })

            match_type = edge_row.get("match_type", "unclassified")
            label_sim = edge_row.get("label_similarity", "")
            xref_conf = edge_row.get("xref_confidence", "")
            if xref_conf and xref_conf != "0.0":
                edge_label = f"{match_type} [{xref_conf}]"
            elif label_sim and label_sim != "0.0":
                edge_label = f"{match_type} ({label_sim})"
            else:
                edge_label = match_type

            # --- enrich edge with conflict/resolution data ---
            edge_decs = _dec_by_xref.get(xref_id, [])
            has_conflict = ""
            conflict_status = ""
            conflict_type = ""
            conflict_resolution = ""
            conflict_detail = ""
            edge_classes = f"match-{match_type}"

            if edge_decs:
                has_conflict = "true"
                # Pick the most relevant decision (prefer open over resolved)
                best = edge_decs[0]
                for d in edge_decs:
                    if d.get("status", "") == "open":
                        best = d
                        break
                dec_status = best.get("status", "")
                dec_type = best.get("decision_type", "")
                dec_resolution = (
                    best.get("auto_decision", "")
                    or best.get("resolution", "")
                )
                dec_detail = (
                    best.get("auto_rationale", "")
                    or best.get("resolution_detail", "")
                )
                conflict_status = "resolved" if dec_status == "resolved" else "open"
                conflict_type = dec_type
                conflict_resolution = dec_resolution
                conflict_detail = dec_detail
                edge_classes += " has-conflict"

                # Append conflict info to edge label
                if conflict_status == "open":
                    edge_label += " \u26a0 " + (dec_type or "conflict")
                else:
                    short_res = dec_resolution.replace("_", " ") if dec_resolution else "resolved"
                    # Truncate long resolutions for the label
                    if len(short_res) > 20:
                        short_res = short_res[:18] + "\u2026"
                    edge_label += f" \u26a0 resolved: {short_res}"
                    edge_classes += " conflict-resolved"

            edge_id = f"edge::{pxref}::{xref_id}"
            edges[edge_id] = {
                "data": {
                    "id": edge_id,
                    "source": concept_node_id,
                    "target": xref_node_id,
                    "label": edge_label,
                    "kind": "xref_edge",
                    "match_type": match_type,
                    "match_type_source": edge_row.get("match_type_source", ""),
                    "source_asserted": edge_row.get("source_asserted", ""),
                    "agreement_level": edge_row.get("agreement_level", ""),
                    "confidence_score": edge_row.get("confidence_score", ""),
                    "label_similarity": label_sim,
                    "mapping_confidence": edge_row.get("mapping_confidence", ""),
                    "label_confidence": edge_row.get("label_confidence", ""),
                    "source_support": edge_row.get("source_support", ""),
                    "structural_confidence": edge_row.get("structural_confidence", ""),
                    "xref_confidence": xref_conf,
                    "xref_is_obsolete": edge_row.get("xref_is_obsolete", ""),
                    "override_status": edge_row.get("override_status", ""),
                    "override_reason": edge_row.get("override_reason", ""),
                    "has_conflict": has_conflict,
                    "conflict_status": conflict_status,
                    "conflict_type": conflict_type,
                    "conflict_resolution": conflict_resolution,
                    "conflict_detail": conflict_detail,
                },
                "classes": edge_classes,
            }

        # --- decisions ---
        for dec in data.decisions_by_pxref.get(pxref, []):
            dec_id = dec.get("decision_id", "")
            if not dec_id:
                continue
            dec_node_id = f"decision::{dec_id}"
            dec_type = dec.get("decision_type", "")
            dec_status = dec.get("status", "")
            dec_resolution = dec.get("auto_decision", "")
            # Build a descriptive label: "conflict [open]" or "conflict [resolved: downgrade_match_type]"
            if dec_resolution:
                dec_label = f"{dec_type}\n[{dec_status}: {dec_resolution}]"
            else:
                dec_label = f"{dec_type}\n[{dec_status}]"
            dec_xref_id = dec.get("xref_id", "")
            decision_nodes[dec_id] = {
                "data": {
                    "id": dec_node_id,
                    "label": dec_label,
                    "kind": "decision",
                    "decision_id": dec_id,
                    "decision_source": dec.get("decision_source", ""),
                    "decision_type": dec_type,
                    "scenario_id": dec.get("scenario_id", ""),
                    "status": dec_status,
                    "auto_decision": dec_resolution,
                    "auto_confidence": dec.get("auto_confidence", ""),
                    "auto_rationale": dec.get("auto_rationale", ""),
                    "resolution": dec.get("resolution", ""),
                    "resolution_detail": dec.get("resolution_detail", ""),
                    "reviewed_by": dec.get("reviewed_by", ""),
                    "xref_id": dec_xref_id,
                    "priority": dec.get("priority", ""),
                },
                "classes": f"decision decision-{dec_status}",
            }
            # Connect decision to its xref node (if the xref is in the graph)
            if dec_xref_id:
                xref_target_id = f"xref::{dec_xref_id}"
                dec_edge_id = f"dec-edge::{dec_id}::{dec_xref_id}"
                edges[dec_edge_id] = {
                    "data": {
                        "id": dec_edge_id,
                        "source": dec_node_id,
                        "target": xref_target_id,
                        "label": dec_resolution or dec_status,
                        "kind": "decision_edge",
                    },
                    "classes": "decision-edge",
                }

    elements = [
        *concept_nodes.values(),
        *xref_nodes.values(),
        *decision_nodes.values(),
        *edges.values(),
    ]

    return {
        "queryIds": query_ids,
        "missingIds": missing,
        "stats": {
            "conceptCount": len(concept_nodes),
            "xrefCount": len(xref_nodes),
            "namespaceCount": len(namespaces),
            "edgeCount": len(edges),
            "decisionCount": len(decision_nodes),
        },
        "manifest": data.manifest,
        "elements": elements,
    }


def load_flagged_concepts(data: DiseaseGraphData, limit: int = 500) -> list[dict[str, Any]]:
    """Return concepts with QC flags for the landing page table."""
    flagged: list[dict[str, Any]] = []
    for pxref, concept in data.concepts_by_pxref.items():
        cardinality = int(concept.get("cardinality_issue_count") or 0)
        obsolete = int(concept.get("obsolete_xref_count") or 0)
        has_obsolete = concept.get("has_obsolete", "").lower() == "true"
        n_decisions = len(data.decisions_by_pxref.get(pxref, []))

        if cardinality == 0 and obsolete == 0 and not has_obsolete and n_decisions == 0:
            continue

        flags: list[str] = []
        if cardinality > 0:
            flags.append(f"cardinality({cardinality})")
        if obsolete > 0 or has_obsolete:
            flags.append(f"obsolete({obsolete})")
        if n_decisions > 0:
            flags.append(f"decisions({n_decisions})")

        flagged.append({
            "primary_xref": pxref,
            "standard_name": concept.get("standard_name", ""),
            "confidence_tier": concept.get("confidence_tier", ""),
            "overall_quality": concept.get("overall_quality", ""),
            "n_sources": concept.get("n_sources", ""),
            "xref_count": concept.get("xref_count", ""),
            "flags": ", ".join(flags),
            "href": f"/disease-id-qa?ids={pxref}",
        })
        if len(flagged) >= limit:
            break

    return flagged


def export_flagged_tsv(data: DiseaseGraphData, limit: int = 5000) -> str:
    """Return flagged concepts as a TSV string for download."""
    rows = load_flagged_concepts(data, limit=limit)
    if not rows:
        return ""
    columns = ["primary_xref", "standard_name", "confidence_tier", "overall_quality",
               "n_sources", "xref_count", "flags"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, delimiter="\t", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def _is_flagged(concept: dict, decisions: list[dict]) -> bool:
    """Return True if a concept has any QC flag."""
    cardinality = int(concept.get("cardinality_issue_count") or 0)
    obsolete = int(concept.get("obsolete_xref_count") or 0)
    has_obsolete = concept.get("has_obsolete", "").lower() == "true"
    return cardinality > 0 or obsolete > 0 or has_obsolete or len(decisions) > 0


def _concept_has_source(pxref: str, source: str, data: DiseaseGraphData) -> bool:
    """Return True if the concept has an xref edge with the given namespace."""
    source_upper = source.upper()
    for edge in data.edges_by_pxref.get(pxref, []):
        if edge.get("xref_namespace", "").upper() == source_upper:
            return True
    return False


def _concept_to_row(
    pxref: str,
    concept: dict,
    data: DiseaseGraphData,
    include_sources: set[str] | None = None,
    exclude_obsolete: bool = False,
) -> dict[str, Any]:
    """Convert a concept dict to a search result row.

    Parameters
    ----------
    include_sources : set[str] | None
        When set, only include xref edge labels whose namespace (upper-cased)
        is in this set.  ``None`` means include all sources.
    exclude_obsolete : bool
        When True, skip xref edges where ``xref_is_obsolete`` is true.
    """
    n_decisions = len(data.decisions_by_pxref.get(pxref, []))
    cardinality = int(concept.get("cardinality_issue_count") or 0)
    obsolete = int(concept.get("obsolete_xref_count") or 0)
    has_obsolete = concept.get("has_obsolete", "").lower() == "true"

    flags: list[str] = []
    if cardinality > 0:
        flags.append(f"cardinality({cardinality})")
    if obsolete > 0 or has_obsolete:
        flags.append(f"obsolete({obsolete})")
    if n_decisions > 0:
        flags.append(f"decisions({n_decisions})")

    # Build compact source labels: "MONDO: Diabetes | OMIM: Type 2 diabetes"
    source_labels: list[str] = []
    for edge in data.edges_by_pxref.get(pxref, []):
        xref_id = edge.get("xref_id", "")
        ns = _normalize_ns(edge.get("xref_namespace", ""))
        if include_sources and ns.upper() not in include_sources:
            continue
        if exclude_obsolete and edge.get("xref_is_obsolete", "").lower() in ("true", "1"):
            continue
        label = edge.get("xref_label", "")
        if not label:
            label_row = data.labels_by_xref_id.get(xref_id, {})
            label = label_row.get("preferred_label", "")
        if ns and label:
            source_labels.append(f"{ns}: {label}")

    return {
        "primary_xref": pxref,
        "ncats_disease_id": concept.get("ncats_disease_id", ""),
        "standard_name": concept.get("standard_name", ""),
        "confidence_tier": concept.get("confidence_tier", ""),
        "overall_quality": concept.get("overall_quality", ""),
        "n_sources": concept.get("n_sources", ""),
        "xref_count": concept.get("xref_count", ""),
        "flags": ", ".join(flags),
        "source_labels": " | ".join(source_labels),
        "definition": concept.get("definition", ""),
        "synonyms": concept.get("synonyms", ""),
        "is_rare": concept.get("is_rare", ""),
        "disease_type": concept.get("disease_type", ""),
        "nodenorm_canonical_curie": concept.get("nodenorm_canonical_curie", ""),
        "nodenorm_canonical_label": concept.get("nodenorm_canonical_label", ""),
        "nodenorm_validation_status": concept.get("nodenorm_validation_status", ""),
        "href": f"/disease-id-qa?ids={pxref}",
    }


def _match_filters(
    pxref: str,
    concept: dict,
    data: DiseaseGraphData,
    *,
    q_lower: str = "",
    filter_mode: str = "all",
    confidence_tier: str = "",
    is_rare: str = "",
    source: str = "",
    quality: str = "",
) -> bool:
    """Return True if the concept passes all filter criteria."""
    # Flagged filter
    if filter_mode == "flagged":
        decisions = data.decisions_by_pxref.get(pxref, [])
        if not _is_flagged(concept, decisions):
            return False

    # Text search
    if q_lower:
        name = concept.get("standard_name", "").lower()
        if q_lower not in pxref.lower() and q_lower not in name:
            return False

    # Confidence tier
    if confidence_tier:
        if concept.get("confidence_tier", "").lower() != confidence_tier.lower():
            return False

    # Quality score — supports exact match or "min-max" range
    if quality:
        q_val = concept.get("overall_quality", "").strip()
        if "-" in quality:
            parts = quality.split("-", 1)
            try:
                q_min, q_max = float(parts[0]), float(parts[1])
                q_num = float(q_val) if q_val else None
                if q_num is None or not (q_min <= q_num < q_max):
                    return False
            except (ValueError, TypeError):
                return False
        else:
            if q_val != quality:
                return False

    # Rare disease
    if is_rare and is_rare != "all":
        concept_rare = concept.get("is_rare", "").lower() == "true"
        if is_rare == "true" and not concept_rare:
            return False
        if is_rare == "false" and concept_rare:
            return False

    # Source namespace
    if source:
        if not _concept_has_source(pxref, source, data):
            return False

    return True


def search_concepts(
    data: DiseaseGraphData,
    q: str = "",
    filter_mode: str = "all",
    page: int = 1,
    per_page: int = 50,
    confidence_tier: str = "",
    is_rare: str = "",
    source: str = "",
    quality: str = "",
) -> dict[str, Any]:
    """Search concepts with pagination and optional filters."""
    q_lower = q.strip().lower()
    matches: list[tuple[str, dict]] = []

    for pxref, concept in data.concepts_by_pxref.items():
        if not _match_filters(
            pxref, concept, data,
            q_lower=q_lower, filter_mode=filter_mode,
            confidence_tier=confidence_tier, is_rare=is_rare,
            source=source, quality=quality,
        ):
            continue
        matches.append((pxref, concept))

    total = len(matches)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    page_items = matches[start : start + per_page]

    rows = [_concept_to_row(pxref, concept, data) for pxref, concept in page_items]

    return {
        "rows": rows,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "query": q,
        "filter": filter_mode,
    }


def export_search_tsv(
    data: DiseaseGraphData,
    q: str = "",
    filter_mode: str = "all",
    limit: int = 10_000,
) -> str:
    """Export search results as a TSV string (no pagination, capped at *limit* rows)."""
    q_lower = q.strip().lower()
    columns = ["primary_xref", "standard_name", "confidence_tier", "overall_quality",
               "n_sources", "xref_count", "flags", "source_labels"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, delimiter="\t", extrasaction="ignore")
    writer.writeheader()

    count = 0
    for pxref, concept in data.concepts_by_pxref.items():
        if filter_mode == "flagged":
            decisions = data.decisions_by_pxref.get(pxref, [])
            if not _is_flagged(concept, decisions):
                continue
        if q_lower:
            name = concept.get("standard_name", "").lower()
            if q_lower not in pxref.lower() and q_lower not in name:
                continue
        writer.writerow(_concept_to_row(pxref, concept, data))
        count += 1
        if count >= limit:
            break

    return buf.getvalue()


def export_filtered_download(
    data: DiseaseGraphData,
    q: str = "",
    filter_mode: str = "all",
    confidence_tier: str = "",
    is_rare: str = "",
    source: str = "",
    quality: str = "",
    columns: list[str] | None = None,
    fmt: str = "tsv",
    limit: int = 50_000,
    include_sources: set[str] | None = None,
    exclude_obsolete: bool = False,
) -> str:
    """Export filtered concepts with selectable columns.

    Parameters
    ----------
    columns : list[str] | None
        Column names to include. ``None`` uses the default set.
    fmt : str
        "tsv" or "csv".
    include_sources : set[str] | None
        When set, only include xref edge labels whose namespace (upper-cased)
        is in this set.  ``None`` means include all sources.
    exclude_obsolete : bool
        When True, omit xref edges where ``xref_is_obsolete`` is true from
        source labels.
    """
    default_columns = [
        "ncats_disease_id", "primary_xref", "standard_name",
        "confidence_tier", "overall_quality", "n_sources", "xref_count",
    ]
    use_columns = columns if columns else default_columns
    delimiter = "," if fmt == "csv" else "\t"

    q_lower = q.strip().lower()
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=use_columns, delimiter=delimiter, extrasaction="ignore")
    writer.writeheader()

    count = 0
    for pxref, concept in data.concepts_by_pxref.items():
        if not _match_filters(
            pxref, concept, data,
            q_lower=q_lower, filter_mode=filter_mode,
            confidence_tier=confidence_tier, is_rare=is_rare,
            source=source, quality=quality,
        ):
            continue
        writer.writerow(_concept_to_row(
            pxref, concept, data,
            include_sources=include_sources,
            exclude_obsolete=exclude_obsolete,
        ))
        count += 1
        if count >= limit:
            break

    return buf.getvalue()


# ---------------------------------------------------------------------------
# SSSOM Export
# ---------------------------------------------------------------------------

_MATCH_TYPE_TO_SKOS: dict[str, str] = {
    "exact": "skos:exactMatch",
    "broad": "skos:broadMatch",
    "narrow": "skos:narrowMatch",
    "related": "skos:relatedMatch",
}

_MATCH_SOURCE_TO_SEMAPV: dict[str, str] = {
    "lexical": "semapv:LexicalMatching",
    "logical": "semapv:LogicalReasoning",
    "manual": "semapv:ManualMappingCuration",
    "composite": "semapv:CompositeMatching",
    "semantic": "semapv:SemanticSimilarityThresholdMatching",
}

_CURIE_MAP: dict[str, str] = {
    "MONDO": "http://purl.obolibrary.org/obo/MONDO_",
    "OMIM": "https://omim.org/entry/",
    "DOID": "http://purl.obolibrary.org/obo/DOID_",
    "Orphanet": "http://www.orpha.net/ORDO/Orphanet_",
    "MedGen": "https://www.ncbi.nlm.nih.gov/medgen/",
    "GARD": "https://rarediseases.info.nih.gov/diseases/",
    "MESH": "http://id.nlm.nih.gov/mesh/",
    "UMLS": "https://uts.nlm.nih.gov/uts/umls/concept/",
    "EFO": "http://www.ebi.ac.uk/efo/EFO_",
    "HP": "http://purl.obolibrary.org/obo/HP_",
    "skos": "http://www.w3.org/2004/02/skos/core#",
    "semapv": "https://w3id.org/semapv/vocab/",
}


def _match_type_to_skos(match_type: str) -> str:
    """Map a match_type string to the corresponding SKOS predicate CURIE."""
    return _MATCH_TYPE_TO_SKOS.get(match_type, "skos:closeMatch")


def _match_source_to_semapv(match_type_source: str) -> str:
    """Map a match_type_source string to a semapv justification CURIE."""
    src = match_type_source.strip().lower()
    for key, val in _MATCH_SOURCE_TO_SEMAPV.items():
        if key in src:
            return val
    return "semapv:UnspecifiedMatching"


def export_sssom(
    data: DiseaseGraphData,
    include_sources: set[str] | None = None,
) -> str:
    """Export xref edges as SSSOM TSV with metadata header."""
    version = data.manifest.get("pipeline_version", "unknown")

    # Build metadata header
    lines: list[str] = []
    lines.append("#curie_map:")
    for prefix, uri in _CURIE_MAP.items():
        lines.append(f"#  {prefix}: {uri}")
    lines.append(f"#mapping_set_id: https://github.com/ncats/IFX_ODIN/disease-harmonizer")
    lines.append(f"#mapping_set_version: {version}")
    lines.append("#creator_label: TargetGraph Disease Harmonizer")
    lines.append("")

    # TSV header
    columns = [
        "subject_id", "subject_label", "predicate_id",
        "object_id", "object_label",
        "mapping_justification", "confidence",
        "subject_source", "object_source", "comment",
    ]
    lines.append("\t".join(columns))

    for pxref, concept in data.concepts_by_pxref.items():
        for edge in data.edges_by_pxref.get(pxref, []):
            ns = _normalize_ns(edge.get("xref_namespace", ""))
            if include_sources and ns.upper() not in {s.upper() for s in include_sources}:
                continue
            xref_id = edge.get("xref_id", "")
            if not xref_id:
                continue
            match_type = edge.get("match_type", "unclassified")
            match_source = edge.get("match_type_source", "")
            label_row = data.labels_by_xref_id.get(xref_id, {})
            object_label = edge.get("xref_label", "") or label_row.get("preferred_label", "")
            if object_label in ("nan", "NaN"):
                object_label = ""
            confidence = edge.get("xref_confidence", "")
            row = [
                pxref,
                concept.get("standard_name", ""),
                _match_type_to_skos(match_type),
                xref_id,
                object_label,
                _match_source_to_semapv(match_source),
                confidence,
                concept.get("primary_xref_source", ""),
                ns,
                edge.get("agreement_level", ""),
            ]
            lines.append("\t".join(row))

    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Public REST API helpers
# ---------------------------------------------------------------------------

def resolve_concept(data: DiseaseGraphData, curie: str) -> dict[str, Any] | None:
    """Resolve a CURIE to full concept data including all xref edges and decisions."""
    pxref = _resolve_to_pxref(data, curie)
    if pxref is None:
        return None
    concept = data.concepts_by_pxref[pxref]
    edges = data.edges_by_pxref.get(pxref, [])
    decisions = data.decisions_by_pxref.get(pxref, [])

    xrefs = []
    for edge in edges:
        xrefs.append({
            "xref_id": edge.get("xref_id", ""),
            "namespace": _normalize_ns(edge.get("xref_namespace", "")),
            "match_type": edge.get("match_type", ""),
            "confidence": edge.get("xref_confidence", ""),
            "agreement_level": edge.get("agreement_level", ""),
            "match_type_source": edge.get("match_type_source", ""),
            "xref_label": edge.get("xref_label", ""),
            "xref_is_obsolete": edge.get("xref_is_obsolete", ""),
            "mapping_confidence": edge.get("mapping_confidence", ""),
            "label_confidence": edge.get("label_confidence", ""),
            "source_support": edge.get("source_support", ""),
            "structural_confidence": edge.get("structural_confidence", ""),
        })

    decision_list = []
    for dec in decisions:
        decision_list.append({
            "decision_id": dec.get("decision_id", ""),
            "decision_type": dec.get("decision_type", ""),
            "xref_id": dec.get("xref_id", ""),
            "status": dec.get("status", ""),
            "auto_decision": dec.get("auto_decision", ""),
            "auto_confidence": dec.get("auto_confidence", ""),
            "auto_rationale": dec.get("auto_rationale", ""),
        })

    return {
        "ncats_disease_id": concept.get("ncats_disease_id", ""),
        "primary_xref": pxref,
        "standard_name": concept.get("standard_name", ""),
        "confidence_tier": concept.get("confidence_tier", ""),
        "overall_quality": concept.get("overall_quality", ""),
        "n_sources": concept.get("n_sources", ""),
        "disease_type": concept.get("disease_type", ""),
        "is_rare": concept.get("is_rare", ""),
        "definition": concept.get("definition", ""),
        "synonyms": concept.get("synonyms", ""),
        "xrefs": xrefs,
        "decisions": decision_list,
        "nodenorm": {
            "canonical_curie": concept.get("nodenorm_canonical_curie", ""),
            "canonical_label": concept.get("nodenorm_canonical_label", ""),
            "validation_status": concept.get("nodenorm_validation_status", ""),
        },
    }


# ---------------------------------------------------------------------------
# Source Agreement Matrix (for heatmap)
# ---------------------------------------------------------------------------

_NS_CASE_NORMALIZE: dict[str, str] = {
    "snomedct": "SNOMEDCT",
    "medgen": "MEDGEN",
}


def _normalize_ns(ns: str) -> str:
    """Normalize namespace casing (e.g. snomedct → SNOMEDCT)."""
    return _NS_CASE_NORMALIZE.get(ns, ns)


def compute_source_agreement_matrix(data: DiseaseGraphData) -> dict[str, Any]:
    """Compute pairwise source co-occurrence overlap for the heatmap.

    For each pair of sources (A, B) this measures Jaccard overlap:
    how many concepts have xrefs from both A and B, divided by concepts
    that have xrefs from either A or B.  This shows which sources tend
    to cover the same disease space.
    """
    # Build per-source concept sets (with namespace normalization)
    source_concepts: dict[str, set[str]] = defaultdict(set)
    for pxref, edges in data.edges_by_pxref.items():
        seen: set[str] = set()
        for edge in edges:
            ns = _normalize_ns(edge.get("xref_namespace", "").strip())
            if ns and ns not in seen:
                seen.add(ns)
                source_concepts[ns].add(pxref)

    # Pick top sources by concept count, exclude very small ones
    ranked = sorted(source_concepts.keys(), key=lambda k: -len(source_concepts[k]))
    sources = [s for s in ranked if len(source_concepts[s]) >= 500][:10]
    if not sources:
        sources = ranked[:6]

    n = len(sources)
    overlap_counts = [[0] * n for _ in range(n)]
    union_counts = [[0] * n for _ in range(n)]
    matrix = [[0.0] * n for _ in range(n)]

    for i in range(n):
        for j in range(i, n):
            sa, sb = source_concepts[sources[i]], source_concepts[sources[j]]
            ov = len(sa & sb)
            un = len(sa | sb)
            overlap_counts[i][j] = ov
            overlap_counts[j][i] = ov
            union_counts[i][j] = un
            union_counts[j][i] = un
            rate = round(ov / un, 4) if un > 0 else 0.0
            matrix[i][j] = rate
            matrix[j][i] = rate

    return {
        "sources": sources,
        "matrix": matrix,
        "counts": overlap_counts,
        "union_counts": union_counts,
    }


# ---------------------------------------------------------------------------
# Source Flow Data (for Sankey diagram)
# ---------------------------------------------------------------------------

def compute_source_flow_data(data: DiseaseGraphData) -> dict[str, Any]:
    """Compute flows: source namespace -> confidence tier."""
    # Count: for each (namespace, tier), how many concepts have an edge in that namespace
    flow_counts: Counter[tuple[str, str]] = Counter()
    all_ns: set[str] = set()
    all_tiers: set[str] = set()

    for pxref, concept in data.concepts_by_pxref.items():
        tier = concept.get("confidence_tier", "").strip() or "unknown"
        all_tiers.add(tier)
        seen_ns: set[str] = set()
        for edge in data.edges_by_pxref.get(pxref, []):
            ns = _normalize_ns(edge.get("xref_namespace", "").strip())
            if ns and ns not in seen_ns:
                seen_ns.add(ns)
                all_ns.add(ns)
                flow_counts[(ns, tier)] += 1

    # Build node list — only top namespaces by count
    ns_totals = Counter({ns: sum(flow_counts[(ns, t)] for t in all_tiers) for ns in all_ns})
    top_ns = [ns for ns, _ in ns_totals.most_common(8)]

    nodes: list[dict[str, str]] = []
    ns_ids: dict[str, str] = {}
    tier_ids: dict[str, str] = {}

    for ns in top_ns:
        nid = f"src_{ns}"
        ns_ids[ns] = nid
        nodes.append({"id": nid, "label": ns, "type": "source"})

    tier_order = [
        "multi_source_supported",
        "single_authoritative_source",
        "needs_review",
        "review_validator_disagreement",
        "review_obsolete_xref",
    ]
    for tier in tier_order:
        if tier in all_tiers:
            tid = f"tier_{tier}"
            tier_ids[tier] = tid
            nodes.append({"id": tid, "label": tier, "type": "tier"})
    # Add any remaining tiers
    for tier in sorted(all_tiers):
        if tier not in tier_ids:
            tid = f"tier_{tier}"
            tier_ids[tier] = tid
            nodes.append({"id": tid, "label": tier, "type": "tier"})

    links: list[dict[str, Any]] = []
    for ns in top_ns:
        for tier in all_tiers:
            count = flow_counts.get((ns, tier), 0)
            if count > 0 and ns in ns_ids and tier in tier_ids:
                links.append({
                    "source": ns_ids[ns],
                    "target": tier_ids[tier],
                    "value": count,
                })

    return {"nodes": nodes, "links": links}


# ---------------------------------------------------------------------------
# Version Diff
# ---------------------------------------------------------------------------

_baseline_singleton: DiseaseGraphData | None = None


def load_baseline_data(baseline_dir: str | Path) -> DiseaseGraphData:
    """Lazily load baseline dataset for version comparison."""
    global _baseline_singleton
    if _baseline_singleton is not None:
        return _baseline_singleton

    baseline_dir = Path(baseline_dir)
    if not baseline_dir.is_dir():
        raise HTTPException(status_code=500, detail=f"Baseline dir not found: {baseline_dir}")

    data = DiseaseGraphData()
    manifest_path = baseline_dir / "manifest.json"
    if manifest_path.exists():
        with open(manifest_path, encoding="utf-8") as fh:
            data.manifest = json.load(fh)

    for row in _read_tsv(baseline_dir / "disease_concepts.tsv"):
        pxref = row.get("primary_xref", "")
        ncats_id = row.get("ncats_disease_id", "")
        if pxref:
            data.concepts_by_pxref[pxref] = row
        if ncats_id:
            data.concepts_by_ncats_id[ncats_id] = row

    for row in _read_tsv(baseline_dir / "disease_xref_edges.tsv"):
        pxref = row.get("primary_xref", "")
        if pxref:
            data.edges_by_pxref[pxref].append(row)

    for row in _read_tsv(baseline_dir / "review_decisions.tsv"):
        pxref = row.get("primary_xref", "")
        if pxref:
            data.decisions_by_pxref[pxref].append(row)

    _baseline_singleton = data
    n_concepts = len(data.concepts_by_pxref)
    n_edges = sum(len(v) for v in data.edges_by_pxref.values())
    print(f"Baseline loaded: {n_concepts:,} concepts, {n_edges:,} edges")
    return data


def compute_version_diff(
    current: DiseaseGraphData,
    baseline: DiseaseGraphData,
) -> dict[str, Any]:
    """Compute delta between two versioned datasets."""
    current_pxrefs = set(current.concepts_by_pxref.keys())
    baseline_pxrefs = set(baseline.concepts_by_pxref.keys())

    added_pxrefs = current_pxrefs - baseline_pxrefs
    removed_pxrefs = baseline_pxrefs - current_pxrefs
    shared_pxrefs = current_pxrefs & baseline_pxrefs

    # Tier changes
    tier_changes: list[dict[str, str]] = []
    quality_changes: list[dict[str, str]] = []
    for pxref in shared_pxrefs:
        cur = current.concepts_by_pxref[pxref]
        base = baseline.concepts_by_pxref[pxref]
        cur_tier = cur.get("confidence_tier", "")
        base_tier = base.get("confidence_tier", "")
        if cur_tier != base_tier:
            tier_changes.append({
                "primary_xref": pxref,
                "standard_name": cur.get("standard_name", ""),
                "old_tier": base_tier,
                "new_tier": cur_tier,
            })
        cur_q = cur.get("overall_quality", "")
        base_q = base.get("overall_quality", "")
        if cur_q != base_q:
            quality_changes.append({
                "primary_xref": pxref,
                "standard_name": cur.get("standard_name", ""),
                "old_quality": base_q,
                "new_quality": cur_q,
            })

    # Edge diffs
    current_edge_keys: set[tuple[str, str]] = set()
    for pxref, edges in current.edges_by_pxref.items():
        for e in edges:
            current_edge_keys.add((pxref, e.get("xref_id", "")))
    baseline_edge_keys: set[tuple[str, str]] = set()
    for pxref, edges in baseline.edges_by_pxref.items():
        for e in edges:
            baseline_edge_keys.add((pxref, e.get("xref_id", "")))

    edges_added = len(current_edge_keys - baseline_edge_keys)
    edges_removed = len(baseline_edge_keys - current_edge_keys)

    # Decision counts
    cur_resolved = sum(
        1 for decs in current.decisions_by_pxref.values()
        for d in decs if d.get("status") == "resolved"
    )
    base_resolved = sum(
        1 for decs in baseline.decisions_by_pxref.values()
        for d in decs if d.get("status") == "resolved"
    )
    decisions_resolved = cur_resolved - base_resolved

    added_concepts = [
        {"primary_xref": p, "standard_name": current.concepts_by_pxref[p].get("standard_name", "")}
        for p in sorted(added_pxrefs)[:500]
    ]
    removed_concepts = [
        {"primary_xref": p, "standard_name": baseline.concepts_by_pxref[p].get("standard_name", "")}
        for p in sorted(removed_pxrefs)[:500]
    ]

    baseline_version = baseline.manifest.get("pipeline_version", "unknown")
    current_version = current.manifest.get("pipeline_version", "unknown")

    return {
        "summary": {
            "concepts_added": len(added_pxrefs),
            "concepts_removed": len(removed_pxrefs),
            "tier_changes": len(tier_changes),
            "quality_changes": len(quality_changes),
            "edges_added": edges_added,
            "edges_removed": edges_removed,
            "decisions_resolved": max(0, decisions_resolved),
        },
        "added_concepts": added_concepts,
        "removed_concepts": removed_concepts,
        "tier_changes": tier_changes[:500],
        "quality_changes": quality_changes[:500],
        "baseline_version": baseline_version,
        "current_version": current_version,
    }


# ---------------------------------------------------------------------------
# Concept Neighborhood Expansion
# ---------------------------------------------------------------------------

def find_concept_neighbors(
    data: DiseaseGraphData,
    pxref: str,
    max_neighbors: int = 10,
) -> list[str]:
    """Find concepts that share xref IDs with the given concept."""
    # Build reverse index on first call (cache on singleton)
    if data._xref_to_pxrefs is None:
        rev: dict[str, set[str]] = defaultdict(set)
        for px, edges in data.edges_by_pxref.items():
            for e in edges:
                xid = e.get("xref_id", "")
                if xid:
                    rev[xid].add(px)
        data._xref_to_pxrefs = dict(rev)

    xref_to_pxrefs = data._xref_to_pxrefs

    # Get all xref_ids for this concept
    my_xrefs = {e.get("xref_id", "") for e in data.edges_by_pxref.get(pxref, []) if e.get("xref_id")}

    # Find other concepts sharing xrefs, ranked by overlap count
    overlap: Counter[str] = Counter()
    for xid in my_xrefs:
        for other in xref_to_pxrefs.get(xid, set()):
            if other != pxref:
                overlap[other] += 1

    return [px for px, _ in overlap.most_common(max_neighbors)]


# ---------------------------------------------------------------------------
# Provenance Chain
# ---------------------------------------------------------------------------

def build_provenance_chain(
    data: DiseaseGraphData,
    pxref: str,
) -> list[dict[str, Any]]:
    """Build ordered provenance steps for a concept."""
    if pxref not in data.concepts_by_pxref:
        return []

    concept = data.concepts_by_pxref[pxref]
    steps: list[dict[str, Any]] = []

    # Step 1: Source assertions (one per xref edge)
    for edge in data.edges_by_pxref.get(pxref, []):
        steps.append({
            "step": "source_assertion",
            "source": _normalize_ns(edge.get("xref_namespace", "")),
            "xref": edge.get("xref_id", ""),
            "match_type": edge.get("match_type", ""),
            "source_asserted": edge.get("source_asserted", ""),
        })

    # Step 2: Confidence scoring (per xref with confidence)
    scored_xrefs: set[str] = set()
    for edge in data.edges_by_pxref.get(pxref, []):
        xid = edge.get("xref_id", "")
        conf = edge.get("xref_confidence", "")
        if xid and conf and xid not in scored_xrefs:
            scored_xrefs.add(xid)
            steps.append({
                "step": "confidence_scoring",
                "xref": xid,
                "scores": {
                    "xref_confidence": conf,
                    "mapping_confidence": edge.get("mapping_confidence", ""),
                    "label_confidence": edge.get("label_confidence", ""),
                    "source_support": edge.get("source_support", ""),
                    "structural_confidence": edge.get("structural_confidence", ""),
                },
            })

    # Step 3: NodeNorm validation
    nn_status = concept.get("nodenorm_validation_status", "")
    if nn_status:
        steps.append({
            "step": "nodenorm_validation",
            "status": nn_status,
            "canonical": concept.get("nodenorm_canonical_curie", ""),
            "canonical_label": concept.get("nodenorm_canonical_label", ""),
        })

    # Step 4: Conflicts and decisions
    for dec in data.decisions_by_pxref.get(pxref, []):
        steps.append({
            "step": "conflict_detected",
            "type": dec.get("decision_type", ""),
            "xref": dec.get("xref_id", ""),
            "decision_source": dec.get("decision_source", ""),
            "priority": dec.get("priority", ""),
        })
        auto_dec = dec.get("auto_decision", "")
        resolution = dec.get("resolution", "")
        if auto_dec or resolution:
            steps.append({
                "step": "auto_decision" if auto_dec else "manual_resolution",
                "decision": auto_dec or resolution,
                "confidence": dec.get("auto_confidence", ""),
                "rationale": dec.get("auto_rationale", "") or dec.get("resolution_detail", ""),
                "xref": dec.get("xref_id", ""),
            })

    # Step 5: Final resolution status
    steps.append({
        "step": "resolution",
        "status": concept.get("confidence_tier", ""),
        "overall_quality": concept.get("overall_quality", ""),
        "n_sources": concept.get("n_sources", ""),
    })

    return steps


# Names of files that can be served from the app_graph directory.
DOWNLOADABLE_FILES = frozenset({
    "disease_concepts.tsv",
    "disease_xref_edges.tsv",
    "xref_labels.tsv",
    "review_decisions.tsv",
    "manifest.json",
})
