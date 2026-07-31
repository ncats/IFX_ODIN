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
    )

    def __init__(self) -> None:
        self.concepts_by_pxref: dict[str, dict] = {}
        self.concepts_by_ncats_id: dict[str, dict] = {}
        self.edges_by_pxref: dict[str, list[dict]] = defaultdict(list)
        self.labels_by_xref_id: dict[str, dict] = {}
        self.decisions_by_pxref: dict[str, list[dict]] = defaultdict(list)
        self.manifest: dict = {}
        self._dashboard_stats: dict | None = None


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
            ns = edge.get("xref_namespace", "").strip()
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
            ns = edge_row.get("xref_namespace", "").lower()
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
                    "xref_namespace": edge_row.get("xref_namespace", ""),
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
) -> dict[str, Any]:
    """Convert a concept dict to a search result row.

    Parameters
    ----------
    include_sources : set[str] | None
        When set, only include xref edge labels whose namespace (upper-cased)
        is in this set.  ``None`` means include all sources.
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
        ns = edge.get("xref_namespace", "")
        if include_sources and ns.upper() not in include_sources:
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
        writer.writerow(_concept_to_row(pxref, concept, data, include_sources=include_sources))
        count += 1
        if count >= limit:
            break

    return buf.getvalue()


# Names of files that can be served from the app_graph directory.
DOWNLOADABLE_FILES = frozenset({
    "disease_concepts.tsv",
    "disease_xref_edges.tsv",
    "xref_labels.tsv",
    "review_decisions.tsv",
    "manifest.json",
})
