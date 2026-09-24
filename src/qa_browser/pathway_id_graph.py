"""Pathway harmonizer app graph loader and explorer helpers."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import os
import re
import threading
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import HTTPException


class PathwayGraphData:
    __slots__ = (
        "nodes",
        "nodes_by_id",
        "ids_to_pathways",
        "edges",
        "edges_by_pathway",
        "review_queue",
        "review_registry",
        "review_registry_by_id",
        "manifest",
        "source_catalog",
        "categories_by_pathway",
        "data_version",
        "_stats",
    )

    def __init__(self) -> None:
        self.nodes: list[dict[str, str]] = []
        self.nodes_by_id: dict[str, dict[str, str]] = {}
        self.ids_to_pathways: dict[str, list[str]] = defaultdict(list)
        self.edges: list[dict[str, str]] = []
        self.edges_by_pathway: dict[str, list[dict[str, str]]] = defaultdict(list)
        self.review_queue: list[dict[str, str]] = []
        self.review_registry: list[dict[str, str]] = []
        self.review_registry_by_id: dict[str, dict[str, str]] = {}
        self.manifest: dict[str, Any] = {}
        self.source_catalog: list[dict[str, str]] = []
        self.categories_by_pathway: dict[str, list[str]] = {}
        self.data_version: str = ""
        self._stats: dict[str, Any] | None = None


_singletons: dict[str, PathwayGraphData] = {}
_singleton_lock = threading.Lock()


def _release_version_from_path(graph_path: Path) -> str:
    """Return the enclosing vX.Y.Z release name for an app_graph directory."""
    for part in reversed(graph_path.parts):
        match = re.fullmatch(r"v?(\d+\.\d+\.\d+)", part)
        if match:
            return match.group(1)
    return ""


DEFAULT_PATHWAY_COLUMNS = [
    "ncats_pathway_id",
    "consolidated_pathway_name",
    "biolink_category",
    "reactome_id",
    "wikipathway_id",
    "panther_id",
    "bioplanet_id",
    "source_namespaces",
    "source_count",
    "similarity_score",
    "gene_member_count",
]

PATHWAY_SOURCE_ORDER = {
    "Reactome": 0,
    "WikiPathway": 1,
    "BioPlanet": 2,
    "PathwayCommons": 3,
    "Panther": 4,
    "NodeNorm": 5,
}

PATHWAY_REVIEW_DECISION_OPTIONS = [
    {
        "value": "accept_as_is",
        "label": "Keep current harmonization",
        "description": "Accept the current harmonized pathway record as-is.",
    },
    {
        "value": "acknowledge",
        "label": "Acknowledge finding",
        "description": "Acknowledge this finding; no corrective action needed.",
    },
    {
        "value": "suppress_warning",
        "label": "Suppress future detection",
        "description": "Suppress future detection of this finding in QC runs.",
    },
    {
        "value": "needs_expert_review",
        "label": "Escalate for domain review",
        "description": "Escalate for domain review without treating the finding as resolved.",
    },
    {
        "value": "defer",
        "label": "Defer",
        "description": "Leave the row open for later review.",
    },
]

PATHWAY_REVIEW_INTAKE_COLUMNS = [
    "App review ID",
    "Pathway ID",
    "Pathway Name",
    "Scenario",
    "Severity",
    "Detail",
    "Status",
    "Human decision",
    "Resolution",
    "Reviewer notes",
    "Reviewed by",
    "Reviewed at",
]


def _read_tsv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with open(path, encoding="utf-8", newline="") as fh:
        return [{k: (v or "") for k, v in row.items()} for row in csv.DictReader(fh, delimiter="\t")]


def _split_pipe(value: str) -> list[str]:
    return [p.strip() for p in str(value or "").split("|") if p.strip()]


def _split_ids(value: str) -> list[str]:
    return [p.strip() for p in re.split(r"[\s,|]+", value or "") if p.strip()]


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(str(value).replace(",", "")))
    except (TypeError, ValueError):
        return default


def _first_date(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text[:10]
    return ""


def _json_safe_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}:{digest}"


def _pathway_lookup_aliases(value: str) -> set[str]:
    text = (value or "").strip()
    if not text:
        return set()
    aliases = {text, text.lower()}
    lower = text.lower()
    if lower.startswith("ifxpathway:"):
        aliases.add(text.split(":", 1)[1])
    elif lower.startswith("r-hsa-") or re.fullmatch(r"R-HSA-\d+", text, re.I):
        aliases.add(f"Reactome:{text}")
    elif re.fullmatch(r"WP\d+", text, re.I):
        aliases.add(f"WikiPathway:{text}")
    elif re.fullmatch(r"P\d{5}", text, re.I):
        aliases.add(f"Panther:{text}")
    elif lower.startswith("bioplanet_"):
        aliases.add(f"BioPlanet:{text}")
    elif lower.startswith(("reactome:", "wikipathway:", "panther:", "bioplanet:")):
        aliases.add(text.split(":", 1)[1])
    return {a for a in aliases if a}


def _add_pathway_index(data: PathwayGraphData, key: str, pathway_id: str) -> None:
    for alias in _pathway_lookup_aliases(key):
        alias_list = data.ids_to_pathways[alias]
        if pathway_id not in alias_list:
            alias_list.append(pathway_id)


def _index_node(data: PathwayGraphData, node: dict[str, str]) -> None:
    pathway_id = node.get("ncats_pathway_id", "")
    if not pathway_id:
        return
    data.nodes.append(node)
    data.nodes_by_id[pathway_id] = node
    keys = {
        pathway_id,
        node.get("consolidated_pathway_name", ""),
        node.get("reactome_id", ""),
        node.get("wikipathway_id", ""),
        node.get("panther_id", ""),
        node.get("bioplanet_id", ""),
    }
    for key in keys:
        _add_pathway_index(data, key, pathway_id)


def load_pathway_graph_data(graph_dir: str | Path) -> PathwayGraphData:
    graph_path = Path(graph_dir)
    if not graph_path.exists():
        raise HTTPException(status_code=500, detail=f"Pathway graph dir does not exist: {graph_path}")
    key = str(graph_path.resolve())
    with _singleton_lock:
        if key in _singletons:
            return _singletons[key]
        data = PathwayGraphData()
        manifest_path = graph_path / "manifest.json"
        if manifest_path.exists():
            try:
                data.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                data.manifest = {}
        data.data_version = _release_version_from_path(graph_path)
        for node in _read_tsv(graph_path / "pathway_nodes.tsv"):
            _index_node(data, node)
        data.edges = _read_tsv(graph_path / "pathway_edges.tsv")
        for edge in data.edges:
            source_id = edge.get("source_id", "")
            if source_id:
                data.edges_by_pathway[source_id].append(edge)
        data.source_catalog = _read_tsv(graph_path / "pathway_source_catalog.tsv")
        # Load QC review files
        qc_dir = graph_path
        if not (qc_dir / "pathway_review_queue.tsv").exists():
            qc_dir = graph_path.parent / "qc"
        data.review_queue = _read_tsv(qc_dir / "pathway_review_queue.tsv")
        data.review_registry = _read_tsv(qc_dir / "pathway_divergence_registry.tsv")
        data.review_registry_by_id = {
            f"{row.get('ncats_pathway_id', '')}::{row.get('scenario', '')}": row
            for row in data.review_registry
            if row.get("ncats_pathway_id")
        }
        _singletons[key] = data
        return data


def load_pathway_categories(categories_file: str | Path, data: PathwayGraphData) -> None:
    """Load BioPlanet functional categories and map them to IFXPathway IDs."""
    path = Path(categories_file)
    if not path.exists():
        return
    # Build bioplanet_id -> [category_name, ...] lookup
    bp_categories: dict[str, list[str]] = defaultdict(list)
    with open(path, encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            bp_id = (row.get("pathway_id") or "").strip()
            cat = (row.get("category_name") or "").strip()
            if bp_id and cat and cat not in bp_categories[bp_id]:
                bp_categories[bp_id].append(cat)
    # Map to ncats_pathway_id via bioplanet_id on each node
    mapped = 0
    for node in data.nodes:
        bp_id = node.get("bioplanet_id", "").strip()
        if bp_id and bp_id in bp_categories:
            ncats_id = node.get("ncats_pathway_id", "")
            if ncats_id:
                data.categories_by_pathway[ncats_id] = bp_categories[bp_id]
                mapped += 1
    # Invalidate cached stats so category_counts are recomputed
    data._stats = None


def _matches_query(node: dict[str, str], q: str) -> bool:
    if not q:
        return True
    needle = q.lower()
    fields = [
        "ncats_pathway_id",
        "consolidated_pathway_name",
        "reactome_id",
        "wikipathway_id",
        "panther_id",
        "bioplanet_id",
        "source_namespaces",
    ]
    return any(needle in str(node.get(field, "")).lower() for field in fields)


def _matches_filters(
    node: dict[str, str],
    source: str = "",
    min_sources: int = 0,
    category: str = "",
    categories_by_pathway: dict[str, list[str]] | None = None,
) -> bool:
    if source and source.lower() not in node.get("source_namespaces", "").lower():
        return False
    if min_sources > 0:
        count = _safe_int(node.get("source_count"), 0)
        if count < min_sources:
            return False
    if category and categories_by_pathway is not None:
        ncats_id = node.get("ncats_pathway_id", "")
        cats = categories_by_pathway.get(ncats_id, [])
        if category.lower() not in (c.lower() for c in cats):
            return False
    return True


def search_pathways(
    data: PathwayGraphData,
    q: str = "",
    source: str = "",
    min_sources: int = 0,
    category: str = "",
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    page = max(int(page or 1), 1)
    per_page = max(1, min(int(per_page or 50), 250))
    cats = data.categories_by_pathway if data.categories_by_pathway else None
    rows = []
    for node in data.nodes:
        if not _matches_query(node, q):
            continue
        if not _matches_filters(node, source=source, min_sources=min_sources, category=category, categories_by_pathway=cats):
            continue
        enriched = dict(node)
        ncats_id = node.get("ncats_pathway_id", "")
        enriched["categories"] = data.categories_by_pathway.get(ncats_id, [])
        rows.append(enriched)
    total = len(rows)
    start = (page - 1) * per_page
    end = start + per_page
    return {
        "rows": rows[start:end],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if total else 0,
    }


def _summarize_pathway_source_versions(rows: list[dict[str, str]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    harmonization_sources = {"reactome", "wikipathways", "panther", "bioplanet"}
    for row in rows:
        source = str(row.get("source_name") or row.get("source") or "").strip()
        if not source:
            continue
        out.append({
            "name": source,
            "source_role": str(row.get("source_role") or (
                "harmonization source" if source.lower() in harmonization_sources
                else "downloaded reference (not currently applied)" if "nodenorm" in source.lower()
                else "enrichment source"
            )).strip(),
            "version": str(row.get("source_version") or "not captured").strip() or "not captured",
            "download_status": str(row.get("dl_status") or "not captured").strip() or "not captured",
            "odin_download_date": _first_date(row.get("dl_timestamp")),
            "odin_transform_date": _first_date(row.get("tf_timestamp")),
            "transform_records": _safe_int(row.get("tf_records")),
            "url": str(row.get("dl_url") or "").strip(),
        })
    return sorted(out, key=lambda r: (PATHWAY_SOURCE_ORDER.get(r["name"], 999), r["name"].lower()))


def compute_pathway_stats(data: PathwayGraphData) -> dict[str, Any]:
    if data._stats is not None:
        return data._stats
    source_counts: Counter[str] = Counter()
    similarity_buckets: Counter[str] = Counter()
    multi_source_count = 0
    gene_member_counts: Counter[str] = Counter()
    source_count_dist: Counter[int] = Counter()
    for node in data.nodes:
        node_sources = _split_pipe(node.get("source_namespaces", ""))
        if len(node_sources) > 1:
            multi_source_count += 1
        source_count_dist[len(node_sources)] += 1
        for source in node_sources:
            source_counts[source] += 1
        sim = node.get("similarity_score", "")
        try:
            score = float(sim)
            if score >= 0.95:
                similarity_buckets["0.95-1.00"] += 1
            elif score >= 0.80:
                similarity_buckets["0.80-0.94"] += 1
            elif score >= 0.50:
                similarity_buckets["0.50-0.79"] += 1
            elif score > 0:
                similarity_buckets["0.01-0.49"] += 1
            else:
                similarity_buckets["0 (single-source)"] += 1
        except (TypeError, ValueError):
            similarity_buckets["unknown"] += 1
        gene_count = _safe_int(node.get("gene_member_count"))
        if gene_count > 100:
            gene_member_counts["100+"] += 1
        elif gene_count > 50:
            gene_member_counts["51-100"] += 1
        elif gene_count > 10:
            gene_member_counts["11-50"] += 1
        elif gene_count > 0:
            gene_member_counts["1-10"] += 1
        else:
            gene_member_counts["0 (no gene edges)"] += 1

    relation_counts = Counter(edge.get("relation_kind", "") or "unknown" for edge in data.edges)
    evidence_source_counts: Counter[str] = Counter()
    for edge in data.edges:
        for src in _split_pipe(edge.get("evidence_source", "")):
            evidence_source_counts[src] += 1
    mapping_status_counts = Counter(edge.get("mapping_status", "") or "unknown" for edge in data.edges)

    category_counts: Counter[str] = Counter()
    for cats in data.categories_by_pathway.values():
        for cat in cats:
            category_counts[cat] += 1

    source_versions = _summarize_pathway_source_versions(data.source_catalog)
    manifest = data.manifest or {}
    nodes_meta = manifest.get("nodes", {}) if isinstance(manifest.get("nodes"), dict) else {}
    edges_meta = manifest.get("edges", {}) if isinstance(manifest.get("edges"), dict) else {}
    total_pathways = len(data.nodes)
    data._stats = {
        "total_pathways": total_pathways,
        "manifest_node_count": _safe_int(nodes_meta.get("count"), total_pathways),
        "total_edges": len(data.edges),
        "manifest_edge_count": _safe_int(edges_meta.get("count"), len(data.edges)),
        "multi_source_count": multi_source_count,
        "multi_source_percent": (multi_source_count / total_pathways * 100) if total_pathways else 0,
        "source_counts": dict(source_counts.most_common()),
        "source_count_distribution": dict(sorted(source_count_dist.items())),
        "similarity_distribution": dict(similarity_buckets.most_common()),
        "gene_member_distribution": dict(gene_member_counts.most_common()),
        "relation_counts": dict(relation_counts.most_common()),
        "evidence_source_counts": dict(evidence_source_counts.most_common()),
        "mapping_status_counts": dict(mapping_status_counts.most_common()),
        "category_counts": dict(category_counts.most_common()),
        "source_catalog": data.source_catalog,
        "source_versions": source_versions,
        "manifest": manifest,
    }
    return data._stats


def _resolve_pathway_ids(data: PathwayGraphData, raw_ids: str) -> tuple[list[str], list[str]]:
    resolved: list[str] = []
    not_found: list[str] = []
    for token in _split_ids(raw_ids):
        hits: list[str] = []
        if token in data.nodes_by_id:
            hits = [token]
        else:
            seen: set[str] = set()
            for alias in _pathway_lookup_aliases(token):
                for hit in data.ids_to_pathways.get(alias, []):
                    if hit not in seen:
                        hits.append(hit)
                        seen.add(hit)
        if not hits:
            not_found.append(token)
            continue
        for hit in hits:
            if hit not in resolved:
                resolved.append(hit)
    return resolved, not_found


def build_pathway_graph_payload(
    data: PathwayGraphData,
    ids: str = "",
) -> dict[str, Any]:
    pathway_ids, not_found = _resolve_pathway_ids(data, ids)
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()

    def add_node(node_id: str, label: str, category: str, payload: dict[str, str]) -> None:
        if node_id in seen_nodes:
            return
        seen_nodes.add(node_id)
        nodes.append({
            "data": {
                "id": node_id,
                "label": label or node_id,
                "category": category,
                **payload,
            },
            "classes": category.replace("biolink:", "").replace(":", "_").lower(),
        })

    for pathway_id in pathway_ids[:25]:
        node = data.nodes_by_id.get(pathway_id)
        if not node:
            continue
        add_node(
            pathway_id,
            node.get("consolidated_pathway_name") or pathway_id,
            "biolink:Pathway",
            {"node_type": "pathway", **node},
        )
        edge_no = 0
        for edge in data.edges_by_pathway.get(pathway_id, [])[:180]:
            target_id = edge.get("target_id", "") or edge.get("target_label", "")
            if not target_id:
                # Edges without target_id get a synthetic gene target from evidence_source
                target_id = _json_safe_id("gene", f"{pathway_id}:{edge_no}")
                target_label = f"gene ({edge.get('evidence_source', 'unknown')})"
            else:
                target_label = edge.get("target_label") or target_id
            if target_id in data.nodes_by_id:
                target_node = data.nodes_by_id[target_id]
                graph_target_id = target_id
                target_category = "biolink:Pathway"
                add_node(
                    graph_target_id,
                    target_node.get("consolidated_pathway_name") or target_id,
                    target_category,
                    {"node_type": "pathway", **target_node},
                )
            else:
                graph_target_id = target_id if ":" in target_id and " " not in target_id else _json_safe_id("object", target_id)
                target_category = edge.get("target_category", "") or "biolink:Gene"
                add_node(
                    graph_target_id,
                    target_label,
                    target_category,
                    {"node_type": "gene", "source_curie": target_id, **edge},
                )
            edge_no += 1
            edges.append({
                "data": {
                    "id": f"{pathway_id}::{edge_no}::{graph_target_id}",
                    "source": pathway_id,
                    "target": graph_target_id,
                    "label": edge.get("relation_kind") or edge.get("predicate") or "has_participant",
                    **edge,
                },
                "classes": (edge.get("relation_kind", "") or "association").replace("_", "-"),
            })
    return {
        "elements": {"nodes": nodes, "edges": edges},
        "resolved_ids": pathway_ids,
        "not_found": not_found,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def export_pathways(
    data: PathwayGraphData,
    q: str = "",
    source: str = "",
    min_sources: int = 0,
    columns: list[str] | None = None,
    fmt: str = "tsv",
) -> str:
    rows = [
        node for node in data.nodes
        if _matches_query(node, q) and _matches_filters(node, source=source, min_sources=min_sources)
    ]
    available = list(data.nodes[0].keys()) if data.nodes else DEFAULT_PATHWAY_COLUMNS
    selected = [col for col in (columns or DEFAULT_PATHWAY_COLUMNS) if col in available]
    if not selected:
        selected = DEFAULT_PATHWAY_COLUMNS
    delimiter = "," if fmt == "csv" else "\t"
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=selected, delimiter=delimiter, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in selected})
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Pathway resolver
# ---------------------------------------------------------------------------


def resolve_pathway_queries(
    data: PathwayGraphData,
    queries: list[str],
) -> list[dict[str, Any]]:
    """Resolve a list of pathway names/IDs to IFXPathway records."""
    results: list[dict[str, Any]] = []
    name_index: dict[str, str] | None = None  # lazy-built

    for raw_query in queries:
        query = raw_query.strip()
        if not query:
            continue
        matches: list[dict[str, Any]] = []

        # 1. Try exact ID resolution via existing lookup
        resolved, _ = _resolve_pathway_ids(data, query)
        if resolved:
            for pid in resolved:
                node = data.nodes_by_id.get(pid, {})
                matches.append({
                    "ncats_pathway_id": pid,
                    "name": node.get("consolidated_pathway_name", ""),
                    "source_ids": " | ".join(filter(None, [
                        node.get("reactome_id", ""),
                        node.get("wikipathway_id", ""),
                        node.get("panther_id", ""),
                        node.get("bioplanet_id", ""),
                    ])),
                    "match_type": "exact_id",
                })

        # 2. Try name substring match if no ID hits
        if not matches:
            needle = query.lower()
            for node in data.nodes:
                name = node.get("consolidated_pathway_name", "")
                if needle in name.lower():
                    pid = node.get("ncats_pathway_id", "")
                    matches.append({
                        "ncats_pathway_id": pid,
                        "name": name,
                        "source_ids": " | ".join(filter(None, [
                            node.get("reactome_id", ""),
                            node.get("wikipathway_id", ""),
                            node.get("panther_id", ""),
                            node.get("bioplanet_id", ""),
                        ])),
                        "match_type": "name_contains",
                    })
                    if len(matches) >= 10:
                        break

        results.append({
            "query": query,
            "resolved": len(matches) > 0,
            "match_count": len(matches),
            "matches": matches,
        })

    return results


def export_pathway_resolver_results(
    results: list[dict[str, Any]],
    fmt: str = "tsv",
) -> str:
    """Export resolver results as TSV/CSV."""
    delimiter = "," if fmt == "csv" else "\t"
    cols = ["query", "status", "ncats_pathway_id", "name", "source_ids", "match_type"]
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=cols, delimiter=delimiter)
    writer.writeheader()
    for result in results:
        if not result["matches"]:
            writer.writerow({
                "query": result["query"],
                "status": "not_found",
                "ncats_pathway_id": "",
                "name": "",
                "source_ids": "",
                "match_type": "not_found",
            })
        else:
            for match in result["matches"]:
                writer.writerow({
                    "query": result["query"],
                    "status": "resolved",
                    **match,
                })
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Cross-entity links
# ---------------------------------------------------------------------------


def build_cross_entity_summary(
    data: PathwayGraphData,
    pathway_id: str,
    target_data: Any = None,
    disease_data: Any = None,
    drug_data: Any = None,
) -> dict[str, Any]:
    """Build cross-entity links for a pathway via its gene members."""
    result: dict[str, Any] = {"disease_links": [], "drug_links": []}
    if not pathway_id or pathway_id not in data.nodes_by_id:
        return result

    # Collect gene IDs from edges
    gene_ids: set[str] = set()
    gene_symbols: set[str] = set()
    for edge in data.edges_by_pathway.get(pathway_id, []):
        gid = edge.get("ncats_gene_id") or edge.get("target_id", "")
        if gid:
            gene_ids.add(gid)
        symbol = (edge.get("gene_symbol") or edge.get("target_label") or "").strip()
        if symbol:
            gene_symbols.add(symbol.upper())

    if not gene_ids:
        return result

    # Disease links via target_data (if available and has gene-disease edges)
    if disease_data is not None and hasattr(disease_data, "associations_by_ncats_id"):
        disease_hits: dict[str, dict[str, Any]] = {}
        for node in getattr(disease_data, "nodes", []):
            disease_id = node.get("ncats_disease_id") or node.get("mondo_id", "")
            associations = disease_data.associations_by_ncats_id.get(disease_id, [])
            associated_genes = {
                assoc.get("ncats_gene_id", "") for assoc in associations
                if assoc.get("ncats_gene_id")
            }
            shared = gene_ids & associated_genes
            if shared and disease_id and disease_id not in disease_hits:
                disease_hits[disease_id] = {
                    "disease_id": disease_id,
                    "name": node.get("consolidated_disease_name", "") or node.get("disease_name", ""),
                    "shared_genes": len(shared),
                }
        result["disease_links"] = sorted(
            disease_hits.values(), key=lambda x: -x["shared_genes"]
        )[:25]

    # Drug links via drug_data (if available and has target edges)
    if drug_data is not None and hasattr(drug_data, "edges_by_drug"):
        drug_hits: dict[str, dict[str, Any]] = {}
        for node in getattr(drug_data, "nodes", []):
            drug_id = node.get("drug_id") or node.get("ncats_drug_id", "")
            target_edges = [
                edge for edge in drug_data.edges_by_drug.get(drug_id, [])
                if edge.get("relation_kind", "drug_target") == "drug_target"
            ]
            target_gene_ids = {
                edge.get("target_id", "") for edge in target_edges
                if str(edge.get("target_id", "")).startswith("IFXGene:")
            }
            target_symbols = {
                edge.get("target_label", "").strip().upper() for edge in target_edges
                if edge.get("target_label", "").strip()
            }
            shared_ids = gene_ids & target_gene_ids
            shared_symbols = gene_symbols & target_symbols
            shared = shared_ids | shared_symbols
            if shared and drug_id and drug_id not in drug_hits:
                drug_hits[drug_id] = {
                    "drug_id": drug_id,
                    "name": node.get("standard_name", "") or node.get("consolidated_drug_name", "") or node.get("drug_name", ""),
                    "target_gene": ", ".join(sorted(shared)[:5]),
                }
        result["drug_links"] = sorted(
            drug_hits.values(), key=lambda x: x["name"]
        )[:25]

    return result


# ---------------------------------------------------------------------------
# Version diff helpers
# ---------------------------------------------------------------------------

def load_pathway_version_data(data_dir: str | Path) -> PathwayGraphData:
    """Load any versioned pathway app_graph directory into a PathwayGraphData."""
    data_dir = Path(data_dir)
    key = str(data_dir.resolve())
    with _singleton_lock:
        if key in _singletons:
            return _singletons[key]
        data = PathwayGraphData()
        manifest_path = data_dir / "manifest.json"
        if manifest_path.exists():
            try:
                data.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                data.manifest = {}
        for node in _read_tsv(data_dir / "pathway_nodes.tsv"):
            _index_node(data, node)
        data.edges = _read_tsv(data_dir / "pathway_edges.tsv")
        for edge in data.edges:
            source_id = edge.get("source_id", "")
            if source_id:
                data.edges_by_pathway[source_id].append(edge)
        data.source_catalog = _read_tsv(data_dir / "pathway_source_catalog.tsv")
        # Extract data version from directory path (e.g. .../v1.0.0/app_graph → 1.0.0)
        data.data_version = _release_version_from_path(data_dir)
        _singletons[key] = data
        return data


def compute_pathway_version_diff(
    current: PathwayGraphData,
    baseline: PathwayGraphData,
) -> dict[str, Any]:
    """Compute delta between two versioned pathway datasets."""
    current_ids = set(current.nodes_by_id.keys())
    baseline_ids = set(baseline.nodes_by_id.keys())

    added_ids = sorted(current_ids - baseline_ids)
    removed_ids = sorted(baseline_ids - current_ids)
    shared_ids = current_ids & baseline_ids

    name_changes: list[dict[str, str]] = []
    source_namespace_changes: list[dict[str, str]] = []
    similarity_changes: list[dict[str, str]] = []

    for pid in sorted(shared_ids):
        cur = current.nodes_by_id[pid]
        base = baseline.nodes_by_id[pid]

        for field, changes_list in [
            ("consolidated_pathway_name", name_changes),
            ("source_namespaces", source_namespace_changes),
            ("similarity_score", similarity_changes),
        ]:
            old_val = base.get(field, "")
            new_val = cur.get(field, "")
            if old_val != new_val:
                changes_list.append({
                    "ncats_pathway_id": pid,
                    "consolidated_pathway_name": cur.get("consolidated_pathway_name") or base.get("consolidated_pathway_name", ""),
                    "old_value": old_val,
                    "new_value": new_val,
                })

    # Edge diffs
    def _edge_key(e: dict[str, str]) -> tuple[str, str, str]:
        return (
            e.get("source_id", ""),
            e.get("target_id", ""),
            e.get("relation_kind", ""),
        )

    current_edge_keys = {_edge_key(e) for e in current.edges}
    baseline_edge_keys = {_edge_key(e) for e in baseline.edges}
    current_edge_by_key = {_edge_key(e): e for e in current.edges}
    baseline_edge_by_key = {_edge_key(e): e for e in baseline.edges}
    edge_evidence_changes = []
    for key in sorted(current_edge_keys & baseline_edge_keys):
        old_value = baseline_edge_by_key[key].get("evidence_source", "")
        new_value = current_edge_by_key[key].get("evidence_source", "")
        if old_value != new_value:
            edge_evidence_changes.append({
                "source_id": key[0], "target_id": key[1],
                "evidence_source": new_value,
                "old_value": old_value, "new_value": new_value,
            })

    # Source version changes
    cur_src = {row.get("source_name", ""): row for row in current.source_catalog}
    base_src = {row.get("source_name", ""): row for row in baseline.source_catalog}
    source_version_changes = []
    for src in sorted(set(cur_src) | set(base_src)):
        old_v = base_src.get(src, {}).get("source_version", "")
        new_v = cur_src.get(src, {}).get("source_version", "")
        if old_v != new_v:
            source_version_changes.append({
                "source_name": src,
                "old_source_version": old_v,
                "new_source_version": new_v,
            })

    node_fields = ("ncats_pathway_id", "consolidated_pathway_name", "biolink_category", "source_namespaces", "source_count", "similarity_score")
    max_rows = 500
    return {
        "baseline_version": baseline.data_version or baseline.manifest.get("version", ""),
        "current_version": current.data_version or current.manifest.get("version", ""),
        "summary": {
            "pathways_old": len(baseline.nodes),
            "pathways_new": len(current.nodes),
            "pathways_added": len(added_ids),
            "pathways_removed": len(removed_ids),
            "pathways_retained": len(shared_ids),
            "edges_old": len(baseline.edges),
            "edges_new": len(current.edges),
            "edges_added": len(current_edge_keys - baseline_edge_keys),
            "edges_removed": len(baseline_edge_keys - current_edge_keys),
            "edge_evidence_changes": len(edge_evidence_changes),
            "name_changes": len(name_changes),
            "source_namespace_changes": len(source_namespace_changes),
            "similarity_changes": len(similarity_changes),
            "source_version_changes": len(source_version_changes),
        },
        "added_pathways": [
            {f: current.nodes_by_id[pid].get(f, "") for f in node_fields}
            for pid in added_ids[:max_rows]
        ],
        "removed_pathways": [
            {f: baseline.nodes_by_id[pid].get(f, "") for f in node_fields}
            for pid in removed_ids[:max_rows]
        ],
        "name_changes": name_changes[:max_rows],
        "source_namespace_changes": source_namespace_changes[:max_rows],
        "similarity_changes": similarity_changes[:max_rows],
        "edge_evidence_changes": edge_evidence_changes[:max_rows],
        "source_version_changes": source_version_changes[:max_rows],
        "truncation": {
            "added_pathways": {"total": len(added_ids), "shown": min(len(added_ids), max_rows)},
            "removed_pathways": {"total": len(removed_ids), "shown": min(len(removed_ids), max_rows)},
            "name_changes": {"total": len(name_changes), "shown": min(len(name_changes), max_rows)},
            "source_namespace_changes": {"total": len(source_namespace_changes), "shown": min(len(source_namespace_changes), max_rows)},
            "similarity_changes": {"total": len(similarity_changes), "shown": min(len(similarity_changes), max_rows)},
            "source_version_changes": {"total": len(source_version_changes), "shown": min(len(source_version_changes), max_rows)},
            "edge_evidence_changes": {"total": len(edge_evidence_changes), "shown": min(len(edge_evidence_changes), max_rows)},
        },
    }


# ---------------------------------------------------------------------------
# Pathway review queue helpers
# ---------------------------------------------------------------------------

def _pathway_review_row_status(row: dict[str, str]) -> str:
    status = (row.get("status") or "").strip().lower()
    return status or "open"


def _pathway_review_row(row: dict[str, str]) -> dict[str, str]:
    pathway_id = row.get("ncats_pathway_id", "")
    scenario = row.get("scenario", "")
    return {
        "app_review_id": _json_safe_id("pwReview", f"{pathway_id}::{scenario}"),
        "ncats_pathway_id": pathway_id,
        "scenario": scenario,
        "severity": row.get("severity", ""),
        "detail": row.get("detail", ""),
        "status": _pathway_review_row_status(row),
        "date_found": row.get("date_found", ""),
        "date_resolved": row.get("date_resolved", ""),
        "reviewed_by": row.get("reviewed_by", ""),
        "resolution": row.get("resolution", ""),
        "resolution_detail": row.get("resolution_detail", ""),
    }


def _pathway_review_items(data: PathwayGraphData) -> list[dict[str, str]]:
    if data.review_registry:
        return [_pathway_review_row(row) for row in data.review_registry]
    return [_pathway_review_row({**row, "status": "open"}) for row in data.review_queue]


def _matches_pathway_review_status(row: dict[str, str], status: str) -> bool:
    wanted = (status or "open").strip().lower()
    observed = row.get("status", "open")
    if wanted in {"", "all"}:
        return True
    if wanted == "resolved":
        return observed in {"resolved", "acknowledged", "wontfix"}
    return observed == wanted


def _matches_pathway_review_query(row: dict[str, str], q: str) -> bool:
    if not q:
        return True
    needle = q.lower()
    fields = [
        "ncats_pathway_id",
        "scenario",
        "severity",
        "detail",
        "resolution",
        "resolution_detail",
    ]
    return any(needle in str(row.get(field, "")).lower() for field in fields)


def build_pathway_review_queue(
    data: PathwayGraphData,
    scenario: str = "",
    severity: str = "",
    status: str = "open",
    q: str = "",
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    page = max(int(page or 1), 1)
    per_page = max(1, min(int(per_page or 50), 250))
    all_rows = _pathway_review_items(data)

    status_counts = Counter(row.get("status", "open") for row in all_rows)
    scenario_counts = Counter(row.get("scenario", "") or "unknown" for row in all_rows)
    severity_counts = Counter(row.get("severity", "") or "unknown" for row in all_rows)

    rows = []
    for row in all_rows:
        if scenario and scenario.lower() != row.get("scenario", "").lower():
            continue
        if severity and severity.lower() != row.get("severity", "").lower():
            continue
        if not _matches_pathway_review_status(row, status):
            continue
        if not _matches_pathway_review_query(row, q):
            continue
        enriched = dict(row)
        pathway_id = row.get("ncats_pathway_id", "")
        node = data.nodes_by_id.get(pathway_id)
        enriched["pathway_name"] = node.get("consolidated_pathway_name", "") if node else ""
        enriched["graph_href"] = f"/pathway-id-qa?ids={pathway_id}&tab=graph" if pathway_id else ""
        rows.append(enriched)

    total = len(rows)
    start = (page - 1) * per_page
    return {
        "rows": rows[start:start + per_page],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if total else 0,
        "status_counts": dict(status_counts.most_common()),
        "scenario_counts": dict(scenario_counts.most_common()),
        "severity_counts": dict(severity_counts.most_common()),
        "decision_options": PATHWAY_REVIEW_DECISION_OPTIONS,
    }


def export_pathway_review_intake_template(
    data: PathwayGraphData,
    scenario: str = "",
    severity: str = "",
    status: str = "open",
    q: str = "",
) -> str:
    payload = build_pathway_review_queue(
        data,
        scenario=scenario,
        severity=severity,
        status=status,
        q=q,
        page=1,
        per_page=250000,
    )
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=PATHWAY_REVIEW_INTAKE_COLUMNS, delimiter="\t", extrasaction="ignore")
    writer.writeheader()
    for row in payload["rows"]:
        writer.writerow({
            "App review ID": row.get("app_review_id", ""),
            "Pathway ID": row.get("ncats_pathway_id", ""),
            "Pathway Name": row.get("pathway_name", ""),
            "Scenario": row.get("scenario", ""),
            "Severity": row.get("severity", ""),
            "Detail": row.get("detail", ""),
            "Status": row.get("status", ""),
            "Human decision": row.get("review_decision", ""),
            "Resolution": row.get("resolution", ""),
            "Reviewer notes": row.get("resolution_detail", ""),
        })
    return buf.getvalue()


def build_batch_pathway_review_payload(
    data: PathwayGraphData,
    scenario: str,
    review_decision: str,
    severity: str = "",
    status: str = "open",
    reviewed_by: str = "",
) -> list[dict[str, str]]:
    """Build intake rows for all items matching a scenario filter."""
    severity_filter = (severity or "").strip().lower()
    status_filter = (status or "open").strip().lower()
    reviewed_by = (reviewed_by or os.getenv("USER") or "app_batch_review").strip()
    reviewed_at = datetime.now(timezone.utc).isoformat()

    allowed = {opt["value"] for opt in PATHWAY_REVIEW_DECISION_OPTIONS}
    if review_decision not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported review_decision: {review_decision}")

    all_rows = _pathway_review_items(data)
    rows: list[dict[str, str]] = []
    for row in all_rows:
        row_status = row.get("status", "open")
        if status_filter and status_filter not in {"", "all"} and row_status != status_filter:
            continue
        if scenario and scenario.lower() != row.get("scenario", "").lower():
            continue
        if severity_filter and severity_filter != row.get("severity", "").lower():
            continue
        pathway_id = row.get("ncats_pathway_id", "")
        node = data.nodes_by_id.get(pathway_id)
        rows.append({
            "App review ID": str(uuid.uuid4()),
            "Pathway ID": pathway_id,
            "Pathway Name": node.get("consolidated_pathway_name", "") if node else "",
            "Scenario": row.get("scenario", ""),
            "Severity": row.get("severity", ""),
            "Detail": row.get("detail", ""),
            "Status": row.get("status", ""),
            "Human decision": review_decision,
            "Resolution": "",
            "Reviewer notes": f"Batch: {scenario}",
            "Reviewed by": reviewed_by,
            "Reviewed at": reviewed_at,
        })
    return rows


def mark_pathway_rows_resolved(
    data: PathwayGraphData,
    scenario: str,
    severity: str = "",
    status: str = "open",
) -> int:
    """Mark matching review/registry rows as resolved in memory."""
    severity_filter = (severity or "").strip().lower()
    status_filter = (status or "open").strip().lower()
    source = data.review_registry if data.review_registry else data.review_queue
    count = 0
    for row in source:
        row_status = (row.get("status", "") or "open").strip().lower()
        if status_filter and status_filter not in {"", "all"} and row_status != status_filter:
            continue
        if scenario and scenario.lower() != row.get("scenario", "").lower():
            continue
        if severity_filter and severity_filter != (row.get("severity", "") or "").lower():
            continue
        row["status"] = "resolved"
        count += 1
    if count:
        data._stats = None
    return count
