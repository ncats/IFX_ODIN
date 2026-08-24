"""Variant harmonizer app graph loader and explorer helpers."""
from __future__ import annotations

import csv
import hashlib
import io
import json
import re
import threading
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

from fastapi import HTTPException


class VariantGraphData:
    __slots__ = (
        "nodes",
        "nodes_by_id",
        "ids_to_variants",
        "edges",
        "edges_by_variant",
        "manifest",
        "source_catalog",
        "_stats",
    )

    def __init__(self) -> None:
        self.nodes: list[dict[str, str]] = []
        self.nodes_by_id: dict[str, dict[str, str]] = {}
        self.ids_to_variants: dict[str, list[str]] = defaultdict(list)
        self.edges: list[dict[str, str]] = []
        self.edges_by_variant: dict[str, list[dict[str, str]]] = defaultdict(list)
        self.manifest: dict[str, Any] = {}
        self.source_catalog: list[dict[str, str]] = []
        self._stats: dict[str, Any] | None = None


_singletons: dict[str, VariantGraphData] = {}
_singleton_lock = threading.Lock()


DEFAULT_VARIANT_COLUMNS = [
    "variant_id",
    "primary_id",
    "name",
    "biolink_category",
    "variant_type",
    "dbsnp_id",
    "clinvar_variation_id",
    "clinvar_allele_id",
    "gene_symbol",
    "gene_curie",
    "assembly",
    "chromosome",
    "position",
    "risk_allele",
    "clinical_significance",
    "review_status",
    "source_namespaces",
    "equivalence_scope",
    "quality_note",
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


def _json_safe_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}:{digest}"


def _variant_lookup_aliases(value: str) -> set[str]:
    text = (value or "").strip()
    if not text:
        return set()
    aliases = {text, text.lower()}
    if text.lower().startswith("dbsnp:rs"):
        bare = text.split(":", 1)[1]
        aliases.update({bare, bare.lower(), bare[2:] if bare.lower().startswith("rs") else bare})
    elif re.fullmatch(r"rs\d+", text, re.I):
        aliases.add(f"dbSNP:{text.lower()}")
    elif re.fullmatch(r"\d+", text):
        aliases.update({f"dbSNP:rs{text}", f"ClinVarVariation:{text}", f"ClinVarAllele:{text}"})
    elif text.lower().startswith(("clinvarvariation:", "clinvarallele:")):
        aliases.add(text.split(":", 1)[1])
    return {a for a in aliases if a}


def _add_variant_index(data: VariantGraphData, key: str, variant_id: str) -> None:
    for alias in _variant_lookup_aliases(key):
        data.ids_to_variants[alias].append(variant_id)


def _index_node(data: VariantGraphData, node: dict[str, str]) -> None:
    variant_id = node.get("variant_id", "")
    if not variant_id:
        return
    data.nodes.append(node)
    data.nodes_by_id[variant_id] = node
    keys = {
        variant_id,
        node.get("variant_key", ""),
        node.get("primary_id", ""),
        node.get("dbsnp_id", ""),
        node.get("clinvar_variation_id", ""),
        node.get("clinvar_allele_id", ""),
        node.get("gene_symbol", ""),
    }
    for key in keys:
        _add_variant_index(data, key, variant_id)
    for source_id in _split_pipe(node.get("source_variant_ids", "")):
        _add_variant_index(data, source_id, variant_id)


def load_variant_graph_data(graph_dir: str | Path) -> VariantGraphData:
    graph_path = Path(graph_dir)
    if not graph_path.exists():
        raise HTTPException(status_code=500, detail=f"Variant graph dir does not exist: {graph_path}")
    key = str(graph_path.resolve())
    with _singleton_lock:
        if key in _singletons:
            return _singletons[key]
        data = VariantGraphData()
        manifest_path = graph_path / "manifest.json"
        if manifest_path.exists():
            try:
                data.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                data.manifest = {}
        for node in _read_tsv(graph_path / "variant_nodes.tsv"):
            _index_node(data, node)
        data.edges = _read_tsv(graph_path / "variant_edges.tsv")
        for edge in data.edges:
            source_id = edge.get("source_id", "")
            if source_id:
                data.edges_by_variant[source_id].append(edge)
        data.source_catalog = _read_tsv(graph_path / "variant_source_catalog.tsv")
        _singletons[key] = data
        return data


def _matches_query(node: dict[str, str], q: str) -> bool:
    if not q:
        return True
    needle = q.lower()
    fields = [
        "variant_id",
        "primary_id",
        "name",
        "variant_key",
        "dbsnp_id",
        "clinvar_variation_id",
        "clinvar_allele_id",
        "gene_symbol",
        "gene_curie",
        "source_variant_ids",
    ]
    return any(needle in str(node.get(field, "")).lower() for field in fields)


def _matches_filters(node: dict[str, str], source: str = "", gene: str = "", significance: str = "") -> bool:
    if source and source.lower() not in node.get("source_namespaces", "").lower():
        return False
    if gene and gene.lower() not in (node.get("gene_symbol", "") + " " + node.get("gene_curie", "")).lower():
        return False
    if significance and significance.lower() not in node.get("clinical_significance", "").lower():
        return False
    return True


def search_variants(
    data: VariantGraphData,
    q: str = "",
    source: str = "",
    gene: str = "",
    significance: str = "",
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    page = max(int(page or 1), 1)
    per_page = max(1, min(int(per_page or 50), 250))
    rows = [
        node for node in data.nodes
        if _matches_query(node, q) and _matches_filters(node, source=source, gene=gene, significance=significance)
    ]
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


def compute_variant_stats(data: VariantGraphData) -> dict[str, Any]:
    if data._stats is not None:
        return data._stats
    source_counts: Counter[str] = Counter()
    significance_counts: Counter[str] = Counter()
    scope_counts: Counter[str] = Counter()
    gene_counts: Counter[str] = Counter()
    for node in data.nodes:
        for source in _split_pipe(node.get("source_namespaces", "")):
            source_counts[source] += 1
        for sig in _split_pipe(node.get("clinical_significance", "")):
            significance_counts[sig] += 1
        scope = node.get("equivalence_scope", "") or "unknown"
        scope_counts[scope] += 1
        gene = node.get("gene_symbol", "")
        if gene:
            gene_counts[gene] += 1
    relation_counts = Counter(edge.get("relation_kind", "") or "unknown" for edge in data.edges)
    predicate_counts = Counter(edge.get("predicate", "") or "unknown" for edge in data.edges)
    object_ns_counts = Counter(edge.get("target_id", "").split(":", 1)[0] for edge in data.edges if edge.get("target_id"))
    data._stats = {
        "total_variants": len(data.nodes),
        "total_edges": len(data.edges),
        "source_counts": dict(source_counts.most_common()),
        "clinical_significance_counts": dict(significance_counts.most_common(20)),
        "equivalence_scope_counts": dict(scope_counts.most_common()),
        "relation_counts": dict(relation_counts.most_common()),
        "predicate_counts": dict(predicate_counts.most_common()),
        "object_namespace_counts": dict(object_ns_counts.most_common()),
        "top_genes": dict(gene_counts.most_common(20)),
        "source_catalog": data.source_catalog,
        "manifest": data.manifest,
    }
    return data._stats


def _resolve_variant_ids(data: VariantGraphData, raw_ids: str) -> tuple[list[str], list[str]]:
    resolved: list[str] = []
    not_found: list[str] = []
    for token in _split_ids(raw_ids):
        hits: list[str] = []
        if token in data.nodes_by_id:
            hits = [token]
        else:
            seen: set[str] = set()
            for alias in _variant_lookup_aliases(token):
                for hit in data.ids_to_variants.get(alias, []):
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


def build_variant_graph_payload(data: VariantGraphData, ids: str = "") -> dict[str, Any]:
    variant_ids, not_found = _resolve_variant_ids(data, ids)
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

    for variant_id in variant_ids[:25]:
        node = data.nodes_by_id.get(variant_id)
        if not node:
            continue
        add_node(
            variant_id,
            node.get("primary_id") or node.get("name") or variant_id,
            "biolink:SequenceVariant",
            {"node_type": "variant", **node},
        )
        for idx, edge in enumerate(data.edges_by_variant.get(variant_id, [])[:120]):
            target_id = edge.get("target_id", "") or edge.get("target_label", "")
            if not target_id:
                continue
            graph_target_id = target_id if ":" in target_id and " " not in target_id else _json_safe_id("object", target_id)
            target_category = edge.get("target_category", "") or "biolink:NamedThing"
            add_node(
                graph_target_id,
                edge.get("target_label") or target_id,
                target_category,
                {"node_type": "object", "source_curie": target_id, **edge},
            )
            edges.append({
                "data": {
                    "id": f"{variant_id}::{idx}::{graph_target_id}",
                    "source": variant_id,
                    "target": graph_target_id,
                    "label": edge.get("predicate") or edge.get("relation_kind") or "associated_with",
                    **edge,
                },
                "classes": edge.get("relation_kind", "").replace("_", "-") or "association",
            })
    return {
        "elements": {"nodes": nodes, "edges": edges},
        "resolved_ids": variant_ids,
        "not_found": not_found,
        "node_count": len(nodes),
        "edge_count": len(edges),
    }


def export_variants(
    data: VariantGraphData,
    q: str = "",
    source: str = "",
    gene: str = "",
    significance: str = "",
    columns: list[str] | None = None,
    fmt: str = "tsv",
) -> str:
    rows = [
        node for node in data.nodes
        if _matches_query(node, q) and _matches_filters(node, source=source, gene=gene, significance=significance)
    ]
    available = list(data.nodes[0].keys()) if data.nodes else DEFAULT_VARIANT_COLUMNS
    selected = [col for col in (columns or DEFAULT_VARIANT_COLUMNS) if col in available]
    if not selected:
        selected = DEFAULT_VARIANT_COLUMNS
    delimiter = "," if fmt == "csv" else "\t"
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=selected, delimiter=delimiter, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in selected})
    return buf.getvalue()
