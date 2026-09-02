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
    "variant_key",
    "primary_id",
    "name",
    "biolink_category",
    "variant_type",
    "dbsnp_id",
    "clinvar_variation_id",
    "clinvar_allele_id",
    "uniprot_id",
    "uniprot_entry_name",
    "uniprot_feature_id",
    "protein_name",
    "omim_gene_mim",
    "omim_allelic_variant_id",
    "clinvar_rcv",
    "mutation",
    "gene_symbol",
    "gene_curie",
    "hgnc_id",
    "assembly",
    "chromosome",
    "position",
    "reference_allele",
    "alternate_allele",
    "risk_allele",
    "protein_position",
    "reference_amino_acid",
    "alternate_amino_acid",
    "amino_acid_change",
    "clinical_significance",
    "clinical_significance_class",
    "source_review_status",
    "review_status",
    "review_strength_score",
    "source_namespaces",
    "source_record_count",
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
    lower = text.lower()
    if lower.startswith("dbsnp:rs"):
        bare = text.split(":", 1)[1]
        aliases.update({bare, bare.lower(), bare[2:] if bare.lower().startswith("rs") else bare})
    elif re.fullmatch(r"rs\d+", text, re.I):
        aliases.add(f"dbSNP:{text.lower()}")
    elif re.fullmatch(r"\d+", text):
        aliases.update({f"dbSNP:rs{text}", f"ClinVarVariation:{text}", f"ClinVarAllele:{text}"})
    elif lower.startswith(("clinvarvariation:", "clinvarallele:")):
        aliases.add(text.split(":", 1)[1])
    elif lower.startswith(("uniprotkb:", "uniprotvar:")):
        aliases.add(text.split(":", 1)[1])
    elif re.fullmatch(r"[A-Z]\d[A-Z0-9]{3,7}", text, re.I):
        # UniProt accession pattern (e.g. P02649, Q9Y6K9)
        aliases.add(f"UniProtKB:{text.upper()}")
    elif re.fullmatch(r"VAR_\d+", text, re.I):
        # UniProt feature ID pattern (e.g. VAR_000664)
        aliases.add(f"UniProtVAR:{text.upper()}")
    return {a for a in aliases if a}


def _add_variant_index(data: VariantGraphData, key: str, variant_id: str) -> None:
    for alias in _variant_lookup_aliases(key):
        alias_list = data.ids_to_variants[alias]
        if variant_id not in alias_list:
            alias_list.append(variant_id)


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
        node.get("name", ""),
        node.get("dbsnp_id", ""),
        node.get("clinvar_variation_id", ""),
        node.get("clinvar_allele_id", ""),
        node.get("clinvar_rcv", ""),
        node.get("uniprot_id", ""),
        node.get("uniprot_entry_name", ""),
        node.get("uniprot_feature_id", ""),
        node.get("protein_name", ""),
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
        "uniprot_id",
        "uniprot_entry_name",
        "uniprot_feature_id",
        "protein_name",
        "amino_acid_change",
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


def build_variant_graph_payload(
    data: VariantGraphData,
    ids: str = "",
    include_overlap: bool = True,
    include_disagreements: bool = True,
) -> dict[str, Any]:
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
        edge_no = 0
        for edge in data.edges_by_variant.get(variant_id, [])[:180]:
            relation_kind = edge.get("relation_kind", "")
            is_overlap = relation_kind.startswith("variant_")
            is_disagreement = relation_kind == "variant_potential_disagreement" or edge.get("conflict_flag") == "TRUE"
            if is_overlap and not include_overlap:
                continue
            if is_disagreement and not include_disagreements:
                continue
            target_id = edge.get("target_id", "") or edge.get("target_label", "")
            if not target_id:
                continue
            if target_id in data.nodes_by_id:
                target_node = data.nodes_by_id[target_id]
                graph_target_id = target_id
                target_category = "biolink:SequenceVariant"
                add_node(
                    graph_target_id,
                    target_node.get("primary_id") or target_node.get("name") or target_id,
                    target_category,
                    {"node_type": "variant", **target_node},
                )
            else:
                graph_target_id = target_id if ":" in target_id and " " not in target_id else _json_safe_id("object", target_id)
                target_category = edge.get("target_category", "") or "biolink:NamedThing"
                add_node(
                    graph_target_id,
                    edge.get("target_label") or target_id,
                    target_category,
                    {"node_type": "object", "source_curie": target_id, **edge},
                )
            edge_no += 1
            edges.append({
                "data": {
                    "id": f"{variant_id}::{edge_no}::{graph_target_id}",
                    "source": variant_id,
                    "target": graph_target_id,
                    "label": relation_kind if is_overlap else (edge.get("predicate") or relation_kind or "associated_with"),
                    **edge,
                },
                "classes": relation_kind.replace("_", "-") or "association",
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


# ---------------------------------------------------------------------------
# Version diff helpers
# ---------------------------------------------------------------------------

def load_variant_version_data(data_dir: str | Path) -> VariantGraphData:
    """Load any versioned variant app_graph directory into a VariantGraphData."""
    data_dir = Path(data_dir)
    key = str(data_dir.resolve())
    with _singleton_lock:
        if key in _singletons:
            return _singletons[key]
        data = VariantGraphData()
        manifest_path = data_dir / "manifest.json"
        if manifest_path.exists():
            try:
                data.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                data.manifest = {}
        for node in _read_tsv(data_dir / "variant_nodes.tsv"):
            _index_node(data, node)
        data.edges = _read_tsv(data_dir / "variant_edges.tsv")
        for edge in data.edges:
            source_id = edge.get("source_id", "")
            if source_id:
                data.edges_by_variant[source_id].append(edge)
        data.source_catalog = _read_tsv(data_dir / "variant_source_catalog.tsv")
        _singletons[key] = data
        return data


def compute_variant_version_diff(
    current: VariantGraphData,
    baseline: VariantGraphData,
) -> dict[str, Any]:
    """Compute delta between two versioned variant datasets."""
    current_ids = set(current.nodes_by_id.keys())
    baseline_ids = set(baseline.nodes_by_id.keys())

    added_ids = sorted(current_ids - baseline_ids)
    removed_ids = sorted(baseline_ids - current_ids)
    shared_ids = current_ids & baseline_ids

    name_changes: list[dict[str, str]] = []
    clinical_significance_changes: list[dict[str, str]] = []
    variant_type_changes: list[dict[str, str]] = []

    for vid in sorted(shared_ids):
        cur = current.nodes_by_id[vid]
        base = baseline.nodes_by_id[vid]

        for field, changes_list in [
            ("name", name_changes),
            ("clinical_significance", clinical_significance_changes),
            ("variant_type", variant_type_changes),
        ]:
            old_val = base.get(field, "")
            new_val = cur.get(field, "")
            if old_val != new_val:
                changes_list.append({
                    "variant_id": vid,
                    "gene_symbol": cur.get("gene_symbol") or base.get("gene_symbol", ""),
                    "name": cur.get("name") or base.get("name", ""),
                    "old_value": old_val,
                    "new_value": new_val,
                })

    # Edge diffs
    def _edge_key(e: dict[str, str]) -> tuple[str, str, str]:
        return (e.get("source_id", ""), e.get("target_id", ""), e.get("relation_kind", ""))

    current_edge_keys = {_edge_key(e) for e in current.edges}
    baseline_edge_keys = {_edge_key(e) for e in baseline.edges}

    # Source version changes
    cur_src = {row.get("source", ""): row for row in current.source_catalog}
    base_src = {row.get("source", ""): row for row in baseline.source_catalog}
    source_version_changes = []
    for src in sorted(set(cur_src) | set(base_src)):
        old_v = base_src.get(src, {}).get("version", "")
        new_v = cur_src.get(src, {}).get("version", "")
        if old_v != new_v:
            source_version_changes.append({
                "source_name": src,
                "old_source_version": old_v,
                "new_source_version": new_v,
            })

    node_fields = ("variant_id", "primary_id", "name", "variant_type", "gene_symbol", "clinical_significance")
    max_rows = 500
    return {
        "baseline_version": baseline.manifest.get("variant_harmonizer_version", ""),
        "current_version": current.manifest.get("variant_harmonizer_version", ""),
        "summary": {
            "variants_old": len(baseline.nodes),
            "variants_new": len(current.nodes),
            "variants_added": len(added_ids),
            "variants_removed": len(removed_ids),
            "variants_retained": len(shared_ids),
            "edges_old": len(baseline.edges),
            "edges_new": len(current.edges),
            "edges_added": len(current_edge_keys - baseline_edge_keys),
            "edges_removed": len(baseline_edge_keys - current_edge_keys),
            "name_changes": len(name_changes),
            "clinical_significance_changes": len(clinical_significance_changes),
            "variant_type_changes": len(variant_type_changes),
            "source_version_changes": len(source_version_changes),
        },
        "added_variants": [
            {f: current.nodes_by_id[vid].get(f, "") for f in node_fields}
            for vid in added_ids[:max_rows]
        ],
        "removed_variants": [
            {f: baseline.nodes_by_id[vid].get(f, "") for f in node_fields}
            for vid in removed_ids[:max_rows]
        ],
        "name_changes": name_changes[:max_rows],
        "clinical_significance_changes": clinical_significance_changes[:max_rows],
        "variant_type_changes": variant_type_changes[:max_rows],
        "source_version_changes": source_version_changes[:max_rows],
        "truncation": {
            "added_variants": {"total": len(added_ids), "shown": min(len(added_ids), max_rows)},
            "removed_variants": {"total": len(removed_ids), "shown": min(len(removed_ids), max_rows)},
            "name_changes": {"total": len(name_changes), "shown": min(len(name_changes), max_rows)},
            "clinical_significance_changes": {"total": len(clinical_significance_changes), "shown": min(len(clinical_significance_changes), max_rows)},
            "variant_type_changes": {"total": len(variant_type_changes), "shown": min(len(variant_type_changes), max_rows)},
            "source_version_changes": {"total": len(source_version_changes), "shown": min(len(source_version_changes), max_rows)},
        },
    }
