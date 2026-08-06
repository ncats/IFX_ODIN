"""Target harmonizer app graph loader and explorer helpers."""
from __future__ import annotations

import csv
import io
import json
import re
import threading
from collections import Counter, defaultdict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from fastapi import HTTPException


class TargetGraphData:
    __slots__ = (
        "nodes",
        "nodes_by_id",
        "ids_to_targets",
        "relation_edges",
        "relation_edges_by_target",
        "type_index",
        "exact_terms",
        "token_index",
        "manifest",
        "_stats",
    )

    def __init__(self) -> None:
        self.nodes: list[dict[str, str]] = []
        self.nodes_by_id: dict[str, dict[str, str]] = {}
        self.ids_to_targets: dict[str, list[str]] = defaultdict(list)
        self.relation_edges: list[dict[str, str]] = []
        self.relation_edges_by_target: dict[str, list[dict[str, str]]] = defaultdict(list)
        self.type_index: dict[str, list[str]] = defaultdict(list)
        self.exact_terms: dict[str, set[str]] = defaultdict(set)
        self.token_index: dict[str, set[str]] = defaultdict(set)
        self.manifest: dict[str, Any] = {}
        self._stats: dict[str, Any] | None = None


_singleton: TargetGraphData | None = None
_singleton_lock = threading.Lock()


def _read_tsv(path: Path) -> list[dict[str, str]]:
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def _norm(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.lower()
    text = re.sub(r"['`’]", "", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def _tokens(value: Any) -> set[str]:
    return {token for token in _norm(value).split() if len(token) > 1}


def _split_pipe(value: Any) -> list[str]:
    text = "" if value is None else str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return []
    return [part.strip() for part in text.split("|") if part.strip()]


def _index_exact(data: TargetGraphData, term: str, target_id: str) -> None:
    norm = _norm(term)
    if norm:
        data.exact_terms[norm].add(target_id)


def _index_tokens(data: TargetGraphData, term: str, target_id: str) -> None:
    for token in _tokens(term):
        data.token_index[token].add(target_id)


def _index_row(data: TargetGraphData, row: dict[str, str]) -> None:
    target_id = row.get("target_id", "")
    if not target_id:
        return
    data.nodes.append(row)
    data.nodes_by_id[target_id] = row
    data.type_index[row.get("target_type", "unknown")].append(target_id)

    for term in (
        target_id,
        row.get("primary_id", ""),
        row.get("symbol", ""),
        row.get("name", ""),
    ):
        _index_exact(data, term, target_id)
    for term in (row.get("symbol", ""), row.get("name", "")):
        _index_tokens(data, term, target_id)
    for xref in _split_pipe(row.get("ids", "")):
        data.ids_to_targets[xref].append(target_id)
        _index_exact(data, xref, target_id)


def _index_edge(data: TargetGraphData, row: dict[str, str]) -> None:
    source_id = row.get("source_id", "")
    target_id = row.get("target_id", "")
    if not source_id or not target_id:
        return
    data.relation_edges.append(row)
    data.relation_edges_by_target[source_id].append(row)
    data.relation_edges_by_target[target_id].append(row)


def load_target_graph_data(data_dir: str | Path) -> TargetGraphData:
    global _singleton
    data_dir = Path(data_dir)
    if _singleton is not None:
        return _singleton
    with _singleton_lock:
        if _singleton is not None:
            return _singleton
        if not data_dir.is_dir():
            raise HTTPException(status_code=500, detail=f"Target app graph dir not found: {data_dir}")
        nodes_path = data_dir / "target_nodes.tsv"
        if not nodes_path.exists():
            raise HTTPException(status_code=500, detail=f"Target nodes file not found: {nodes_path}")

        data = TargetGraphData()
        manifest_path = data_dir / "manifest.json"
        if manifest_path.exists():
            with open(manifest_path, encoding="utf-8") as fh:
                data.manifest = json.load(fh)
        with open(nodes_path, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh, delimiter="\t"):
                _index_row(data, row)
        edges_path = data_dir / "target_edges.tsv"
        if edges_path.exists():
            with open(edges_path, newline="", encoding="utf-8") as fh:
                for row in csv.DictReader(fh, delimiter="\t"):
                    _index_edge(data, row)
        _singleton = data
        print(f"Target graph loaded: {len(data.nodes):,} targets, {len(data.relation_edges):,} relation edges")
        return data


def compute_target_stats(data: TargetGraphData) -> dict[str, Any]:
    if data._stats is not None:
        return data._stats
    type_counts = Counter(row.get("target_type", "unknown") or "unknown" for row in data.nodes)
    namespace_counts: Counter[str] = Counter()
    canonical_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    annotation_counts: Counter[str] = Counter(data.manifest.get("annotation_counts") or {})
    has_manifest_annotation_counts = bool(annotation_counts)
    for row in data.nodes:
        for ns in _split_pipe(row.get("id_namespaces", "")):
            namespace_counts[ns] += 1
        status = row.get("canonical_status", "") or "not_applicable"
        canonical_counts[status] += 1
        for src in _split_pipe(row.get("source_namespaces", "").replace(",", "|")):
            source_counts[src] += 1
        if not has_manifest_annotation_counts:
            annotations = _parse_annotations(row)
            annotation_counts.update(annotations.keys())
    edge_counts = Counter(data.manifest.get("edge_predicate_counts") or {})
    if not edge_counts:
        edge_counts = Counter(row.get("predicate", "unknown") or "unknown" for row in data.relation_edges)
    stats = {
        "total_targets": len(data.nodes),
        "type_counts": dict(type_counts.most_common()),
        "identifier_namespace_counts": dict(namespace_counts.most_common()),
        "canonical_status_counts": dict(canonical_counts.most_common(20)),
        "source_counts": dict(source_counts.most_common(20)),
        "edge_predicate_counts": dict(edge_counts.most_common()),
        "annotation_counts": dict(annotation_counts.most_common()),
        "download_column_groups": _download_column_groups(annotation_counts),
        "manifest": data.manifest,
    }
    data._stats = stats
    return stats


def _resolve_target_id(data: TargetGraphData, value: str) -> str | None:
    value = (value or "").strip()
    if value in data.nodes_by_id:
        return value
    candidates = data.ids_to_targets.get(value)
    if candidates:
        return sorted(candidates, key=lambda tid: _target_id_rank(data, tid))[0]
    norm = _norm(value)
    exact = data.exact_terms.get(norm)
    if exact:
        return sorted(exact, key=lambda tid: _target_id_rank(data, tid))[0]
    return None


def _target_score(query_norm: str, row: dict[str, str]) -> float:
    values = [row.get("symbol", ""), row.get("name", ""), row.get("primary_id", ""), row.get("target_id", "")]
    norms = [_norm(v) for v in values if v]
    if query_norm in norms:
        return 1.0
    best = max((SequenceMatcher(None, query_norm, n).ratio() for n in norms), default=0.0)
    if any(n.startswith(query_norm) for n in norms if query_norm):
        best = max(best, 0.92)
    return best


def _type_rank(target_type: str) -> int:
    return {"gene": 0, "protein": 1, "transcript": 2}.get(target_type, 9)


def _canonical_rank(row: dict[str, str]) -> int:
    status = (row.get("canonical_status", "") or "").lower()
    category = (row.get("category", "") or "").lower()
    quality = (row.get("quality_note", "") or "").lower()
    if "true|canonical" in status or category == "canonical" or quality == "reviewed_representative":
        return 0
    if "canonical" in status:
        return 1
    if "alternate" in category:
        return 3
    return 2


def _target_id_rank(data: TargetGraphData, target_id: str) -> tuple[int, int, str]:
    row = data.nodes_by_id.get(target_id, {})
    return (
        _type_rank(row.get("target_type", "")),
        _canonical_rank(row),
        row.get("symbol", "") or row.get("name", "") or target_id,
    )


def _parse_annotations(row: dict[str, str]) -> dict[str, Any]:
    raw = row.get("source_annotations", "")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _target_to_row(row: dict[str, str]) -> dict[str, Any]:
    return {
        "target_id": row.get("target_id", ""),
        "target_type": row.get("target_type", ""),
        "primary_id": row.get("primary_id", ""),
        "symbol": row.get("symbol", ""),
        "name": row.get("name", ""),
        "description": row.get("description", ""),
        "category": row.get("category", ""),
        "ids": row.get("ids", ""),
        "id_namespaces": row.get("id_namespaces", ""),
        "source_namespaces": row.get("source_namespaces", ""),
        "mapping_ratio": row.get("mapping_ratio", ""),
        "quality_note": row.get("quality_note", ""),
        "canonical_status": row.get("canonical_status", ""),
        "canonical_ifx_id": row.get("canonical_ifx_id", ""),
        "source_annotations": row.get("source_annotations", ""),
    }


def _edge_classes(predicate: str) -> str:
    if predicate == "biolink:transcribed_to":
        return "target-relation-edge transcription-edge"
    if predicate == "biolink:translates_to":
        return "target-relation-edge translation-edge"
    if predicate == "biolink:has_gene_product":
        return "target-relation-edge gene-product-edge"
    return "target-relation-edge related-edge"


def _annotation_group(field: str) -> str:
    lower = field.lower()
    if lower.startswith("hgnc_") or lower.startswith("ncbi_") or lower in {"consolidated_location", "ensembl_strand"}:
        return "Gene annotations"
    if lower.startswith("uniprot") or lower.startswith("protein_") or lower.startswith("mapping_"):
        return "Protein annotations"
    if lower.startswith("ensembl_trans") or lower.startswith("refseq_"):
        return "Transcript annotations"
    if lower.endswith("_provenance") or lower.endswith("_mapping_score") or lower.startswith("total_mapping"):
        return "Provenance and support"
    return "Source annotations"


def _download_column_groups(annotation_counts: Counter[str]) -> list[dict[str, Any]]:
    groups: dict[str, list[str]] = {
        "Core": [
            "target_id", "target_type", "primary_id", "symbol", "name", "description", "category",
        ],
        "Identifiers": [
            "ids", "id_namespaces", "source_namespaces",
        ],
        "Quality and status": [
            "mapping_ratio", "quality_note", "canonical_status", "canonical_ifx_id", "createdAt", "updatedAt",
        ],
    }
    for field, _count in annotation_counts.most_common():
        groups.setdefault(_annotation_group(field), []).append(field)
    return [{"label": label, "columns": columns} for label, columns in groups.items()]


def _filter_targets(data: TargetGraphData, q: str = "", target_type: str = "", namespace: str = "") -> list[dict[str, str]]:
    q_norm = _norm(q)
    target_type = (target_type or "").strip().lower()
    namespace = (namespace or "").strip()

    if q_norm:
        candidate_ids: set[str] = set(data.exact_terms.get(q_norm, set()))
        for token in _tokens(q):
            ids = data.token_index.get(token, set())
            if len(ids) <= 5000:
                candidate_ids.update(ids)
        candidates = [data.nodes_by_id[tid] for tid in candidate_ids if tid in data.nodes_by_id]
    else:
        candidates = data.nodes

    filtered: list[dict[str, str]] = []
    for row in candidates:
        if target_type and row.get("target_type", "").lower() != target_type:
            continue
        if namespace and namespace not in _split_pipe(row.get("id_namespaces", "")):
            continue
        filtered.append(row)

    sort_key = (
        (lambda row: (
            -_target_score(q_norm, row),
            _type_rank(row.get("target_type", "")),
            _canonical_rank(row),
            row.get("symbol", "") or row.get("name", ""),
        ))
        if q_norm else
        (lambda row: (
            _type_rank(row.get("target_type", "")),
            _canonical_rank(row),
            row.get("symbol", "") or row.get("name", ""),
        ))
    )
    filtered.sort(key=sort_key)
    return filtered


def search_targets(
    data: TargetGraphData,
    q: str = "",
    target_type: str = "",
    namespace: str = "",
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    q_norm = _norm(q)
    target_type = (target_type or "").strip().lower()
    namespace = (namespace or "").strip()
    per_page = max(1, min(per_page, 200))

    filtered = _filter_targets(data, q=q, target_type=target_type, namespace=namespace)

    total = len(filtered)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    rows = [_target_to_row(row) for row in filtered[start:start + per_page]]
    return {"rows": rows, "total": total, "page": page, "per_page": per_page, "total_pages": total_pages}


def build_target_graph_payload(
    data: TargetGraphData,
    ids: str,
    max_identifier_nodes: int = 120,
    max_relation_edges: int = 450,
) -> dict[str, Any]:
    selected: list[str] = []
    for token in re.split(r"[\s,|]+", ids or ""):
        target_id = _resolve_target_id(data, token)
        if target_id and target_id not in selected:
            selected.append(target_id)
    if not selected:
        return {"elements": [], "selected": []}

    elements: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()
    seen_edges: set[str] = set()

    def add_node(node_id: str, label: str, classes: str, payload: dict[str, Any] | None = None) -> None:
        if node_id in seen_nodes:
            return
        seen_nodes.add(node_id)
        elements.append({"data": {"id": node_id, "label": label, **(payload or {})}, "classes": classes})

    def add_edge(source: str, target: str, label: str, classes: str, payload: dict[str, Any] | None = None) -> None:
        payload = payload or {}
        evidence_key = payload.get("evidence_identifier", "")
        edge_id = f"{source}->{target}:{label}:{evidence_key}"
        if edge_id in seen_edges:
            return
        seen_edges.add(edge_id)
        elements.append({
            "data": {"id": edge_id, "source": source, "target": target, "label": label, **payload},
            "classes": classes,
        })

    def add_target_node(target_id: str, classes: str = "") -> None:
        target = data.nodes_by_id.get(target_id)
        if not target:
            return
        base_classes = f"target {target.get('target_type', '')}"
        if classes:
            base_classes = f"{base_classes} {classes}"
        add_node(
            target_id,
            target.get("symbol") or target.get("name") or target_id,
            base_classes,
            _target_to_row(target),
        )

    for target_id in selected:
        row = data.nodes_by_id[target_id]
        add_target_node(target_id)
        for xref in _split_pipe(row.get("ids", ""))[:max_identifier_nodes]:
            xid = f"xref:{xref}"
            add_node(xid, xref, "xref", {"xref_id": xref, "namespace": xref.split(":", 1)[0]})
            add_edge(
                target_id,
                xid,
                "biolink:same_as",
                "identifier-edge",
                {
                    "kind": "identifier_edge",
                    "predicate": "biolink:same_as",
                    "evidence_identifier": xref,
                    "evidence_namespace": xref.split(":", 1)[0] if ":" in xref else "",
                },
            )

    expanded: set[str] = set()
    frontier: set[str] = set(selected)
    relation_edge_count = 0
    for _depth in range(2):
        next_frontier: set[str] = set()
        for target_id in sorted(frontier, key=lambda tid: _target_id_rank(data, tid)):
            if target_id in expanded:
                continue
            expanded.add(target_id)
            relation_edges = sorted(
                data.relation_edges_by_target.get(target_id, []),
                key=lambda edge: (
                    edge.get("predicate", ""),
                    _target_id_rank(data, edge.get("target_id", "")),
                    edge.get("evidence_identifier", ""),
                ),
            )
            for edge in relation_edges:
                if relation_edge_count >= max_relation_edges:
                    break
                source_id = edge.get("source_id", "")
                target_id2 = edge.get("target_id", "")
                if source_id not in data.nodes_by_id or target_id2 not in data.nodes_by_id:
                    continue
                add_target_node(source_id, "neighbor" if source_id not in selected else "")
                add_target_node(target_id2, "neighbor" if target_id2 not in selected else "")
                predicate = edge.get("predicate", "") or "biolink:related_to"
                add_edge(
                    source_id,
                    target_id2,
                    predicate,
                    _edge_classes(predicate),
                    {
                        "kind": "target_relation_edge",
                        "predicate": predicate,
                        "relation_kind": edge.get("relation_kind", ""),
                        "evidence_identifier": edge.get("evidence_identifier", ""),
                        "evidence_namespace": edge.get("evidence_namespace", ""),
                        "evidence_source": edge.get("evidence_source", ""),
                        "support_tier": edge.get("support_tier", ""),
                        "support_ratio": edge.get("support_ratio", ""),
                        "support_score": edge.get("support_score", ""),
                    },
                )
                relation_edge_count += 1
                if source_id not in expanded:
                    next_frontier.add(source_id)
                if target_id2 not in expanded:
                    next_frontier.add(target_id2)
        frontier = next_frontier
    return {"elements": elements, "selected": selected}


def export_targets(
    data: TargetGraphData,
    q: str = "",
    target_type: str = "",
    namespace: str = "",
    fmt: str = "tsv",
    columns: list[str] | None = None,
) -> str:
    rows = _filter_targets(data, q=q, target_type=target_type, namespace=namespace)
    default_columns = [
        "target_id", "target_type", "primary_id", "symbol", "name", "description", "category",
        "ids", "id_namespaces", "source_namespaces", "mapping_ratio", "quality_note",
        "canonical_status", "canonical_ifx_id",
    ]
    columns = [col for col in (columns or default_columns) if col]
    if not columns:
        columns = default_columns
    delimiter = "," if fmt == "csv" else "\t"
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, delimiter=delimiter, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        base = _target_to_row(row)
        annotations = _parse_annotations(row)
        writer.writerow({col: base.get(col, annotations.get(col, "")) for col in columns})
    return buf.getvalue()
