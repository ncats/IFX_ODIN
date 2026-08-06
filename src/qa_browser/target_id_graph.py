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
        _singleton = data
        print(f"Target graph loaded: {len(data.nodes):,} targets")
        return data


def compute_target_stats(data: TargetGraphData) -> dict[str, Any]:
    if data._stats is not None:
        return data._stats
    type_counts = Counter(row.get("target_type", "unknown") or "unknown" for row in data.nodes)
    namespace_counts: Counter[str] = Counter()
    canonical_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    for row in data.nodes:
        for ns in _split_pipe(row.get("id_namespaces", "")):
            namespace_counts[ns] += 1
        status = row.get("canonical_status", "") or "not_applicable"
        canonical_counts[status] += 1
        for src in _split_pipe(row.get("source_namespaces", "").replace(",", "|")):
            source_counts[src] += 1
    stats = {
        "total_targets": len(data.nodes),
        "type_counts": dict(type_counts.most_common()),
        "identifier_namespace_counts": dict(namespace_counts.most_common()),
        "canonical_status_counts": dict(canonical_counts.most_common(20)),
        "source_counts": dict(source_counts.most_common(20)),
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
        return candidates[0]
    norm = _norm(value)
    exact = data.exact_terms.get(norm)
    if exact:
        return sorted(exact)[0]
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
    }


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

    if q_norm:
        filtered.sort(
            key=lambda row: (
                -_target_score(q_norm, row),
                _type_rank(row.get("target_type", "")),
                _canonical_rank(row),
                row.get("symbol", "") or row.get("name", ""),
            )
        )
    else:
        filtered.sort(key=lambda row: (
            _type_rank(row.get("target_type", "")),
            _canonical_rank(row),
            row.get("symbol", "") or row.get("name", ""),
        ))

    total = len(filtered)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    rows = [_target_to_row(row) for row in filtered[start:start + per_page]]
    return {"rows": rows, "total": total, "page": page, "per_page": per_page, "total_pages": total_pages}


def build_target_graph_payload(data: TargetGraphData, ids: str, max_identifier_nodes: int = 120) -> dict[str, Any]:
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

    def add_edge(source: str, target: str, label: str, classes: str) -> None:
        edge_id = f"{source}->{target}:{label}"
        if edge_id in seen_edges:
            return
        seen_edges.add(edge_id)
        elements.append({"data": {"id": edge_id, "source": source, "target": target, "label": label}, "classes": classes})

    for target_id in selected:
        row = data.nodes_by_id[target_id]
        add_node(
            target_id,
            row.get("symbol") or row.get("name") or target_id,
            f"target {row.get('target_type', '')}",
            _target_to_row(row),
        )
        for xref in _split_pipe(row.get("ids", ""))[:max_identifier_nodes]:
            xid = f"xref:{xref}"
            add_node(xid, xref, "xref", {"xref_id": xref, "namespace": xref.split(":", 1)[0]})
            add_edge(target_id, xid, "has_identifier", "identifier-edge")
            neighbor_ids = sorted(
                data.ids_to_targets.get(xref, []),
                key=lambda tid: (
                    _type_rank(data.nodes_by_id.get(tid, {}).get("target_type", "")),
                    _canonical_rank(data.nodes_by_id.get(tid, {})),
                    data.nodes_by_id.get(tid, {}).get("symbol", "") or data.nodes_by_id.get(tid, {}).get("name", ""),
                ),
            )
            for neighbor_id in neighbor_ids[:8]:
                if neighbor_id == target_id or neighbor_id in selected:
                    continue
                neighbor = data.nodes_by_id.get(neighbor_id)
                if not neighbor:
                    continue
                add_node(
                    neighbor_id,
                    neighbor.get("symbol") or neighbor.get("name") or neighbor_id,
                    f"target neighbor {neighbor.get('target_type', '')}",
                    _target_to_row(neighbor),
                )
                add_edge(neighbor_id, xid, "shares_identifier", "shared-identifier-edge")
    return {"elements": elements, "selected": selected}


def export_targets(data: TargetGraphData, q: str = "", target_type: str = "", namespace: str = "", fmt: str = "tsv") -> str:
    result = search_targets(data, q=q, target_type=target_type, namespace=namespace, page=1, per_page=200_000)
    columns = [
        "target_id", "target_type", "primary_id", "symbol", "name", "category",
        "ids", "id_namespaces", "source_namespaces", "mapping_ratio", "canonical_status",
    ]
    delimiter = "," if fmt == "csv" else "\t"
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=columns, delimiter=delimiter, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(result["rows"])
    return buf.getvalue()
