"""Drug harmonizer app graph loader and explorer helpers."""
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


class DrugGraphData:
    __slots__ = (
        "nodes",
        "nodes_by_id",
        "ids_to_drugs",
        "edges",
        "edges_by_drug",
        "review_queue",
        "review_registry",
        "review_registry_by_id",
        "manifest",
        "source_catalog",
        "_stats",
    )

    def __init__(self) -> None:
        self.nodes: list[dict[str, str]] = []
        self.nodes_by_id: dict[str, dict[str, str]] = {}
        self.ids_to_drugs: dict[str, list[str]] = defaultdict(list)
        self.edges: list[dict[str, str]] = []
        self.edges_by_drug: dict[str, list[dict[str, str]]] = defaultdict(list)
        self.review_queue: list[dict[str, str]] = []
        self.review_registry: list[dict[str, str]] = []
        self.review_registry_by_id: dict[str, dict[str, str]] = {}
        self.manifest: dict[str, Any] = {}
        self.source_catalog: list[dict[str, str]] = []
        self._stats: dict[str, Any] | None = None


_singletons: dict[str, DrugGraphData] = {}
_singleton_lock = threading.Lock()

DEFAULT_DRUG_COLUMNS = [
    "drug_id",
    "primary_id",
    "standard_name",
    "biolink_category",
    "drug_scope",
    "source_namespaces",
    "inchikey",
    "unii",
    "pubchem_cid",
    "chembl_id",
    "chebi_id",
    "drugcentral_id",
    "rxcui",
    "approval_status",
    "source_support",
    "structural_key_type",
    "evidence_tier",
    "quality_flags",
]

DRUG_REVIEW_DECISION_OPTIONS = [
    {
        "value": "accept_component",
        "label": "Accept merged component",
        "description": "Keep the current harmonized drug component as-is.",
    },
    {
        "value": "split_component",
        "label": "Split component",
        "description": "The row likely combines chemically distinct substances.",
    },
    {
        "value": "use_source_standard",
        "label": "Use source-standard ID",
        "description": "Prefer the pipeline's source-priority identifier over the current canonical ID.",
    },
    {
        "value": "use_nodenorm_canonical",
        "label": "Use NodeNorm canonical",
        "description": "Prefer the NodeNorm-preferred CURIE when it is chemically appropriate.",
    },
    {
        "value": "omit_xref",
        "label": "Omit xref/source assertion",
        "description": "Drop a source assertion from future exact identity merging.",
    },
    {
        "value": "needs_expert_review",
        "label": "needs_expert_review",
        "description": "Escalate for domain review without treating the conflict as resolved.",
    },
    {
        "value": "defer",
        "label": "Defer",
        "description": "Leave the row open for later review.",
    },
]

DRUG_REVIEW_INTAKE_COLUMNS = [
    "App review ID",
    "Registry ID",
    "Drug ID",
    "Drug Name",
    "Primary ID",
    "Source-standard primary ID",
    "Issue type",
    "Issue label",
    "Severity",
    "Source",
    "Source value",
    "Consensus value",
    "NodeNorm canonical CURIE",
    "NodeNorm canonical label",
    "NodeNorm validation status",
    "Recommended action",
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


def _json_safe_id(prefix: str, value: str) -> str:
    digest = hashlib.sha1(value.encode("utf-8")).hexdigest()[:12]
    return f"{prefix}:{digest}"


def _drug_lookup_aliases(value: str) -> set[str]:
    text = (value or "").strip()
    if not text:
        return set()
    aliases = {text, text.lower()}
    lower = text.lower()
    if lower.startswith(("chembl.compound:", "chebi:", "drugcentral:", "pubchem.compound:", "unii:", "rxcui:", "cas:", "inchikey:")):
        aliases.add(text.split(":", 1)[1])
    elif re.fullmatch(r"CHEMBL\d+", text, re.I):
        aliases.add(f"CHEMBL.COMPOUND:{text.upper()}")
    elif re.fullmatch(r"\d+", text):
        aliases.update({f"PUBCHEM.COMPOUND:{text}", f"RXCUI:{text}", f"DrugCentral:{text}"})
    elif re.fullmatch(r"[A-Z]{14}-[A-Z]{10}-[A-Z]", text, re.I):
        aliases.add(f"InChIKey:{text.upper()}")
    elif re.fullmatch(r"[A-Z0-9]{10}", text, re.I):
        aliases.add(f"UNII:{text.upper()}")
    return {a for a in aliases if a}


def _add_index(data: DrugGraphData, key: str, drug_id: str) -> None:
    for alias in _drug_lookup_aliases(key):
        data.ids_to_drugs[alias].append(drug_id)


def _index_node(data: DrugGraphData, node: dict[str, str]) -> None:
    drug_id = node.get("drug_id", "")
    if not drug_id:
        return
    data.nodes.append(node)
    data.nodes_by_id[drug_id] = node
    fields = [
        "drug_id", "entity_key", "primary_id", "standard_name", "inchikey", "unii",
        "pubchem_cid", "chembl_id", "chebi_id", "drugcentral_id", "rxcui", "cas",
    ]
    for field in fields:
        _add_index(data, node.get(field, ""), drug_id)
    for field in ("source_ids", "xrefs", "synonyms"):
        for value in _split_pipe(node.get(field, "")):
            _add_index(data, value, drug_id)


def load_drug_graph_data(graph_dir: str | Path) -> DrugGraphData:
    graph_path = Path(graph_dir)
    if not graph_path.exists():
        raise HTTPException(status_code=500, detail=f"Drug graph dir does not exist: {graph_path}")
    key = str(graph_path.resolve())
    with _singleton_lock:
        if key in _singletons:
            return _singletons[key]
        data = DrugGraphData()
        manifest_path = graph_path / "manifest.json"
        if manifest_path.exists():
            try:
                data.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                data.manifest = {}
        for node in _read_tsv(graph_path / "drug_nodes.tsv"):
            _index_node(data, node)
        data.edges = _read_tsv(graph_path / "drug_edges.tsv")
        for edge in data.edges:
            source_id = edge.get("source_id", "")
            if source_id:
                data.edges_by_drug[source_id].append(edge)
        data.review_queue = _read_tsv(graph_path / "drug_review_queue.tsv")
        data.review_registry = _read_tsv(graph_path / "drug_divergence_registry.tsv")
        data.review_registry_by_id = {
            row.get("registry_id", ""): row
            for row in data.review_registry
            if row.get("registry_id")
        }
        data.source_catalog = _read_tsv(graph_path / "drug_source_catalog.tsv")
        _singletons[key] = data
        return data


def _matches_query(node: dict[str, str], q: str) -> bool:
    if not q:
        return True
    needle = q.lower()
    fields = [
        "drug_id", "primary_id", "standard_name", "entity_key", "synonyms", "source_ids", "xrefs",
        "inchikey", "unii", "pubchem_cid", "chembl_id", "chebi_id", "drugcentral_id", "rxcui", "cas",
    ]
    return any(needle in str(node.get(field, "")).lower() for field in fields)


def _matches_filters(node: dict[str, str], source: str = "", tier: str = "", key_type: str = "") -> bool:
    if source and source.lower() not in node.get("source_namespaces", "").lower():
        return False
    if tier and tier.lower() != node.get("evidence_tier", "").lower():
        return False
    if key_type and key_type.lower() != node.get("structural_key_type", "").lower():
        return False
    return True


def search_drugs(
    data: DrugGraphData,
    q: str = "",
    source: str = "",
    tier: str = "",
    key_type: str = "",
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    page = max(int(page or 1), 1)
    per_page = max(1, min(int(per_page or 50), 250))
    rows = [node for node in data.nodes if _matches_query(node, q) and _matches_filters(node, source, tier, key_type)]
    total = len(rows)
    start = (page - 1) * per_page
    return {
        "rows": rows[start:start + per_page],
        "total": total,
        "page": page,
        "per_page": per_page,
        "pages": (total + per_page - 1) // per_page if total else 0,
    }


def compute_drug_stats(data: DrugGraphData) -> dict[str, Any]:
    if data._stats is not None:
        return data._stats
    source_counts: Counter[str] = Counter()
    tier_counts: Counter[str] = Counter()
    key_counts: Counter[str] = Counter()
    scope_counts: Counter[str] = Counter()
    for node in data.nodes:
        for source in _split_pipe(node.get("source_namespaces", "")):
            source_counts[source] += 1
        tier_counts[node.get("evidence_tier", "") or "unknown"] += 1
        key_counts[node.get("structural_key_type", "") or "unknown"] += 1
        for scope in _split_pipe(node.get("drug_scope", "")) or ["unknown"]:
            scope_counts[scope] += 1
    relation_counts = Counter(edge.get("relation_kind", "") or "unknown" for edge in data.edges)
    predicate_counts = Counter(edge.get("predicate", "") or "unknown" for edge in data.edges)
    target_category_counts = Counter(edge.get("target_category", "") or "unknown" for edge in data.edges)
    review_status_counts = Counter(
        row.get("status", "") or "open"
        for row in data.review_registry
    )
    if not review_status_counts and data.review_queue:
        review_status_counts["open"] = len(data.review_queue)
    review_issue_counts = Counter(
        row.get("issue_type", "") or "unknown"
        for row in (data.review_registry or data.review_queue)
    )
    data._stats = {
        "total_drugs": len(data.nodes),
        "total_edges": len(data.edges),
        "open_review_rows": int(review_status_counts.get("open", 0)),
        "resolved_or_acknowledged_review_rows": int(
            review_status_counts.get("resolved", 0) + review_status_counts.get("acknowledged", 0)
        ),
        "source_counts": dict(source_counts.most_common()),
        "evidence_tier_counts": dict(tier_counts.most_common()),
        "structural_key_counts": dict(key_counts.most_common()),
        "drug_scope_counts": dict(scope_counts.most_common(20)),
        "relation_counts": dict(relation_counts.most_common()),
        "predicate_counts": dict(predicate_counts.most_common()),
        "target_category_counts": dict(target_category_counts.most_common()),
        "review_status_counts": dict(review_status_counts.most_common()),
        "review_issue_counts": dict(review_issue_counts.most_common()),
        "source_catalog": data.source_catalog,
        "manifest": data.manifest,
    }
    return data._stats


def _review_row_status(row: dict[str, str]) -> str:
    status = (row.get("status") or "").strip().lower()
    return status or "open"


def _review_row_from_registry(row: dict[str, str]) -> dict[str, str]:
    return {
        "app_review_id": _json_safe_id("drugReview", row.get("registry_id") or row.get("drug_id") or json.dumps(row, sort_keys=True)),
        "registry_id": row.get("registry_id", ""),
        "drug_id": row.get("drug_id", ""),
        "standard_name": row.get("standard_name", ""),
        "primary_id": row.get("primary_id", ""),
        "source_standard_primary_id": row.get("source_standard_primary_id", ""),
        "issue_type": row.get("issue_type", ""),
        "issue_label": row.get("issue_label", ""),
        "severity": row.get("severity", ""),
        "source": row.get("source", ""),
        "source_value": row.get("source_value", ""),
        "consensus_value": row.get("consensus_value", ""),
        "evidence_tier": row.get("evidence_tier", ""),
        "source_namespaces": row.get("source_namespaces", ""),
        "quality_flags": row.get("quality_flags", ""),
        "nodenorm_canonical_curie": row.get("nodenorm_canonical_curie", ""),
        "nodenorm_canonical_label": row.get("nodenorm_canonical_label", ""),
        "nodenorm_validation_status": row.get("nodenorm_validation_status", ""),
        "recommended_action": row.get("recommended_action") or row.get("auto_rationale", ""),
        "review_decision": row.get("review_decision", ""),
        "resolution": row.get("resolution", ""),
        "review_notes": row.get("review_notes", ""),
        "status": _review_row_status(row),
    }


def _drug_review_items(data: DrugGraphData) -> list[dict[str, str]]:
    if data.review_registry:
        registry_items = [_review_row_from_registry(row) for row in data.review_registry]
        queue_actions = {
            row.get("registry_id", ""): row.get("recommended_action", "")
            for row in data.review_queue
            if row.get("registry_id")
        }
        for row in registry_items:
            if not row.get("recommended_action"):
                row["recommended_action"] = queue_actions.get(row.get("registry_id", ""), "")
        return registry_items
    return [_review_row_from_registry({**row, "status": "open"}) for row in data.review_queue]


def _matches_review_status(row: dict[str, str], status: str) -> bool:
    wanted = (status or "open").strip().lower()
    observed = _review_row_status(row)
    if wanted in {"", "all"}:
        return True
    if wanted == "resolved":
        return observed in {"resolved", "acknowledged", "wontfix"}
    return observed == wanted


def _matches_review_query(row: dict[str, str], q: str) -> bool:
    if not q:
        return True
    needle = q.lower()
    fields = [
        "registry_id",
        "drug_id",
        "standard_name",
        "primary_id",
        "source_standard_primary_id",
        "issue_type",
        "issue_label",
        "source",
        "source_value",
        "consensus_value",
        "quality_flags",
        "source_namespaces",
        "nodenorm_canonical_curie",
        "nodenorm_canonical_label",
    ]
    return any(needle in str(row.get(field, "")).lower() for field in fields)


def build_drug_review_queue(
    data: DrugGraphData,
    issue_type: str = "",
    source: str = "",
    severity: str = "",
    status: str = "open",
    q: str = "",
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    page = max(int(page or 1), 1)
    per_page = max(1, min(int(per_page or 50), 250))
    all_rows = _drug_review_items(data)

    status_counts = Counter(_review_row_status(row) for row in all_rows)
    issue_counts = Counter(row.get("issue_type", "") or "unknown" for row in all_rows)
    source_counts = Counter(row.get("source", "") or "unknown" for row in all_rows)
    severity_counts = Counter(row.get("severity", "") or "unknown" for row in all_rows)

    rows = []
    for row in all_rows:
        if issue_type and issue_type.lower() != row.get("issue_type", "").lower():
            continue
        if source:
            source_needle = source.lower()
            if source_needle not in row.get("source", "").lower() and source_needle not in row.get("source_namespaces", "").lower():
                continue
        if severity and severity.lower() != row.get("severity", "").lower():
            continue
        if not _matches_review_status(row, status):
            continue
        if not _matches_review_query(row, q):
            continue
        enriched = dict(row)
        enriched["graph_href"] = f"/drug-id-qa?ids={row.get('drug_id', '')}&tab=graph" if row.get("drug_id") else ""
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
        "issue_counts": dict(issue_counts.most_common()),
        "source_counts": dict(source_counts.most_common()),
        "severity_counts": dict(severity_counts.most_common()),
        "decision_options": DRUG_REVIEW_DECISION_OPTIONS,
    }


def export_drug_review_intake_template(
    data: DrugGraphData,
    issue_type: str = "",
    source: str = "",
    severity: str = "",
    status: str = "open",
    q: str = "",
) -> str:
    payload = build_drug_review_queue(
        data,
        issue_type=issue_type,
        source=source,
        severity=severity,
        status=status,
        q=q,
        page=1,
        per_page=250000,
    )
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=DRUG_REVIEW_INTAKE_COLUMNS, delimiter="\t", extrasaction="ignore")
    writer.writeheader()
    for row in payload["rows"]:
        writer.writerow({
            "App review ID": row.get("app_review_id", ""),
            "Registry ID": row.get("registry_id", ""),
            "Drug ID": row.get("drug_id", ""),
            "Drug Name": row.get("standard_name", ""),
            "Primary ID": row.get("primary_id", ""),
            "Source-standard primary ID": row.get("source_standard_primary_id", ""),
            "Issue type": row.get("issue_type", ""),
            "Issue label": row.get("issue_label", ""),
            "Severity": row.get("severity", ""),
            "Source": row.get("source", ""),
            "Source value": row.get("source_value", ""),
            "Consensus value": row.get("consensus_value", ""),
            "NodeNorm canonical CURIE": row.get("nodenorm_canonical_curie", ""),
            "NodeNorm canonical label": row.get("nodenorm_canonical_label", ""),
            "NodeNorm validation status": row.get("nodenorm_validation_status", ""),
            "Recommended action": row.get("recommended_action", ""),
            "Human decision": row.get("review_decision", ""),
            "Resolution": row.get("resolution", ""),
            "Reviewer notes": row.get("review_notes", ""),
        })
    return buf.getvalue()


def _resolve_drug_ids(data: DrugGraphData, raw_ids: str) -> tuple[list[str], list[str]]:
    resolved: list[str] = []
    not_found: list[str] = []
    for token in _split_ids(raw_ids):
        hits: list[str] = []
        if token in data.nodes_by_id:
            hits = [token]
        else:
            seen = set()
            for alias in _drug_lookup_aliases(token):
                for hit in data.ids_to_drugs.get(alias, []):
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


def build_drug_graph_payload(data: DrugGraphData, ids: str = "") -> dict[str, Any]:
    drug_ids, not_found = _resolve_drug_ids(data, ids)
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()

    def add_node(node_id: str, label: str, category: str, payload: dict[str, str]) -> None:
        if node_id in seen_nodes:
            return
        seen_nodes.add(node_id)
        nodes.append({
            "data": {"id": node_id, "label": label or node_id, "category": category, **payload},
            "classes": category.replace("biolink:", "").replace(":", "_").lower(),
        })

    for drug_id in drug_ids[:25]:
        node = data.nodes_by_id.get(drug_id)
        if not node:
            continue
        add_node(drug_id, node.get("standard_name") or node.get("primary_id") or drug_id, node.get("biolink_category") or "biolink:Drug", {"node_type": "drug", **node})
        for edge_no, edge in enumerate(data.edges_by_drug.get(drug_id, [])[:220], start=1):
            target_id = edge.get("target_id") or edge.get("target_label")
            if not target_id:
                continue
            if target_id in data.nodes_by_id:
                target_node = data.nodes_by_id[target_id]
                graph_target_id = target_id
                add_node(graph_target_id, target_node.get("standard_name") or target_node.get("primary_id") or target_id, target_node.get("biolink_category") or "biolink:Drug", {"node_type": "drug", **target_node})
            else:
                graph_target_id = target_id if ":" in target_id and " " not in target_id else _json_safe_id("object", target_id)
                category = edge.get("target_category") or "biolink:NamedThing"
                node_type = "target" if category in {"biolink:Protein", "biolink:Gene"} else "xref"
                add_node(graph_target_id, edge.get("target_label") or target_id, category, {"node_type": node_type, "source_curie": target_id, **edge})
            edges.append({
                "data": {
                    "id": f"{drug_id}::{edge_no}::{graph_target_id}",
                    "source": drug_id,
                    "target": graph_target_id,
                    "label": edge.get("relation_kind") or edge.get("predicate") or "related_to",
                    **edge,
                },
                "classes": (edge.get("relation_kind") or "association").replace("_", "-"),
            })
    return {"elements": {"nodes": nodes, "edges": edges}, "resolved_ids": drug_ids, "not_found": not_found, "node_count": len(nodes), "edge_count": len(edges)}


def export_drugs(
    data: DrugGraphData,
    q: str = "",
    source: str = "",
    tier: str = "",
    key_type: str = "",
    columns: list[str] | None = None,
    fmt: str = "tsv",
) -> str:
    rows = [node for node in data.nodes if _matches_query(node, q) and _matches_filters(node, source, tier, key_type)]
    available = list(data.nodes[0].keys()) if data.nodes else DEFAULT_DRUG_COLUMNS
    selected = [col for col in (columns or DEFAULT_DRUG_COLUMNS) if col in available]
    if not selected:
        selected = DEFAULT_DRUG_COLUMNS
    delimiter = "," if fmt == "csv" else "\t"
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=selected, delimiter=delimiter, extrasaction="ignore")
    writer.writeheader()
    for row in rows:
        writer.writerow({col: row.get(col, "") for col in selected})
    return buf.getvalue()
