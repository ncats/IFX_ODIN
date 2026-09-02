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
        "source_update_report",
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
        self.source_update_report: list[dict[str, str]] = []
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


def _class_token(value: str) -> str:
    return re.sub(r"[^a-z0-9_-]+", "-", str(value or "").replace("_", "-").lower()).strip("-")


def _pipe_join(values: list[Any]) -> str:
    seen: list[str] = []
    for value in values:
        for part in str(value or "").split("|"):
            text = part.strip()
            if text and text not in seen:
                seen.append(text)
    return "|".join(seen)


def _edge_count(value: Any) -> int:
    try:
        return max(1, int(float(str(value or "1"))))
    except ValueError:
        return 1


DRUG_SOURCE_ORDER = {
    "PubChem": 0,
    "ChEMBL": 1,
    "ChEBI": 2,
    "GSRS": 3,
    "RxNorm": 4,
    "UniChem": 5,
    "DrugCentral": 6,
    "NCATS Inxight Drugs": 7,
    "NodeNorm Chemical": 8,
}

DRUG_SOURCE_ROLES = {
    "PubChem": "Observed CID enrichment and structure properties",
    "ChEMBL": "Approved/max-phase molecule source",
    "ChEBI": "Ontology and chemical classification source",
    "GSRS": "NCATS substance registry / UNII source",
    "RxNorm": "RXCUI xref layer; full product context currently xref-only",
    "UniChem": "Planned cross-reference enrichment; disabled until batch-file ingestion",
    "DrugCentral": "Drug identity, approval, and target-interaction context",
    "NCATS Inxight Drugs": "GSRS/Inxight activity and target context",
    "NodeNorm Chemical": "Validator/canonical CURIE service, not an asserting source",
}


def _canonical_drug_source_name(value: str) -> str:
    text = str(value or "").strip()
    if text.startswith("RxNorm"):
        return "RxNorm"
    return text or "Unknown"


def _first_date(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text[:10]
    return ""


def _source_sort_key(row: dict[str, Any]) -> tuple[int, str]:
    name = str(row.get("name") or row.get("source") or "")
    return (DRUG_SOURCE_ORDER.get(name, 999), name.lower())


def summarize_drug_source_versions(data: DrugGraphData) -> list[dict[str, Any]]:
    """Collapse download/transform rows into one dashboard row per source."""
    by_source: dict[str, dict[str, Any]] = {}

    def entry_for(name: str) -> dict[str, Any]:
        source = _canonical_drug_source_name(name)
        if source not in by_source:
            by_source[source] = {
                "name": source,
                "versions": [],
                "version": "",
                "source_release_date": "",
                "odin_download_date": "",
                "odin_transform_date": "",
                "download_status": "",
                "transform_status": "",
                "download_mode": "",
                "download_changed": "",
                "transform_output_rows": 0,
                "url": "",
                "notes": [],
                "note": "",
                "role": DRUG_SOURCE_ROLES.get(source, ""),
            }
        return by_source[source]

    def add_version(entry: dict[str, Any], version: str) -> None:
        version = str(version or "").strip()
        if version and version not in {"unknown", "not captured"} and version not in entry["versions"]:
            entry["versions"].append(version)

    for row in data.source_catalog:
        entry = entry_for(row.get("source", ""))
        add_version(entry, row.get("version", ""))
        if row.get("download_start") or row.get("download_end"):
            entry["odin_download_date"] = _first_date(entry["odin_download_date"], row.get("download_end"), row.get("download_start"))
            entry["download_changed"] = str(row.get("changed") or entry["download_changed"] or "").strip()
        if row.get("transform_start") or row.get("transform_end"):
            entry["odin_transform_date"] = _first_date(entry["odin_transform_date"], row.get("transform_end"), row.get("transform_start"))
        try:
            entry["transform_output_rows"] += int(float(str(row.get("records") or "0")))
        except ValueError:
            pass

    for row in data.source_update_report:
        entry = entry_for(row.get("source", ""))
        artifact = str(row.get("artifact_type") or "").strip().lower()
        add_version(entry, row.get("version", ""))
        source_date = _first_date(row.get("source_release_date"), row.get("last_modified_iso"))
        if source_date and not entry["source_release_date"]:
            entry["source_release_date"] = source_date
        if artifact == "download":
            entry["download_status"] = row.get("status", "") or entry["download_status"]
            entry["download_mode"] = row.get("download_mode", "") or entry["download_mode"]
            entry["download_changed"] = row.get("changed", "") or entry["download_changed"]
            entry["odin_download_date"] = _first_date(entry["odin_download_date"], row.get("download_end"), row.get("download_start"))
        elif artifact == "transform":
            entry["transform_status"] = row.get("status", "") or entry["transform_status"]
            entry["odin_transform_date"] = _first_date(entry["odin_transform_date"], row.get("transform_end"), row.get("transform_start"))
        if row.get("url") and not entry["url"]:
            entry["url"] = row.get("url", "")
        note = row.get("notes") or row.get("skip_reason") or ""
        if note and note not in entry["notes"]:
            entry["notes"].append(note)
        try:
            records = int(float(str(row.get("records") or "0")))
        except ValueError:
            records = 0
        if artifact == "transform" and records:
            entry["transform_output_rows"] = max(int(entry["transform_output_rows"]), records)

    for entry in by_source.values():
        versions = entry.pop("versions", [])
        entry["version"] = "|".join(versions) if versions else "not captured"
        if not entry["source_release_date"] and re.fullmatch(r"\d{4}-\d{2}-\d{2}", entry["version"]):
            entry["source_release_date"] = entry["version"]
        if not entry["source_release_date"] and re.fullmatch(r"\d{4}_\d{2}_\d{2}", entry["version"]):
            entry["source_release_date"] = entry["version"].replace("_", "-")
        entry["note"] = " | ".join(entry.pop("notes", []))
        if not entry["download_status"] and not entry["odin_download_date"]:
            entry["download_status"] = "not applicable"
        if not entry["transform_status"] and entry["odin_transform_date"]:
            entry["transform_status"] = "transformed"
        if entry["transform_output_rows"] == 0 and entry["download_status"] in {"disabled", "xref_only"}:
            entry["note"] = entry["note"] or DRUG_SOURCE_ROLES.get(entry["name"], "")

    return sorted(by_source.values(), key=_source_sort_key)


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
        data.source_update_report = _read_tsv(graph_path / "drug_source_update_report.tsv")
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
    biolink_counts: Counter[str] = Counter()
    nodenorm_counts: Counter[str] = Counter()
    multi_source_count = 0
    for node in data.nodes:
        node_sources = _split_pipe(node.get("source_namespaces", ""))
        if len(node_sources) > 1:
            multi_source_count += 1
        for source in node_sources:
            source_counts[source] += 1
        tier_counts[node.get("evidence_tier", "") or "unknown"] += 1
        key_counts[node.get("structural_key_type", "") or "unknown"] += 1
        for scope in _split_pipe(node.get("drug_scope", "")) or ["unknown"]:
            scope_counts[scope] += 1
        biolink_counts[node.get("biolink_category", "") or "unknown"] += 1
        nodenorm_counts[node.get("nodenorm_validation_status", "") or "unknown"] += 1
    relation_counts = Counter(edge.get("relation_kind", "") or "unknown" for edge in data.edges)
    predicate_counts = Counter(edge.get("predicate", "") or "unknown" for edge in data.edges)
    target_category_counts = Counter(edge.get("target_category", "") or "unknown" for edge in data.edges)
    source_versions = summarize_drug_source_versions(data)
    source_update_status_counts = Counter()
    for row in source_versions:
        for status in (row.get("download_status", ""), row.get("transform_status", "")):
            if status and status not in {"not applicable"}:
                source_update_status_counts[str(status)] += 1
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
    total_drugs = len(data.nodes)
    nodenorm_resolved_count = total_drugs - int(nodenorm_counts.get("not_normalized", 0))
    full_total = int(data.manifest.get("counts", {}).get("full_nodes_available") or total_drugs)
    data._stats = {
        "total_drugs": total_drugs,
        "full_total_drugs": full_total,
        "total_edges": len(data.edges),
        "multi_source_count": multi_source_count,
        "multi_source_percent": (multi_source_count / total_drugs * 100) if total_drugs else 0,
        "source_only_count": total_drugs - multi_source_count,
        "nodenorm_resolved_count": nodenorm_resolved_count,
        "nodenorm_resolved_percent": (nodenorm_resolved_count / total_drugs * 100) if total_drugs else 0,
        "identity_xref_edges": int(relation_counts.get("identity_xref", 0)),
        "context_xref_edges": int(relation_counts.get("external_identifier_xref", 0) + relation_counts.get("source_provenance_xref", 0)),
        "target_association_edges": int(relation_counts.get("drug_target", 0)),
        "literature_edges": int(relation_counts.get("literature_evidence", 0)),
        "open_review_rows": int(review_status_counts.get("open", 0)),
        "resolved_or_acknowledged_review_rows": int(
            review_status_counts.get("resolved", 0) + review_status_counts.get("acknowledged", 0)
        ),
        "source_counts": dict(source_counts.most_common()),
        "evidence_tier_counts": dict(tier_counts.most_common()),
        "structural_key_counts": dict(key_counts.most_common()),
        "drug_scope_counts": dict(scope_counts.most_common(20)),
        "biolink_category_counts": dict(biolink_counts.most_common()),
        "nodenorm_status_counts": dict(nodenorm_counts.most_common()),
        "relation_counts": dict(relation_counts.most_common()),
        "predicate_counts": dict(predicate_counts.most_common()),
        "target_category_counts": dict(target_category_counts.most_common()),
        "review_status_counts": dict(review_status_counts.most_common()),
        "review_issue_counts": dict(review_issue_counts.most_common()),
        "source_catalog": data.source_catalog,
        "source_versions": source_versions,
        "source_update_status_counts": dict(source_update_status_counts.most_common()),
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


def build_drug_graph_payload(data: DrugGraphData, ids: str = "", include_targets: bool = True) -> dict[str, Any]:
    drug_ids, not_found = _resolve_drug_ids(data, ids)
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    seen_nodes: set[str] = set()

    def add_node(node_id: str, label: str, category: str, payload: dict[str, str]) -> None:
        if node_id in seen_nodes:
            return
        seen_nodes.add(node_id)
        classes = {
            _class_token(category.replace("biolink:", "")),
            _class_token(payload.get("node_type", "")),
            _class_token(payload.get("relation_kind", "")),
            _class_token(payload.get("xref_prefix", "")),
        }
        nodes.append({
            "data": {"id": node_id, "label": label or node_id, "category": category, **payload},
            "classes": " ".join(sorted(c for c in classes if c)),
        })

    for drug_id in drug_ids[:25]:
        node = data.nodes_by_id.get(drug_id)
        if not node:
            continue
        add_node(drug_id, node.get("standard_name") or node.get("primary_id") or drug_id, node.get("biolink_category") or "biolink:Drug", {"node_type": "drug", **node})
        raw_edges = data.edges_by_drug.get(drug_id, [])
        if not include_targets:
            raw_edges = [edge for edge in raw_edges if edge.get("relation_kind") != "drug_target"]
        graph_edges = _aggregate_graph_edges(raw_edges)[:220]
        for edge_no, edge in enumerate(graph_edges, start=1):
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
                "classes": " ".join(filter(None, [
                    _class_token(edge.get("relation_kind", "")),
                    _class_token(edge.get("evidence_layer", "")),
                    _class_token(edge.get("predicate", "")),
                ])),
            })
    return {"elements": {"nodes": nodes, "edges": edges}, "resolved_ids": drug_ids, "not_found": not_found, "node_count": len(nodes), "edge_count": len(edges)}


def _aggregate_graph_edges(raw_edges: list[dict[str, str]]) -> list[dict[str, str]]:
    combine_fields = [
        "predicate",
        "target_label",
        "source_labels",
        "evidence_layer",
        "evidence_source",
        "evidence_id",
        "supporting_sources",
        "activity_type",
        "activity_value",
        "activity_unit",
        "mechanism_of_action",
        "action_type",
    ]
    merged: dict[tuple[str, ...], dict[str, str]] = {}
    detail_rows: dict[tuple[str, ...], dict[tuple[str, ...], dict[str, str]]] = defaultdict(dict)
    for edge in raw_edges:
        key = _graph_edge_key(edge)
        if key not in merged:
            out = dict(edge)
            out["edge_record_count"] = str(edge.get("edge_record_count") or "1")
            if clean_label := str(edge.get("target_label", "") or "").strip():
                out["target_labels"] = clean_label
            merged[key] = out
        else:
            existing = merged[key]
            existing["edge_record_count"] = str(_edge_count(existing.get("edge_record_count")) + _edge_count(edge.get("edge_record_count")))
            for field in combine_fields:
                existing[field] = _pipe_join([existing.get(field, ""), edge.get(field, "")])
            existing["target_labels"] = _pipe_join([existing.get("target_labels", ""), edge.get("target_label", "")])
        for detail in _edge_evidence_details(edge):
            detail_key = _edge_detail_key(detail)
            if detail_key in detail_rows[key]:
                stored = detail_rows[key][detail_key]
                stored["edge_record_count"] = str(_edge_count(stored.get("edge_record_count")) + _edge_count(detail.get("edge_record_count")))
            else:
                detail_rows[key][detail_key] = detail
    for key, row in merged.items():
        details = list(detail_rows.get(key, {}).values())
        if details:
            row["evidence_details"] = json.dumps(details, separators=(",", ":"))
    return list(merged.values())


def _graph_edge_key(edge: dict[str, str]) -> tuple[str, ...]:
    kind = str(edge.get("relation_kind", "") or "").strip()
    if kind in {"drug_target", "rxnorm_product_ingredient"}:
        return (
            str(edge.get("source_id", "") or "").strip(),
            str(edge.get("target_id", "") or "").strip(),
            kind,
            str(edge.get("target_category", "") or "").strip(),
        )
    return (
        str(edge.get("source_id", "") or "").strip(),
        str(edge.get("target_id", "") or "").strip(),
        kind,
        str(edge.get("xref_prefix", "") or "").strip(),
        str(edge.get("target_category", "") or "").strip(),
    )


def _edge_evidence_details(edge: dict[str, str]) -> list[dict[str, str]]:
    existing = edge.get("evidence_details", "")
    if existing:
        try:
            parsed = json.loads(existing)
            if isinstance(parsed, list):
                return [
                    {str(k): str(v) for k, v in row.items() if v not in ("", None)}
                    for row in parsed
                    if isinstance(row, dict)
                ]
        except json.JSONDecodeError:
            pass
    detail_fields = [
        "predicate",
        "target_label",
        "evidence_layer",
        "evidence_source",
        "evidence_id",
        "activity_type",
        "activity_value",
        "activity_unit",
        "mechanism_of_action",
        "action_type",
        "supporting_sources",
    ]
    detail = {field: str(edge.get(field, "") or "").strip() for field in detail_fields if str(edge.get(field, "") or "").strip()}
    detail["edge_record_count"] = str(edge.get("edge_record_count") or "1")
    return [detail] if detail else []


def _edge_detail_key(detail: dict[str, str]) -> tuple[str, ...]:
    fields = [
        "predicate",
        "target_label",
        "evidence_layer",
        "evidence_source",
        "evidence_id",
        "activity_type",
        "activity_value",
        "activity_unit",
        "mechanism_of_action",
        "action_type",
        "supporting_sources",
    ]
    return tuple(str(detail.get(field, "") or "").strip() for field in fields)


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


def reload_drug_graph_data(graph_dir: str | Path) -> DrugGraphData:
    key = str(Path(graph_dir).resolve())
    with _singleton_lock:
        _singletons.pop(key, None)
    return load_drug_graph_data(graph_dir)


def resolve_drug(data: DrugGraphData, query_id: str) -> dict[str, Any]:
    drug_ids, not_found = _resolve_drug_ids(data, query_id)
    if not drug_ids:
        return {"resolved": False, "query": query_id, "not_found": [query_id]}
    drug_id = drug_ids[0]
    node = data.nodes_by_id.get(drug_id, {})
    edges = data.edges_by_drug.get(drug_id, [])
    return {
        "resolved": True,
        "query": query_id,
        "drug_id": drug_id,
        "node": node,
        "edges": edges,
        "edge_count": len(edges),
        "all_resolved_ids": drug_ids,
    }


def find_drug_neighbors(data: DrugGraphData, drug_id: str) -> dict[str, Any]:
    node = data.nodes_by_id.get(drug_id)
    if not node:
        return {"drug_id": drug_id, "neighbors": [], "shared_ids": {}}
    xref_ids: set[str] = set()
    for field in ("source_ids", "xrefs"):
        for value in _split_pipe(node.get(field, "")):
            if value.strip():
                xref_ids.add(value.strip())
    for field in ("inchikey", "unii", "pubchem_cid", "chembl_id", "chebi_id", "drugcentral_id", "rxcui", "cas"):
        value = (node.get(field) or "").strip()
        if value:
            xref_ids.add(value)
    neighbor_ids: set[str] = set()
    shared_ids: dict[str, list[str]] = {}
    for xref_id in xref_ids:
        for alias in _drug_lookup_aliases(xref_id):
            for hit in data.ids_to_drugs.get(alias, []):
                if hit != drug_id:
                    neighbor_ids.add(hit)
                    shared_ids.setdefault(hit, [])
                    if xref_id not in shared_ids[hit]:
                        shared_ids[hit].append(xref_id)
    neighbors = [data.nodes_by_id[nid] for nid in sorted(neighbor_ids) if nid in data.nodes_by_id]
    return {"drug_id": drug_id, "neighbors": neighbors, "shared_ids": shared_ids}


SSSOM_COLUMNS = [
    "subject_id",
    "subject_label",
    "predicate_id",
    "object_id",
    "object_label",
    "object_category",
    "mapping_justification",
    "confidence",
    "subject_source",
    "comment",
]


def _sssom_predicate(relation_kind: str) -> str:
    if relation_kind in {"identity_xref"}:
        return "skos:exactMatch"
    return "skos:closeMatch"


def _sssom_justification(relation_kind: str) -> str:
    if relation_kind == "identity_xref":
        return "semapv:LexicalMatching"
    if relation_kind == "drug_target":
        return "semapv:ManualMappingCuration"
    return "semapv:UnspecifiedMatching"


def _sssom_confidence(relation_kind: str, supporting_sources: str) -> str:
    try:
        count = int(supporting_sources or "0")
    except ValueError:
        count = 0
    if relation_kind == "identity_xref" and count >= 2:
        return "0.95"
    if relation_kind == "identity_xref":
        return "0.80"
    if relation_kind == "external_identifier_xref":
        return "0.70"
    return "0.50"


def export_drug_sssom(
    data: DrugGraphData,
    include_sources: list[str] | None = None,
) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=SSSOM_COLUMNS, delimiter="\t", extrasaction="ignore")
    writer.writeheader()
    for edge in data.edges:
        source_id = edge.get("source_id", "")
        relation_kind = edge.get("relation_kind", "")
        if relation_kind in {"literature_evidence", "rxnorm_product_ingredient"}:
            continue
        if include_sources:
            evidence = edge.get("evidence_source", "")
            if not any(src in evidence for src in include_sources):
                continue
        node = data.nodes_by_id.get(source_id, {})
        writer.writerow({
            "subject_id": source_id,
            "subject_label": node.get("standard_name", ""),
            "predicate_id": _sssom_predicate(relation_kind),
            "object_id": edge.get("target_id", ""),
            "object_label": edge.get("target_label", ""),
            "object_category": edge.get("target_category", ""),
            "mapping_justification": _sssom_justification(relation_kind),
            "confidence": _sssom_confidence(relation_kind, edge.get("supporting_sources", "")),
            "subject_source": node.get("source_namespaces", ""),
            "comment": f"relation_kind={relation_kind}",
        })
    return buf.getvalue()


def iter_drug_sssom_bytes(
    data: DrugGraphData,
    include_sources: list[str] | None = None,
) -> bytes:
    return export_drug_sssom(data, include_sources=include_sources).encode("utf-8")


# ---------------------------------------------------------------------------
# Version diff helpers
# ---------------------------------------------------------------------------

def load_drug_version_data(data_dir: str | Path) -> DrugGraphData:
    """Load any versioned drug app_graph directory into a DrugGraphData."""
    data_dir = Path(data_dir)
    key = str(data_dir.resolve())
    with _singleton_lock:
        if key in _singletons:
            return _singletons[key]
        data = DrugGraphData()
        manifest_path = data_dir / "manifest.json"
        if manifest_path.exists():
            try:
                data.manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                data.manifest = {}
        for node in _read_tsv(data_dir / "drug_nodes.tsv"):
            _index_node(data, node)
        data.edges = _read_tsv(data_dir / "drug_edges.tsv")
        for edge in data.edges:
            source_id = edge.get("source_id", "")
            if source_id:
                data.edges_by_drug[source_id].append(edge)
        data.source_catalog = _read_tsv(data_dir / "drug_source_catalog.tsv")
        data.source_update_report = _read_tsv(data_dir / "drug_source_update_report.tsv")
        _singletons[key] = data
        return data


def compute_drug_version_diff(
    current: DrugGraphData,
    baseline: DrugGraphData,
) -> dict[str, Any]:
    """Compute delta between two versioned drug datasets."""
    current_ids = set(current.nodes_by_id.keys())
    baseline_ids = set(baseline.nodes_by_id.keys())

    added_ids = sorted(current_ids - baseline_ids)
    removed_ids = sorted(baseline_ids - current_ids)
    shared_ids = current_ids & baseline_ids

    name_changes: list[dict[str, str]] = []
    tier_changes: list[dict[str, str]] = []
    approval_changes: list[dict[str, str]] = []
    inchikey_changes: list[dict[str, str]] = []

    for did in sorted(shared_ids):
        cur = current.nodes_by_id[did]
        base = baseline.nodes_by_id[did]

        for field, changes_list in [
            ("standard_name", name_changes),
            ("evidence_tier", tier_changes),
            ("approval_status", approval_changes),
            ("inchikey", inchikey_changes),
        ]:
            old_val = base.get(field, "")
            new_val = cur.get(field, "")
            if old_val != new_val:
                changes_list.append({
                    "drug_id": did,
                    "standard_name": cur.get("standard_name") or base.get("standard_name", ""),
                    "old_value": old_val,
                    "new_value": new_val,
                })

    # Edge diffs
    def _edge_key(e: dict[str, str]) -> tuple[str, str, str]:
        return (e.get("source_id", ""), e.get("target_id", ""), e.get("relation_kind", ""))

    current_edge_keys = {_edge_key(e) for e in current.edges}
    baseline_edge_keys = {_edge_key(e) for e in baseline.edges}

    # Source version changes
    cur_sources = summarize_drug_source_versions(current)
    base_sources = summarize_drug_source_versions(baseline)
    cur_src_map = {row["name"]: row for row in cur_sources}
    base_src_map = {row["name"]: row for row in base_sources}
    source_version_changes = []
    for src in sorted(set(cur_src_map) | set(base_src_map)):
        old_v = base_src_map.get(src, {}).get("version", "")
        new_v = cur_src_map.get(src, {}).get("version", "")
        if old_v != new_v:
            source_version_changes.append({
                "source_name": src,
                "old_source_version": old_v,
                "new_source_version": new_v,
            })

    max_rows = 500
    return {
        "baseline_version": baseline.manifest.get("drug_harmonizer_version", ""),
        "current_version": current.manifest.get("drug_harmonizer_version", ""),
        "summary": {
            "drugs_old": len(baseline.nodes),
            "drugs_new": len(current.nodes),
            "drugs_added": len(added_ids),
            "drugs_removed": len(removed_ids),
            "drugs_retained": len(shared_ids),
            "edges_old": len(baseline.edges),
            "edges_new": len(current.edges),
            "edges_added": len(current_edge_keys - baseline_edge_keys),
            "edges_removed": len(baseline_edge_keys - current_edge_keys),
            "standard_name_changes": len(name_changes),
            "evidence_tier_changes": len(tier_changes),
            "approval_status_changes": len(approval_changes),
            "inchikey_changes": len(inchikey_changes),
            "source_version_changes": len(source_version_changes),
        },
        "added_drugs": [
            {f: current.nodes_by_id[did].get(f, "") for f in ("drug_id", "primary_id", "standard_name", "biolink_category", "evidence_tier")}
            for did in added_ids[:max_rows]
        ],
        "removed_drugs": [
            {f: baseline.nodes_by_id[did].get(f, "") for f in ("drug_id", "primary_id", "standard_name", "biolink_category", "evidence_tier")}
            for did in removed_ids[:max_rows]
        ],
        "standard_name_changes": name_changes[:max_rows],
        "evidence_tier_changes": tier_changes[:max_rows],
        "approval_status_changes": approval_changes[:max_rows],
        "inchikey_changes": inchikey_changes[:max_rows],
        "source_version_changes": source_version_changes[:max_rows],
        "truncation": {
            "added_drugs": {"total": len(added_ids), "shown": min(len(added_ids), max_rows)},
            "removed_drugs": {"total": len(removed_ids), "shown": min(len(removed_ids), max_rows)},
            "standard_name_changes": {"total": len(name_changes), "shown": min(len(name_changes), max_rows)},
            "evidence_tier_changes": {"total": len(tier_changes), "shown": min(len(tier_changes), max_rows)},
            "approval_status_changes": {"total": len(approval_changes), "shown": min(len(approval_changes), max_rows)},
            "inchikey_changes": {"total": len(inchikey_changes), "shown": min(len(inchikey_changes), max_rows)},
            "source_version_changes": {"total": len(source_version_changes), "shown": min(len(source_version_changes), max_rows)},
        },
    }
