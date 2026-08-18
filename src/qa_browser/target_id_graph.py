"""Target harmonizer app graph loader and explorer helpers."""
from __future__ import annotations

import csv
import io
import json
import os
import re
import threading
import uuid
from collections import Counter, defaultdict
from datetime import datetime, timezone
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

from fastapi import HTTPException


_VALIDATION_SOURCES = {"nodenorm", "error"}


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
        "divergence_gene",
        "divergence_protein",
        "divergence_transcript",
        "_divergence_stats",
        "gene_biotype_counts",
        "qc_dir",
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
        self.divergence_gene: list[dict[str, str]] = []
        self.divergence_protein: list[dict[str, str]] = []
        self.divergence_transcript: list[dict[str, str]] = []
        self._divergence_stats: dict[str, Any] | None = None
        self.gene_biotype_counts: dict[str, int] = {}
        self.qc_dir: str = ""


_singleton: TargetGraphData | None = None
_singleton_lock = threading.Lock()
_target_review_lock = threading.Lock()

TARGET_REVIEW_DECISION_OPTIONS = [
    {"value": "accept_consensus", "label": "Accept consensus"},
    {"value": "accept_sole_source", "label": "Accept sole source"},
    {"value": "accept_low_score", "label": "Accept low score"},
    {"value": "trust_source", "label": "Trust source value"},
    {"value": "trust_refseq", "label": "Trust RefSeq"},
    {"value": "trust_uniprot", "label": "Trust UniProt"},
    {"value": "trust_ensembl", "label": "Trust Ensembl"},
    {"value": "trust_ncbi", "label": "Trust NCBI"},
    {"value": "trust_hgnc", "label": "Trust HGNC"},
    {"value": "cosmetic_ignore", "label": "Cosmetic / ignore"},
    {"value": "use_replacement", "label": "Use replacement"},
    {"value": "needs_expert_review", "label": "Needs expert review"},
]

# ── Triage categories ──
# Classify each divergence row into a triage bucket so users can batch-decide
# the easy ones first.  Categories ordered from easiest to hardest.
TRIAGE_CATEGORIES = [
    {"key": "coverage_gap", "label": "Coverage Gap",
     "description": "Only one source provides this field — expected, not an error."},
    {"key": "low_mapping", "label": "Low Mapping Support",
     "description": "Mapping support ratio is below threshold but may still be correct."},
    {"key": "cosmetic", "label": "Cosmetic Difference",
     "description": "Minor formatting or notation difference between sources."},
    {"key": "source_conflict", "label": "Source Conflict",
     "description": "Sources disagree on a substantive value — needs human review."},
]
_TRIAGE_ORDER = {cat["key"]: i for i, cat in enumerate(TRIAGE_CATEGORIES)}

_TRIAGE_SUGGESTED_DECISION: dict[str, str] = {
    "coverage_gap": "accept_sole_source",
    "low_mapping": "accept_low_score",
    "cosmetic": "cosmetic_ignore",
    "source_conflict": "",  # default — overridden per-row by xref suggestion
}


REVIEW_GROUPS = [
    {
        "key": "protein_refseq_authority",
        "label": "Protein RefSeq Authority",
        "description": "RefSeq protein-accession disagreements where the QC policy already prefers RefSeq for RefSeq IDs.",
        "recommended_decision": "trust_refseq",
    },
    {
        "key": "protein_uniprot_authority",
        "label": "Protein UniProt Authority",
        "description": "UniProt accession disagreements where the QC policy already prefers UniProt for UniProt IDs.",
        "recommended_decision": "trust_uniprot",
    },
    {
        "key": "protein_ensembl_authority",
        "label": "Protein Ensembl Authority",
        "description": "Ensembl protein-ID disagreements where the QC policy already prefers Ensembl for Ensembl IDs.",
        "recommended_decision": "trust_ensembl",
    },
    {
        "key": "protein_isoform_context",
        "label": "Protein Isoform Context",
        "description": "Protein identifier conflicts needing isoform/canonical-product context before accepting one source.",
        "recommended_decision": "needs_expert_review",
    },
    {
        "key": "gene_ncbi_authority",
        "label": "Gene NCBI Authority",
        "description": "NCBI Gene identifier conflicts where the QC policy already prefers NCBI for NCBI Gene IDs.",
        "recommended_decision": "trust_ncbi",
    },
    {
        "key": "gene_hgnc_authority",
        "label": "Gene HGNC Authority",
        "description": "HGNC identifier or symbol conflicts where the QC policy already prefers HGNC.",
        "recommended_decision": "trust_hgnc",
    },
    {
        "key": "gene_symbol_conflict",
        "label": "Gene Symbol Conflict",
        "description": "Gene symbol disagreements, including aliases and changed symbols, that need symbol-history review.",
        "recommended_decision": "needs_expert_review",
    },
    {
        "key": "gene_hgnc_identifier_conflict",
        "label": "Gene HGNC ID Conflict",
        "description": "HGNC identifier disagreements without a current high-confidence authority decision.",
        "recommended_decision": "needs_expert_review",
    },
    {
        "key": "gene_omim_identifier_conflict",
        "label": "Gene OMIM ID Conflict",
        "description": "OMIM identifier disagreements that should be checked against gene/disease inheritance context.",
        "recommended_decision": "needs_expert_review",
    },
    {
        "key": "gene_ensembl_identifier_conflict",
        "label": "Gene Ensembl ID Conflict",
        "description": "Ensembl gene identifier disagreements without a current high-confidence authority decision.",
        "recommended_decision": "needs_expert_review",
    },
    {
        "key": "coverage_gap",
        "label": "Coverage Gap",
        "description": "Only one source provides the field. This is usually expected coverage difference.",
        "recommended_decision": "accept_sole_source",
    },
    {
        "key": "low_mapping_support",
        "label": "Low Mapping Support",
        "description": "Identifier mapping support is below threshold but the row can often be accepted as low-confidence evidence.",
        "recommended_decision": "accept_low_score",
    },
    {
        "key": "cosmetic_metadata",
        "label": "Cosmetic Metadata",
        "description": "Formatting, punctuation, or metadata-only differences that do not change the identifier mapping.",
        "recommended_decision": "cosmetic_ignore",
    },
    {
        "key": "other_conflict",
        "label": "Other Conflict",
        "description": "Conflict pattern not yet assigned to a more specific review group.",
        "recommended_decision": "needs_expert_review",
    },
]
_REVIEW_GROUP_BY_KEY = {group["key"]: group for group in REVIEW_GROUPS}
_REVIEW_GROUP_ORDER = {group["key"]: i for i, group in enumerate(REVIEW_GROUPS)}


def classify_triage_category(row: dict[str, str]) -> str:
    """Assign a triage category to a divergence row.

    Returns the cached ``_triage_category`` if already computed (set once
    at load time), otherwise computes it fresh.

    Categories:
        coverage_gap   — sole_source with no scenario (expected gaps)
        low_mapping    — low_score divergence type
        cosmetic       — already auto-decided as cosmetic_ignore, or
                         description-only conflicts (scenario G06)
        source_conflict — everything else (real conflicts needing review)
    """
    cached = row.get("_triage_category")
    if cached:
        return cached

    div_type = (row.get("divergence_type", "") or "").lower()
    auto_dec = (row.get("auto_decision", "") or "").lower()
    scenario = (row.get("scenario_id", "") or "").upper()

    if div_type == "sole_source":
        return "coverage_gap"
    if div_type == "low_score":
        return "low_mapping"
    if auto_dec == "cosmetic_ignore" or scenario in ("G06", "G07"):
        return "cosmetic"
    return "source_conflict"


def classify_review_group(entity_type: str, row: dict[str, str]) -> str:
    """Assign a specific review group within the broader triage category.

    The group is deliberately policy-facing: it tells a reviewer what kind of
    decision can be made in bulk and which conflicts still need scientific
    inspection.
    """
    cached = row.get("_review_group")
    if cached:
        return cached

    etype = (entity_type or "").strip().lower()
    div_type = (row.get("divergence_type", "") or "").lower()
    auto_dec = (row.get("auto_decision", "") or "").lower()
    scenario = (row.get("scenario_id", "") or "").upper()
    namespace = row.get("namespace", "") or ""

    if div_type == "sole_source":
        return "coverage_gap"
    if div_type == "low_score":
        return "low_mapping_support"
    if auto_dec == "cosmetic_ignore" or scenario in ("G06", "G07"):
        return "cosmetic_metadata"

    if etype == "protein":
        if auto_dec == "trust_refseq":
            return "protein_refseq_authority"
        if auto_dec == "trust_uniprot":
            return "protein_uniprot_authority"
        if auto_dec == "trust_ensembl":
            return "protein_ensembl_authority"
        if namespace in {"Ensembl_Protein_ID", "RefSeq_Protein_ID", "UniProt_ID"}:
            return "protein_isoform_context"

    if etype == "gene":
        if auto_dec == "trust_ncbi":
            return "gene_ncbi_authority"
        if auto_dec == "trust_hgnc":
            return "gene_hgnc_authority"
        if namespace == "Symbol":
            return "gene_symbol_conflict"
        if namespace == "HGNC_ID":
            return "gene_hgnc_identifier_conflict"
        if namespace == "OMIM_ID":
            return "gene_omim_identifier_conflict"
        if namespace == "Ensembl_ID":
            return "gene_ensembl_identifier_conflict"

    return "other_conflict"


# ── Cross-reference enrichment ──
# Map divergence-registry namespace names to the ID prefix used in the
# target graph ``ids`` pipe-list.
_NAMESPACE_TO_ID_PREFIX: dict[str, str] = {
    "Ensembl_Protein_ID": "ENSEMBL:",
    "Ensembl_ID": "ENSEMBL:",
    "Ensembl_Transcript_ID": "ENSEMBL:",
    "RefSeq_Protein_ID": "RefSeq:",
    "RefSeq_Transcript_ID": "RefSeq:",
    "UniProt_ID": "UniProtKB:",
    "NCBI_ID": "NCBIGene:",
    "HGNC_ID": "HGNC:",
    "OMIM_ID": "MIM:",
}


def _lookup_id_in_graph(
    data: TargetGraphData, raw_value: str, namespace: str,
) -> dict[str, Any] | None:
    """Look up a single ID in the target graph.

    Returns ``None`` if the namespace is not mappable (e.g. Description,
    Location, Symbol).  Otherwise returns a dict with ``exists`` bool plus
    target details when found.

    Handles edge cases:
    - Values that already include the prefix (e.g. ``HGNC:6943``)
    - Pipe-delimited compound values (e.g. ``MIM:312095|465000``)
    - Versioned IDs (e.g. ``ENSP00000223321.4``)
    """
    prefix = _NAMESPACE_TO_ID_PREFIX.get(namespace)
    if prefix is None:
        return None
    val = (raw_value or "").strip()
    if not val:
        return None

    # Handle pipe-delimited compound values — split and look up the first
    if "|" in val:
        parts = [p.strip() for p in val.split("|") if p.strip()]
        if len(parts) > 1:
            first_lu = _lookup_id_in_graph(data, parts[0], namespace)
            return first_lu

    def _try_lookup(v: str) -> list[str]:
        """Try prefixed, already-prefixed, and direct lookups."""
        # If value already starts with the expected prefix, use as-is
        if v.startswith(prefix):
            hits = data.ids_to_targets.get(v, [])
            if hits:
                return hits
        # Try adding prefix
        key = f"{prefix}{v}"
        hits = data.ids_to_targets.get(key, [])
        if hits:
            return hits
        # Try direct (value might be indexed without prefix)
        return data.ids_to_targets.get(v, [])

    targets = _try_lookup(val)

    if not targets:
        # Try version-stripped (e.g. ENSP00000223321.4 → ENSP00000223321)
        val_no_ver = re.sub(r"\.\d+$", "", val)
        if val_no_ver != val:
            targets = _try_lookup(val_no_ver)

    if not targets:
        return {"exists": False, "value": raw_value}

    results = []
    for tid in targets[:3]:
        node = data.nodes_by_id.get(tid)
        if node:
            results.append({
                "target_id": tid,
                "primary_id": node.get("primary_id", ""),
                "symbol": node.get("symbol", ""),
                "canonical_status": node.get("canonical_status", ""),
                "target_type": node.get("target_type", ""),
            })
    return {"exists": True, "value": raw_value, "targets": results}


def _enrich_conflict_xrefs(
    data: TargetGraphData, row: dict[str, str],
) -> dict[str, Any]:
    """Enrich a conflict row with cross-reference lookups from the graph.

    Called only for the paginated output rows (typically 50) so cost is low.
    Returns a dict keyed by source name with lookup results plus parent-gene
    information when multiple IDs resolve to the same gene.
    """
    namespace = row.get("namespace", "")
    source_value = row.get("source_value", "")
    source_name = (row.get("source", "") or "").lower()
    consensus_value = row.get("consensus_value", "")
    other_sources_raw = row.get("other_sources", "")

    lookups: dict[str, Any] = {}

    # Look up the primary source_value
    if source_value:
        lu = _lookup_id_in_graph(data, source_value, namespace)
        if lu is not None:
            lookups[source_name] = lu

    # Parse other_sources (protein format: "uniprot: ENSP00000455744, ensembl: ...")
    if other_sources_raw:
        for part in re.split(r"[,;]\s*", other_sources_raw):
            match = re.match(r"^\s*(\w+)\s*:\s*(.+?)\s*$", part)
            if match:
                other_src = match.group(1).lower()
                other_val = match.group(2)
                lu = _lookup_id_in_graph(data, other_val, namespace)
                if lu is not None:
                    lookups[other_src] = lu

    # For gene conflicts (no other_sources), look up consensus_value too
    if consensus_value and not other_sources_raw:
        lu = _lookup_id_in_graph(data, consensus_value, namespace)
        if lu is not None:
            lookups["consensus"] = lu

    if not lookups:
        return {"lookups": lookups}

    # Determine if all found IDs map to the same parent gene
    all_target_ids: set[str] = set()
    for lu in lookups.values():
        if lu.get("exists"):
            for t in lu.get("targets", []):
                all_target_ids.add(t["target_id"])

    result: dict[str, Any] = {"lookups": lookups}

    if len(all_target_ids) > 1:
        parent_genes: dict[str, dict[str, str]] = {}
        for tid in all_target_ids:
            for edge in data.relation_edges_by_target.get(tid, []):
                if edge.get("target_id") == tid:
                    parent = edge.get("source_id", "")
                    parent_node = data.nodes_by_id.get(parent)
                    if parent_node and parent_node.get("target_type") == "gene":
                        parent_genes[tid] = {
                            "gene_id": parent,
                            "symbol": parent_node.get("symbol", ""),
                        }
        if parent_genes:
            result["parent_genes"] = parent_genes
            unique_genes = {pg["gene_id"] for pg in parent_genes.values()}
            result["same_gene"] = len(unique_genes) == 1
            if len(unique_genes) == 1:
                gene = next(iter(parent_genes.values()))
                result["shared_gene_symbol"] = gene["symbol"]

    return result


def _compute_xref_suggestion(
    data: TargetGraphData,
    row: dict[str, str],
    xref_context: dict[str, Any],
) -> dict[str, str]:
    """Compute an automated resolution suggestion from xref enrichment.

    Heuristics:
    - One ID obsolete (not in graph), other exists → trust valid source (high)
    - Both exist, same gene, one maps to target matching row_id → trust that source (medium)
    - Both exist, different genes → needs_expert_review (high)
    - Both exist, same gene, can't pick → needs_expert_review (low)
    - No lookups → no suggestion
    """
    lookups = xref_context.get("lookups", {})
    if not lookups:
        return {}

    # Partition into existing vs missing
    existing: dict[str, dict[str, Any]] = {}
    missing: dict[str, dict[str, Any]] = {}
    for src_name, lu in lookups.items():
        if lu.get("exists"):
            existing[src_name] = lu
        else:
            missing[src_name] = lu

    # Scenario: one obsolete, one exists
    if missing and existing and len(existing) == 1:
        valid_src = next(iter(existing))
        # Map generic source names to decision options
        decision_map = {
            "refseq": "trust_refseq",
            "uniprot": "trust_uniprot",
            "ensembl": "trust_ensembl",
            "ncbi": "trust_ncbi",
            "hgnc": "trust_hgnc",
        }
        decision = decision_map.get(valid_src, "trust_source")
        obsolete_srcs = ", ".join(missing.keys())
        return {
            "suggested_decision": decision,
            "suggestion_rationale": f"{obsolete_srcs} ID not in graph (likely obsolete); {valid_src} ID is valid",
            "suggestion_confidence": "high",
        }

    # All exist — check gene relationships
    if len(existing) >= 2:
        same_gene = xref_context.get("same_gene")
        row_id = row.get("row_id", "")

        if same_gene is False:
            # Different genes — needs expert
            return {
                "suggested_decision": "needs_expert_review",
                "suggestion_rationale": "IDs map to different genes — requires expert judgment",
                "suggestion_confidence": "high",
            }

        if same_gene is True:
            # Same gene — try to identify which source matches the row target
            for src_name, lu in existing.items():
                for t in lu.get("targets", []):
                    primary_id = t.get("primary_id", "")
                    target_id = t.get("target_id", "")
                    # Protein rows: row_id is UniProt accession, primary_id is UniProtKB:accession
                    if row_id and (row_id in primary_id or row_id in target_id):
                        decision_map = {
                            "refseq": "trust_refseq",
                            "uniprot": "trust_uniprot",
                            "ensembl": "trust_ensembl",
                            "ncbi": "trust_ncbi",
                            "hgnc": "trust_hgnc",
                        }
                        decision = decision_map.get(src_name, "trust_source")
                        gene_sym = xref_context.get("shared_gene_symbol", "same gene")
                        return {
                            "suggested_decision": decision,
                            "suggestion_rationale": f"Both map to {gene_sym}; {src_name} matches target {row_id}",
                            "suggestion_confidence": "medium",
                        }

            # Gene rows: check if row_id maps via ENSEMBL: prefix
            if row_id:
                gene_key = f"ENSEMBL:{row_id}"
                gene_targets = data.ids_to_targets.get(gene_key, [])
                if gene_targets:
                    for src_name, lu in existing.items():
                        for t in lu.get("targets", []):
                            if t.get("target_id") in gene_targets:
                                decision_map = {
                                    "refseq": "trust_refseq",
                                    "uniprot": "trust_uniprot",
                                    "ensembl": "trust_ensembl",
                                    "ncbi": "trust_ncbi",
                                    "hgnc": "trust_hgnc",
                                }
                                decision = decision_map.get(src_name, "trust_source")
                                gene_sym = xref_context.get("shared_gene_symbol", "same gene")
                                return {
                                    "suggested_decision": decision,
                                    "suggestion_rationale": f"Both map to {gene_sym}; {src_name} maps to row target",
                                    "suggestion_confidence": "medium",
                                }

            # Same gene but can't determine correct source
            return {
                "suggested_decision": "needs_expert_review",
                "suggestion_rationale": "Both IDs map to same gene but cannot determine correct mapping",
                "suggestion_confidence": "low",
            }

    return {}


TARGET_REVIEW_INTAKE_COLUMNS = [
    "Registry ID", "Entity Type", "Review decision", "Replacement value",
    "Reviewer notes", "Standard Name", "Row ID", "Source", "Namespace",
    "Divergence Type", "Source Value", "Consensus Value", "Scenario ID",
    "Auto Decision", "Auto Confidence", "Auto Rationale",
    "Reviewed by", "Reviewed at", "App review ID",
]


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


def load_target_graph_data(
    data_dir: str | Path,
    qc_dir: str | Path = "",
) -> TargetGraphData:
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

        # Resolve QC directory: explicit > manifest-inferred > relative to data_dir
        resolved_qc = Path(qc_dir) if qc_dir else Path()
        if not (resolved_qc.is_dir() if qc_dir else False):
            # Try to infer from manifest source_files (paths relative to pipeline root)
            source_files = data.manifest.get("source_files", {})
            for rel_path in source_files.values():
                if "target_data" in rel_path:
                    # Walk up from data_dir to find pipeline root
                    for ancestor in [data_dir] + list(data_dir.parents):
                        candidate = ancestor / rel_path
                        if candidate.exists():
                            # Found pipeline root — qc is at src/data/publicdata/target_data/qc
                            td_idx = rel_path.find("target_data/")
                            if td_idx >= 0:
                                resolved_qc = ancestor / rel_path[:td_idx + len("target_data")] / "qc"
                                if resolved_qc.is_dir():
                                    break
                    if resolved_qc.is_dir():
                        break
        if not resolved_qc.is_dir():
            # Fallback: relative to data_dir
            for candidate in [
                data_dir.parent / "qc",
                data_dir / "qc",
            ]:
                if candidate.is_dir():
                    resolved_qc = candidate
                    break
        data.qc_dir = str(resolved_qc) if resolved_qc.is_dir() else ""

        # Load divergence registries from qc/ directory
        if data.qc_dir:
            qc_path = Path(data.qc_dir)
            _divergence_files = [
                ("gene_divergence_registry.tsv", "divergence_gene"),
                ("protein_divergence_registry.tsv", "divergence_protein"),
                ("transcript_divergence_registry.tsv", "divergence_transcript"),
            ]
            for filename, attr in _divergence_files:
                path = qc_path / filename
                if path.exists():
                    rows = _read_tsv(path)
                    setattr(data, attr, rows)
                    if rows:
                        print(f"  Loaded {len(rows):,} {attr.replace('divergence_', '')} divergence rows")

            # Pre-compute triage and review groups on every divergence row (avoids
            # recomputing on each API request across 847K rows).
            for etype, rows in [
                ("gene", data.divergence_gene),
                ("protein", data.divergence_protein),
                ("transcript", data.divergence_transcript),
            ]:
                for row in rows:
                    row["_triage_category"] = classify_triage_category(row)
                    row["_review_group"] = classify_review_group(etype, row)

            # Reconcile previously-saved review decisions so resolved rows
            # don't reappear as "open" after a server restart.
            _reconcile_prior_decisions(data, qc_path)

            # Load gene biotype from provenance CSV if available
            _load_gene_biotype(data, qc_path)

        _singleton = data
        print(f"Target graph loaded: {len(data.nodes):,} targets, {len(data.relation_edges):,} relation edges")
        if data.qc_dir:
            print(f"  QC dir: {data.qc_dir}")
        return data


def _load_gene_biotype(data: TargetGraphData, qc_dir: Path) -> None:
    """Load gene biotype distribution from gene_mapping_provenance.csv."""
    # Look for provenance CSV relative to qc_dir (sibling cleaned/sources/)
    candidates = [
        qc_dir.parent / "cleaned" / "sources" / "gene_mapping_provenance.csv",
    ]
    for path in candidates:
        if not path.exists():
            continue
        biotype: Counter[str] = Counter()
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                # Prefer Ensembl biotype (most granular), fall back to NCBI, then HGNC
                bt = (
                    row.get("ensembl_gene_type", "").strip()
                    or row.get("ncbi_gene_type", "").strip()
                    or row.get("hgnc_gene_type", "").strip()
                )
                if bt:
                    biotype[bt] += 1
        data.gene_biotype_counts = dict(biotype.most_common())
        if biotype:
            print(f"  Loaded gene biotype distribution: {len(biotype)} types, {sum(biotype.values()):,} genes")
        return


def _reconcile_prior_decisions(data: TargetGraphData, qc_dir: Path) -> None:
    """Read the review decisions TSV and mark matching rows as resolved.

    This ensures previously-saved decisions survive a server restart.
    Only reads the 'Registry ID' column to build a lookup set, so the
    cost is proportional to the decisions file size, not divergence rows.
    """
    decisions_path = qc_dir / "review" / "app_completed" / "target_app_review_decisions.tsv"
    if not decisions_path.exists():
        return
    resolved_ids: set[str] = set()
    try:
        with open(decisions_path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                rid = row.get("Registry ID", "").strip()
                if rid:
                    resolved_ids.add(rid)
    except Exception as exc:
        print(f"  Warning: could not read prior decisions: {exc}")
        return
    if not resolved_ids:
        return
    count = 0
    for rows in (data.divergence_gene, data.divergence_protein, data.divergence_transcript):
        for row in rows:
            if row.get("registry_id", "") in resolved_ids:
                row["status"] = "resolved"
                count += 1
    print(f"  Reconciled {count:,} previously-resolved rows from {len(resolved_ids):,} decisions")


def compute_divergence_stats(data: TargetGraphData) -> dict[str, Any]:
    """Single-pass divergence statistics across gene + protein + transcript."""
    if data._divergence_stats is not None:
        return data._divergence_stats

    entity_counts: dict[str, int] = {}
    type_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    auto_decision_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    namespace_counts: Counter[str] = Counter()
    triage_counts: Counter[str] = Counter()
    review_group_counts: Counter[str] = Counter()
    review_group_open_counts: Counter[str] = Counter()
    review_group_resolved_counts: Counter[str] = Counter()
    confidence_values: list[float] = []
    source_breakdown: dict[str, dict[str, Any]] = {}
    open_conflicts = 0
    actionable_count = 0
    total = 0

    for entity_type, rows in [
        ("gene", data.divergence_gene),
        ("protein", data.divergence_protein),
        ("transcript", data.divergence_transcript),
    ]:
        entity_counts[entity_type] = len(rows)
        for row in rows:
            total += 1
            div_type = row.get("divergence_type", "") or ""
            status = row.get("status", "") or "open"
            auto_dec = row.get("auto_decision", "") or ""
            source = row.get("source", "") or ""
            ns = row.get("namespace", "") or ""

            type_counts[div_type] += 1
            status_counts[status] += 1
            if auto_dec:
                auto_decision_counts[auto_dec] += 1
            if source:
                source_counts[source] += 1
            if ns:
                namespace_counts[ns] += 1

            conf_str = row.get("auto_confidence", "")
            try:
                conf = float(conf_str)
                confidence_values.append(conf)
            except (ValueError, TypeError):
                pass

            triage_counts[classify_triage_category(row)] += 1
            review_group = classify_review_group(entity_type, row)
            review_group_counts[review_group] += 1
            if status == "resolved":
                review_group_resolved_counts[review_group] += 1
            else:
                review_group_open_counts[review_group] += 1

            if div_type == "conflict" and status == "open":
                open_conflicts += 1
            if auto_dec == "needs_expert_review":
                actionable_count += 1

            # Per-source breakdown
            if source:
                bd = source_breakdown.setdefault(source, {
                    "total": 0, "conflict_count": 0, "sole_source_count": 0,
                    "low_score_count": 0, "confidence_sum": 0.0,
                    "confidence_n": 0, "needs_expert_count": 0,
                })
                bd["total"] += 1
                if div_type == "conflict":
                    bd["conflict_count"] += 1
                elif div_type == "sole_source":
                    bd["sole_source_count"] += 1
                elif div_type == "low_score":
                    bd["low_score_count"] += 1
                try:
                    bd["confidence_sum"] += float(conf_str)
                    bd["confidence_n"] += 1
                except (ValueError, TypeError):
                    pass
                if auto_dec == "needs_expert_review":
                    bd["needs_expert_count"] += 1

    # Build confidence histogram (0.0–1.0 in 10 buckets)
    confidence_distribution: dict[str, int] = {}
    if confidence_values:
        for v in confidence_values:
            bucket = min(int(v * 10), 9)
            label = f"{bucket * 10}-{(bucket + 1) * 10}%"
            confidence_distribution[label] = confidence_distribution.get(label, 0) + 1

    # Finalize source breakdown with avg_confidence
    for bd in source_breakdown.values():
        n = bd.pop("confidence_n")
        s = bd.pop("confidence_sum")
        bd["avg_confidence"] = round(s / n, 3) if n else None

    result: dict[str, Any] = {
        "total_divergences": total,
        "divergences_by_entity": entity_counts,
        "divergence_type_counts": dict(type_counts.most_common()),
        "status_counts": dict(status_counts.most_common()),
        "auto_decision_counts": dict(auto_decision_counts.most_common()),
        "source_divergence_counts": dict(source_counts.most_common()),
        "namespace_divergence_counts": dict(namespace_counts.most_common()),
        "confidence_distribution": confidence_distribution,
        "source_divergence_breakdown": dict(
            sorted(source_breakdown.items(), key=lambda kv: -kv[1]["total"])
        ),
        "open_conflicts": open_conflicts,
        "actionable_count": actionable_count,
        "triage_category_counts": dict(
            sorted(triage_counts.items(), key=lambda kv: _TRIAGE_ORDER.get(kv[0], 9))
        ),
        "review_group_counts": dict(
            sorted(review_group_counts.items(), key=lambda kv: _REVIEW_GROUP_ORDER.get(kv[0], 99))
        ),
        "review_group_open_counts": dict(
            sorted(review_group_open_counts.items(), key=lambda kv: _REVIEW_GROUP_ORDER.get(kv[0], 99))
        ),
        "review_group_resolved_counts": dict(
            sorted(review_group_resolved_counts.items(), key=lambda kv: _REVIEW_GROUP_ORDER.get(kv[0], 99))
        ),
        "review_groups": REVIEW_GROUPS,
    }
    data._divergence_stats = result
    return result


def compute_target_stats(data: TargetGraphData) -> dict[str, Any]:
    if data._stats is not None:
        return data._stats
    type_counts = Counter(row.get("target_type", "unknown") or "unknown" for row in data.nodes)
    namespace_counts: Counter[str] = Counter()
    canonical_counts: Counter[str] = Counter()
    # Sources per entity type (excluding validation-only sources like nodenorm)
    gene_sources: Counter[str] = Counter()
    protein_sources: Counter[str] = Counter()
    transcript_sources: Counter[str] = Counter()
    canonical_count = 0
    annotation_counts: Counter[str] = Counter(data.manifest.get("annotation_counts") or {})
    has_manifest_annotation_counts = bool(annotation_counts)
    for row in data.nodes:
        target_type = row.get("target_type", "")
        for ns in _split_pipe(row.get("id_namespaces", "")):
            namespace_counts[ns] += 1
        # Canonical status only tracked for proteins
        if target_type == "protein":
            status = row.get("canonical_status", "") or "not_applicable"
            canonical_counts[status] += 1
        if _canonical_rank(row) == 0:
            canonical_count += 1
        for src in _split_pipe(row.get("source_namespaces", "").replace(",", "|")):
            if src.lower() in _VALIDATION_SOURCES:
                continue
            if target_type == "gene":
                gene_sources[src] += 1
            elif target_type == "protein":
                protein_sources[src] += 1
            elif target_type == "transcript":
                transcript_sources[src] += 1
        if not has_manifest_annotation_counts:
            annotations = _parse_annotations(row)
            annotation_counts.update(annotations.keys())
    edge_counts = Counter(data.manifest.get("edge_predicate_counts") or {})
    if not edge_counts:
        edge_counts = Counter(row.get("predicate", "unknown") or "unknown" for row in data.relation_edges)
    # All real sources combined (for "sources covered" stat)
    all_sources: set[str] = set(gene_sources) | set(protein_sources) | set(transcript_sources)
    stats = {
        "total_targets": len(data.nodes),
        "type_counts": dict(type_counts.most_common()),
        "identifier_namespace_counts": dict(namespace_counts.most_common()),
        "canonical_status_counts": dict(canonical_counts.most_common(20)),
        "gene_sources": dict(gene_sources.most_common()),
        "protein_sources": dict(protein_sources.most_common()),
        "transcript_sources": dict(transcript_sources.most_common()),
        "edge_predicate_counts": dict(edge_counts.most_common()),
        "annotation_counts": dict(annotation_counts.most_common()),
        "download_column_groups": _download_column_groups(annotation_counts),
        "manifest": data.manifest,
        "canonical_count": canonical_count,
        "sources_covered": len(all_sources),
        "gene_biotype_counts": data.gene_biotype_counts,
        "divergence": compute_divergence_stats(data),
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
        "createdAt": row.get("createdAt", ""),
        "updatedAt": row.get("updatedAt", ""),
    }


def _edge_classes(predicate: str) -> str:
    if predicate == "biolink:transcribed_to":
        return "target-relation-edge transcription-edge"
    if predicate == "biolink:translates_to":
        return "target-relation-edge translation-edge"
    if predicate == "biolink:has_gene_product":
        return "target-relation-edge gene-product-edge"
    return "target-relation-edge related-edge"


def _build_edge_element(
    source_id: str,
    target_id: str,
    edge_row: dict[str, str],
) -> dict[str, Any]:
    predicate = edge_row.get("predicate", "") or "biolink:related_to"
    evidence_key = edge_row.get("evidence_identifier", "")
    edge_id = f"{source_id}->{target_id}:{predicate}:{evidence_key}"
    return {
        "data": {
            "id": edge_id,
            "source": source_id,
            "target": target_id,
            "label": predicate,
            "kind": "target_relation_edge",
            "predicate": predicate,
            "relation_kind": edge_row.get("relation_kind", ""),
            "evidence_identifier": evidence_key,
            "evidence_namespace": edge_row.get("evidence_namespace", ""),
            "evidence_source": edge_row.get("evidence_source", ""),
            "support_tier": edge_row.get("support_tier", ""),
            "support_ratio": edge_row.get("support_ratio", ""),
            "support_score": edge_row.get("support_score", ""),
        },
        "classes": _edge_classes(predicate),
    }


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


def _is_canonical(row: dict[str, str]) -> bool:
    return _canonical_rank(row) == 0


def _product_node_label(row: dict[str, str]) -> str:
    """Label with source identifier and canonical tag."""
    primary_id = row.get("primary_id", "")
    label = primary_id or row.get("symbol", "") or row.get("target_id", "")
    if _is_canonical(row):
        label = f"{label}\n\u2605 canonical"
    return label


def _build_product_element(
    data: TargetGraphData,
    product_id: str,
    parent_id: str,
    edge_row: dict[str, str],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Build a (node, edge) pair for one relation product."""
    product = data.nodes_by_id.get(product_id)
    if not product:
        return None
    product_type = product.get("target_type", "")
    canonical = _is_canonical(product)
    classes = f"target {product_type}"
    if canonical:
        classes += " canonical"
    node = {
        "data": {
            "id": product_id,
            "label": _product_node_label(product),
            **_target_to_row(product),
        },
        "classes": classes,
    }
    edge = _build_edge_element(parent_id, product_id, edge_row)
    return node, edge


def _collect_products_by_type(
    data: TargetGraphData,
    parent_id: str,
    max_expandable: int = 5,
) -> tuple[list[dict], dict[str, list[dict]], dict[str, int]]:
    """Collect products split into initial (canonical) and expandable.

    Shows the proper hierarchy: gene → transcript → protein.
    The initial view connects the canonical protein to its translating
    transcript via ``biolink:translates_to`` (not gene→protein).

    Returns (initial_elements, expandable_by_type, total_counts).
    """
    # Step 1: Collect gene→transcript and gene→protein edges
    transcript_info: list[tuple[str, dict[str, str], dict[str, str]]] = []
    protein_info: list[tuple[str, dict[str, str], dict[str, str]]] = []
    seen: set[str] = set()

    for edge_row in data.relation_edges_by_target.get(parent_id, []):
        predicate = edge_row.get("predicate", "")
        if predicate not in ("biolink:transcribed_to", "biolink:has_gene_product"):
            continue
        source_id = edge_row.get("source_id", "")
        target_id = edge_row.get("target_id", "")
        product_id = target_id if source_id == parent_id else source_id
        if product_id == parent_id or product_id in seen:
            continue
        seen.add(product_id)
        product = data.nodes_by_id.get(product_id)
        if not product:
            continue
        product_type = product.get("target_type", "")
        if product_type == "transcript":
            transcript_info.append((product_id, product, edge_row))
        elif product_type == "protein":
            protein_info.append((product_id, product, edge_row))

    # Sort: canonical first
    transcript_info.sort(key=lambda x: _canonical_rank(x[1]))
    protein_info.sort(key=lambda x: _canonical_rank(x[1]))

    total_counts: dict[str, int] = {}
    if transcript_info:
        total_counts["transcript"] = len(transcript_info)
    if protein_info:
        total_counts["protein"] = len(protein_info)
    if not transcript_info and not protein_info:
        return [], {}, total_counts

    # Step 2: Build translates_to index (protein → (transcript, edge))
    transcript_id_set = {t[0] for t in transcript_info}
    translates_to: dict[str, tuple[str, dict[str, str]]] = {}
    for pid, _prow, _pedge in protein_info:
        for edge_row in data.relation_edges_by_target.get(pid, []):
            if edge_row.get("predicate") != "biolink:translates_to":
                continue
            src = edge_row.get("source_id", "")
            tgt = edge_row.get("target_id", "")
            tid = src if tgt == pid else tgt
            if tid in transcript_id_set:
                translates_to[pid] = (tid, edge_row)
                break

    # Step 3: Build initial elements — gene → transcript → protein
    initial: list[dict] = []
    initial_transcript_id: str | None = None
    initial_protein_id: str | None = None

    if protein_info:
        initial_protein_id = protein_info[0][0]

        if initial_protein_id in translates_to:
            # Use the transcript that translates to the canonical protein
            initial_transcript_id, tt_edge = translates_to[initial_protein_id]
            gene_product_edge = protein_info[0][2]
            for tid, _trow, tedge in transcript_info:
                if tid == initial_transcript_id:
                    result = _build_product_element(data, tid, parent_id, tedge)
                    if result:
                        initial.extend(result)
                    break
            # Connect protein to transcript via translates_to
            result = _build_product_element(data, initial_protein_id, initial_transcript_id, tt_edge)
            if result:
                initial.extend(result)
            initial.append(_build_edge_element(parent_id, initial_protein_id, gene_product_edge))
        else:
            # Fallback: no translates_to found
            if transcript_info:
                initial_transcript_id = transcript_info[0][0]
                result = _build_product_element(data, initial_transcript_id, parent_id, transcript_info[0][2])
                if result:
                    initial.extend(result)
            result = _build_product_element(data, initial_protein_id, parent_id, protein_info[0][2])
            if result:
                initial.extend(result)
    elif transcript_info:
        initial_transcript_id = transcript_info[0][0]
        result = _build_product_element(data, initial_transcript_id, parent_id, transcript_info[0][2])
        if result:
            initial.extend(result)

    # Step 4: Expandable products
    expandable_by_type: dict[str, list[dict]] = {}

    remaining_transcripts = [
        (tid, trow, tedge) for tid, trow, tedge in transcript_info
        if tid != initial_transcript_id
    ][:max_expandable]
    if remaining_transcripts:
        exp_t: list[dict] = []
        for tid, _trow, tedge in remaining_transcripts:
            result = _build_product_element(data, tid, parent_id, tedge)
            if result:
                exp_t.extend(result)
        if exp_t:
            expandable_by_type["transcript"] = exp_t

    remaining_proteins = [
        (pid, prow, pedge) for pid, prow, pedge in protein_info
        if pid != initial_protein_id
    ][:max_expandable]
    if remaining_proteins:
        exp_p: list[dict] = []
        for pid, _prow, pedge in remaining_proteins:
            used_translation_edge = False
            # Use translates_to if the translating transcript is visible
            if pid in translates_to:
                tt_tid, tt_edge = translates_to[pid]
                # Safe to use: transcript is either initial or gene is always there
                if tt_tid == initial_transcript_id or any(
                    t[0] == tt_tid for t in remaining_transcripts
                ):
                    result = _build_product_element(data, pid, tt_tid, tt_edge)
                    used_translation_edge = True
                else:
                    result = _build_product_element(data, pid, parent_id, pedge)
            else:
                result = _build_product_element(data, pid, parent_id, pedge)
            if result:
                exp_p.extend(result)
                if used_translation_edge:
                    exp_p.append(_build_edge_element(parent_id, pid, pedge))
        if exp_p:
            expandable_by_type["protein"] = exp_p

    return initial, expandable_by_type, total_counts


def build_target_graph_payload(
    data: TargetGraphData,
    ids: str,
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

    def add_element(element: dict[str, Any]) -> None:
        payload = element.get("data", {})
        element_id = payload.get("id", "")
        if payload.get("source") and payload.get("target"):
            if element_id in seen_edges:
                return
            seen_edges.add(element_id)
            elements.append(element)
            return
        if element_id in seen_nodes:
            return
        seen_nodes.add(element_id)
        elements.append(element)

    def add_target_node(target_id: str) -> None:
        row = data.nodes_by_id[target_id]
        target_type = row.get("target_type", "")
        canonical = _is_canonical(row)
        classes = f"target {target_type}"
        if canonical:
            classes += " canonical"
        label = _product_node_label(row) if canonical else (
            row.get("symbol") or row.get("name") or target_id
        )
        add_node(target_id, label, classes, _target_to_row(row))

    # Build selected nodes and keep gene anchors for product expansion.
    context_genes: list[str] = []

    def add_context_gene(gene_id: str) -> None:
        if gene_id and gene_id not in context_genes:
            context_genes.append(gene_id)

    for target_id in selected:
        add_target_node(target_id)
        row = data.nodes_by_id[target_id]
        if row.get("target_type") == "gene":
            add_context_gene(target_id)
            continue
        for edge_row in data.relation_edges_by_target.get(target_id, []):
            predicate = edge_row.get("predicate", "")
            if predicate not in {
                "biolink:transcribed_to",
                "biolink:translates_to",
                "biolink:has_gene_product",
            }:
                continue
            source_id = edge_row.get("source_id", "")
            edge_target_id = edge_row.get("target_id", "")
            if not source_id or not edge_target_id:
                continue
            for node_id in (source_id, edge_target_id):
                if node_id in data.nodes_by_id:
                    add_target_node(node_id)
                    if data.nodes_by_id[node_id].get("target_type") == "gene":
                        add_context_gene(node_id)
            add_element(_build_edge_element(source_id, edge_target_id, edge_row))

    # Add initial canonical products and collect expandable products from gene anchors.
    expandable_transcripts: dict[str, list[dict]] = {}
    expandable_proteins: dict[str, list[dict]] = {}
    product_counts: dict[str, dict[str, int]] = {}
    for target_id in context_genes:
        initial, expandable_by_type, counts = _collect_products_by_type(data, target_id)
        # Add canonical transcript + protein to initial elements
        for element in initial:
            add_element(element)
        if expandable_by_type.get("transcript"):
            expandable_transcripts[target_id] = expandable_by_type["transcript"]
        if expandable_by_type.get("protein"):
            expandable_proteins[target_id] = expandable_by_type["protein"]
        if counts:
            product_counts[target_id] = counts
        # Annotate gene node with total and expandable counts
        for el in elements:
            if el.get("data", {}).get("id") == target_id:
                for ptype, count in counts.items():
                    el["data"][f"total_{ptype}_count"] = str(count)
                    el["data"][f"expandable_{ptype}_count"] = str(max(0, count - 1))
                break

    result: dict[str, Any] = {"elements": elements, "selected": selected}
    if expandable_transcripts:
        result["expandableTranscripts"] = expandable_transcripts
    if expandable_proteins:
        result["expandableProteins"] = expandable_proteins
    if product_counts:
        result["productCounts"] = product_counts
    return result


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


def build_target_review_queue(
    data: TargetGraphData,
    entity_type: str = "",
    divergence_type: str = "",
    source: str = "",
    status: str = "open",
    auto_decision: str = "",
    triage_category: str = "",
    review_group: str = "",
    q: str = "",
    page: int = 1,
    per_page: int = 50,
) -> dict[str, Any]:
    """Build a paginated, filterable review queue from divergence registries."""
    entity_type = (entity_type or "").strip().lower()
    divergence_type = (divergence_type or "").strip().lower()
    source_filter = (source or "").strip().lower()
    status_filter = (status or "").strip().lower()
    auto_decision_filter = (auto_decision or "").strip().lower()
    triage_filter = (triage_category or "").strip().lower()
    review_group_filter = (review_group or "").strip().lower()
    q_lower = (q or "").strip().lower()

    # ── Pre-pass: total counts across ALL divergences (regardless of filters) ──
    triage_category_totals: Counter[str] = Counter()
    review_group_totals: Counter[str] = Counter()
    total_all = 0
    total_resolved = 0
    for etype, rows in [
        ("gene", data.divergence_gene),
        ("protein", data.divergence_protein),
        ("transcript", data.divergence_transcript),
    ]:
        for row in rows:
            total_all += 1
            row_triage = classify_triage_category(row)
            row_group = classify_review_group(etype, row)
            triage_category_totals[row_triage] += 1
            review_group_totals[row_group] += 1
            if (row.get("status", "") or "open").lower() == "resolved":
                total_resolved += 1

    # Combine all divergence lists with entity_type annotation
    combined: list[tuple[str, dict[str, str], str, str]] = []
    for etype, rows in [
        ("gene", data.divergence_gene),
        ("protein", data.divergence_protein),
        ("transcript", data.divergence_transcript),
    ]:
        for row in rows:
            combined.append((etype, row, classify_triage_category(row), classify_review_group(etype, row)))

    # ── Shared-filter pass: apply all filters EXCEPT entity_type, triage, and review group ──
    def _passes_shared(etype: str, row: dict[str, str], row_triage: str, row_group: str) -> bool:
        row_div_type = (row.get("divergence_type", "") or "").lower()
        row_status = (row.get("status", "") or "open").lower()
        row_source = (row.get("source", "") or "").lower()
        row_auto_dec = (row.get("auto_decision", "") or "").lower()
        if divergence_type and row_div_type != divergence_type:
            return False
        if status_filter and row_status != status_filter:
            return False
        if source_filter and row_source != source_filter:
            return False
        if auto_decision_filter and row_auto_dec != auto_decision_filter:
            return False
        if q_lower:
            haystack = " ".join([
                row.get("standard_name", ""),
                row.get("row_id", ""),
                row.get("source_value", ""),
                row.get("consensus_value", ""),
                row.get("auto_rationale", ""),
                row.get("registry_id", ""),
            ]).lower()
            if q_lower not in haystack:
                return False
        return True

    # Two-pass counting for cross-filter stability:
    # - entity_type_counts: from base_pool filtered by triage (but NOT entity_type)
    # - triage_counts: from base_pool filtered by entity_type (but NOT triage)
    entity_type_counts: Counter[str] = Counter()
    triage_counts: Counter[str] = Counter()
    review_group_counts: Counter[str] = Counter()
    divergence_type_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    auto_decision_counts: Counter[str] = Counter()
    filtered: list[tuple[str, dict[str, str], str, str]] = []

    for etype, row, row_triage, row_group in combined:
        if not _passes_shared(etype, row, row_triage, row_group):
            continue
        row_div_type = (row.get("divergence_type", "") or "").lower()
        row_status = (row.get("status", "") or "open").lower()
        row_source = (row.get("source", "") or "").lower()
        row_auto_dec = (row.get("auto_decision", "") or "").lower()

        # Entity counts: apply triage + review group filters but NOT entity_type
        if (not triage_filter or row_triage == triage_filter) and (
            not review_group_filter or row_group == review_group_filter
        ):
            entity_type_counts[etype] += 1

        # Triage counts: apply entity_type + review group filters but NOT triage
        if (not entity_type or etype == entity_type) and (
            not review_group_filter or row_group == review_group_filter
        ):
            triage_counts[row_triage] += 1

        # Review group counts: apply entity_type + triage filters but NOT review group
        if (not entity_type or etype == entity_type) and (
            not triage_filter or row_triage == triage_filter
        ):
            review_group_counts[row_group] += 1

        # All filters for final list and remaining facets
        if entity_type and etype != entity_type:
            continue
        if triage_filter and row_triage != triage_filter:
            continue
        if review_group_filter and row_group != review_group_filter:
            continue

        divergence_type_counts[row_div_type] += 1
        if row_source:
            source_counts[row_source] += 1
        status_counts[row_status] += 1
        if row_auto_dec:
            auto_decision_counts[row_auto_dec] += 1
        filtered.append((etype, row, row_triage, row_group))

    # Sort: source_conflict first, then cosmetic, low_mapping, coverage_gap.
    # Within each category, lowest confidence first.
    def _sort_key(item: tuple[str, dict[str, str], str, str]) -> tuple:
        etype, row, triage, group = item
        try:
            conf = float(row.get("auto_confidence", "1.0"))
        except (ValueError, TypeError):
            conf = 1.0
        return (
            _TRIAGE_ORDER.get(triage, 9),
            _REVIEW_GROUP_ORDER.get(group, 99),
            conf,
            row.get("standard_name", "").lower(),
        )

    filtered.sort(key=_sort_key)

    # Paginate
    total = len(filtered)
    per_page = max(1, min(per_page, 200))
    total_pages = max(1, (total + per_page - 1) // per_page)
    page = max(1, min(page, total_pages))
    start = (page - 1) * per_page
    rows_out: list[dict[str, Any]] = []
    for etype, row, triage, group in filtered[start:start + per_page]:
        group_info = _REVIEW_GROUP_BY_KEY.get(group, {})
        out = dict(row)
        out["entity_type"] = etype
        out["triage_category"] = triage
        out["review_group"] = group
        out["review_group_label"] = group_info.get("label", group)
        out["review_group_description"] = group_info.get("description", "")
        out["review_group_recommended_decision"] = group_info.get("recommended_decision", "")
        out["suggested_decision"] = (
            group_info.get("recommended_decision")
            or _TRIAGE_SUGGESTED_DECISION.get(triage, "")
        )
        # Enrich conflict rows with cross-reference lookups (only for page)
        if triage == "source_conflict":
            xref_ctx = _enrich_conflict_xrefs(data, row)
            out["xref_context"] = xref_ctx
            # Compute automated suggestion from xref enrichment
            suggestion = _compute_xref_suggestion(data, row, xref_ctx)
            if suggestion:
                out["suggested_decision"] = suggestion.get("suggested_decision", "")
                out["suggestion_rationale"] = suggestion.get("suggestion_rationale", "")
                out["suggestion_confidence"] = suggestion.get("suggestion_confidence", "")
        rows_out.append(out)

    return {
        "rows": rows_out,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
        "entity_type_counts": dict(entity_type_counts.most_common()),
        "divergence_type_counts": dict(divergence_type_counts.most_common()),
        "source_counts": dict(source_counts.most_common()),
        "status_counts": dict(status_counts.most_common()),
        "auto_decision_counts": dict(auto_decision_counts.most_common()),
        "triage_category_counts": dict(
            sorted(triage_counts.items(), key=lambda kv: _TRIAGE_ORDER.get(kv[0], 9))
        ),
        "review_group_counts": dict(
            sorted(review_group_counts.items(), key=lambda kv: _REVIEW_GROUP_ORDER.get(kv[0], 99))
        ),
        "triage_categories": TRIAGE_CATEGORIES,
        "review_groups": REVIEW_GROUPS,
        "triage_category_totals": dict(triage_category_totals.most_common()),
        "review_group_totals": dict(
            sorted(review_group_totals.items(), key=lambda kv: _REVIEW_GROUP_ORDER.get(kv[0], 99))
        ),
        "total_all": total_all,
        "total_resolved": total_resolved,
        "decision_options": TARGET_REVIEW_DECISION_OPTIONS,
    }


def build_batch_review_payload(
    data: TargetGraphData,
    triage_category: str,
    review_decision: str,
    entity_type: str = "",
    source: str = "",
    status: str = "open",
    review_group: str = "",
    reviewed_by: str = "",
) -> list[dict[str, str]]:
    """Build review rows for all items matching a triage category.

    Used for batch-applying a decision to an entire category at once.
    """
    entity_type = (entity_type or "").strip().lower()
    source_filter = (source or "").strip().lower()
    status_filter = (status or "").strip().lower()
    review_group_filter = (review_group or "").strip().lower()
    reviewed_by = (reviewed_by or os.getenv("USER") or "app_batch_review").strip()
    reviewed_at = datetime.now(timezone.utc).isoformat()

    allowed = {opt["value"] for opt in TARGET_REVIEW_DECISION_OPTIONS}
    if review_decision not in allowed:
        raise HTTPException(status_code=400, detail=f"Unsupported review_decision: {review_decision}")

    rows: list[dict[str, str]] = []
    for etype, divergence_rows in [
        ("gene", data.divergence_gene),
        ("protein", data.divergence_protein),
        ("transcript", data.divergence_transcript),
    ]:
        if entity_type and etype != entity_type:
            continue
        for row in divergence_rows:
            row_status = (row.get("status", "") or "open").lower()
            row_source = (row.get("source", "") or "").lower()
            if status_filter and row_status != status_filter:
                continue
            if source_filter and row_source != source_filter:
                continue
            if triage_category and classify_triage_category(row) != triage_category:
                continue
            if review_group_filter and classify_review_group(etype, row) != review_group_filter:
                continue
            rows.append({
                "Registry ID": row.get("registry_id", ""),
                "Entity Type": etype,
                "Review decision": review_decision,
                "Replacement value": "",
                "Reviewer notes": f"Batch: {review_group_filter or triage_category}",
                "Standard Name": row.get("standard_name", ""),
                "Row ID": row.get("row_id", ""),
                "Source": row.get("source", ""),
                "Namespace": row.get("namespace", ""),
                "Divergence Type": row.get("divergence_type", ""),
                "Source Value": row.get("source_value", ""),
                "Consensus Value": row.get("consensus_value", ""),
                "Scenario ID": row.get("scenario_id", ""),
                "Auto Decision": row.get("auto_decision", ""),
                "Auto Confidence": row.get("auto_confidence", ""),
                "Auto Rationale": row.get("auto_rationale", ""),
                "Reviewed by": reviewed_by,
                "Reviewed at": reviewed_at,
                "App review ID": str(uuid.uuid4()),
            })
    return rows


def _invalidate_divergence_caches(data: TargetGraphData) -> None:
    """Clear cached stats so they are recomputed after status changes."""
    data._divergence_stats = None
    data._stats = None


def mark_rows_resolved_by_registry_ids(
    data: TargetGraphData, registry_ids: set[str],
) -> int:
    """Mark matching divergence rows as resolved in memory."""
    count = 0
    for rows in (data.divergence_gene, data.divergence_protein, data.divergence_transcript):
        for row in rows:
            if row.get("registry_id", "") in registry_ids and row.get("status", "open") != "resolved":
                row["status"] = "resolved"
                count += 1
    if count:
        _invalidate_divergence_caches(data)
    return count


def mark_rows_resolved_by_triage(
    data: TargetGraphData,
    triage_category: str,
    entity_type: str = "",
    source: str = "",
    status: str = "open",
    review_group: str = "",
) -> int:
    """Mark all divergence rows matching a triage category as resolved in memory."""
    entity_type = (entity_type or "").strip().lower()
    source_filter = (source or "").strip().lower()
    status_filter = (status or "").strip().lower()
    review_group_filter = (review_group or "").strip().lower()
    count = 0
    for etype, rows in [
        ("gene", data.divergence_gene),
        ("protein", data.divergence_protein),
        ("transcript", data.divergence_transcript),
    ]:
        if entity_type and etype != entity_type:
            continue
        for row in rows:
            row_status = (row.get("status", "") or "open").lower()
            row_source = (row.get("source", "") or "").lower()
            if status_filter and row_status != status_filter:
                continue
            if source_filter and row_source != source_filter:
                continue
            if triage_category and classify_triage_category(row) != triage_category:
                continue
            if review_group_filter and classify_review_group(etype, row) != review_group_filter:
                continue
            row["status"] = "resolved"
            count += 1
    if count:
        _invalidate_divergence_caches(data)
    return count


def validate_and_build_target_review_rows(payload: dict) -> list[dict[str, str]]:
    """Validate review payload and return rows ready for TSV append."""
    raw_rows = payload.get("decisions") or payload.get("rows") or []
    if isinstance(raw_rows, dict):
        raw_rows = [raw_rows]
    if not isinstance(raw_rows, list):
        raise HTTPException(status_code=400, detail="decisions must be a list.")

    reviewed_by = str(payload.get("reviewed_by") or os.getenv("USER") or "app_review").strip()
    reviewed_at = datetime.now(timezone.utc).isoformat()
    allowed = {opt["value"] for opt in TARGET_REVIEW_DECISION_OPTIONS}
    rows: list[dict[str, str]] = []
    for raw in raw_rows:
        if not isinstance(raw, dict):
            continue
        registry_id = str(raw.get("registry_id") or raw.get("Registry ID") or "").strip()
        decision = str(raw.get("review_decision") or raw.get("Review decision") or "").strip()
        if not registry_id:
            raise HTTPException(status_code=400, detail="Each review decision needs registry_id.")
        if not decision:
            raise HTTPException(status_code=400, detail="Each review decision needs review_decision.")
        if decision not in allowed:
            raise HTTPException(status_code=400, detail=f"Unsupported review_decision: {decision}")

        rows.append({
            "Registry ID": registry_id,
            "Entity Type": str(raw.get("entity_type") or raw.get("Entity Type") or "").strip(),
            "Review decision": decision,
            "Replacement value": str(raw.get("replacement") or raw.get("Replacement value") or "").strip(),
            "Reviewer notes": str(raw.get("notes") or raw.get("Reviewer notes") or "").strip(),
            "Standard Name": str(raw.get("standard_name") or raw.get("Standard Name") or "").strip(),
            "Row ID": str(raw.get("row_id") or raw.get("Row ID") or "").strip(),
            "Source": str(raw.get("source") or raw.get("Source") or "").strip(),
            "Namespace": str(raw.get("namespace") or raw.get("Namespace") or "").strip(),
            "Divergence Type": str(raw.get("divergence_type") or raw.get("Divergence Type") or "").strip(),
            "Source Value": str(raw.get("source_value") or raw.get("Source Value") or "").strip(),
            "Consensus Value": str(raw.get("consensus_value") or raw.get("Consensus Value") or "").strip(),
            "Scenario ID": str(raw.get("scenario_id") or raw.get("Scenario ID") or "").strip(),
            "Auto Decision": str(raw.get("auto_decision") or raw.get("Auto Decision") or "").strip(),
            "Auto Confidence": str(raw.get("auto_confidence") or raw.get("Auto Confidence") or "").strip(),
            "Auto Rationale": str(raw.get("auto_rationale") or raw.get("Auto Rationale") or "").strip(),
            "Reviewed by": reviewed_by,
            "Reviewed at": reviewed_at,
            "App review ID": str(raw.get("app_review_id") or uuid.uuid4()),
        })
    if not rows:
        raise HTTPException(status_code=400, detail="No review decisions provided.")
    return rows


def append_target_review_rows(rows: list[dict[str, str]], review_file_path: str | Path) -> str:
    """Thread-safe append of review rows to TSV file."""
    path = Path(review_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with _target_review_lock:
        exists = path.exists() and path.stat().st_size > 0
        with open(path, "a", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=TARGET_REVIEW_INTAKE_COLUMNS,
                delimiter="\t",
                extrasaction="ignore",
            )
            if not exists:
                writer.writeheader()
            writer.writerows(rows)
    return str(path)
