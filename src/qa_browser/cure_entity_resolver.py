import csv
import math
import os
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

try:
    import pandas as pd
except Exception:  # pragma: no cover - the page reports a missing dependency.
    pd = None


DEFAULT_CURE_DRUG_INPUT_FILE = Path(
    "/Users/mainejl/Documents/Projects/ODIN/CUREID/drug_resolver2.6/data/input/"
    "2026.04.09-all_cureid_drugs.csv"
)
DEFAULT_CURE_DRUG_REVIEW_DIR = Path(
    "/Users/mainejl/Documents/Projects/ODIN/CUREID/drug_resolver2.6/data/phase1c_review/"
    "reviewer_batches_current"
)

_GREEK = {
    "α": "alpha",
    "Α": "alpha",
    "β": "beta",
    "Β": "beta",
    "γ": "gamma",
    "Γ": "gamma",
    "δ": "delta",
    "Δ": "delta",
}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "as",
    "by",
    "for",
    "in",
    "of",
    "or",
    "the",
    "to",
    "with",
}
_DRUG_PRODUCT_RE = re.compile(
    r"\b("
    r"mg|mcg|g|ml|iu|unit|units|tablet|tab|capsule|cap|oral|injection|injectable|"
    r"solution|suspension|cream|ointment|gel|patch|spray|syrup|infusion|intravenous|"
    r"subcutaneous|topical|extended release|delayed release"
    r")\b",
    re.I,
)
_DISEASE_HINT_RE = re.compile(
    r"\b("
    r"syndrome|disease|disorder|deficiency|neoplasm|cancer|carcinoma|leukemia|"
    r"lymphoma|anemia|ataxia|dystrophy|malformation|phenotype"
    r")\b",
    re.I,
)


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = "".join(_GREEK.get(ch, ch) for ch in text)
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.lower()
    text = text.replace("\u00b5", "u")
    text = re.sub(r"['’`]", "", text)
    text = re.sub(r"[/+&]", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def token_set(value: Any) -> set[str]:
    return {
        token
        for token in normalize_text(value).split()
        if (len(token) > 1 or token.isdigit()) and token not in _STOPWORDS
    }


def split_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, float) and math.isnan(value):
        return []
    text = str(value).strip()
    if not text or text.lower() in {"nan", "none", "null"}:
        return []
    parts = re.split(r"\s*(?:\||;|\n)\s*", text)
    values: list[str] = []
    for part in parts:
        part = part.strip()
        if part and part.lower() not in {"nan", "none", "null"}:
            values.append(part)
    return values


def compact_id(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and math.isnan(value):
        return ""
    text = str(value).strip()
    if not text or text.lower() == "nan":
        return ""
    if re.fullmatch(r"\d+\.0", text):
        return text[:-2]
    return text


@dataclass
class ResolverTerm:
    term: str
    concept_id: str
    label: str
    entity_type: str
    source: str
    term_kind: str
    identifiers: list[str] = field(default_factory=list)
    biolink_category: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    norm: str = ""
    tokens: set[str] = field(default_factory=set)

    def __post_init__(self) -> None:
        self.term = str(self.term or "").strip()
        self.norm = normalize_text(self.term)
        self.tokens = token_set(self.term)


class CureEntityResolverIndex:
    def __init__(self, terms: Iterable[ResolverTerm], warnings: Iterable[str] = ()):
        self.terms = [term for term in terms if term.norm]
        self.warnings = list(warnings)
        self.exact_index: dict[str, list[int]] = defaultdict(list)
        self.token_index: dict[str, list[int]] = defaultdict(list)
        self.source_counts: dict[str, int] = defaultdict(int)
        self.type_counts: dict[str, int] = defaultdict(int)

        for idx, term in enumerate(self.terms):
            self.exact_index[term.norm].append(idx)
            for token in term.tokens:
                self.token_index[token].append(idx)
            self.source_counts[term.source] += 1
            self.type_counts[term.entity_type] += 1

    def stats(self) -> dict[str, Any]:
        return {
            "term_count": len(self.terms),
            "exact_label_count": len(self.exact_index),
            "entity_types": dict(sorted(self.type_counts.items())),
            "sources": dict(sorted(self.source_counts.items())),
            "warnings": self.warnings,
        }

    def resolve_many(
        self,
        queries: Iterable[str],
        *,
        entity_type: str = "auto",
        top_k: int = 5,
    ) -> list[dict[str, Any]]:
        return [
            self.resolve_one(query, entity_type=entity_type, top_k=top_k)
            for query in queries
            if str(query or "").strip()
        ]

    def resolve_one(self, query: str, *, entity_type: str = "auto", top_k: int = 5) -> dict[str, Any]:
        query = str(query or "").strip()
        query_norm = normalize_text(query)
        query_tokens = token_set(query)
        detected_type = detect_entity_type(query)
        requested_type = (entity_type or "auto").strip().lower()
        type_filter = detected_type if requested_type == "auto" and detected_type != "unknown" else ""
        if requested_type != "auto":
            type_filter = requested_type

        candidate_indices: set[int] = set(self.exact_index.get(query_norm, []))
        for token in query_tokens:
            candidate_indices.update(self.token_index.get(token, []))

        scored: dict[str, dict[str, Any]] = {}
        for idx in candidate_indices:
            term = self.terms[idx]
            if type_filter and term.entity_type != type_filter:
                continue
            row = score_term(query, query_norm, query_tokens, term, detected_type)
            current = scored.get(term.concept_id)
            if current is None or row["resolver_score"] > current["resolver_score"]:
                scored[term.concept_id] = row

        candidates = sorted(
            scored.values(),
            key=lambda row: (
                -row["resolver_score"],
                -row["lexical_score"],
                row["entity_type"],
                row["label"],
            ),
        )[: max(1, min(top_k, 20))]

        warnings: list[str] = []
        if not candidates:
            warnings.append("no_local_candidate")
        elif candidates[0]["resolver_score"] < 0.72:
            warnings.append("low_confidence")
        if len(candidates) > 1 and candidates[0]["resolver_score"] - candidates[1]["resolver_score"] <= 0.04:
            warnings.append("ambiguous_top_candidates")
        if detected_type == "drug" and _DRUG_PRODUCT_RE.search(query):
            warnings.append("drug_product_level_check")

        return {
            "query": query,
            "normalized_query": query_norm,
            "detected_entity_type": detected_type,
            "requested_entity_type": requested_type,
            "warnings": warnings,
            "candidates": candidates,
        }


def detect_entity_type(query: str) -> str:
    if _DRUG_PRODUCT_RE.search(query):
        return "drug"
    if _DISEASE_HINT_RE.search(query):
        return "disease"
    return "unknown"


def score_term(
    query: str,
    query_norm: str,
    query_tokens: set[str],
    term: ResolverTerm,
    detected_type: str,
) -> dict[str, Any]:
    label_norm = normalize_text(term.label)
    lexical = SequenceMatcher(None, query_norm, term.norm).ratio()
    overlap = len(query_tokens & term.tokens)
    union = len(query_tokens | term.tokens) or 1
    jaccard = overlap / union
    containment = overlap / (min(len(query_tokens), len(term.tokens)) or 1)

    exact = query_norm == term.norm
    substring = bool(query_norm and (query_norm in term.norm or term.norm in query_norm))
    same_token_set = len(query_tokens) >= 2 and query_tokens == term.tokens
    score = (0.52 * lexical) + (0.33 * jaccard) + (0.15 * containment)
    if exact:
        if label_norm == query_norm:
            score = 1.0
        elif term.term_kind in {"standard_name", "label"}:
            score = 0.98
        elif term.term_kind == "synonym":
            score = 0.96 if label_norm == query_norm else 0.92
        elif term.term_kind == "xref_label":
            match_type = str(term.evidence.get("match_type") or "").lower()
            score = 0.94 if match_type == "exact" else 0.9
        elif term.term_kind in {"active_ingredient", "brand_product"}:
            score = 0.96
        else:
            score = 0.95
    elif same_token_set:
        score = max(score, 0.94)
    elif substring and containment >= 0.75 and jaccard >= 0.65:
        score = max(score, 0.88)
    if detected_type != "unknown" and detected_type == term.entity_type:
        score = min(1.0, score + 0.025)
    elif detected_type != "unknown" and term.entity_type != detected_type:
        score = max(0.0, score - 0.04)

    if exact and label_norm == query_norm and term.term_kind in {"label", "standard_name", "primary_xref"}:
        match_status = "exact_label"
    elif exact and label_norm != query_norm and term.term_kind == "xref_label":
        match_status = "exact_xref_label_to_related_concept"
    elif exact:
        match_status = f"exact_{term.term_kind}"
    elif same_token_set:
        match_status = "token_set_match"
    elif term.term_kind in {"active_ingredient", "brand_product"} and score >= 0.78:
        match_status = term.term_kind
    elif score >= 0.86:
        match_status = "strong_lexical"
    elif score >= 0.72:
        match_status = "possible_lexical"
    else:
        match_status = "weak"

    return {
        "concept_id": term.concept_id,
        "label": term.label,
        "entity_type": term.entity_type,
        "biolink_category": term.biolink_category,
        "matched_term": term.term,
        "term_kind": term.term_kind,
        "source": term.source,
        "identifiers": term.identifiers[:12],
        "resolver_score": round(score, 4),
        "lexical_score": round(lexical, 4),
        "token_jaccard": round(jaccard, 4),
        "token_containment": round(containment, 4),
        "match_status": match_status,
        "evidence": term.evidence,
    }


def build_cure_entity_resolver_index(disease_graph_dir: str | Path | None = None) -> CureEntityResolverIndex:
    warnings: list[str] = []
    terms: list[ResolverTerm] = []

    drug_input = Path(os.getenv("CURE_ENTITY_DRUG_INPUT_FILE", str(DEFAULT_CURE_DRUG_INPUT_FILE)))
    drug_review_dir = Path(os.getenv("CURE_ENTITY_DRUG_REVIEW_DIR", str(DEFAULT_CURE_DRUG_REVIEW_DIR)))

    terms.extend(load_cure_drug_input_terms(drug_input, warnings))
    terms.extend(load_cure_phase1c_terms(drug_review_dir, warnings))
    if disease_graph_dir:
        terms.extend(load_disease_terms(Path(disease_graph_dir), warnings))

    return CureEntityResolverIndex(terms, warnings)


def load_cure_drug_input_terms(path: Path, warnings: list[str]) -> list[ResolverTerm]:
    if not path.exists():
        warnings.append(f"CURE drug input not found: {path}")
        return []
    terms: list[ResolverTerm] = []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        rows = list(reader)

    # The source CSV has a metadata row before the real header.
    if rows and "CURE Drug ID" not in rows[0]:
        with open(path, newline="", encoding="utf-8-sig") as fh:
            raw_rows = list(csv.reader(fh))
        if len(raw_rows) >= 2:
            header = raw_rows[1]
            rows = [dict(zip(header, row)) for row in raw_rows[2:]]

    for row in rows:
        cure_id = compact_id(row.get("CURE Drug ID"))
        raw_name = compact_id(row.get("CURE Drug Name"))
        standard_id = compact_id(row.get("Standard ID"))
        standard_name = compact_id(row.get("Standard Name"))
        if not cure_id and not raw_name:
            continue
        concept_id = f"CURE-ID:Drug:{cure_id}" if cure_id else f"CURE-ID:Drug:raw:{normalize_text(raw_name)}"
        label = standard_name or raw_name
        identifiers = [standard_id] if standard_id else []
        evidence = {
            "cure_drug_id": cure_id,
            "raw_drug_name": raw_name,
            "standard_id": standard_id,
            "standard_name": standard_name,
        }
        for term, kind in ((raw_name, "raw_label"), (standard_name, "standard_name"), (standard_id, "identifier")):
            if term:
                terms.append(
                    ResolverTerm(
                        term=term,
                        concept_id=concept_id,
                        label=label,
                        entity_type="drug",
                        biolink_category="biolink:Drug",
                        source="cure_drug_input",
                        term_kind=kind,
                        identifiers=identifiers,
                        evidence=evidence,
                    )
                )
    return terms


def load_cure_phase1c_terms(directory: Path, warnings: list[str]) -> list[ResolverTerm]:
    if pd is None:
        warnings.append("pandas is not available; CURE Phase 1C workbooks were skipped.")
        return []
    if not directory.exists():
        warnings.append(f"CURE Phase 1C review directory not found: {directory}")
        return []

    terms: list[ResolverTerm] = []
    for workbook in sorted(directory.glob("phase1c_review_batch*_current.xlsx")):
        if workbook.name.startswith("~$"):
            continue
        try:
            df = pd.read_excel(workbook)
        except Exception as exc:
            warnings.append(f"Could not read {workbook}: {exc}")
            continue
        for _, row in df.iterrows():
            terms.extend(_phase1c_row_terms(row.to_dict(), workbook.name))
    return terms


def _phase1c_row_terms(row: dict[str, Any], workbook_name: str) -> list[ResolverTerm]:
    cure_id = compact_id(row.get("Cure ID"))
    raw_name = compact_id(row.get("Raw drug name"))
    proposed = compact_id(row.get("Proposed resolver query"))
    corrected = compact_id(row.get("Corrected resolver query"))
    generic = compact_id(row.get("Generic/concept label"))
    active = compact_id(row.get("Active ingredients"))
    brand = compact_id(row.get("Brand/product candidates"))
    phase1a = compact_id(row.get("Phase 1A final output"))
    label = corrected or generic or proposed or phase1a or raw_name
    if not label and not cure_id:
        return []
    concept_id = f"CURE-ID:Drug:{cure_id}" if cure_id else f"CURE-ID:Drug:review:{normalize_text(label)}"
    evidence = {
        "cure_drug_id": cure_id,
        "raw_drug_name": raw_name,
        "proposed_resolver_query": proposed,
        "generic_or_concept_label": generic,
        "active_ingredients": active,
        "brand_product_candidates": brand,
        "dose_strength": compact_id(row.get("Dose/strength")),
        "formulation_route": compact_id(row.get("Formulation/route")),
        "phase1b_confidence": compact_id(row.get("Phase 1B confidence")),
        "phase1b_action": compact_id(row.get("Phase 1B action")),
        "current_registry_status": compact_id(row.get("Current registry status")),
        "review_batch": workbook_name,
    }
    term_values: list[tuple[str, str]] = [
        (raw_name, "raw_label"),
        (proposed, "proposed_query"),
        (corrected, "corrected_query"),
        (generic, "label"),
        (phase1a, "phase1a_output"),
    ]
    term_values.extend((value, "active_ingredient") for value in split_values(active))
    term_values.extend((value, "brand_product") for value in split_values(brand))

    terms: list[ResolverTerm] = []
    seen: set[tuple[str, str]] = set()
    for term, kind in term_values:
        term = compact_id(term)
        if not term:
            continue
        key = (normalize_text(term), kind)
        if key in seen:
            continue
        seen.add(key)
        terms.append(
            ResolverTerm(
                term=term,
                concept_id=concept_id,
                label=label,
                entity_type="drug",
                biolink_category="biolink:Drug",
                source="cure_phase1c_review",
                term_kind=kind,
                identifiers=[],
                evidence=evidence,
            )
        )
    return terms


def load_disease_terms(graph_dir: Path, warnings: list[str]) -> list[ResolverTerm]:
    concepts_file = graph_dir / "disease_concepts.tsv"
    xref_edges_file = graph_dir / "disease_xref_edges.tsv"
    if not concepts_file.exists():
        warnings.append(f"Disease concepts file not found: {concepts_file}")
        return []

    terms: list[ResolverTerm] = []
    concept_labels: dict[str, dict[str, str]] = {}
    with open(concepts_file, newline="", encoding="utf-8") as fh:
        for row in csv.DictReader(fh, delimiter="\t"):
            primary = row.get("primary_xref", "")
            ncats_id = row.get("ncats_disease_id", "")
            label = row.get("standard_name", "")
            biolink = row.get("disease_type", "") or "biolink:Disease"
            identifiers = [value for value in (primary, ncats_id) if value]
            concept_labels[primary] = {
                "label": label,
                "ncats_id": ncats_id,
                "biolink": biolink,
            }
            evidence = {
                "primary_xref": primary,
                "ncats_disease_id": ncats_id,
                "confidence_tier": row.get("confidence_tier", ""),
                "n_sources": row.get("n_sources", ""),
                "xref_count": row.get("xref_count", ""),
                "is_rare": row.get("is_rare", ""),
            }
            for term, kind in (
                (label, "standard_name"),
                (primary, "primary_xref"),
            ):
                if term:
                    terms.append(
                        ResolverTerm(
                            term=term,
                            concept_id=primary or ncats_id,
                            label=label or primary,
                            entity_type="disease",
                            biolink_category=biolink,
                            source="disease_harmonizer",
                            term_kind=kind,
                            identifiers=identifiers,
                            evidence=evidence,
                        )
                    )
            for synonym in split_values(row.get("synonyms", "")):
                terms.append(
                    ResolverTerm(
                        term=synonym,
                        concept_id=primary or ncats_id,
                        label=label or primary,
                        entity_type="disease",
                        biolink_category=biolink,
                        source="disease_harmonizer",
                        term_kind="synonym",
                        identifiers=identifiers,
                        evidence=evidence,
                    )
                )

    if xref_edges_file.exists():
        with open(xref_edges_file, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh, delimiter="\t"):
                primary = row.get("primary_xref", "")
                concept = concept_labels.get(primary)
                if not concept:
                    continue
                xref_id = row.get("xref_id", "")
                xref_label = row.get("xref_label", "")
                identifiers = [value for value in (primary, concept.get("ncats_id"), xref_id) if value]
                evidence = {
                    "primary_xref": primary,
                    "xref_id": xref_id,
                    "xref_namespace": row.get("xref_namespace", ""),
                    "match_type": row.get("match_type", ""),
                    "xref_confidence": row.get("xref_confidence", ""),
                }
                for term, kind in ((xref_id, "xref_id"), (xref_label, "xref_label")):
                    if term:
                        terms.append(
                            ResolverTerm(
                                term=term,
                                concept_id=primary,
                                label=concept["label"] or primary,
                                entity_type="disease",
                                biolink_category=concept["biolink"],
                                source="disease_harmonizer_xref",
                                term_kind=kind,
                                identifiers=identifiers,
                                evidence=evidence,
                            )
                        )
    return terms
