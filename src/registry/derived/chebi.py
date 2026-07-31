import csv
import gzip
import re
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from src.registry.fetchers import ArtifactFile, DerivedArtifact, DerivedArtifactBuilder, ResolvedDependency


CHEBI_SOURCE = "chebi"
ENDOGENOUS_HUMAN_METABOLITES_DATASET = "endogenous_human_metabolites"
ENDOGENOUS_FILE_NAME = "chebi_endogenous_human_metabolites.tsv"
FULL_FILE_NAME = "chebi_full.tsv"
DATA_DICTIONARY_FILE_NAME = "chebi_endogenous_human_metabolites_data_dictionary.tsv"
CHEMICAL_ENTITY_ROOT_ID = "CHEBI:24431"
HUMAN_METABOLITE_ROLE_ID = "CHEBI:77746"
CHEBI_ROLE_PREDICATE = "RO:0000087"

EXOGENOUS_ROOT_IDS = {
    "CHEBI:35703": "xenobiotic",
    "CHEBI:23888": "drug",
    "CHEBI:35610": "antineoplastic agent",
    "CHEBI:33281": "antimicrobial agent",
}
DIAGNOSTIC_ANCESTOR_ROOT_IDS = {
    "CHEBI:52217": "pharmaceutical",
}
DRUG_REFERENCE_PREFIXES = ("drugbank", "drugcentral", "rxnorm", "dailymed", "ttd", "pharmgkb")
EXCLUSION_TERMS = (
    "drug",
    "pharmaceutical",
    "xenobiotic",
    "antibiotic",
    "antimicrobial",
    "pesticide",
    "herbicide",
    "fungicide",
    "pollutant",
    "contaminant",
    "toxin",
    "synthetic",
    "illicit",
    "narcotic",
    "psychoactive",
    "food additive",
    "preservative",
    "dye",
    "solvent",
)

CHEBI_PROPERTY_FIELD_MAP = {
    "chemrof:generalized_empirical_formula": "formula",
    "chemrof:monoisotopic_mass": "monoisotopic_mass",
    "chemrof:smiles_string": "smiles",
    "chemrof:inchi_string": "inchi",
    "chemrof:inchi_key_string": "inchi_key",
}

_QUOTED_VALUE_RE = re.compile(r'"([^"]*)"')
_SYNONYM_RE = re.compile(r'^"(?P<value>.*)"\s+(?P<scope>[A-Z]+)(?:\s+(?P<type>\S+))?')

FIELD_DEFINITIONS = [
    {
        "column": "chebi_id",
        "type": "string",
        "transform": "Copied from the ChEBI OBO term id tag. Expected form is CHEBI:<number>.",
    },
    {
        "column": "name",
        "type": "string",
        "transform": "Copied from the first ChEBI OBO name tag.",
    },
    {
        "column": "definition",
        "type": "string",
        "transform": "Extracted from the quoted text of the first ChEBI OBO def tag when present.",
    },
    {
        "column": "synonyms",
        "type": "pipe-delimited string",
        "transform": "Extracted from ChEBI OBO synonym tags; only the quoted synonym text is retained.",
    },
    {
        "column": "xrefs",
        "type": "pipe-delimited string",
        "transform": "Copied from ChEBI OBO xref tags with trailing qualifier blocks removed.",
    },
    {
        "column": "formula",
        "type": "string",
        "transform": "Value of property_value chemrof:generalized_empirical_formula when present.",
    },
    {
        "column": "monoisotopic_mass",
        "type": "string",
        "transform": "Value of property_value chemrof:monoisotopic_mass when present.",
    },
    {
        "column": "smiles",
        "type": "string",
        "transform": "Value of property_value chemrof:smiles_string when present.",
    },
    {
        "column": "inchi",
        "type": "string",
        "transform": "Value of property_value chemrof:inchi_string when present.",
    },
    {
        "column": "inchi_key",
        "type": "string",
        "transform": "Value of property_value chemrof:inchi_key_string when present.",
    },
    {
        "column": "is_chemical_entity_descendant",
        "type": "boolean string",
        "transform": "true when the term id is CHEBI:24431 or has CHEBI:24431 in its transitive is_a ancestry.",
    },
    {
        "column": "has_human_metabolite_role",
        "type": "boolean string",
        "transform": "true when a direct RO:0000087 has role target is CHEBI:77746 or descends from CHEBI:77746.",
    },
    {
        "column": "is_pharmaceutical_descendant",
        "type": "boolean string",
        "transform": "true when the term or one of its direct has role targets has CHEBI:52217 in transitive is_a ancestry.",
    },
    {
        "column": "is_obsolete",
        "type": "boolean string",
        "transform": "true when the ChEBI OBO term has is_obsolete: true.",
    },
    {
        "column": "has_drug_xref",
        "type": "boolean string",
        "transform": "true when any xref starts with drugbank, drugcentral, rxnorm, dailymed, ttd, or pharmgkb, case-insensitive.",
    },
    {
        "column": "drug_xrefs",
        "type": "pipe-delimited string",
        "transform": "Matching xrefs that triggered has_drug_xref, with trailing qualifier blocks removed.",
    },
    {
        "column": "is_exogenous_descendant",
        "type": "boolean string",
        "transform": "true when the term or one of its direct has role targets is or descends from CHEBI:35703, CHEBI:23888, CHEBI:35610, or CHEBI:33281.",
    },
    {
        "column": "exogenous_ancestor_ids",
        "type": "pipe-delimited string",
        "transform": "Matched exogenous roots from the historical R-script rule, formatted as CHEBI:<id>:<label>.",
    },
    {
        "column": "has_forbidden_label_text",
        "type": "boolean string",
        "transform": (
            "true when name, definition, or synonym text contains a forbidden term using case-insensitive "
            f"substring matching. Forbidden terms: {', '.join(EXCLUSION_TERMS)}."
        ),
    },
    {
        "column": "forbidden_label_terms",
        "type": "pipe-delimited string",
        "transform": (
            "Forbidden terms matched in name, definition, or synonym text. "
            f"Possible values: {', '.join(EXCLUSION_TERMS)}."
        ),
    },
]


class ChebiEndogenousHumanMetabolitesBuilder(DerivedArtifactBuilder):
    source = CHEBI_SOURCE
    dataset = ENDOGENOUS_HUMAN_METABOLITES_DATASET

    def build(
        self,
        *,
        config: dict,
        dependencies: List[ResolvedDependency],
        dest: Path,
        version: str,
    ) -> DerivedArtifact:
        dependency = _require_dependency(dependencies, source=CHEBI_SOURCE, dataset="ontology_full")
        output_config = config.get("output") or {}
        endogenous_path = dest / output_config.get("file_name", ENDOGENOUS_FILE_NAME)
        full_path = dest / output_config.get("full_file_name", FULL_FILE_NAME)
        data_dictionary_path = dest / output_config.get("data_dictionary_file_name", DATA_DICTIONARY_FILE_NAME)
        stats = build_endogenous_human_metabolites(
            chebi_obo_file=dependency.file("chebi.obo.gz"),
            endogenous_path=endogenous_path,
            full_path=full_path,
            data_dictionary_path=data_dictionary_path,
        )
        return DerivedArtifact(
            source=self.source,
            dataset=self.dataset,
            version=version,
            version_date=dependency.manifest.get("version_date") or dependency.version,
            derived_from=[
                {
                    "snapshot_id": dependency.snapshot_id,
                    "manifest_uri": dependency.manifest_uri,
                }
            ],
            transform=config.get("transform") or {
                "name": "chebi_endogenous_human_metabolites",
                "version": 1,
            },
            files=[
                ArtifactFile(endogenous_path, "text/tab-separated-values"),
                ArtifactFile(full_path, "text/tab-separated-values"),
                ArtifactFile(data_dictionary_path, "text/tab-separated-values"),
            ],
            stats=stats,
        )


def build_endogenous_human_metabolites(
    *,
    chebi_obo_file: Path,
    endogenous_path: Path,
    full_path: Path,
    data_dictionary_path: Path,
) -> Dict:
    terms = list(_iter_terms(chebi_obo_file))
    parents_by_id = {
        _first(term, "id"): {
            _target_id(value)
            for value in term.get("is_a", [])
            if _target_id(value)
        }
        for term in terms
        if term.get("id")
    }
    ancestors_cache: Dict[str, Set[str]] = {}

    full_rows = []
    endogenous_rows = []
    stats = {
        "term_count": len(terms),
        "row_count": 0,
        "full_row_count": 0,
        "chemical_entity_descendant_count": 0,
        "endogenous_human_metabolite_count": 0,
        "has_human_metabolite_role_count": 0,
        "obsolete_count": 0,
        "drug_xref_count": 0,
        "exogenous_descendant_count": 0,
        "forbidden_label_text_count": 0,
    }

    for term in terms:
        term_id = _first(term, "id")
        if not term_id:
            continue
        is_chemical_entity_descendant = _is_chemical_entity_descendant(term_id, parents_by_id, ancestors_cache)
        if is_chemical_entity_descendant:
            stats["chemical_entity_descendant_count"] += 1
        human_metabolite_role_ids = _role_ids_at_or_below(term, HUMAN_METABOLITE_ROLE_ID, parents_by_id, ancestors_cache)
        if human_metabolite_role_ids:
            stats["has_human_metabolite_role_count"] += 1
        is_obsolete = _is_obsolete(term)
        if is_obsolete:
            stats["obsolete_count"] += 1

        drug_xrefs = _matching_drug_xrefs(term)
        diagnostic_ancestors = _matching_diagnostic_ancestors(term, parents_by_id, ancestors_cache)
        exogenous_ancestors = _matching_exogenous_ancestors(term, parents_by_id, ancestors_cache)
        label_terms = _matching_label_terms(term)
        if drug_xrefs:
            stats["drug_xref_count"] += 1
        if exogenous_ancestors:
            stats["exogenous_descendant_count"] += 1
        if label_terms:
            stats["forbidden_label_text_count"] += 1

        row = _output_row(
            term,
            is_chemical_entity_descendant=is_chemical_entity_descendant,
            human_metabolite_role_ids=human_metabolite_role_ids,
            diagnostic_ancestors=diagnostic_ancestors,
            is_obsolete=is_obsolete,
            drug_xrefs=drug_xrefs,
            exogenous_ancestors=exogenous_ancestors,
            label_terms=label_terms,
        )
        full_rows.append(row)
        if _is_endogenous_human_metabolite_row(row):
            endogenous_rows.append(row)

    endogenous_path.parent.mkdir(parents=True, exist_ok=True)
    _write_tsv(endogenous_path, endogenous_rows)
    _write_tsv(full_path, full_rows)
    write_data_dictionary(data_dictionary_path)

    stats["row_count"] = len(endogenous_rows)
    stats["full_row_count"] = len(full_rows)
    stats["endogenous_human_metabolite_count"] = len(endogenous_rows)
    return stats


def _fieldnames() -> List[str]:
    return [definition["column"] for definition in FIELD_DEFINITIONS]


def _write_tsv(path: Path, rows: List[Dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=_fieldnames())
        writer.writeheader()
        writer.writerows(sorted(rows, key=lambda row: _chebi_sort_key(row["chebi_id"])))


def _is_endogenous_human_metabolite_row(row: Dict[str, str]) -> bool:
    return (
        row["is_chemical_entity_descendant"] == "true"
        and row["is_obsolete"] == "false"
        and row["has_drug_xref"] == "false"
        and row["is_exogenous_descendant"] == "false"
        and row["has_forbidden_label_text"] == "false"
    )


def write_data_dictionary(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=["column", "type", "transform"])
        writer.writeheader()
        writer.writerows(FIELD_DEFINITIONS)


def _require_dependency(
    dependencies: List[ResolvedDependency],
    *,
    source: str,
    dataset: str,
) -> ResolvedDependency:
    matches = [
        dependency
        for dependency in dependencies
        if dependency.source == source and dependency.dataset == dataset
    ]
    if not matches:
        raise LookupError(f"Missing derived artifact dependency {source}/{dataset}")
    if len(matches) > 1:
        raise ValueError(f"Multiple derived artifact dependencies match {source}/{dataset}")
    return matches[0]


def _iter_terms(path: Path) -> Iterable[Dict[str, List[str]]]:
    current: Optional[Dict[str, List[str]]] = None
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.rstrip("\n")
            if line == "[Term]":
                if current is not None:
                    yield current
                current = {}
                continue
            if line.startswith("["):
                if current is not None:
                    yield current
                    current = None
                continue
            if current is None or not line or line.startswith("!"):
                continue
            if ": " not in line:
                continue
            key, value = line.split(": ", 1)
            current.setdefault(key, []).append(value)
    if current is not None:
        yield current


def _first(term: Dict[str, List[str]], key: str) -> Optional[str]:
    values = term.get(key) or []
    return values[0] if values else None


def _target_id(value: str) -> Optional[str]:
    body = value.split(" ! ", 1)[0].strip()
    parts = body.split(maxsplit=2)
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    return parts[1]


def _relationship_target_ids(term: Dict[str, List[str]], predicate: str) -> Set[str]:
    target_ids = set()
    for value in term.get("relationship", []) or []:
        body = value.split(" ! ", 1)[0].strip()
        parts = body.split(maxsplit=2)
        if len(parts) >= 2 and parts[0] == predicate:
            target_ids.add(parts[1])
    return target_ids


def _ancestors(
    term_id: str,
    parents_by_id: Dict[str, Set[str]],
    cache: Dict[str, Set[str]],
    visiting: Optional[Set[str]] = None,
) -> Set[str]:
    if term_id in cache:
        return cache[term_id]
    visiting = visiting or set()
    if term_id in visiting:
        return set()
    visiting.add(term_id)
    ancestors = set()
    for parent_id in parents_by_id.get(term_id, set()):
        ancestors.add(parent_id)
        ancestors.update(_ancestors(parent_id, parents_by_id, cache, visiting))
    visiting.remove(term_id)
    cache[term_id] = ancestors
    return ancestors


def _is_chemical_entity_descendant(
    term_id: str,
    parents_by_id: Dict[str, Set[str]],
    ancestors_cache: Dict[str, Set[str]],
) -> bool:
    ancestors = _ancestors(term_id, parents_by_id, ancestors_cache)
    return term_id == CHEMICAL_ENTITY_ROOT_ID or CHEMICAL_ENTITY_ROOT_ID in ancestors


def _role_ids_at_or_below(
    term: Dict[str, List[str]],
    role_root_id: str,
    parents_by_id: Dict[str, Set[str]],
    ancestors_cache: Dict[str, Set[str]],
) -> List[str]:
    role_ids = []
    for role_id in sorted(_relationship_target_ids(term, CHEBI_ROLE_PREDICATE), key=_chebi_sort_key):
        role_ancestors = _ancestors(role_id, parents_by_id, ancestors_cache)
        if role_id == role_root_id or role_root_id in role_ancestors:
            role_ids.append(role_id)
    return role_ids


def _is_obsolete(term: Dict[str, List[str]]) -> bool:
    return any(value.lower() == "true" for value in term.get("is_obsolete", []) or [])


def _matching_drug_xrefs(term: Dict[str, List[str]]) -> List[str]:
    matches = []
    for xref in term.get("xref", []) or []:
        normalized = _strip_qualifier(xref).lower()
        if any(normalized.startswith(prefix) for prefix in DRUG_REFERENCE_PREFIXES):
            matches.append(_strip_qualifier(xref))
    return sorted(set(matches), key=str.lower)


def _matching_exogenous_ancestors(
    term: Dict[str, List[str]],
    parents_by_id: Dict[str, Set[str]],
    ancestors_cache: Dict[str, Set[str]],
) -> List[str]:
    checked_ids = _term_and_role_ancestor_ids(term, parents_by_id, ancestors_cache)
    return [
        f"{ancestor_id}:{EXOGENOUS_ROOT_IDS[ancestor_id]}"
        for ancestor_id in sorted(checked_ids, key=_chebi_sort_key)
        if ancestor_id in EXOGENOUS_ROOT_IDS
    ]


def _matching_diagnostic_ancestors(
    term: Dict[str, List[str]],
    parents_by_id: Dict[str, Set[str]],
    ancestors_cache: Dict[str, Set[str]],
) -> Set[str]:
    checked_ids = _term_and_role_ancestor_ids(term, parents_by_id, ancestors_cache)
    return {
        DIAGNOSTIC_ANCESTOR_ROOT_IDS[ancestor_id]
        for ancestor_id in checked_ids
        if ancestor_id in DIAGNOSTIC_ANCESTOR_ROOT_IDS
    }


def _term_and_role_ancestor_ids(
    term: Dict[str, List[str]],
    parents_by_id: Dict[str, Set[str]],
    ancestors_cache: Dict[str, Set[str]],
) -> Set[str]:
    term_id = _first(term, "id")
    if not term_id:
        return set()
    checked_ids = {term_id}
    checked_ids.update(_ancestors(term_id, parents_by_id, ancestors_cache))
    for role_id in _relationship_target_ids(term, CHEBI_ROLE_PREDICATE):
        checked_ids.add(role_id)
        checked_ids.update(_ancestors(role_id, parents_by_id, ancestors_cache))
    return checked_ids


def _matching_label_terms(term: Dict[str, List[str]]) -> List[str]:
    texts = []
    name = _first(term, "name")
    if name:
        texts.append(name)
    definition = _definition(term)
    if definition:
        texts.append(definition)
    texts.extend(_synonym_values(term))
    lower_texts = [text.lower() for text in texts if text]
    return [
        exclusion_term
        for exclusion_term in EXCLUSION_TERMS
        if any(exclusion_term in text for text in lower_texts)
    ]


def _output_row(
    term: Dict[str, List[str]],
    *,
    is_chemical_entity_descendant: bool,
    human_metabolite_role_ids: List[str],
    diagnostic_ancestors: Set[str],
    is_obsolete: bool,
    drug_xrefs: List[str],
    exogenous_ancestors: List[str],
    label_terms: List[str],
) -> Dict[str, str]:
    properties = _properties(term)
    return {
        "chebi_id": _first(term, "id") or "",
        "name": _first(term, "name") or "",
        "definition": _definition(term) or "",
        "synonyms": "|".join(_synonym_values(term)),
        "xrefs": "|".join(_strip_qualifier(xref) for xref in term.get("xref", []) or []),
        "formula": properties.get("formula", ""),
        "monoisotopic_mass": properties.get("monoisotopic_mass", ""),
        "smiles": properties.get("smiles", ""),
        "inchi": properties.get("inchi", ""),
        "inchi_key": properties.get("inchi_key", ""),
        "is_chemical_entity_descendant": _bool_string(is_chemical_entity_descendant),
        "has_human_metabolite_role": _bool_string(bool(human_metabolite_role_ids)),
        "is_pharmaceutical_descendant": _bool_string("pharmaceutical" in diagnostic_ancestors),
        "is_obsolete": _bool_string(is_obsolete),
        "has_drug_xref": _bool_string(bool(drug_xrefs)),
        "drug_xrefs": "|".join(drug_xrefs),
        "is_exogenous_descendant": _bool_string(bool(exogenous_ancestors)),
        "exogenous_ancestor_ids": "|".join(exogenous_ancestors),
        "has_forbidden_label_text": _bool_string(bool(label_terms)),
        "forbidden_label_terms": "|".join(label_terms),
    }


def _definition(term: Dict[str, List[str]]) -> Optional[str]:
    raw_definition = _first(term, "def")
    if not raw_definition:
        return None
    match = _QUOTED_VALUE_RE.search(raw_definition)
    return match.group(1) if match else raw_definition


def _synonym_values(term: Dict[str, List[str]]) -> List[str]:
    values = []
    for raw_synonym in term.get("synonym", []) or []:
        match = _SYNONYM_RE.match(raw_synonym)
        if match:
            values.append(match.group("value"))
        else:
            quoted = _QUOTED_VALUE_RE.search(raw_synonym)
            values.append(quoted.group(1) if quoted else raw_synonym)
    return values


def _properties(term: Dict[str, List[str]]) -> Dict[str, str]:
    properties = {}
    for raw_property in term.get("property_value", []) or []:
        predicate = raw_property.split(maxsplit=1)[0]
        field = CHEBI_PROPERTY_FIELD_MAP.get(predicate)
        if not field:
            continue
        match = _QUOTED_VALUE_RE.search(raw_property)
        if match:
            properties[field] = match.group(1)
    return properties


def _strip_qualifier(value: str) -> str:
    return value.split(" {", 1)[0].strip()


def _bool_string(value: bool) -> str:
    return "true" if value else "false"


def _chebi_sort_key(value: str) -> Tuple[int, str]:
    if value.startswith("CHEBI:"):
        try:
            return int(value.split(":", 1)[1]), value
        except ValueError:
            pass
    return 10**12, value
