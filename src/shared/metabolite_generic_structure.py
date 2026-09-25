"""Shared generic-structure classification for metabolite identifiers."""

from __future__ import annotations

import re
from typing import Iterable, Optional


GENERIC_STRUCTURE_CLASSIFIER_VERSION = "generic-structure-v2"
_GENERIC_STRUCTURE_TOKEN_RE = re.compile(r"(?<![A-Za-z])R\d*(?![a-z])")


def structure_values(structure: dict) -> list[tuple[str, str]]:
    values = []
    for field in ("smiles", "formula", "inchi"):
        value = structure.get(field)
        if value:
            values.append((field, str(value)))
    return values


def value_has_generic_structure_token(field: str, value: str) -> bool:
    if field != "inchi" and "*" in value:  # * is multiplicity in InChI, not a wildcard
        return True
    if field in {"smiles", "formula", "inchi"} and _GENERIC_STRUCTURE_TOKEN_RE.search(value):
        return True
    return False


def generic_structure_evidence(structures: Iterable[dict]) -> list[dict]:
    evidence = []
    for structure in structures or []:
        for field, value in structure_values(structure):
            if value_has_generic_structure_token(field, value):
                evidence.append({
                    "source": structure.get("source"),
                    "source_id": structure.get("source_id"),
                    "field": field,
                    "value": value,
                })
    return evidence


def classify_generic_structure(structures: Iterable[dict]) -> Optional[bool]:
    """Return True for generic, False for structured-specific, or None if unknown."""
    structures = list(structures or [])
    if generic_structure_evidence(structures):
        return True
    if any(structure_values(structure) for structure in structures):
        return False
    return None
