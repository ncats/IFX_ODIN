import math
import re
from typing import Optional

from rdkit.Chem import GetPeriodicTable

from src.shared.metabolite_generic_structure import value_has_generic_structure_token


CHEBI_AVERAGE_MASS_ABSOLUTE_TOLERANCE = 0.5
CHEBI_MONOISOTOPIC_MASS_ABSOLUTE_TOLERANCE = 0.5
_CHEBI_FORMULA_TOKEN = re.compile(r"([A-Z][a-z]?)(\d*)")
_CHEBI_FORMULA_COMPONENT = re.compile(r"(\d*)(.+)")
_CHEBI_UNRESOLVED_FORMULA_TOKEN = re.compile(r"(?:R|X)(?![a-z])|[?*]|\)n(?:$|[^a-z])")
_CHEBI_PERIODIC_TABLE = GetPeriodicTable()
_CHEBI_ATOMIC_NUMBERS = {
    _CHEBI_PERIODIC_TABLE.GetElementSymbol(atomic_number): atomic_number
    for atomic_number in range(1, 119)
}


def validated_chebi_mass_values(
    formula: Optional[str],
    mass: Optional[str],
    monoisotopic_mass: Optional[str],
    smiles: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Return only whole-formula ChEBI masses suitable for comparison.

    ChEBI publishes partial masses for formulas containing unresolved R/X
    groups. It also contains generic structures whose aggregate formula is
    complete; for those records, retain each reported mass independently when
    it agrees with the corresponding formula-derived value.
    """
    normalized_formula = (formula or "").strip()
    has_generic_structure = bool(
        smiles and value_has_generic_structure_token("smiles", smiles)
    )
    has_unresolved_formula = bool(
        normalized_formula
        and _CHEBI_UNRESOLVED_FORMULA_TOKEN.search(normalized_formula)
    )
    if not has_generic_structure and not has_unresolved_formula:
        return mass, monoisotopic_mass
    if has_unresolved_formula:
        return None, None
    if not normalized_formula:
        return None, None

    calculated = chebi_formula_masses(normalized_formula)
    if calculated is None:
        return None, None
    calculated_mass, calculated_monoisotopic_mass = calculated
    return (
        _matching_mass(
            mass,
            calculated_mass,
            CHEBI_AVERAGE_MASS_ABSOLUTE_TOLERANCE,
        ),
        _matching_mass(
            monoisotopic_mass,
            calculated_monoisotopic_mass,
            CHEBI_MONOISOTOPIC_MASS_ABSOLUTE_TOLERANCE,
        ),
    )


def _matching_mass(
    reported: Optional[str],
    calculated: float,
    tolerance: float,
) -> Optional[str]:
    if reported is None:
        return None
    try:
        reported_value = float(reported)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(reported_value):
        return None
    if math.isclose(reported_value, calculated, rel_tol=0.0, abs_tol=tolerance):
        return reported
    return None


def chebi_formula_masses(formula: str) -> Optional[tuple[float, float]]:
    if not formula:
        return None
    average_mass = 0.0
    monoisotopic_mass = 0.0
    for raw_component in formula.split("."):
        component_match = _CHEBI_FORMULA_COMPONENT.fullmatch(raw_component)
        if component_match is None:
            return None
        multiplier = int(component_match.group(1) or "1")
        component = component_match.group(2)
        position = 0
        for match in _CHEBI_FORMULA_TOKEN.finditer(component):
            if match.start() != position:
                return None
            symbol = match.group(1)
            atomic_number = _CHEBI_ATOMIC_NUMBERS.get(symbol)
            if atomic_number is None:
                return None
            count = multiplier * int(match.group(2) or "1")
            average_mass += count * _CHEBI_PERIODIC_TABLE.GetAtomicWeight(atomic_number)
            monoisotopic_mass += count * _CHEBI_PERIODIC_TABLE.GetMostCommonIsotopeMass(
                atomic_number
            )
            position = match.end()
        if position != len(component):
            return None
    return average_mass, monoisotopic_mass
