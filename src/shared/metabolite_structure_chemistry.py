from typing import Mapping, Optional


STRUCTURE_CALCULATION_METHOD = (
    "rdkit.Chem.GetMolFrags+Descriptors.MolWt+rdMolDescriptors.CalcExactMolWt"
)
DERIVED_INCHI_KEY_METHOD = "rdkit.Chem.inchi.MolToInchiKey"

METABOLITE_STRUCTURE_DERIVED_FIELDS = (
    "calculated_mw",
    "calculated_monoisotopic_mass",
    "structure_components",
    "structure_calculation_input_field",
    "structure_calculation_method",
    "structure_calculation_method_version",
    "structure_calculation_error",
    "derived_inchi_key_prefix",
    "derived_inchi_key",
    "derived_inchi_key_input_field",
    "derived_inchi_key_method",
    "derived_inchi_key_method_version",
    "derived_inchi_key_error",
)


def _clean_text(value: object) -> Optional[str]:
    if value is None:
        return None
    cleaned = str(value).strip()
    return cleaned or None


def _number(value: float) -> str:
    """Serialize calculated masses deterministically without discarding precision."""
    return format(float(value), ".12g")


def calculate_smiles_chemistry(smiles: Optional[str], input_field: Optional[str]) -> dict:
    """Calculate isotope-aware whole-structure and per-fragment chemistry."""
    if not smiles:
        return {}
    try:
        import rdkit
        from rdkit import Chem, rdBase
        from rdkit.Chem import Descriptors, inchi, rdMolDescriptors
    except ImportError as exc:
        raise RuntimeError(
            "RDKit is required to calculate metabolite structure chemistry during ingest"
        ) from exc

    metadata = {
        "structure_calculation_input_field": input_field,
        "structure_calculation_method": STRUCTURE_CALCULATION_METHOD,
        "structure_calculation_method_version": rdkit.__version__,
    }
    try:
        with rdBase.BlockLogs():
            molecule = Chem.MolFromSmiles(smiles)
    except Exception as exc:
        return {**metadata, "structure_calculation_error": f"smiles_parse_error:{type(exc).__name__}"}
    if molecule is None:
        return {**metadata, "structure_calculation_error": "smiles_parse_failed"}
    if molecule.GetNumAtoms() == 0:
        return {**metadata, "structure_calculation_error": "empty_structure"}
    if any(atom.GetAtomicNum() == 0 or atom.HasQuery() for atom in molecule.GetAtoms()) or any(
        bond.HasQuery() for bond in molecule.GetBonds()
    ):
        return {**metadata, "structure_calculation_error": "query_structure_not_supported"}

    try:
        fragments = Chem.GetMolFrags(molecule, asMols=True, sanitizeFrags=True)
        components = []
        for fragment in fragments:
            component_smiles = Chem.MolToSmiles(fragment, canonical=True, isomericSmiles=True)
            component_inchi_key = None
            try:
                with rdBase.BlockLogs():
                    component_inchi_key = inchi.MolToInchiKey(fragment) or None
            except Exception:
                pass
            components.append({
                "smiles": component_smiles,
                "molecular_formula": rdMolDescriptors.CalcMolFormula(
                    fragment,
                    separateIsotopes=True,
                    abbreviateHIsotopes=True,
                ),
                "mw": _number(Descriptors.MolWt(fragment)),
                "monoisotopic_mass": _number(rdMolDescriptors.CalcExactMolWt(fragment)),
                "formal_charge": Chem.GetFormalCharge(fragment),
                "inchi_key_prefix": component_inchi_key.split("-", 1)[0] if component_inchi_key else None,
                "inchi_key": component_inchi_key,
            })
        components.sort(key=lambda item: (
            item["smiles"],
            item["molecular_formula"],
            item["monoisotopic_mass"],
            item["formal_charge"],
        ))
        return {
            **metadata,
            "calculated_mw": _number(Descriptors.MolWt(molecule)),
            "calculated_monoisotopic_mass": _number(rdMolDescriptors.CalcExactMolWt(molecule)),
            "structure_components": components,
        }
    except Exception as exc:
        return {
            **metadata,
            "structure_calculation_error": f"structure_calculation_error:{type(exc).__name__}",
        }


def derive_inchi_key_from_smiles(
    *,
    iso_smiles: Optional[str] = None,
    isomeric_smiles: Optional[str] = None,
    canonical_smiles: Optional[str] = None,
) -> dict:
    """Derive an InChIKey using the same structure precedence as mass calculation."""
    candidates = (
        ("iso_smiles", _clean_text(iso_smiles)),
        ("isomeric_smiles", _clean_text(isomeric_smiles)),
        ("canonical_smiles", _clean_text(canonical_smiles)),
    )
    input_field, smiles = next(
        ((field_name, value) for field_name, value in candidates if value),
        (None, None),
    )
    if smiles is None:
        return {}

    try:
        import rdkit
        from rdkit import Chem, rdBase
        from rdkit.Chem import inchi
    except ImportError as exc:
        raise RuntimeError(
            "RDKit is required to derive InChIKeys from metabolite SMILES"
        ) from exc

    metadata = {
        "derived_inchi_key_input_field": input_field,
        "derived_inchi_key_method": DERIVED_INCHI_KEY_METHOD,
        "derived_inchi_key_method_version": rdkit.__version__,
    }
    try:
        with rdBase.BlockLogs():
            molecule = Chem.MolFromSmiles(smiles)
    except Exception as exc:
        return {
            **metadata,
            "derived_inchi_key_error": f"smiles_parse_error:{type(exc).__name__}",
        }
    if molecule is None:
        return {**metadata, "derived_inchi_key_error": "smiles_parse_failed"}

    try:
        with rdBase.BlockLogs():
            derived_inchi_key = _clean_text(inchi.MolToInchiKey(molecule))
    except Exception as exc:
        return {
            **metadata,
            "derived_inchi_key_error": f"inchi_key_generation_error:{type(exc).__name__}",
        }
    if derived_inchi_key is None:
        return {
            **metadata,
            "derived_inchi_key_error": "inchi_key_generation_failed",
        }
    return {
        **metadata,
        "derived_inchi_key_prefix": derived_inchi_key.split("-", 1)[0],
        "derived_inchi_key": derived_inchi_key,
    }


def calculate_metabolite_chem_props_derivatives(properties: Mapping) -> dict:
    """Return a complete replacement for structure-managed chem-prop fields."""
    cleared = {
        field_name: [] if field_name == "structure_components" else None
        for field_name in METABOLITE_STRUCTURE_DERIVED_FIELDS
    }
    candidates = (
        ("iso_smiles", _clean_text(properties.get("iso_smiles"))),
        ("isomeric_smiles", _clean_text(properties.get("isomeric_smiles"))),
        ("canonical_smiles", _clean_text(properties.get("canonical_smiles"))),
    )
    input_field, smiles = next(
        ((field_name, value) for field_name, value in candidates if value),
        (None, None),
    )
    if smiles is None:
        return cleared
    return {
        **cleared,
        **calculate_smiles_chemistry(smiles, input_field),
        **derive_inchi_key_from_smiles(
            iso_smiles=properties.get("iso_smiles"),
            isomeric_smiles=properties.get("isomeric_smiles"),
            canonical_smiles=properties.get("canonical_smiles"),
        ),
    }
