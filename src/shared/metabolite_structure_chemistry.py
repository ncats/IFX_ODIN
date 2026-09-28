from typing import Optional


STRUCTURE_CALCULATION_METHOD = (
    "rdkit.Chem.GetMolFrags+Descriptors.MolWt+rdMolDescriptors.CalcExactMolWt"
)


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
