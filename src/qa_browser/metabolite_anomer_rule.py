"""Chemistry helpers for the optional free-anomer harmonization rule."""

from __future__ import annotations

from collections import Counter
from typing import Iterable, Optional

from rdkit import Chem, rdBase
from rdkit.Chem import inchi


FREE_ANOMER_RULE_ID = "merge_free_anomeric_forms"
FREE_ANOMER_ALGORITHM_VERSION = "free-anomer-v2"
CARBOHYDRATE_FAMILY_ALGORITHM_VERSION = "carbohydrate-family-v1"
CHEBI_CARBOHYDRATE_ROOT_ID = "CHEBI:16646"
CHEBI_POLYMER_ROOT_IDS = ("CHEBI:60027", "CHEBI:18154")


def free_anomeric_oh_atoms(mol: Chem.Mol) -> list[int]:
    """Return ring carbons bonded to a ring oxygen and an external neutral OH."""
    matches = []
    for atom in mol.GetAtoms():
        if atom.GetAtomicNum() != 6 or not atom.IsInRing():
            continue
        has_ring_oxygen = any(
            neighbor.GetAtomicNum() == 8 and neighbor.IsInRing()
            for neighbor in atom.GetNeighbors()
        )
        has_external_oh = any(
            neighbor.GetAtomicNum() == 8
            and not neighbor.IsInRing()
            and neighbor.GetDegree() == 1
            and neighbor.GetFormalCharge() == 0
            and neighbor.GetTotalNumHs() > 0
            for neighbor in atom.GetNeighbors()
        )
        if has_ring_oxygen and has_external_oh:
            matches.append(atom.GetIdx())
    return matches


def generated_carbohydrate_structure(smiles: Optional[str]) -> tuple[Optional[dict], Optional[str]]:
    """Generate comparable carbohydrate keys directly from one source SMILES."""
    if not smiles:
        return None, "missing_smiles"
    with rdBase.BlockLogs():
        mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return None, "unparseable_smiles"
    if any(atom.GetAtomicNum() == 0 for atom in mol.GetAtoms()):
        return None, "wildcard_structure"

    original_key = inchi.MolToInchiKey(mol)
    if not original_key:
        return None, "inchikey_generation_failed"

    atom_indexes = free_anomeric_oh_atoms(mol)
    result = {
        "source_smiles": smiles,
        "source_inchi_key": original_key,
        "free_anomeric_atom_indexes": atom_indexes,
        "free_anomeric_center_count": len(atom_indexes),
        "formal_charge": Chem.GetFormalCharge(mol),
        "algorithm_version": CARBOHYDRATE_FAMILY_ALGORITHM_VERSION,
    }
    if len(atom_indexes) > 1:
        result.update({
            "comparable": False,
            "classification_reason": "multiple_free_anomeric_centers",
            "family_inchi_key": None,
        })
        return result, None

    anomeric_atom_index = atom_indexes[0] if atom_indexes else None
    unassigned_atom_indexes = [
        atom_index
        for atom_index, assignment in Chem.FindMolChiralCenters(
            mol,
            includeUnassigned=True,
            useLegacyImplementation=False,
        )
        if assignment == "?" and atom_index != anomeric_atom_index
    ]
    if anomeric_atom_index is None:
        result["unassigned_stereocenter_atom_indexes"] = unassigned_atom_indexes
        if unassigned_atom_indexes:
            result.update({
                "comparable": False,
                "classification_reason": "under_specified_stereochemistry",
                "family_inchi_key": None,
            })
            return result, None
        result.update({
            "comparable": True,
            "classification_reason": "fully_specified_structure",
            "normalized_smiles": Chem.MolToSmiles(mol, isomericSmiles=True),
            "normalized_inchi_key": original_key,
            "family_inchi_key": original_key,
            "anomeric_atom_index": None,
        })
        return result, None

    normalized = Chem.Mol(mol)
    normalized.GetAtomWithIdx(anomeric_atom_index).SetChiralTag(Chem.ChiralType.CHI_UNSPECIFIED)
    Chem.AssignStereochemistry(normalized, cleanIt=True, force=True)
    normalized_key = inchi.MolToInchiKey(normalized)
    if not normalized_key:
        return None, "inchikey_generation_failed"
    result.update({
        "normalized_smiles": Chem.MolToSmiles(normalized, isomericSmiles=True),
        "normalized_inchi_key": normalized_key,
        "anomeric_atom_index": anomeric_atom_index,
        "unassigned_stereocenter_atom_indexes": unassigned_atom_indexes,
    })
    if unassigned_atom_indexes:
        result.update({
            "comparable": False,
            "classification_reason": "under_specified_stereochemistry",
            "family_inchi_key": None,
        })
        return result, None
    result.update({
        "comparable": True,
        "classification_reason": "free_anomer_normalized",
        "family_inchi_key": normalized_key,
    })
    return result, None


def normalized_free_anomer_structure(smiles: Optional[str]) -> tuple[Optional[dict], Optional[str]]:
    """Clear chirality only at one free anomeric OH-bearing carbon."""
    structure, error = generated_carbohydrate_structure(smiles)
    if error:
        return None, error
    if structure["free_anomeric_center_count"] != 1:
        return None, "free_anomeric_center_count_not_one"
    return {
        "source_smiles": structure["source_smiles"],
        "source_inchi_key": structure["source_inchi_key"],
        "normalized_smiles": structure["normalized_smiles"],
        "normalized_inchi_key": structure["normalized_inchi_key"],
        "anomeric_atom_index": structure["anomeric_atom_index"],
        "formal_charge": structure["formal_charge"],
    }, None


def evaluate_free_anomer_candidate(row: dict) -> dict:
    """Evaluate one direct ChEBI child/parent pair and retain an audit result."""
    child = row.get("child") or {}
    parent = row.get("parent") or {}
    decision = {
        "child_id": child.get("id"),
        "child_name": child.get("name"),
        "parent_id": parent.get("id"),
        "parent_name": parent.get("name"),
        "relationship_key": row.get("relationship_key"),
        "relationship_id": row.get("relationship_id"),
        "relationship_predicate": row.get("relationship_predicate") or "is_a",
        "algorithm_version": FREE_ANOMER_ALGORITHM_VERSION,
        "accepted": False,
    }

    child_formula = str(child.get("formula") or "").strip()
    parent_formula = str(parent.get("formula") or "").strip()
    if not child_formula or not parent_formula:
        decision["reason"] = "missing_formula"
        return decision
    if "n" in child_formula or "n" in parent_formula:
        decision["reason"] = "repeat_formula"
        return decision
    if child_formula != parent_formula:
        decision["reason"] = "formula_mismatch"
        return decision

    child_charge = "" if child.get("charge") is None else str(child.get("charge")).strip()
    parent_charge = "" if parent.get("charge") is None else str(parent.get("charge")).strip()
    if not child_charge or not parent_charge:
        decision["reason"] = "missing_charge"
        return decision
    if child_charge != parent_charge:
        decision["reason"] = "charge_mismatch"
        return decision

    child_structure, child_error = normalized_free_anomer_structure(child.get("smiles"))
    if child_error:
        decision["reason"] = f"child_{child_error}"
        return decision
    parent_structure, parent_error = normalized_free_anomer_structure(parent.get("smiles"))
    if parent_error:
        decision["reason"] = f"parent_{parent_error}"
        return decision

    decision["child_structure"] = child_structure
    decision["parent_structure"] = parent_structure
    if child_structure["formal_charge"] != parent_structure["formal_charge"]:
        decision["reason"] = "structure_charge_mismatch"
        return decision
    if child_structure["source_inchi_key"] == parent_structure["source_inchi_key"]:
        decision["reason"] = "source_structures_already_identical"
        return decision
    if child_structure["normalized_inchi_key"] != parent_structure["normalized_inchi_key"]:
        decision["reason"] = "normalized_inchi_key_mismatch"
        return decision

    decision["accepted"] = True
    decision["reason"] = "free_anomer_match"
    decision["normalized_inchi_key"] = child_structure["normalized_inchi_key"]
    return decision


def summarize_free_anomer_decisions(decisions: Iterable[dict]) -> dict:
    decisions = list(decisions)
    reasons = Counter(decision.get("reason") or "unknown" for decision in decisions)
    accepted = [decision for decision in decisions if decision.get("accepted")]
    return {
        "free_anomer_candidate_pair_count": len(decisions),
        "free_anomer_accepted_pair_count": len(accepted),
        "free_anomer_rejected_pair_count": len(decisions) - len(accepted),
        "free_anomer_rejection_reason_counts": {
            reason: count
            for reason, count in sorted(reasons.items())
            if reason != "free_anomer_match"
        },
    }
