from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from fastapi import HTTPException
from openpyxl import load_workbook

try:
    from rdkit import Chem
    from rdkit.Chem import inchi
except ImportError:  # pragma: no cover - depends on QA runtime image
    Chem = None
    inchi = None


QA_BROWSER_DIR = Path(__file__).resolve().parent
MULTIRAMP_WORKBOOK = QA_BROWSER_DIR / "data" / "ramp" / "20260604_final_all_metabolites_ramp_xrefs_multRamp.xlsx"


def parse_ramp_ids(raw: str) -> list[str]:
    ids = []
    for token in (raw or "").replace("|", " ").replace(",", " ").split():
        normalized = token.strip().upper()
        if normalized.startswith("RAMP_C_") and normalized not in ids:
            ids.append(normalized)
    return ids


def parse_hit_count(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except (TypeError, ValueError):
        try:
            return int(float(str(value).strip()))
        except (TypeError, ValueError):
            return 0


def fetch_analytes(db_path: Path, ramp_ids: list[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in ramp_ids)
    query = f"""
        SELECT rampId, type, common_name
        FROM analyte
        WHERE rampId IN ({placeholders})
        ORDER BY rampId
    """
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        return [dict(row) for row in con.execute(query, ramp_ids)]


def fetch_source_rows(db_path: Path, ramp_ids: list[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in ramp_ids)
    query = f"""
        SELECT
            s.rampId,
            a.common_name AS rampName,
            s.sourceId,
            s.IDtype,
            s.dataSource,
            s.commonName AS sourceName,
            s.priorityHMDBStatus,
            s.pathwayCount
        FROM source s
        JOIN analyte a ON a.rampId = s.rampId
        WHERE s.rampId IN ({placeholders})
        ORDER BY s.rampId, s.dataSource, s.IDtype, s.sourceId
    """
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        return [dict(row) for row in con.execute(query, ramp_ids)]


def fetch_chem_props(db_path: Path, ramp_ids: list[str]) -> list[dict[str, Any]]:
    placeholders = ",".join("?" for _ in ramp_ids)
    query = f"""
        SELECT
            ramp_id,
            chem_data_source,
            chem_source_id,
            mw,
            monoisotop_mass,
            common_name,
            mol_formula,
            iso_smiles,
            inchi_key
        FROM chem_props
        WHERE ramp_id IN ({placeholders})
        ORDER BY ramp_id, chem_data_source, chem_source_id
    """
    with sqlite3.connect(db_path) as con:
        con.row_factory = sqlite3.Row
        return [dict(row) for row in con.execute(query, ramp_ids)]


def source_node_id(source_id: str) -> str:
    return f"source::{source_id}"


def inchi_key_node_id(inchi_key: str) -> str:
    return f"inchikey::{inchi_key}"


def stereo_free_node_id(inchi_key: str) -> str:
    return f"stereo-free::{inchi_key}"


def stereo_free_inchi_key(smiles: str | None) -> tuple[str, str, str]:
    if not smiles:
        return "", "", ""
    if Chem is None or inchi is None:
        return "", "", "RDKit is not installed"
    mol = Chem.MolFromSmiles(smiles)
    if mol is None:
        return "", "", "RDKit could not parse SMILES"
    Chem.RemoveStereochemistry(mol)
    no_stereo_smiles = Chem.MolToSmiles(mol, isomericSmiles=False)
    return inchi.MolToInchiKey(mol), no_stereo_smiles, ""


def format_number(value: Any) -> str:
    if value is None or value == "":
        return ""
    try:
        return f"{float(value):.6g}"
    except (TypeError, ValueError):
        return str(value)


def join_values(values: set[str]) -> str:
    return ", ".join(sorted(value for value in values if value))


def finalize_sets(data: dict[str, Any]) -> None:
    for key, value in list(data.items()):
        if isinstance(value, set):
            data[key] = join_values(value)


def build_ramp_graph_payload(db_path: Path, ramp_ids: list[str]) -> dict[str, Any]:
    if not db_path.exists():
        raise HTTPException(status_code=500, detail=f"RaMP SQLite database not found: {db_path}")
    if not ramp_ids:
        raise HTTPException(status_code=400, detail="Provide at least one RAMP_C_* ID.")

    analytes = fetch_analytes(db_path, ramp_ids)
    found = {row["rampId"] for row in analytes}
    missing = [ramp_id for ramp_id in ramp_ids if ramp_id not in found]
    source_rows = fetch_source_rows(db_path, ramp_ids)
    chem_rows = fetch_chem_props(db_path, ramp_ids)
    chem_by_ramp_source: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in chem_rows:
        chem_by_ramp_source[(row["ramp_id"], row["chem_source_id"])].append(row)

    ramp_nodes: dict[str, dict[str, Any]] = {}
    source_nodes: dict[str, dict[str, Any]] = {}
    inchi_key_nodes: dict[str, dict[str, Any]] = {}
    stereo_free_nodes: dict[str, dict[str, Any]] = {}
    edges: dict[str, dict[str, Any]] = {}
    inchi_edges: dict[str, dict[str, Any]] = {}
    stereo_free_edges: dict[str, dict[str, Any]] = {}
    stereo_free_errors: set[str] = set()

    for row in source_rows:
        ramp_nodes.setdefault(row["rampId"], {
            "data": {
                "id": row["rampId"],
                "label": row["rampId"],
                "name": row["rampName"],
                "kind": "ramp",
                "sourceIds": set(),
                "dataSources": set(),
                "sourceNames": set(),
                "molecularWeights": set(),
                "monoisotopicMasses": set(),
                "formulas": set(),
                "smiles": set(),
                "inchiKeys": set(),
            },
            "classes": "ramp",
        })
        source_id = row["sourceId"]
        ramp_nodes[row["rampId"]]["data"]["sourceIds"].add(source_id)
        ramp_nodes[row["rampId"]]["data"]["dataSources"].add(row["dataSource"] or "")
        ramp_nodes[row["rampId"]]["data"]["sourceNames"].add(row["sourceName"] or "")

        source_nodes.setdefault(source_id, {
            "data": {
                "id": source_node_id(source_id),
                "label": source_id,
                "sourceId": source_id,
                "idType": row["IDtype"],
                "kind": "source",
                "names": set(),
                "dataSources": set(),
                "molecularWeights": set(),
                "monoisotopicMasses": set(),
                "formulas": set(),
                "smiles": set(),
                "inchiKeys": set(),
            },
            "classes": f"source {str(row['IDtype']).lower()}",
        })
        source_nodes[source_id]["data"]["names"].add(row["sourceName"] or "")
        source_nodes[source_id]["data"]["dataSources"].add(row["dataSource"] or "")

        edge_id = f"edge::{row['rampId']}::{source_id}"
        edges.setdefault(edge_id, {
            "data": {
                "id": edge_id,
                "source": row["rampId"],
                "target": source_node_id(source_id),
                "sourceId": source_id,
                "label": set(),
            },
        })
        edges[edge_id]["data"]["label"].add(row["dataSource"] or "")

        for chem_row in chem_by_ramp_source.get((row["rampId"], source_id), []):
            for data in (ramp_nodes[row["rampId"]]["data"], source_nodes[source_id]["data"]):
                data["molecularWeights"].add(format_number(chem_row["mw"]))
                data["monoisotopicMasses"].add(format_number(chem_row["monoisotop_mass"]))
                data["formulas"].add(chem_row["mol_formula"] or "")
                data["smiles"].add(chem_row["iso_smiles"] or "")
                data["inchiKeys"].add(chem_row["inchi_key"] or "")
            inchi_key = chem_row["inchi_key"] or ""
            if not inchi_key:
                continue
            inchi_key_nodes.setdefault(inchi_key, {
                "data": {
                    "id": inchi_key_node_id(inchi_key),
                    "label": inchi_key,
                    "inchiKey": inchi_key,
                    "kind": "inchiKey",
                    "names": set(),
                    "formulas": set(),
                    "molecularWeights": set(),
                },
                "classes": "inchi-key",
            })
            inchi_key_nodes[inchi_key]["data"]["names"].add(chem_row["common_name"] or "")
            inchi_key_nodes[inchi_key]["data"]["formulas"].add(chem_row["mol_formula"] or "")
            inchi_key_nodes[inchi_key]["data"]["molecularWeights"].add(format_number(chem_row["mw"]))
            inchi_edge_id = f"inchi::{source_id}::{inchi_key}"
            inchi_edges.setdefault(inchi_edge_id, {
                "data": {
                    "id": inchi_edge_id,
                    "source": source_node_id(source_id),
                    "target": inchi_key_node_id(inchi_key),
                    "label": "InChIKey",
                },
                "classes": "inchi-key-edge",
            })
            stereo_free_key, no_stereo_smiles, stereo_error = stereo_free_inchi_key(chem_row["iso_smiles"])
            if stereo_error:
                stereo_free_errors.add(stereo_error)
            if not stereo_free_key:
                continue
            stereo_free_nodes.setdefault(stereo_free_key, {
                "data": {
                    "id": stereo_free_node_id(stereo_free_key),
                    "label": stereo_free_key,
                    "stereoFreeInchiKey": stereo_free_key,
                    "kind": "stereoFreeKey",
                    "names": set(),
                    "formulas": set(),
                    "smiles": set(),
                    "noStereoSmiles": set(),
                    "molecularWeights": set(),
                    "monoisotopicMasses": set(),
                    "inchiKeys": set(),
                },
                "classes": "stereo-free-key",
            })
            stereo_free_nodes[stereo_free_key]["data"]["names"].add(chem_row["common_name"] or "")
            stereo_free_nodes[stereo_free_key]["data"]["formulas"].add(chem_row["mol_formula"] or "")
            stereo_free_nodes[stereo_free_key]["data"]["smiles"].add(chem_row["iso_smiles"] or "")
            stereo_free_nodes[stereo_free_key]["data"]["noStereoSmiles"].add(no_stereo_smiles)
            stereo_free_nodes[stereo_free_key]["data"]["molecularWeights"].add(format_number(chem_row["mw"]))
            stereo_free_nodes[stereo_free_key]["data"]["monoisotopicMasses"].add(format_number(chem_row["monoisotop_mass"]))
            stereo_free_nodes[stereo_free_key]["data"]["inchiKeys"].add(inchi_key)
            stereo_edge_id = f"stereo-free::{inchi_key}::{stereo_free_key}"
            stereo_free_edges.setdefault(stereo_edge_id, {
                "data": {
                    "id": stereo_edge_id,
                    "source": inchi_key_node_id(inchi_key),
                    "target": stereo_free_node_id(stereo_free_key),
                    "inchiKey": inchi_key,
                    "stereoFreeInchiKey": stereo_free_key,
                    "noStereoSmiles": no_stereo_smiles,
                    "label": "no-stereo",
                },
                "classes": "stereo-free-edge",
            })

    elements = [
        *ramp_nodes.values(),
        *source_nodes.values(),
        *inchi_key_nodes.values(),
        *stereo_free_nodes.values(),
        *edges.values(),
        *inchi_edges.values(),
        *stereo_free_edges.values(),
    ]
    for element in elements:
        finalize_sets(element.get("data") or {})

    return {
        "queryRampIds": ramp_ids,
        "missingRampIds": missing,
        "stats": {
            "rampCount": len(ramp_nodes),
            "sourceCount": len(source_nodes),
            "inchiKeyCount": len(inchi_key_nodes),
            "stereoFreeKeyCount": len(stereo_free_nodes),
            "rampSourceEdgeCount": len(edges),
            "inchiKeyEdgeCount": len(inchi_edges),
            "stereoFreeEdgeCount": len(stereo_free_edges),
        },
        "warnings": sorted(stereo_free_errors),
        "analytes": analytes,
        "sourceRows": source_rows,
        "chemRows": chem_rows,
        "elements": elements,
    }


def load_multiramp_rows(limit: int = 500) -> list[dict[str, Any]]:
    if not MULTIRAMP_WORKBOOK.exists():
        return []

    workbook = load_workbook(MULTIRAMP_WORKBOOK, read_only=True, data_only=True)
    worksheet = workbook["concatenated_xrefs"]
    rows = worksheet.iter_rows(values_only=True)
    header = [str(value) if value is not None else "" for value in next(rows)]
    indexes = {name: idx for idx, name in enumerate(header)}
    required = ["Standardized name", "final_RaMP_ids", "final_RaMP_hit_count"]
    if any(name not in indexes for name in required):
        workbook.close()
        return []

    output: list[dict[str, Any]] = []
    for workbook_row_index, row in enumerate(rows, start=2):
        hit_count = parse_hit_count(row[indexes["final_RaMP_hit_count"]])
        if hit_count <= 1:
            continue
        ramp_ids = parse_ramp_ids(str(row[indexes["final_RaMP_ids"]] or ""))
        if len(ramp_ids) <= 1:
            continue
        item = {
            "workbookRow": workbook_row_index,
            "standardizedName": row[indexes["Standardized name"]] or "",
            "chemicalName": row[indexes.get("CHEMICAL_NAME", -1)] if "CHEMICAL_NAME" in indexes else "",
            "chemId": row[indexes.get("CHEM_ID", -1)] if "CHEM_ID" in indexes else "",
            "metabolonId": row[indexes.get("METABOLON_ID", -1)] if "METABOLON_ID" in indexes else "",
            "finalRampIds": " | ".join(ramp_ids),
            "finalRampHitCount": hit_count,
            "href": "/ramp-id-qa?ids=" + "%20%7C%20".join(ramp_ids),
        }
        output.append(item)
        if len(output) >= limit:
            break
    workbook.close()
    return output


def build_multiramp_navigation(raw_ids: str, limit: int = 5000) -> dict[str, Any] | None:
    current_ids = parse_ramp_ids(raw_ids)
    if not current_ids:
        return None
    current_set = set(current_ids)
    rows = load_multiramp_rows(limit=limit)
    current_index = None
    for index, row in enumerate(rows):
        if set(parse_ramp_ids(row["finalRampIds"])) == current_set:
            current_index = index
            break
    if current_index is None:
        return None
    current = rows[current_index]
    return {
        "current": current,
        "previous": rows[current_index - 1] if current_index > 0 else None,
        "next": rows[current_index + 1] if current_index + 1 < len(rows) else None,
        "index": current_index + 1,
        "total": len(rows),
    }
