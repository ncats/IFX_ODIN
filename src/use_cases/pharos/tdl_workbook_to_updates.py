#!/usr/bin/env python3
"""Convert Tudor/Pharos TDL workbooks into the tdl_updates.csv adapter format."""

import argparse
import csv
from collections import Counter
from datetime import datetime
from pathlib import Path


DEFAULT_WORKBOOK = "/Users/mainejl/Downloads/PharosTDL_UniProt_20260904.xlsx"
DEFAULT_OUTPUT = "input_files/manual/target_graph/tdl_updates.csv"
DEFAULT_REGISTRY_CREDENTIALS = "./src/use_cases/secrets/aws_ifx_registry.yaml"
VALID_TDLS = {"Tclin", "Tchem", "Tbio", "Tdark"}


def _cell_text(value) -> str:
    return str(value or "").strip()


def convert_workbook(workbook_path: Path, output_path: Path) -> Counter:
    import openpyxl

    wb = openpyxl.load_workbook(workbook_path, read_only=True, data_only=True)
    ws = wb.active
    header = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    idx = {str(name).strip(): i for i, name in enumerate(header) if name is not None and str(name).strip()}
    missing = [column for column in ("uniprot_id", "tdl") if column not in idx]
    if missing:
        raise ValueError(f"Workbook is missing required column(s): {', '.join(missing)}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    seen = set()
    with output_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "UniProt",
                "Symbol",
                "Name",
                "Target Development Level",
                "new TDLs",
                "idg_family",
            ],
        )
        writer.writeheader()
        for row in ws.iter_rows(min_row=2, values_only=True):
            uniprot = _cell_text(row[idx["uniprot_id"]])
            tdl = _cell_text(row[idx["tdl"]])
            if not uniprot:
                continue
            if tdl not in VALID_TDLS:
                raise ValueError(f"Invalid TDL {tdl!r} for UniProt {uniprot}")
            if uniprot in seen:
                raise ValueError(f"Duplicate UniProt accession in workbook: {uniprot}")
            seen.add(uniprot)
            symbol = _cell_text(row[idx["symbol"]]) if "symbol" in idx else ""
            name = _cell_text(row[idx["name"]]) if "name" in idx else ""
            idg_family = _cell_text(row[idx["idg_family"]]) if "idg_family" in idx else ""
            writer.writerow(
                {
                    "UniProt": uniprot,
                    "Symbol": symbol,
                    "Name": name,
                    "Target Development Level": tdl,
                    "new TDLs": tdl,
                    "idg_family": idg_family,
                }
            )
            counts[tdl] += 1
    return counts


def register_tdl_updates(
    output_path: Path,
    registry_credentials: str,
    *,
    upload: bool,
) -> str:
    from src.core.registry_publication import publish_source_file

    version = datetime.fromtimestamp(output_path.stat().st_mtime).date().isoformat()
    snapshot_id = f"target_graph:tdl_updates:{version}"
    if upload:
        publish_source_file(
            snapshot_id,
            output_path,
            registry_credentials,
            capture_method="manual",
            metadata={
                "description": "Manual Pharos TDL overrides converted from a workbook.",
                "converted_from": str(output_path),
            },
        )
    return snapshot_id


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Convert a Pharos TDL workbook to input_files/manual/target_graph/tdl_updates.csv.",
    )
    parser.add_argument("workbook", nargs="?", default=DEFAULT_WORKBOOK, help="Input .xlsx workbook.")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help=f"Output CSV path (default: {DEFAULT_OUTPUT}).")
    parser.add_argument(
        "--register",
        action="store_true",
        help="After conversion, cache or publish target_graph:tdl_updates.",
    )
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="With --register, keep only the converted local file instead of uploading.",
    )
    parser.add_argument(
        "--registry-credentials",
        default=DEFAULT_REGISTRY_CREDENTIALS,
        help=f"Path to registry credentials YAML (default: {DEFAULT_REGISTRY_CREDENTIALS}).",
    )
    args = parser.parse_args()

    workbook_path = Path(args.workbook)
    output_path = Path(args.output)
    counts = convert_workbook(workbook_path, output_path)
    total = sum(counts.values())
    print(f"Wrote {total:,} TDL override rows -> {output_path}")
    print("TDL counts:")
    for tdl in ("Tclin", "Tchem", "Tbio", "Tdark"):
        print(f"  {tdl}: {counts.get(tdl, 0):,}")

    if args.register:
        snapshot_id = register_tdl_updates(
            output_path,
            args.registry_credentials,
            upload=not args.no_upload,
        )
        action = "Prepared locally" if args.no_upload else "Published"
        print(f"{action} target_graph:tdl_updates from {output_path}")
        if not args.no_upload:
            print("Registry ref:")
            print(f"  data_source: {snapshot_id}")


if __name__ == "__main__":
    main()
