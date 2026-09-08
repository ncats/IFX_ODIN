#!/usr/bin/env python3
"""Convert Tudor/Pharos TDL workbooks into the tdl_updates.csv adapter format."""

import argparse
import csv
import shutil
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
    registry_cache_dir: Path,
    *,
    upload: bool,
) -> Path:
    from src.core.data_registry import DataRegistry
    from src.registry.manifest import build_source_snapshot_manifest, file_entry, write_manifest

    version = datetime.fromtimestamp(output_path.stat().st_mtime).date().isoformat()
    work_dir = registry_cache_dir / "_tdl_updates_work" / "target_graph" / "tdl_updates" / version
    work_dir.mkdir(parents=True, exist_ok=True)
    staged_file = work_dir / "tdl_updates.csv"
    if output_path.resolve() != staged_file.resolve():
        shutil.copy2(output_path, staged_file)

    entry = file_entry(
        local_path=staged_file,
        source_url=f"manual://target_graph/tdl_updates/{output_path.name}",
        storage_uri=None,
        content_type="text/csv",
    )
    manifest = build_source_snapshot_manifest(
        source="target_graph",
        dataset="tdl_updates",
        version=version,
        version_date=version,
        download_date=None,
        homepage=None,
        upstream_urls=[entry["source_url"]],
        files=[entry],
        downloaded_by="tdl_workbook_to_updates",
        extra={
            "description": "Manual Pharos TDL override file converted from Tudor/Pharos workbook.",
            "provenance": {"converted_from": str(output_path)},
        },
    )
    manifest_path = work_dir / "manifest.yaml"
    write_manifest(manifest, manifest_path)

    registry = (
        DataRegistry.from_registry_credentials(registry_credentials)
        if upload
        else DataRegistry.local(cache_dir=registry_cache_dir)
    )
    registry.upload_snapshot(manifest_path)
    return manifest_path


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
        help="With --register, write only to the local registry cache instead of uploading.",
    )
    parser.add_argument(
        "--registry-credentials",
        default=DEFAULT_REGISTRY_CREDENTIALS,
        help=f"Path to registry credentials YAML (default: {DEFAULT_REGISTRY_CREDENTIALS}).",
    )
    parser.add_argument(
        "--registry-cache-dir",
        default="./registry_cache",
        help="Local registry cache/work directory for publishing.",
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
        manifest_path = register_tdl_updates(
            output_path,
            args.registry_credentials,
            Path(args.registry_cache_dir),
            upload=not args.no_upload,
        )
        action = "Cached local" if args.no_upload else "Published"
        print(f"{action} target_graph:tdl_updates from {output_path}")
        print(f"Manifest: {manifest_path}")
        print("Registry ref:")
        print(f"  data_source: target_graph:tdl_updates:{manifest_path.parent.name}")


if __name__ == "__main__":
    main()
