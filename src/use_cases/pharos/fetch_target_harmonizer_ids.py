#!/usr/bin/env python3
"""Fetch harmonized gene/transcript/protein IDs from the Target Harmonizer API
and publish them to the IFX data registry.

Keith can run this script to pull the latest harmonized entity IDs from the
Target Harmonizer Explorer and publish them as a new registry snapshot. After
publishing, update the version dates in ``target_graph.yaml`` to point to the
new snapshot.

Usage
-----
# Fetch + publish to registry (default: ifxdev Target Harmonizer)
python -m src.use_cases.pharos.fetch_target_harmonizer_ids

# Custom API base URL
python -m src.use_cases.pharos.fetch_target_harmonizer_ids \
    --api-url https://ifxdev.ncats.nih.gov/odin-qa

# Fetch only (no registry upload) — writes files to --output-dir
python -m src.use_cases.pharos.fetch_target_harmonizer_ids --no-upload --output-dir ./target_ids

# Custom registry credentials
python -m src.use_cases.pharos.fetch_target_harmonizer_ids \
    --registry-credentials ./src/use_cases/secrets/aws_ifx_registry.yaml

After publishing, update target_graph.yaml or impatient_target_graph.yaml::

    # Change the version dates to match the new snapshot
    data_source: {kind: derived_snapshot, snapshot_id: target_graph:gene_ids:2026-08-31}
    data_source: {kind: derived_snapshot, snapshot_id: target_graph:protein_ids:2026-08-31}
    data_source: {kind: derived_snapshot, snapshot_id: target_graph:transcript_ids:2026-08-31}

"""

import argparse
import csv
import re
import shutil
import sys
from datetime import date
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry


DEFAULT_API_URL = "https://ifxdev.ncats.nih.gov/odin-qa"
DEFAULT_REGISTRY_CREDENTIALS = "./src/use_cases/secrets/aws_ifx_registry.yaml"
DEFAULT_PRODUCER_REPOSITORY = "https://github.com/ncats/IFX_Harmonizers"
DEFAULT_TDL_UPDATES_FILE = "input_files/manual/target_graph/tdl_updates.csv"
DEFAULT_UNIPROT_MAPPING_FILE = (
    "../TargetGraph/TargetGraph7/src/data/publicdata/target_data/cleaned/sources/uniprotkb_mapping.csv"
)
REGISTRY_SOURCE = "target_graph"

ENTITY_TYPES = [
    ("gene", "gene_ids", "gene_ids.tsv"),
    ("protein", "protein_ids", "protein_ids.tsv"),
    ("transcript", "transcript_ids", "transcript_ids.tsv"),
]

def _make_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def fetch_entity_ids(api_url: str, entity_type: str, session: requests.Session) -> str:
    """Fetch entity IDs TSV from the Target Harmonizer API."""
    url = f"{api_url.rstrip('/')}/target-id-qa/api/entity-ids/{entity_type}"
    print(f"Fetching {entity_type} IDs from {url} ...")
    resp = session.get(url, timeout=300)
    resp.raise_for_status()
    content = resp.text
    line_count = content.count("\n")
    print(f"  → {line_count:,} rows ({len(content):,} bytes)")
    return content


def read_entity_ids_from_file(path: Path, entity_type: str) -> str:
    if not path.exists():
        raise FileNotFoundError(f"{entity_type} IDs file does not exist: {path}")
    content = path.read_text(encoding="utf-8")
    line_count = content.count("\n")
    print(f"Reading {entity_type} IDs from {path} ...")
    print(f"  → {line_count:,} rows ({len(content):,} bytes)")
    return content


def copy_local_file(source_path: Path, output_path: Path, label: str) -> None:
    if not source_path.exists():
        raise FileNotFoundError(f"{label} file does not exist: {source_path}")
    shutil.copy2(source_path, output_path)
    print(f"Reading {label} from {source_path} ...")
    print(f"  → {output_path.stat().st_size:,} bytes")


def _split_multi(value: str | None) -> set[str]:
    if not value:
        return set()
    return {part.strip() for part in re.split(r"[|,]", str(value)) if part.strip()}


def _tdl_override_isoforms(tdl_updates_path: Path) -> set[str]:
    if not tdl_updates_path.exists():
        print(f"  ⚠ TDL updates file not found, skipping isoform override validation: {tdl_updates_path}")
        return set()
    isoforms: set[str] = set()
    with tdl_updates_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            uniprot = (row.get("UniProt") or row.get("uniprot_id") or "").strip()
            if re.fullmatch(r"[A-Z0-9]+-\d+", uniprot):
                isoforms.add(uniprot)
    return isoforms


def _protein_ids_indexed_uniprot_values(protein_ids_path: Path) -> set[str]:
    values: set[str] = set()
    with protein_ids_path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for row in reader:
            for column in ("uniprot_isoform", "uniprot_id"):
                values.update(_split_multi(row.get(column)))
            ids = row.get("ids") or ""
            for identifier in _split_multi(ids):
                if identifier.startswith("UniProtKB:"):
                    values.add(identifier.split(":", 1)[1])
    return values


def validate_protein_isoform_overrides(protein_ids_path: Path, tdl_updates_path: Path) -> None:
    isoform_overrides = _tdl_override_isoforms(tdl_updates_path)
    if not isoform_overrides:
        return
    indexed_uniprot_values = _protein_ids_indexed_uniprot_values(protein_ids_path)
    missing = sorted(isoform for isoform in isoform_overrides if isoform not in indexed_uniprot_values)
    if missing:
        preview = ", ".join(missing[:10])
        extra = f" (+{len(missing) - 10} more)" if len(missing) > 10 else ""
        raise RuntimeError(
            "Protein ID export is missing exact UniProt isoform accession(s) required by "
            f"{tdl_updates_path}: {preview}{extra}. Use a protein_ids.tsv export that includes "
            "the uniprot_isoform column before registering target_graph:protein_ids."
        )
    print(f"  → Validated {len(isoform_overrides):,} isoform TDL override(s) against protein_ids.tsv")


def publish_to_registry(
    file_path: Path,
    dataset: str,
    version: str,
    registry_credentials: str,
    source_url: str | None,
    inputs: list[str],
    producer_release: str,
    producer_revision: str,
) -> None:
    from src.core.registry_publication import publish_derived_file

    result = publish_derived_file(
        f"{REGISTRY_SOURCE}:{dataset}:{version}",
        file_path,
        registry_credentials,
        inputs=inputs,
        producer_release=producer_release,
        producer_repository=DEFAULT_PRODUCER_REPOSITORY,
        producer_revision=producer_revision,
        transform_name="target_harmonizer_export",
        validation={"size_bytes": file_path.stat().st_size, "nonempty": file_path.stat().st_size > 0},
        metadata={
            "description": f"Harmonized {dataset} from Target Harmonizer",
            "export_url": source_url,
        },
    )
    print(f"  → Published to registry: {result.snapshot_id}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch harmonized entity IDs from Target Harmonizer and publish to registry.",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Target Harmonizer base URL (default: {DEFAULT_API_URL})",
    )
    parser.add_argument(
        "--output-dir",
        default="./registry_exports",
        help="Local staging directory for downloaded files (default: ./registry_exports)",
    )
    parser.add_argument(
        "--version",
        default=date.today().isoformat(),
        help="Version label for the snapshot (default: today's date)",
    )
    parser.add_argument(
        "--registry-credentials",
        default=DEFAULT_REGISTRY_CREDENTIALS,
        help=f"Path to registry credentials YAML (default: {DEFAULT_REGISTRY_CREDENTIALS})",
    )
    parser.add_argument(
        "--input",
        action="append",
        default=[],
        help="Exact derived input as source=source:dataset:version (also derived= or external=); repeatable.",
    )
    parser.add_argument("--producer-release", help="IFX Harmonizers release that produced the export.")
    parser.add_argument("--producer-revision", help="Full IFX Harmonizers Git commit hash.")
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Download files only, skip registry upload",
    )
    parser.add_argument(
        "--entity-types",
        nargs="+",
        choices=["gene", "protein", "transcript"],
        default=["gene", "protein", "transcript"],
        help="Which entity types to fetch (default: all three)",
    )
    parser.add_argument(
        "--gene-file",
        help="Use this local gene_ids.tsv instead of fetching gene IDs from the API.",
    )
    parser.add_argument(
        "--protein-file",
        help="Use this local protein_ids.tsv instead of fetching protein IDs from the API.",
    )
    parser.add_argument(
        "--transcript-file",
        help="Use this local transcript_ids.tsv instead of fetching transcript IDs from the API.",
    )
    parser.add_argument(
        "--uniprot-mapping-file",
        default=DEFAULT_UNIPROT_MAPPING_FILE,
        help=(
            "Use this local uniprotkb_mapping.csv as target_graph:uniprot_mapping "
            f"(default: {DEFAULT_UNIPROT_MAPPING_FILE})."
        ),
    )
    parser.add_argument(
        "--skip-uniprot-mapping",
        action="store_true",
        help="Do not stage target_graph:uniprot_mapping. Only use this if protein resolver snapshots are not needed.",
    )
    parser.add_argument(
        "--tdl-updates-file",
        default=DEFAULT_TDL_UPDATES_FILE,
        help=(
            "TDL override CSV used to validate exact protein isoform IDs "
            f"(default: {DEFAULT_TDL_UPDATES_FILE})."
        ),
    )
    parser.add_argument(
        "--skip-isoform-validation",
        action="store_true",
        help="Skip validation that UniProt isoforms in tdl_updates.csv exist in protein_ids.tsv.",
    )
    args = parser.parse_args()
    if not args.no_upload and (not args.input or not args.producer_release or not args.producer_revision):
        parser.error("upload requires --input, --producer-release, and --producer-revision")

    output_dir = Path(args.output_dir)
    session = _make_session()

    print(f"Target Harmonizer API: {args.api_url}")
    print(f"Version: {args.version}")
    print(f"Output: {output_dir}")
    print()
    published_refs: list[str] = []
    publication_failures: list[str] = []

    for entity_type, dataset, filename in ENTITY_TYPES:
        if entity_type not in args.entity_types:
            continue

        local_file_arg = getattr(args, f"{entity_type}_file")
        if local_file_arg:
            content = read_entity_ids_from_file(Path(local_file_arg), entity_type)
        else:
            content = fetch_entity_ids(args.api_url, entity_type, session)

        # Write to local directory in registry layout
        file_dir = output_dir / dataset / args.version
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / filename
        file_path.write_text(content, encoding="utf-8")
        print(f"  → Saved to {file_path}")

        if entity_type == "protein" and not args.skip_isoform_validation:
            validate_protein_isoform_overrides(file_path, Path(args.tdl_updates_file))

        if not args.no_upload:
            try:
                source_url = None if local_file_arg else (
                    f"{args.api_url.rstrip('/')}/target-id-qa/api/entity-ids/{entity_type}"
                )
                publish_to_registry(
                    file_path,
                    dataset,
                    args.version,
                    args.registry_credentials,
                    source_url,
                    args.input,
                    args.producer_release,
                    args.producer_revision,
                )
                published_refs.append(f"{REGISTRY_SOURCE}:{dataset}:{args.version}")
            except Exception as exc:
                print(f"  ⚠ Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)
                publication_failures.append(f"{dataset}: {exc}")

        print()

    if "protein" in args.entity_types and not args.skip_uniprot_mapping:
        dataset = "uniprot_mapping"
        filename = "uniprotkb_mapping.csv"
        file_dir = output_dir / dataset / args.version
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / filename
        copy_local_file(Path(args.uniprot_mapping_file), file_path, "UniProt mapping")
        print(f"  → Saved to {file_path}")

        if not args.no_upload:
            try:
                publish_to_registry(
                    file_path,
                    dataset,
                    args.version,
                    args.registry_credentials,
                    None,
                    args.input,
                    args.producer_release,
                    args.producer_revision,
                )
                published_refs.append(
                    f"{REGISTRY_SOURCE}:uniprot_mapping:{args.version}"
                )
            except Exception as exc:
                print(f"  ⚠ Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)
                publication_failures.append(f"uniprot_mapping: {exc}")
        print()

    print("Done.")
    if args.no_upload:
        print(f"\nFiles were staged in {output_dir}; nothing was registered.")
    else:
        print("\nRegistered dataset refs:")
        for snapshot_id in published_refs:
            print(
                "  data_source: {kind: derived_snapshot, snapshot_id: "
                f"{snapshot_id}" + "}"
            )
        if published_refs:
            print("\nNext: pin these dataset versions directly in the ODIN resolver configuration.")
        if publication_failures:
            raise RuntimeError(
                "Registry publication failed for: " + "; ".join(publication_failures)
            )


if __name__ == "__main__":
    main()
