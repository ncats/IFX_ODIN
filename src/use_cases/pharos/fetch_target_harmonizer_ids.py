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
    data_source: target_graph:gene_ids:2026-08-31        # ← new date
    data_source: target_graph:protein_ids:2026-08-31     # ← new date
    data_source: target_graph:transcript_ids:2026-08-31  # ← new date

    # If you pass --register-resolvers, use the printed resolver_snapshot refs too.
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

RESOLVER_BY_ENTITY_TYPE = {
    "gene": "tg_genes",
    "protein": "tg_proteins",
    "transcript": "tg_transcripts",
}

CONTENT_TYPES = {
    "gene_ids": "text/tab-separated-values",
    "protein_ids": "text/tab-separated-values",
    "transcript_ids": "text/tab-separated-values",
    "uniprot_mapping": "text/csv",
}


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


def write_source_manifest(
    output_dir: Path,
    dataset: str,
    version: str,
    filename: str,
    content_type: str,
) -> Path:
    """Write a source snapshot manifest next to a staged file."""
    from src.registry.manifest import build_source_snapshot_manifest, file_entry, write_manifest

    file_path = output_dir / dataset / version / filename
    entry = file_entry(
        local_path=file_path,
        source_url=None,
        storage_uri=None,
        content_type=content_type,
    )
    manifest = build_source_snapshot_manifest(
        source=REGISTRY_SOURCE,
        dataset=dataset,
        version=version,
        version_date=version,
        download_date=None,
        homepage=None,
        upstream_urls=[],
        files=[entry],
        downloaded_by="fetch_target_harmonizer_ids",
        extra={"description": f"Harmonized {dataset} from Target Harmonizer API"},
    )
    manifest_path = file_path.parent / "manifest.yaml"
    write_manifest(manifest, manifest_path)
    return manifest_path


def cache_source_snapshot_locally(manifest_path: Path, output_dir: Path) -> None:
    """Copy a source snapshot into the filesystem-backed registry cache."""
    from src.core.data_registry import DataRegistry

    registry = DataRegistry.local(cache_dir=output_dir)
    registry.upload_snapshot(manifest_path)
    print("  → Cached local registry snapshot")


def publish_to_registry(manifest_path: Path, registry_credentials: str):
    """Publish a source snapshot manifest to the IFX data registry."""
    from src.core.data_registry import DataRegistry

    registry = DataRegistry.from_registry_credentials(registry_credentials)
    uploaded = registry.upload_snapshot(manifest_path)
    print(f"  → Published to registry: {len(uploaded)} files uploaded")
    for uri in uploaded:
        print(f"    {uri}")


def register_resolver_snapshots(
    entity_types: list[str],
    output_dir: Path,
    registry_credentials: str | None = None,
    upload: bool = True,
) -> list[str]:
    """Register target resolver manifests against the latest uploaded source snapshots."""
    from src.core.data_registry import DataRegistry
    from src.registry.manifest import read_manifest

    if upload:
        if not registry_credentials:
            raise ValueError("registry_credentials is required when upload=True")
        registry = DataRegistry.from_registry_credentials(registry_credentials)
    else:
        registry = DataRegistry.local(cache_dir=output_dir)
    snapshot_ids = []
    resolver_dir = output_dir / "_resolver_snapshot_work"
    for entity_type in entity_types:
        resolver = RESOLVER_BY_ENTITY_TYPE[entity_type]
        manifest_path = registry.register_resolver_snapshot(
            REGISTRY_SOURCE,
            resolver,
            dest=resolver_dir,
            upload=upload,
        )
        if not upload:
            registry.upload_resolver_snapshot(manifest_path)
        manifest = read_manifest(manifest_path)
        snapshot_id = manifest["snapshot_id"]
        snapshot_ids.append(snapshot_id)
        print(f"  → Registered resolver snapshot: {snapshot_id}")
    return snapshot_ids


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
        default="./registry_cache",
        help="Local directory for downloaded files (default: ./registry_cache)",
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
    parser.add_argument(
        "--register-resolvers",
        action="store_true",
        help=(
            "After source snapshot upload succeeds, register refreshed tg_genes/"
            "tg_proteins/tg_transcripts resolver snapshots."
        ),
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    session = _make_session()

    print(f"Target Harmonizer API: {args.api_url}")
    print(f"Version: {args.version}")
    print(f"Output: {output_dir}")
    print()

    staged_entity_types = []
    uploaded_entity_types = []

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

        manifest_path = write_source_manifest(
            output_dir,
            dataset,
            args.version,
            filename,
            CONTENT_TYPES[dataset],
        )
        cache_source_snapshot_locally(manifest_path, output_dir)
        staged_entity_types.append(entity_type)

        if not args.no_upload:
            try:
                publish_to_registry(manifest_path, args.registry_credentials)
                uploaded_entity_types.append(entity_type)
            except Exception as exc:
                print(f"  ⚠ Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)

        print()

    if "protein" in args.entity_types and not args.skip_uniprot_mapping:
        dataset = "uniprot_mapping"
        filename = "uniprotkb_mapping.csv"
        file_dir = output_dir / dataset / args.version
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / filename
        copy_local_file(Path(args.uniprot_mapping_file), file_path, "UniProt mapping")
        print(f"  → Saved to {file_path}")

        manifest_path = write_source_manifest(
            output_dir,
            dataset,
            args.version,
            filename,
            CONTENT_TYPES[dataset],
        )
        cache_source_snapshot_locally(manifest_path, output_dir)

        if not args.no_upload:
            try:
                publish_to_registry(manifest_path, args.registry_credentials)
            except Exception as exc:
                print(f"  ⚠ Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)
        print()

    print("Done.")
    print(f"\nLocal data_source refs staged in {output_dir}:")
    for entity_type, dataset, _ in ENTITY_TYPES:
        if entity_type in args.entity_types:
            print(f"  data_source: {REGISTRY_SOURCE}:{dataset}:{args.version}")
    if "protein" in args.entity_types and not args.skip_uniprot_mapping:
        print(f"  data_source: {REGISTRY_SOURCE}:uniprot_mapping:{args.version}")

    if args.register_resolvers:
        resolver_entity_types = staged_entity_types if args.no_upload else uploaded_entity_types
        if not resolver_entity_types:
            print("\nResolver snapshots were not registered because no source snapshots were available.")
        else:
            mode = "local registry cache" if args.no_upload else "uploaded registry inputs"
            print(f"\nRegistering resolver snapshots from {mode}:")
            snapshot_ids = register_resolver_snapshots(
                resolver_entity_types,
                output_dir,
                registry_credentials=args.registry_credentials,
                upload=not args.no_upload,
            )
            print("\nUpdate resolver_snapshot values to:")
            for snapshot_id in snapshot_ids:
                print(f"  resolver_snapshot: {snapshot_id}")
    elif not args.no_upload:
        print("\nNext: register refreshed resolver snapshots before rebuilding a graph.")


if __name__ == "__main__":
    main()
