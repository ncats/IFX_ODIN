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

After publishing, update target_graph.yaml::

    # Change the version dates to match the new snapshot
    data_source: target_graph:gene_ids:2026-08-31        # ← new date
    data_source: target_graph:protein_ids:2026-08-31     # ← new date
    data_source: target_graph:transcript_ids:2026-08-31  # ← new date
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry


DEFAULT_API_URL = "https://ifxdev.ncats.nih.gov/odin-qa"
DEFAULT_REGISTRY_CREDENTIALS = "./src/use_cases/secrets/aws_ifx_registry.yaml"
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


def publish_to_registry(
    output_dir: Path,
    dataset: str,
    version: str,
    filename: str,
    registry_credentials: str,
):
    """Publish a single entity ID file to the IFX data registry."""
    from src.core.data_registry import DataRegistry
    from src.registry.manifest import build_source_snapshot_manifest, file_entry, write_manifest

    file_path = output_dir / dataset / version / filename
    entry = file_entry(
        local_path=file_path,
        source_url=None,
        storage_uri=None,
        content_type="text/tab-separated-values",
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

    registry = DataRegistry.from_credentials(registry_credentials)
    uploaded = registry.upload_snapshot(manifest_path)
    print(f"  → Published to registry: {len(uploaded)} files uploaded")
    for uri in uploaded:
        print(f"    {uri}")


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
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    session = _make_session()

    print(f"Target Harmonizer API: {args.api_url}")
    print(f"Version: {args.version}")
    print(f"Output: {output_dir}")
    print()

    for entity_type, dataset, filename in ENTITY_TYPES:
        if entity_type not in args.entity_types:
            continue

        # Fetch from API
        content = fetch_entity_ids(args.api_url, entity_type, session)

        # Write to local directory in registry layout
        file_dir = output_dir / dataset / args.version
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / filename
        file_path.write_text(content, encoding="utf-8")
        print(f"  → Saved to {file_path}")

        # Publish to registry
        if not args.no_upload:
            try:
                publish_to_registry(
                    output_dir, dataset, args.version, filename,
                    args.registry_credentials,
                )
            except Exception as exc:
                print(f"  ⚠ Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)

        print()

    print("Done.")
    if not args.no_upload:
        print(f"\nUpdate target_graph.yaml data_source versions to: {args.version}")
        print("For example:")
        for entity_type, dataset, _ in ENTITY_TYPES:
            if entity_type in args.entity_types:
                print(f"  data_source: {REGISTRY_SOURCE}:{dataset}:{args.version}")


if __name__ == "__main__":
    main()
