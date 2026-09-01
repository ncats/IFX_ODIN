#!/usr/bin/env python3
"""Fetch harmonized disease IDs from the Disease Harmonizer API
and publish them to the IFX data registry.

Keith can run this script to pull the latest harmonized disease IDs from the
Disease Harmonizer Explorer and publish them as a new registry snapshot. After
publishing, update the version dates in ``disease_graph.yaml`` to point to the
new snapshot.

Usage
-----
# Fetch + publish to registry (default: ifxdev Disease Harmonizer)
python -m src.use_cases.pharos.fetch_disease_harmonizer_ids

# Custom API base URL
python -m src.use_cases.pharos.fetch_disease_harmonizer_ids \
    --api-url https://ifxdev.ncats.nih.gov/odin-qa

# Fetch only (no registry upload) — writes files to --output-dir
python -m src.use_cases.pharos.fetch_disease_harmonizer_ids --no-upload --output-dir ./disease_ids

# Custom registry credentials
python -m src.use_cases.pharos.fetch_disease_harmonizer_ids \
    --registry-credentials ./src/use_cases/secrets/aws_ifx_registry.yaml

After publishing, update disease_graph.yaml::

    # Change the version dates to match the new snapshot
    data_source: disease_graph:disease_concepts:2026-09-01        # <- new date
    data_source: disease_graph:disease_xref_edges:2026-09-01      # <- new date
    data_source: disease_graph:disease_hierarchy_edges:2026-09-01  # <- new date
    data_source: disease_graph:xref_labels:2026-09-01              # <- new date
"""

import argparse
import sys
from datetime import date
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter, Retry


DEFAULT_API_URL = "https://ifxdev.ncats.nih.gov/odin-qa"
DEFAULT_REGISTRY_CREDENTIALS = "./src/use_cases/secrets/aws_ifx_registry.yaml"
REGISTRY_SOURCE = "disease_graph"

# NOTE: disease_source_catalog.tsv is served from a metadata/ subdirectory,
# not from the app_graph root, so it is not available via the /download/{filename}
# endpoint. If needed, a new endpoint would have to be added to app.py.
ENTITY_TYPES = [
    ("concepts",             "disease_concepts",                 "disease_concepts.tsv"),
    ("edges",                "disease_xref_edges",               "disease_xref_edges.tsv"),
    ("hierarchy",            "disease_hierarchy_edges",           "disease_hierarchy_edges.tsv"),
    ("labels",               "xref_labels",                      "xref_labels.tsv"),
    ("clinical_descendants", "disease_clinical_descendant_edges", "disease_clinical_descendant_edges.tsv"),
    ("decisions",            "review_decisions",                  "review_decisions.tsv"),
    ("gene_associations",    "disease_gene_associations",         "disease_gene_associations.tsv"),
    ("manifest",             "manifest",                          "manifest.json"),
]


def _make_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def fetch_disease_file(api_url: str, filename: str, session: requests.Session) -> str:
    """Fetch a disease harmonizer file from the download endpoint."""
    url = f"{api_url.rstrip('/')}/disease-id-qa/download/{filename}"
    print(f"Fetching {filename} from {url} ...")
    resp = session.get(url, timeout=300)
    resp.raise_for_status()
    content = resp.text
    line_count = content.count("\n")
    print(f"  -> {line_count:,} rows ({len(content):,} bytes)")
    return content


def publish_to_registry(
    output_dir: Path,
    dataset: str,
    version: str,
    filename: str,
    registry_credentials: str,
):
    """Publish a single disease file to the IFX data registry."""
    from src.core.data_registry import DataRegistry
    from src.registry.manifest import build_source_snapshot_manifest, file_entry, write_manifest

    file_path = output_dir / dataset / version / filename
    content_type = "application/json" if filename.endswith(".json") else "text/tab-separated-values"
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
        downloaded_by="fetch_disease_harmonizer_ids",
        extra={"description": f"Harmonized {dataset} from Disease Harmonizer API"},
    )
    manifest_path = file_path.parent / "manifest.yaml"
    write_manifest(manifest, manifest_path)

    registry = DataRegistry.from_credentials(registry_credentials)
    uploaded = registry.upload_snapshot(manifest_path)
    print(f"  -> Published to registry: {len(uploaded)} files uploaded")
    for uri in uploaded:
        print(f"    {uri}")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch harmonized disease IDs from Disease Harmonizer and publish to registry.",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Disease Harmonizer base URL (default: {DEFAULT_API_URL})",
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
    all_file_types = [ft for ft, _, _ in ENTITY_TYPES]
    parser.add_argument(
        "--file-types",
        nargs="+",
        choices=all_file_types,
        default=all_file_types,
        help="Which file types to fetch (default: all)",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    session = _make_session()

    print(f"Disease Harmonizer API: {args.api_url}")
    print(f"Version: {args.version}")
    print(f"Output: {output_dir}")
    print()

    for file_type, dataset, filename in ENTITY_TYPES:
        if file_type not in args.file_types:
            continue

        # Fetch from API
        content = fetch_disease_file(args.api_url, filename, session)

        # Write to local directory in registry layout
        file_dir = output_dir / dataset / args.version
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / filename
        file_path.write_text(content, encoding="utf-8")
        print(f"  -> Saved to {file_path}")

        # Publish to registry
        if not args.no_upload:
            try:
                publish_to_registry(
                    output_dir, dataset, args.version, filename,
                    args.registry_credentials,
                )
            except Exception as exc:
                print(f"  Warning: Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)

        print()

    print("Done.")
    if not args.no_upload:
        print(f"\nUpdate disease_graph.yaml data_source versions to: {args.version}")
        print("For example:")
        for file_type, dataset, _ in ENTITY_TYPES:
            if file_type in args.file_types:
                print(f"  data_source: {REGISTRY_SOURCE}:{dataset}:{args.version}")


if __name__ == "__main__":
    main()
