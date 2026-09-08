#!/usr/bin/env python3
"""Fetch full drug harmonizer graph files and publish them to the IFX registry.

The impatient_target_graph build can use these snapshots instead of reading
DrugCentral, IUPHAR, and ChEMBL ligand data directly. The files come from the
Drug Harmonizer Explorer app graph export.

Usage
-----
# Fetch + publish to registry
python -m src.use_cases.pharos.fetch_drug_harmonizer_ids

# Fetch from a local QA Browser server and do not upload
python -m src.use_cases.pharos.fetch_drug_harmonizer_ids \
    --api-url http://127.0.0.1:8050 \
    --no-upload \
    --output-dir ./registry_cache

After publishing, update impatient_target_graph.yaml::

    data_source: drug_graph:drug_nodes:2026-09-04
    data_source: drug_graph:drug_edges:2026-09-04
"""

import argparse
import sys
from datetime import date
from pathlib import Path
from typing import BinaryIO

import requests
from requests.adapters import HTTPAdapter, Retry


DEFAULT_API_URL = "https://ifxdev.ncats.nih.gov/odin-qa"
DEFAULT_REGISTRY_CREDENTIALS = "./src/use_cases/secrets/aws_ifx_registry.yaml"
REGISTRY_SOURCE = "drug_graph"

FILE_TYPES = [
    ("nodes", "drug_nodes", "drug_nodes_full.tsv", "text/tab-separated-values"),
    ("edges", "drug_edges", "drug_edges_full.tsv", "text/tab-separated-values"),
    ("manifest", "manifest", "manifest.json", "application/json"),
]


def _make_session() -> requests.Session:
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.mount("http://", HTTPAdapter(max_retries=retries))
    return session


def _count_lines(path: Path) -> int:
    lines = 0
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            lines += chunk.count(b"\n")
    return lines


def _copy_stream(src: BinaryIO, dst: BinaryIO) -> None:
    for chunk in iter(lambda: src.read(1024 * 1024), b""):
        if chunk:
            dst.write(chunk)


def fetch_drug_file(api_url: str, filename: str, session: requests.Session, output_path: Path) -> None:
    url = f"{api_url.rstrip('/')}/drug-id-qa/download/{filename}"
    print(f"Fetching {filename} from {url} ...")
    with session.get(url, timeout=300, stream=True) as resp:
        resp.raise_for_status()
        with output_path.open("wb") as handle:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
    print(f"  -> {_count_lines(output_path):,} lines ({output_path.stat().st_size:,} bytes)")


def read_local_file(path: Path, filename: str, output_path: Path) -> None:
    if path.is_dir():
        path = path / filename
    if not path.exists():
        raise FileNotFoundError(path)
    print(f"Reading {filename} from {path} ...")
    with path.open("rb") as src, output_path.open("wb") as dst:
        _copy_stream(src, dst)
    print(f"  -> {_count_lines(output_path):,} lines ({output_path.stat().st_size:,} bytes)")


def publish_to_registry(
    manifest_path: Path,
    registry_credentials: str,
):
    from src.core.data_registry import DataRegistry

    registry = DataRegistry.from_registry_credentials(registry_credentials)
    uploaded = registry.upload_snapshot(manifest_path)
    print(f"  -> Published to registry: {len(uploaded)} files uploaded")
    for uri in uploaded:
        print(f"    {uri}")


def write_source_manifest(
    output_dir: Path,
    dataset: str,
    version: str,
    filename: str,
    content_type: str,
) -> Path:
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
        downloaded_by="fetch_drug_harmonizer_ids",
        extra={"description": f"Harmonized {dataset} from Drug Harmonizer API"},
    )
    manifest_path = file_path.parent / "manifest.yaml"
    write_manifest(manifest, manifest_path)
    return manifest_path


def cache_source_snapshot_locally(manifest_path: Path, output_dir: Path) -> None:
    from src.core.data_registry import DataRegistry

    registry = DataRegistry.local(cache_dir=output_dir)
    registry.upload_snapshot(manifest_path)
    print("  -> Cached local registry snapshot")


def main():
    parser = argparse.ArgumentParser(
        description="Fetch full drug harmonizer graph files and publish to registry.",
    )
    parser.add_argument(
        "--api-url",
        default=DEFAULT_API_URL,
        help=f"Drug Harmonizer base URL (default: {DEFAULT_API_URL})",
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
    parser.add_argument("--no-upload", action="store_true", help="Download files only, skip registry upload")
    parser.add_argument(
        "--file-types",
        nargs="+",
        choices=[item[0] for item in FILE_TYPES],
        default=[item[0] for item in FILE_TYPES],
        help="Which file types to fetch (default: all)",
    )
    parser.add_argument(
        "--local-dir",
        help="Read files from this app_graph directory instead of fetching from the API.",
    )
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    session = _make_session()

    print(f"Drug Harmonizer API: {args.api_url}")
    print(f"Version: {args.version}")
    print(f"Output: {output_dir}")
    print()

    for file_type, dataset, filename, content_type in FILE_TYPES:
        if file_type not in args.file_types:
            continue
        file_dir = output_dir / dataset / args.version
        file_dir.mkdir(parents=True, exist_ok=True)
        file_path = file_dir / filename
        if args.local_dir:
            read_local_file(Path(args.local_dir), filename, file_path)
        else:
            fetch_drug_file(args.api_url, filename, session, file_path)
        print(f"  -> Saved to {file_path}")

        manifest_path = write_source_manifest(
            output_dir,
            dataset,
            args.version,
            filename,
            content_type,
        )
        cache_source_snapshot_locally(manifest_path, output_dir)

        if not args.no_upload:
            try:
                publish_to_registry(manifest_path, args.registry_credentials)
            except Exception as exc:
                print(f"  Warning: Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)
        print()

    print("Done.")
    print(f"\nDrug graph data_source refs staged in {output_dir}:")
    print(f"  data_source: {REGISTRY_SOURCE}:drug_nodes:{args.version}")
    print(f"  data_source: {REGISTRY_SOURCE}:drug_edges:{args.version}")


if __name__ == "__main__":
    main()
