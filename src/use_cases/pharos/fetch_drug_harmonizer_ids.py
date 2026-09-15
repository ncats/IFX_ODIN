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
    --output-dir ./registry_exports

After publishing, update impatient_target_graph.yaml::

    data_source: {kind: derived_snapshot, snapshot_id: drug_graph:drug_nodes:2026-09-04}
    data_source: {kind: derived_snapshot, snapshot_id: drug_graph:drug_edges:2026-09-04}
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
DEFAULT_PRODUCER_REPOSITORY = "https://github.com/ncats/IFX_Harmonizers"
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
    file_path: Path,
    dataset: str,
    version: str,
    registry_credentials: str,
    source_url: str | None,
    inputs: list[str],
    producer_release: str,
    producer_revision: str,
):
    from src.core.registry_publication import publish_derived_file

    result = publish_derived_file(
        f"{REGISTRY_SOURCE}:{dataset}:{version}",
        file_path,
        registry_credentials,
        inputs=inputs,
        producer_release=producer_release,
        producer_repository=DEFAULT_PRODUCER_REPOSITORY,
        producer_revision=producer_revision,
        transform_name="drug_harmonizer_export",
        validation={"size_bytes": file_path.stat().st_size, "nonempty": file_path.stat().st_size > 0},
        metadata={
            "description": f"Harmonized {dataset} from Drug Harmonizer",
            "export_url": source_url,
        },
    )
    print(f"  -> Published to registry: {result.snapshot_id}")


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
    if not args.no_upload and (not args.input or not args.producer_release or not args.producer_revision):
        parser.error("upload requires --input, --producer-release, and --producer-revision")

    output_dir = Path(args.output_dir)
    session = _make_session()

    print(f"Drug Harmonizer API: {args.api_url}")
    print(f"Version: {args.version}")
    print(f"Output: {output_dir}")
    print()
    published_refs: list[str] = []
    publication_failures: list[str] = []

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

        if not args.no_upload:
            try:
                source_url = None if args.local_dir else (
                    f"{args.api_url.rstrip('/')}/drug-id-qa/download/{filename}"
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
                print(f"  Warning: Registry upload failed: {exc}", file=sys.stderr)
                print(f"    File saved locally at {file_path}", file=sys.stderr)
                publication_failures.append(f"{dataset}: {exc}")
        print()

    print("Done.")
    if args.no_upload:
        print(f"\nFiles were staged in {output_dir}; nothing was registered.")
    else:
        print("\nRegistered drug graph dataset refs:")
        for snapshot_id in published_refs:
            print(
                "  data_source: {kind: derived_snapshot, snapshot_id: "
                f"{snapshot_id}" + "}"
            )
        if publication_failures:
            raise RuntimeError(
                "Registry publication failed for: " + "; ".join(publication_failures)
            )


if __name__ == "__main__":
    main()
