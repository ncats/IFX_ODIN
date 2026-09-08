#!/usr/bin/env python3
"""Stage Target Harmonizer evidence bundle files as local registry snapshots.

This is for local `impatient_target_graph` rebuilds that should consume the
completed IFX target harmonizer release artifacts instead of Keith's S3-backed
registry snapshots.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any

from src.core.data_registry import DataRegistry
from src.registry.manifest import build_source_snapshot_manifest, file_entry, write_manifest


DEFAULT_TARGET_REPO = Path("/Users/mainejl/Documents/Projects/ODIN/TargetGraph/TargetGraph7")
DEFAULT_REGISTRY_CACHE_DIR = Path("./registry_cache")


STAGED_FILES = {
    ("go", "ontology", "go-basic.json"): ("go", "ontology", "go-basic.json", "application/json"),
    ("go", "goa_human_uniprot", "goa_human_uniprot.gaf.gz"): (
        "go",
        "goa_human_uniprot",
        "goa_human_uniprot.gaf.gz",
        "application/gzip",
    ),
    ("go", "goa_human_go", "goa_human_go.gaf.gz"): (
        "go",
        "goa_human_go",
        "goa_human_go.gaf.gz",
        "application/gzip",
    ),
    ("ncbi", "generif", "generif_9606.csv"): (
        "target_graph",
        "generif",
        "generif_9606.csv",
        "text/csv",
    ),
    ("jensenlab", "protein_counts", "protein_counts.tsv"): (
        "jensenlab",
        "protein_counts",
        "protein_counts.tsv",
        "text/tab-separated-values",
    ),
    ("antibodypedia", "scraped_results", "antibodypedia_scraped_results.csv"): (
        "antibodypedia",
        "scraped_results",
        "antibodypedia_scraped_results.csv",
        "text/csv",
    ),
}


def _load_json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8") as handle:
        return json.load(handle)


def _latest_evidence_manifest(target_repo: Path) -> Path:
    manifests = sorted(
        (target_repo / "src/data/publicdata/target_data").glob("v*/evidence/target_evidence_manifest.json"),
        key=lambda path: path.stat().st_mtime,
    )
    if not manifests:
        raise FileNotFoundError("No target evidence manifest found under target_data/v*/evidence")
    return manifests[-1]


def _source_versions(target_repo: Path) -> dict[tuple[str, str], str]:
    versions: dict[tuple[str, str], str] = {}
    metadata_dir = target_repo / "src/data/publicdata/target_data/metadata"

    go_metadata = metadata_dir / "dl_go_evidence_metadata.json"
    if go_metadata.exists():
        for item in _load_json(go_metadata).get("downloads", []) or []:
            source = item.get("source")
            dataset = item.get("dataset")
            version = item.get("version")
            if source and dataset and version:
                versions[(source, dataset)] = str(version)

    jensen_metadata = metadata_dir / "dl_jensenlab_pm_metadata.json"
    if jensen_metadata.exists():
        payload = _load_json(jensen_metadata)
        version = payload.get("source_version")
        if version:
            versions[("jensenlab", "protein_counts")] = str(version)

    generif_metadata = metadata_dir / "dl_generif_metadata.json"
    if generif_metadata.exists():
        version = _load_json(generif_metadata).get("source_version")
        if version:
            versions[("target_graph", "generif")] = str(version)

    return versions


def _evidence_version(record: dict[str, Any], target_source: str, target_dataset: str, versions: dict[tuple[str, str], str]) -> str:
    if (target_source, target_dataset) in versions:
        return versions[(target_source, target_dataset)]
    source = str(record.get("source") or "")
    dataset = str(record.get("dataset") or "")
    if (source, dataset) in versions:
        return versions[(source, dataset)]

    path = Path(str(record.get("source_path") or record.get("path") or ""))
    if target_source == "antibodypedia":
        match = re.search(r"antibodypedia_scraped_results_(.+)\.csv$", path.name)
        if match:
            return match.group(1)

    generated_at = str(record.get("generated_at") or "")
    if generated_at:
        return generated_at[:10]
    return datetime.fromtimestamp(path.stat().st_mtime).date().isoformat()


def _stage_one(
    *,
    source_path: Path,
    source: str,
    dataset: str,
    version: str,
    output_name: str,
    content_type: str,
    registry_cache_dir: Path,
    upstream_url: str,
    description: str,
) -> Path:
    work_dir = registry_cache_dir / "_local_evidence_work" / source / dataset / version
    work_dir.mkdir(parents=True, exist_ok=True)
    staged_path = work_dir / output_name
    if source_path.resolve() != staged_path.resolve():
        shutil.copy2(source_path, staged_path)

    entry = file_entry(
        local_path=staged_path,
        source_url=upstream_url,
        storage_uri=None,
        content_type=content_type,
    )
    manifest = build_source_snapshot_manifest(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version[:10],
        download_date=None,
        homepage=None,
        upstream_urls=[upstream_url],
        files=[entry],
        downloaded_by="stage_target_evidence_for_impatient_graph",
        extra={"description": description},
    )
    manifest_path = work_dir / "manifest.yaml"
    write_manifest(manifest, manifest_path)

    registry = DataRegistry.local(cache_dir=registry_cache_dir)
    registry.upload_snapshot(manifest_path)
    return manifest_path


def stage_evidence(
    evidence_manifest: Path,
    target_repo: Path,
    registry_cache_dir: Path,
) -> list[tuple[str, Path]]:
    manifest = _load_json(evidence_manifest)
    versions = _source_versions(target_repo)
    refs: list[tuple[str, Path]] = []

    for record in manifest.get("files", []) or []:
        key = (
            str(record.get("source") or ""),
            str(record.get("dataset") or ""),
            str(record.get("name") or record.get("path") or ""),
        )
        mapping = STAGED_FILES.get(key)
        if not mapping:
            continue
        source, dataset, output_name, content_type = mapping
        source_path = Path(str(record.get("path") or ""))
        if not source_path.is_absolute():
            source_path = target_repo / source_path
        if not source_path.exists():
            raise FileNotFoundError(source_path)

        version = _evidence_version(record, source, dataset, versions)
        upstream_url = str(record.get("source_path") or record.get("path") or source_path)
        manifest_path = _stage_one(
            source_path=source_path,
            source=source,
            dataset=dataset,
            version=version,
            output_name=output_name,
            content_type=content_type,
            registry_cache_dir=registry_cache_dir,
            upstream_url=upstream_url,
            description=f"Local impatient_target_graph evidence staged from {evidence_manifest}",
        )
        refs.append((f"{source}:{dataset}:{version}", manifest_path))

    return refs


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Stage completed Target Harmonizer evidence bundle files into a local registry cache.",
    )
    parser.add_argument("--target-repo", default=str(DEFAULT_TARGET_REPO))
    parser.add_argument("--evidence-manifest", help="Path to target_evidence_manifest.json. Defaults to latest v*/evidence manifest.")
    parser.add_argument("--registry-cache-dir", default=str(DEFAULT_REGISTRY_CACHE_DIR))
    args = parser.parse_args()

    target_repo = Path(args.target_repo)
    evidence_manifest = Path(args.evidence_manifest) if args.evidence_manifest else _latest_evidence_manifest(target_repo)
    registry_cache_dir = Path(args.registry_cache_dir)

    refs = stage_evidence(evidence_manifest, target_repo, registry_cache_dir)
    if not refs:
        raise RuntimeError(f"No graph evidence files were staged from {evidence_manifest}")

    print(f"Staged {len(refs)} local source snapshot(s) from {evidence_manifest}:")
    for ref, manifest_path in refs:
        print(f"  data_source: {ref}")
        print(f"    manifest: {manifest_path}")


if __name__ == "__main__":
    main()
