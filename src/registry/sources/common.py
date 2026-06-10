import gzip
import shutil
from pathlib import Path
from typing import Dict, List, Optional

from src.registry.download import download_url
from src.registry.manifest import (
    MANIFEST_FILENAME,
    build_source_snapshot_manifest,
    file_entry,
    manifest_checksum,
    storage_prefix,
    write_manifest,
)
from src.registry.storage import MinioStorage, load_minio_credentials, s3_uri


def register_multi_file_last_modified_snapshot(
    *,
    source: str,
    dataset: str,
    urls: List[str],
    dest: Path,
    homepage: Optional[str] = None,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
    version: Optional[str] = None,
    version_description: Optional[str] = None,
    compress: bool = False,
) -> Path:
    work_dir = dest / source / dataset / "pending"
    downloaded = []
    for url in urls:
        downloaded.append((*download_url(url, work_dir, timeout=timeout), url))

    version_dates = [metadata.get("version_date") for local_path, metadata, url in downloaded if metadata.get("version_date")]
    if not version_dates:
        raise ValueError(f"Could not determine version_date from Last-Modified for {source}:{dataset}")
    version_date = max(version_dates)
    snapshot_version = version or version_date

    final_dir = dest / source / dataset / snapshot_version
    final_dir.mkdir(parents=True, exist_ok=True)

    moved = []
    for local_path, metadata, url in downloaded:
        final_path = final_dir / local_path.name
        if final_path != local_path:
            local_path.replace(final_path)
        if compress and not final_path.name.endswith(".gz"):
            compressed_path = final_path.with_name(f"{final_path.name}.gz")
            with final_path.open("rb") as source_handle:
                with gzip.open(compressed_path, "wb") as compressed_handle:
                    shutil.copyfileobj(source_handle, compressed_handle)
            final_path.unlink()
            final_path = compressed_path
            metadata["content_type"] = "application/gzip"
        moved.append((final_path, metadata, url))
    if work_dir.exists():
        try:
            work_dir.rmdir()
        except OSError:
            pass

    storage = None
    bucket = None
    if upload:
        if not minio_credentials:
            raise ValueError("minio_credentials is required when upload=True")
        storage = MinioStorage(load_minio_credentials(minio_credentials))
        bucket = storage.bucket

    object_prefix = storage_prefix(source, dataset, snapshot_version)
    file_entries = []
    evidence_files: List[Dict[str, Optional[str]]] = []
    for final_path, metadata, url in moved:
        storage_uri = s3_uri(bucket, f"{object_prefix}/{final_path.name}") if bucket else None
        file_entries.append(
            file_entry(
                final_path,
                metadata.get("final_url") or url,
                storage_uri,
                metadata.get("content_type"),
            )
        )
        evidence_files.append({
            "path": final_path.name,
            "url": metadata.get("final_url") or url,
            "last_modified": metadata.get("last_modified"),
        })

    manifest = build_source_snapshot_manifest(
        source=source,
        dataset=dataset,
        version=snapshot_version,
        version_date=version_date,
        download_date=None,
        homepage=homepage,
        upstream_urls=urls,
        files=file_entries,
        extra={
            "version_method": {
                "type": "multi_file_max_last_modified" if len(file_entries) > 1 else "single_file_last_modified",
                "description": version_description or "Use the max HTTP Last-Modified date across all files as version and version_date.",
                "evidence": {
                    "files": evidence_files,
                },
            }
        },
    )
    manifest_path = final_dir / MANIFEST_FILENAME
    write_manifest(manifest, manifest_path)

    if storage:
        for entry, (final_path, _, _) in zip(file_entries, moved):
            storage.upload_file(final_path, f"{object_prefix}/{final_path.name}", entry["content_type"])
        storage.upload_file(manifest_path, f"{object_prefix}/{MANIFEST_FILENAME}", "application/x-yaml")

    print(f"Wrote {manifest_path}")
    print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
    if bucket:
        print(f"Uploaded snapshot to s3://{bucket}/{object_prefix}/")
    return manifest_path
