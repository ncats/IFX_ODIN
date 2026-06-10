from pathlib import Path
from typing import Optional

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
from src.registry.sources.common import register_multi_file_last_modified_snapshot


JENSENLAB_TISSUES_URL = "https://download.jensenlab.org/human_tissue_integrated_full.tsv"
JENSENLAB_HOMEPAGE = "https://jensenlab.org/resources/proteomics/"
JENSENLAB_TISSUES_FILE_NAME = "human_tissue_integrated_full.tsv"
JENSENLAB_DISEASE_URLS = [
    "https://download.jensenlab.org/human_disease_knowledge_filtered.tsv",
    "https://download.jensenlab.org/human_disease_experiments_filtered.tsv",
    "https://download.jensenlab.org/human_disease_textmining_filtered.tsv",
]
JENSENLAB_TINX_URLS = [
    "https://download.jensenlab.org/human_textmining_mentions.tsv",
    "https://download.jensenlab.org/disease_textmining_mentions.tsv",
]
JENSENLAB_PROTEIN_COUNTS_URLS = [
    "https://download.jensenlab.org/protein_counts.tsv",
]


def register_tissues(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "jensenlab"
    dataset = "tissues"
    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(JENSENLAB_TISSUES_URL, work_dir, timeout=timeout)
    version_date = metadata.get("version_date")
    if not version_date:
        raise ValueError(f"Could not determine JensenLab tissues version_date from Last-Modified for {JENSENLAB_TISSUES_URL}")
    version = version_date

    final_dir = dest / source / dataset / version
    final_dir.mkdir(parents=True, exist_ok=True)
    final_path = final_dir / local_path.name
    if final_path != local_path:
        local_path.replace(final_path)
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

    object_prefix = storage_prefix(source, dataset, version)
    storage_uri = s3_uri(bucket, f"{object_prefix}/{final_path.name}") if bucket else None
    manifest = build_source_snapshot_manifest(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        download_date=None,
        homepage=JENSENLAB_HOMEPAGE,
        upstream_urls=[JENSENLAB_TISSUES_URL],
        files=[
            file_entry(
                local_path=final_path,
                source_url=metadata.get("final_url") or JENSENLAB_TISSUES_URL,
                storage_uri=storage_uri,
                content_type=metadata.get("content_type"),
            )
        ],
        extra={
            "version_method": {
                "type": "single_file_last_modified",
                "description": "Use the HTTP Last-Modified header as both version and version_date.",
                "evidence": {
                    "url": metadata.get("final_url") or JENSENLAB_TISSUES_URL,
                    "last_modified": metadata.get("last_modified"),
                },
            }
        },
    )
    manifest_path = final_dir / MANIFEST_FILENAME
    write_manifest(manifest, manifest_path)

    if storage:
        storage.upload_file(final_path, f"{object_prefix}/{final_path.name}", manifest["files"][0]["content_type"])
        storage.upload_file(manifest_path, f"{object_prefix}/{MANIFEST_FILENAME}", "application/x-yaml")

    print(f"Wrote {manifest_path}")
    print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
    if bucket:
        print(f"Uploaded snapshot to s3://{bucket}/{object_prefix}/")
    return manifest_path


def register_diseases(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="jensenlab",
        dataset="diseases",
        urls=JENSENLAB_DISEASE_URLS,
        dest=dest,
        homepage=JENSENLAB_HOMEPAGE,
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use max Last-Modified across JensenLab disease knowledge, experiments, and text-mining files.",
    )


def register_tinx(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="jensenlab",
        dataset="tinx",
        urls=JENSENLAB_TINX_URLS,
        dest=dest,
        homepage=JENSENLAB_HOMEPAGE,
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use max Last-Modified across JensenLab protein and disease text-mining mention files.",
        compress=True,
    )


def register_protein_counts(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="jensenlab",
        dataset="protein_counts",
        urls=JENSENLAB_PROTEIN_COUNTS_URLS,
        dest=dest,
        homepage=JENSENLAB_HOMEPAGE,
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the JensenLab protein_counts.tsv Last-Modified header as version and version_date.",
    )
