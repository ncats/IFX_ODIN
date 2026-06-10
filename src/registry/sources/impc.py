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


IMPC_URL = "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/results/genotype-phenotype-assertions-IMPC.csv.gz"
IMPC_HOMEPAGE = "https://www.mousephenotype.org/"
IMPC_FILE_NAME = "genotype-phenotype-assertions-IMPC.csv.gz"


def register_genotype_phenotype_assertions(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "impc"
    dataset = "genotype_phenotype_assertions"
    version = IMPC_FILE_NAME
    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(IMPC_URL, work_dir, timeout=timeout)
    version_date = metadata.get("version_date")
    if not version_date:
        raise ValueError(f"Could not determine IMPC version_date from Last-Modified for {IMPC_URL}")

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
        homepage=IMPC_HOMEPAGE,
        upstream_urls=[IMPC_URL],
        files=[file_entry(final_path, metadata.get("final_url") or IMPC_URL, storage_uri, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "single_file_last_modified",
                "description": "Use the source file name as version and the HTTP Last-Modified header as version_date.",
                "evidence": {
                    "url": metadata.get("final_url") or IMPC_URL,
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

