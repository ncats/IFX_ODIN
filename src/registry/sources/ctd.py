import gzip
import re
from datetime import datetime
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


CTD_CURATED_GENES_DISEASES_URL = "https://ctdbase.org/reports/CTD_curated_genes_diseases.tsv.gz"
CTD_HOMEPAGE = "https://ctdbase.org/"
CTD_REPORT_CREATED_RE = re.compile(
    r"([A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2} [A-Z]{3,4} \d{4})$"
)


def extract_report_created(path: Path) -> tuple[str, str]:
    report_created = None
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith("# Report created:"):
                report_created = line.split(":", 1)[1].strip()
                break
            if not line.startswith("#"):
                break

    if not report_created:
        raise ValueError(f"Could not find CTD report creation date in {path}")

    match = CTD_REPORT_CREATED_RE.search(report_created)
    if match is None:
        raise ValueError(f"Could not parse CTD report creation date: {report_created}")

    version_date = datetime.strptime(match.group(1), "%a %b %d %H:%M:%S %Z %Y").date().isoformat()
    return report_created, version_date


def register_curated_genes_diseases(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "ctd"
    dataset = "curated_genes_diseases"
    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(CTD_CURATED_GENES_DISEASES_URL, work_dir, timeout=timeout)
    report_created, version_date = extract_report_created(local_path)
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
        homepage=CTD_HOMEPAGE,
        upstream_urls=[CTD_CURATED_GENES_DISEASES_URL],
        files=[
            file_entry(
                local_path=final_path,
                source_url=metadata.get("final_url") or CTD_CURATED_GENES_DISEASES_URL,
                storage_uri=storage_uri,
                content_type=metadata.get("content_type"),
            )
        ],
        extra={
            "version_method": {
                "type": "downloaded_file_header",
                "description": "Parse '# Report created:' from the CTD curated genes-diseases gzip header.",
                "evidence": {
                    "file": final_path.name,
                    "report_created": report_created,
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

