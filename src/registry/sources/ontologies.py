from pathlib import Path
from typing import Optional
import json

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


def register_uberon(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="uberon",
        dataset="ontology",
        urls=["http://purl.obolibrary.org/obo/uberon.obo"],
        dest=dest,
        homepage="https://obofoundry.org/ontology/uberon.html",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the Uberon OBO HTTP Last-Modified header as version and version_date.",
    )


def register_go_basic(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="go",
        dataset="ontology",
        urls=["https://current.geneontology.org/ontology/go-basic.json"],
        dest=dest,
        homepage="https://geneontology.org/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the GO go-basic.json HTTP Last-Modified header as version and version_date.",
    )


def register_goa_human_uniprot(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="go",
        dataset="goa_human_uniprot",
        urls=["https://ftp.ebi.ac.uk/pub/databases/GO/goa/HUMAN/goa_human.gaf.gz"],
        dest=dest,
        homepage="https://geneontology.org/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the EBI GOA human GAF HTTP Last-Modified header as version and version_date.",
    )


def register_goa_human_go(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="go",
        dataset="goa_human_go",
        urls=["https://current.geneontology.org/annotations/goa_human.gaf.gz"],
        dest=dest,
        homepage="https://geneontology.org/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the GO-hosted human GAF HTTP Last-Modified header as version and version_date.",
    )


def register_mondo(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="mondo",
        dataset="ontology",
        urls=["https://purl.obolibrary.org/obo/mondo.json"],
        dest=dest,
        homepage="https://mondo.monarchinitiative.org/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the MONDO JSON HTTP Last-Modified header as version and version_date.",
    )


def register_disease_ontology(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "disease_ontology"
    dataset = "ontology"
    url = "https://purl.obolibrary.org/obo/doid.json"
    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(url, work_dir, timeout=timeout)

    with local_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    graph_meta = ((payload.get("graphs") or [{}])[0].get("meta") or {})
    basic_values = graph_meta.get("basicPropertyValues") or []
    version = None
    for entry in basic_values:
        if entry.get("pred") == "http://www.w3.org/2002/07/owl#versionInfo":
            version = entry.get("val")
            break
    if not version:
        version_uri = graph_meta.get("version")
        if version_uri:
            version = version_uri.rstrip("/").rsplit("/", 2)[-2]
    if not version:
        raise ValueError("Could not determine Disease Ontology version from doid.json metadata")

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
        version_date=version,
        download_date=None,
        homepage="https://disease-ontology.org/",
        upstream_urls=[url],
        files=[file_entry(final_path, metadata.get("final_url") or url, storage_uri, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "owl_version_info",
                "description": "Use owl#versionInfo embedded in doid.json as version and version_date.",
                "evidence": {
                    "url": metadata.get("final_url") or url,
                    "graph_version": graph_meta.get("version"),
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
