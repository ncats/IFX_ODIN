import re
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

from src.registry.download import download_url
from src.registry.manifest import (
    MANIFEST_FILENAME,
    build_source_snapshot_manifest,
    file_entry,
    manifest_checksum,
    parse_http_date_to_iso,
    storage_prefix,
    write_manifest,
)
from src.registry.sources.common import register_multi_file_last_modified_snapshot
from src.registry.storage import MinioStorage, load_minio_credentials, s3_uri


def _upload_manifest_snapshot(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    homepage: str,
    urls: List[str],
    files: List[tuple[Path, Dict[str, Optional[str]], str]],
    dest: Path,
    minio_credentials: Optional[Path],
    upload: bool,
    version_method: Dict,
) -> Path:
    final_dir = dest / source / dataset / version
    final_dir.mkdir(parents=True, exist_ok=True)

    moved = []
    for local_path, metadata, url in files:
        final_path = final_dir / local_path.name
        if final_path != local_path:
            local_path.replace(final_path)
        moved.append((final_path, metadata, url))

    storage = None
    bucket = None
    if upload:
        if not minio_credentials:
            raise ValueError("minio_credentials is required when upload=True")
        storage = MinioStorage(load_minio_credentials(minio_credentials))
        bucket = storage.bucket

    object_prefix = storage_prefix(source, dataset, version)
    file_entries = []
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

    manifest = build_source_snapshot_manifest(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        download_date=None,
        homepage=homepage,
        upstream_urls=urls,
        files=file_entries,
        extra={"version_method": version_method},
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


def register_reactome(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "reactome"
    dataset = "pathways"
    urls = [
        "https://reactome.org/download/current/ReactomePathways.gmt.zip",
        "https://reactome.org/download/current/ReactomePathwaysRelation.txt",
        "https://reactome.org/download/current/UniProt2Reactome_All_Levels.txt",
        "https://reactome.org/download/current/interactors/reactome.homo_sapiens.interactions.tab-delimited.txt",
    ]
    version_url = "https://reactome.org/ContentService/data/database/version"
    version_response = requests.get(version_url, timeout=timeout)
    version_response.raise_for_status()
    version = version_response.text.strip()

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(url, work_dir, timeout=timeout), url) for url in urls]
    interactor_metadata = downloaded[-1][1]
    version_date = interactor_metadata.get("version_date")
    if not version_date:
        raise ValueError("Could not determine Reactome version_date from interactor Last-Modified header")

    return _upload_manifest_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://reactome.org/",
        urls=[*urls, version_url],
        files=downloaded,
        dest=dest,
        minio_credentials=minio_credentials,
        upload=upload,
        version_method={
            "type": "reactome_database_version",
            "description": "Use Reactome ContentService database version as version and interactor file Last-Modified as version_date.",
            "evidence": {
                "version_url": version_url,
                "interactor_last_modified": interactor_metadata.get("last_modified"),
            },
        },
    )


def register_pathwaycommons(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "pathwaycommons"
    dataset = "pc_hgnc"
    file_url = "https://download.baderlab.org/PathwayCommons/PC2/v14/pc-hgnc.gmt.gz"
    datasources_url = "https://download.baderlab.org/PathwayCommons/PC2/v14/datasources.txt"
    response = requests.get(datasources_url, timeout=timeout)
    response.raise_for_status()
    match = re.search(r"PC version (\d+) (\d+ \w+ \d+)", response.text)
    if not match:
        raise ValueError("Could not parse PathwayCommons version from datasources.txt")
    version = match.group(1)
    version_date = datetime.strptime(match.group(2), "%d %b %Y").date().isoformat()

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(file_url, work_dir, timeout=timeout), file_url)]
    return _upload_manifest_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://www.pathwaycommons.org/",
        urls=[file_url, datasources_url],
        files=downloaded,
        dest=dest,
        minio_credentials=minio_credentials,
        upload=upload,
        version_method={
            "type": "pathwaycommons_datasources_txt",
            "description": "Parse PC version and release date from datasources.txt.",
            "evidence": {"datasources_url": datasources_url, "matched_text": match.group(0)},
        },
    )


def register_panther_classes(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="panther",
        dataset="protein_classes",
        urls=[
            "https://data.pantherdb.org/PANTHER19.0/ontology/Protein_Class_19.0",
            "https://data.pantherdb.org/PANTHER19.0/ontology/Protein_class_relationship",
            "https://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/PTHR19.0_human",
        ],
        dest=dest,
        homepage="https://pantherdb.org/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version="19.0",
        version_description="Use PANTHER release 19.0 from source URLs as version and max Last-Modified across class, relationship, and human sequence-classification files as version_date.",
    )


def register_wikipathways(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "wikipathways"
    dataset = "human_gmt"
    listing_url = "https://data.wikipathways.org/current/gmt/"
    response = requests.get(listing_url, timeout=timeout)
    response.raise_for_status()
    match = re.search(r"(wikipathways-(\d{4})(\d{2})(\d{2})-gmt-Homo_sapiens\.gmt)", response.text)
    if not match:
        raise ValueError("Could not find WikiPathways Homo sapiens GMT file")
    file_name, year, month, day = match.groups()
    file_url = f"{listing_url}{file_name}"
    version = f"{year}-{month}-{day}"

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(file_url, work_dir, timeout=timeout), file_url)]
    return _upload_manifest_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version,
        homepage="https://www.wikipathways.org/",
        urls=[listing_url, file_url],
        files=downloaded,
        dest=dest,
        minio_credentials=minio_credentials,
        upload=upload,
        version_method={
            "type": "wikipathways_filename_date",
            "description": "Scrape current GMT directory and use the YYYYMMDD date embedded in the Homo sapiens GMT filename.",
            "evidence": {"listing_url": listing_url, "file_name": file_name},
        },
    )
