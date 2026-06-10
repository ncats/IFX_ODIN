import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from src.registry.download import download_url, http_metadata
from src.registry.manifest import (
    MANIFEST_FILENAME,
    build_source_snapshot_manifest,
    file_entry,
    manifest_checksum,
    parse_http_date_to_iso,
    storage_prefix,
    today_utc,
    write_manifest,
)
from src.registry.sources.common import register_multi_file_last_modified_snapshot
from src.registry.storage import MinioStorage, load_minio_credentials, s3_uri


def _upload_snapshot(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    homepage: str,
    urls: List[str],
    downloaded: List[Tuple[Path, Dict[str, Optional[str]], str]],
    dest: Path,
    minio_credentials: Optional[Path],
    upload: bool,
    version_method: Dict,
) -> Path:
    final_dir = dest / source / dataset / version
    final_dir.mkdir(parents=True, exist_ok=True)

    moved = []
    for local_path, metadata, url in downloaded:
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


def _download_stream_without_head(
    url: str,
    dest_dir: Path,
    file_name: str,
    timeout: int,
) -> Tuple[Path, Dict[str, Optional[str]]]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / file_name
    with requests.get(url, stream=True, allow_redirects=True, timeout=timeout) as response:
        response.raise_for_status()
        with dest_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        return dest_path, {
            "last_modified": response.headers.get("Last-Modified"),
            "version_date": parse_http_date_to_iso(response.headers.get("Last-Modified")),
            "content_type": response.headers.get("Content-Type"),
            "content_length": response.headers.get("Content-Length"),
            "final_url": response.url,
        }


def register_iuphar(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="iuphar",
        dataset="ligands_interactions",
        urls=[
            "https://www.guidetopharmacology.org/DATA/ligands.csv",
            "https://www.guidetopharmacology.org/DATA/interactions.csv",
        ],
        dest=dest,
        homepage="https://www.guidetopharmacology.org/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use max Last-Modified across IUPHAR ligands and interactions CSV files.",
    )


def register_uniprot(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "uniprot"
    dataset = "human"
    urls = [
        "https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=(*)+AND+(model_organism:9606)",
        "https://rest.uniprot.org/uniprotkb/stream?compressed=true&format=json&query=(reviewed:true)+AND+(model_organism:9606)",
    ]
    release_probe_url = "https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=json&size=1&query=accession:P04637"
    probe = requests.head(release_probe_url, timeout=timeout)
    probe.raise_for_status()
    version = probe.headers.get("x-uniprot-release")
    version_date = probe.headers.get("x-uniprot-release-date")
    if not version:
        raise ValueError("UniProt response did not include x-uniprot-release")

    work_dir = dest / source / dataset / "pending"
    downloaded = [
        (*_download_stream_without_head(urls[0], work_dir, "uniprot-human.json.gz", timeout), urls[0]),
        (*_download_stream_without_head(urls[1], work_dir, "uniprot-human-reviewed.json.gz", timeout), urls[1]),
    ]
    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://www.uniprot.org/",
        urls=[*urls, release_probe_url],
        downloaded=downloaded,
        dest=dest,
        minio_credentials=minio_credentials,
        upload=upload,
        version_method={
            "type": "uniprot_release_headers",
            "description": "Use x-uniprot-release and x-uniprot-release-date headers from a small UniProt accession probe.",
            "evidence": {
                "probe_url": release_probe_url,
                "x_uniprot_release": version,
                "x_uniprot_release_date": version_date,
            },
        },
    )


def register_bioplex(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="bioplex",
        dataset="ppi",
        urls=[
            "https://bioplex.hms.harvard.edu/data/BioPlex_293T_Network_10K_Dec_2019.tsv",
            "https://bioplex.hms.harvard.edu/data/BioPlex_HCT116_Network_5.5K_Dec_2019.tsv",
        ],
        dest=dest,
        homepage="https://bioplex.hms.harvard.edu/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version="3.0",
        version_description="Use BioPlex 3.0 from source filenames as version and max Last-Modified across 293T/HCT116 files as version_date.",
    )


def register_string(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="string",
        dataset="protein_links_human",
        urls=["https://stringdb-downloads.org/download/protein.links.v12.0/9606.protein.links.v12.0.txt.gz"],
        dest=dest,
        homepage="https://string-db.org/",
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version="12.0",
        version_description="Use STRING version 12.0 from source URL as version and file Last-Modified as version_date.",
    )


def register_gtex(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "gtex"
    dataset = "expression_v11"
    version = "GTEx Analysis Version 11"
    version_date = "2025-08-22"
    urls = [
        "https://storage.googleapis.com/adult-gtex/bulk-gex/v11/rna-seq/GTEx_Analysis_2025-08-22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz",
        "https://storage.googleapis.com/adult-gtex/annotations/v11/metadata-files/GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt",
        "https://storage.googleapis.com/adult-gtex/annotations/v11/metadata-files/GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt",
    ]
    names = [
        "GTEx_Analysis_2025_08_22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz",
        "GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt",
        "GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt",
    ]
    work_dir = dest / source / dataset / "pending"
    downloaded = [
        (*download_url(url, work_dir, timeout=timeout, file_name=name), url)
        for url, name in zip(urls, names)
    ]
    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version="v11",
        version_date=version_date,
        homepage="https://gtexportal.org/",
        urls=urls,
        downloaded=downloaded,
        dest=dest,
        minio_credentials=minio_credentials,
        upload=upload,
        version_method={
            "type": "explicit_gtex_v11_release",
            "description": "Use explicit GTEx Analysis Version 11 and release date from the existing workflow.",
            "evidence": {"version": version, "version_date": version_date},
        },
    )


def register_hpa(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "hpa"
    dataset = "tissue_expression"
    urls = [
        "https://www.proteinatlas.org/download/tsv/normal_ihc_data.tsv.zip",
        "https://www.proteinatlas.org/download/tsv/rna_tissue_hpa.tsv.zip",
    ]
    about_url = "https://www.proteinatlas.org/about/download"
    about = requests.get(about_url, timeout=timeout)
    about.raise_for_status()
    match = re.search(r"version ([\d.]+)", about.text)
    version = match.group(1) if match else None
    if not version:
        raise ValueError("Could not parse HPA version from download page")
    rna_metadata = http_metadata(urls[1], timeout=timeout)
    version_date = rna_metadata.get("version_date")
    if not version_date:
        raise ValueError("Could not determine HPA version_date from RNA file Last-Modified")

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(url, work_dir, timeout=timeout), url) for url in urls]
    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://www.proteinatlas.org/",
        urls=[*urls, about_url],
        downloaded=downloaded,
        dest=dest,
        minio_credentials=minio_credentials,
        upload=upload,
        version_method={
            "type": "hpa_download_page_version",
            "description": "Parse HPA version from download page and use RNA tissue file Last-Modified as version_date.",
            "evidence": {"about_url": about_url, "rna_last_modified": rna_metadata.get("last_modified")},
        },
    )


def register_surechembl_patent_discovery(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    source = "surechembl"
    dataset = "patent_discovery"
    bulk_data_url = "https://ftp.ebi.ac.uk/pub/databases/chembl/SureChEMBL/bulk_data/"
    listing = requests.get(bulk_data_url, timeout=timeout)
    listing.raise_for_status()
    versions = sorted(set(re.findall(r'href="([0-9]{4}-[0-9]{2}-[0-9]{2})/"', listing.text)))
    if not versions:
        raise ValueError("Could not find dated SureChEMBL bulk_data release directories")
    version = versions[-1]
    version_date = version
    base_url = f"{bulk_data_url}{version}"
    names = [
        "patents.parquet",
        "biomedical_entities.parquet",
        "biomedical_locations.parquet",
        "biomedical_types.parquet",
        "fields.parquet",
    ]
    urls = [f"{base_url}/{name}" for name in names]

    work_dir = dest / source / dataset / "pending"
    downloaded = [
        (*download_url(url, work_dir, timeout=timeout, file_name=name), url)
        for url, name in zip(urls, names)
    ]
    return _upload_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://chembl.gitbook.io/surechembl/",
        urls=urls,
        downloaded=downloaded,
        dest=dest,
        minio_credentials=minio_credentials,
        upload=upload,
        version_method={
            "type": "latest_dated_directory",
            "description": "Parse dated SureChEMBL bulk_data release directories and use the newest YYYY-MM-DD directory as version.",
            "evidence": {"bulk_data_url": bulk_data_url, "available_versions": versions[-5:]},
        },
    )
