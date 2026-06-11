import re
from pathlib import Path

import requests

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.download import download_url
from src.registry.sources.snapshot_helpers import build_downloaded_snapshot


SURECHEMBL_BULK_DATA_URL = "https://ftp.ebi.ac.uk/pub/databases/chembl/SureChEMBL/bulk_data/"


def latest_surechembl_patent_discovery_version(timeout: int = 60) -> str:
    listing = requests.get(SURECHEMBL_BULK_DATA_URL, timeout=timeout)
    listing.raise_for_status()
    versions = sorted(set(re.findall(r'href="([0-9]{4}-[0-9]{2}-[0-9]{2})/"', listing.text)))
    if not versions:
        raise ValueError("Could not find dated SureChEMBL bulk_data release directories")
    return versions[-1]


def fetch_surechembl_patent_discovery(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    source = "surechembl"
    dataset = "patent_discovery"
    bulk_data_url = SURECHEMBL_BULK_DATA_URL
    version = latest_surechembl_patent_discovery_version(timeout=timeout)
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
    return build_downloaded_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://chembl.gitbook.io/surechembl/",
        urls=urls,
        downloaded=downloaded,
        dest=dest,
        version_method={
            "type": "latest_dated_directory",
            "description": "Parse dated SureChEMBL bulk_data release directories and use the newest YYYY-MM-DD directory as version.",
            "evidence": {"bulk_data_url": bulk_data_url, "version": version},
        },
    )


class SurechemblPatentDiscoveryFetcher(SourceFunctionFetcher):
    source = "surechembl"
    dataset = "patent_discovery"
    fetch_function = staticmethod(fetch_surechembl_patent_discovery)
    latest_version_function = staticmethod(latest_surechembl_patent_discovery_version)
