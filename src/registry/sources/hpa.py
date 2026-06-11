import re
from pathlib import Path

import requests

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.download import download_url, http_metadata
from src.registry.sources.snapshot_helpers import build_downloaded_snapshot


HPA_URLS = [
    "https://www.proteinatlas.org/download/tsv/normal_ihc_data.tsv.zip",
    "https://www.proteinatlas.org/download/tsv/rna_tissue_hpa.tsv.zip",
]
HPA_ABOUT_URL = "https://www.proteinatlas.org/about/download"


def latest_hpa_tissue_expression_version(timeout: int = 60) -> str:
    about = requests.get(HPA_ABOUT_URL, timeout=timeout)
    about.raise_for_status()
    match = re.search(r"version ([\d.]+)", about.text)
    version = match.group(1).rstrip(".") if match else None
    if not version:
        raise ValueError("Could not parse HPA version from download page")
    return version


def fetch_hpa(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    source = "hpa"
    dataset = "tissue_expression"
    urls = HPA_URLS
    about_url = HPA_ABOUT_URL
    version = latest_hpa_tissue_expression_version(timeout=timeout)
    rna_metadata = http_metadata(urls[1], timeout=timeout)
    version_date = rna_metadata.get("version_date")
    if not version_date:
        raise ValueError("Could not determine HPA version_date from RNA file Last-Modified")

    work_dir = dest / source / dataset / "pending"
    downloaded = [(*download_url(url, work_dir, timeout=timeout), url) for url in urls]
    return build_downloaded_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://www.proteinatlas.org/",
        urls=[*urls, about_url],
        downloaded=downloaded,
        dest=dest,
        version_method={
            "type": "hpa_download_page_version",
            "description": "Parse HPA version from download page and use RNA tissue file Last-Modified as version_date.",
            "evidence": {"about_url": about_url, "rna_last_modified": rna_metadata.get("last_modified")},
        },
    )


class HpaTissueExpressionFetcher(SourceFunctionFetcher):
    source = "hpa"
    dataset = "tissue_expression"
    fetch_function = staticmethod(fetch_hpa)
    latest_version_function = staticmethod(latest_hpa_tissue_expression_version)
