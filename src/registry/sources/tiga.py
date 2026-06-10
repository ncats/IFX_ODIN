import re
from pathlib import Path
from typing import Optional

import requests

from src.registry.sources.common import register_multi_file_last_modified_snapshot


TIGA_BASE_URL = "https://unmtid-dbs.net/download/TIGA"
TIGA_HOMEPAGE = "https://unmtid-shinyapps.net/tiga/"
TIGA_URLS = [
    f"{TIGA_BASE_URL}/latest/tiga_gene-trait_stats.tsv",
    f"{TIGA_BASE_URL}/latest/tiga_gene-trait_provenance.tsv",
]


def latest_tiga_version(timeout: int = 60) -> Optional[str]:
    response = requests.get(f"{TIGA_BASE_URL}/", timeout=timeout)
    response.raise_for_status()
    versions = re.findall(r'href="([0-9]{8})/"', response.text)
    return sorted(versions)[-1] if versions else None


def register_gene_trait(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="tiga",
        dataset="gene_trait",
        urls=TIGA_URLS,
        dest=dest,
        homepage=TIGA_HOMEPAGE,
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version=latest_tiga_version(timeout=timeout),
        version_description=(
            "Use the latest YYYYMMDD directory listed at the TIGA download root as version, "
            "and max Last-Modified across latest stats/provenance files as version_date."
        ),
    )
