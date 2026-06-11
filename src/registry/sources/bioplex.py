from pathlib import Path
import re

import requests

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import fetch_multi_file_last_modified_snapshot


BIOPLEX_HOMEPAGE = "https://bioplex.hms.harvard.edu/"
BIOPLEX_URLS = [
    "https://bioplex.hms.harvard.edu/data/BioPlex_293T_Network_10K_Dec_2019.tsv",
    "https://bioplex.hms.harvard.edu/data/BioPlex_HCT116_Network_5.5K_Dec_2019.tsv",
]


def latest_bioplex_version(timeout: int = 60) -> str:
    response = requests.get(BIOPLEX_HOMEPAGE, timeout=timeout)
    response.raise_for_status()
    versions = [
        tuple(int(part) for part in match.split("."))
        for match in re.findall(r"BioPlex\s+(\d+(?:\.\d+)+)", response.text, flags=re.IGNORECASE)
    ]
    if not versions:
        raise ValueError("Could not parse latest BioPlex version from BioPlex homepage")
    return ".".join(str(part) for part in sorted(versions)[-1])


def fetch_bioplex(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="bioplex",
        dataset="ppi",
        urls=BIOPLEX_URLS,
        dest=dest,
        homepage=BIOPLEX_HOMEPAGE,
        timeout=timeout,
        version=latest_bioplex_version(timeout=timeout),
        version_description="Parse the BioPlex homepage release text as version and use max Last-Modified across 293T/HCT116 files as version_date.",
    )


class BioplexPpiFetcher(SourceFunctionFetcher):
    source = "bioplex"
    dataset = "ppi"
    fetch_function = staticmethod(fetch_bioplex)
    latest_version_function = staticmethod(latest_bioplex_version)
