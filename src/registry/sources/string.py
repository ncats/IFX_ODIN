from pathlib import Path
import re

import requests

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import fetch_multi_file_last_modified_snapshot


STRING_DOWNLOAD_INDEX = "https://stringdb-downloads.org/download/"


def latest_string_version(timeout: int = 60) -> str:
    response = requests.get(STRING_DOWNLOAD_INDEX, timeout=timeout)
    response.raise_for_status()
    versions = [
        tuple(int(part) for part in match.split("."))
        for match in re.findall(r"protein\.links\.v(\d+(?:\.\d+)*)/", response.text)
    ]
    if not versions:
        raise ValueError("Could not parse latest STRING protein.links version from download index")
    return ".".join(str(part) for part in sorted(set(versions))[-1])


def fetch_string(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    version = latest_string_version(timeout=timeout)
    return fetch_multi_file_last_modified_snapshot(
        source="string",
        dataset="protein_links_human",
        urls=[f"https://stringdb-downloads.org/download/protein.links.v{version}/9606.protein.links.v{version}.txt.gz"],
        dest=dest,
        homepage="https://string-db.org/",
        timeout=timeout,
        version=version,
        version_description="Parse the latest protein.links version from the STRING download index as version and use file Last-Modified as version_date.",
    )


class StringHumanProteinLinksFetcher(SourceFunctionFetcher):
    source = "string"
    dataset = "protein_links_human"
    fetch_function = staticmethod(fetch_string)
    latest_version_function = staticmethod(latest_string_version)
