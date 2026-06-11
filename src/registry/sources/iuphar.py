from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


IUPHAR_URLS = [
    "https://www.guidetopharmacology.org/DATA/ligands.csv",
    "https://www.guidetopharmacology.org/DATA/interactions.csv",
]


def fetch_iuphar(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="iuphar",
        dataset="ligands_interactions",
        urls=IUPHAR_URLS,
        dest=dest,
        homepage="https://www.guidetopharmacology.org/",
        timeout=timeout,
        version_description="Use max Last-Modified across IUPHAR ligands and interactions CSV files.",
    )


def latest_iuphar_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(IUPHAR_URLS, timeout=timeout)


class IupharLigandInteractionsFetcher(SourceFunctionFetcher):
    source = "iuphar"
    dataset = "ligands_interactions"
    fetch_function = staticmethod(fetch_iuphar)
    latest_version_function = staticmethod(latest_iuphar_version)
