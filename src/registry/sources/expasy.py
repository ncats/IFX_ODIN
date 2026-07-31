from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher, SourceSnapshot
from src.registry.sources.common import (
    fetch_multi_file_last_modified_snapshot,
    latest_version_from_last_modified_urls,
)


EXPASY_ENZYME_URLS = [
    "https://ftp.expasy.org/databases/enzyme/enzclass.txt",
    "https://ftp.expasy.org/databases/enzyme/enzyme.dat",
]


def latest_expasy_enzyme_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(EXPASY_ENZYME_URLS, timeout=timeout)


def fetch_expasy_enzyme(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    return fetch_multi_file_last_modified_snapshot(
        source="expasy",
        dataset="enzyme",
        urls=EXPASY_ENZYME_URLS,
        dest=dest,
        homepage="https://enzyme.expasy.org/",
        timeout=timeout,
        version_description=(
            "Use the max HTTP Last-Modified date across ExPASy enzyme class "
            "and enzyme detail files as version and version_date."
        ),
    )


class ExpasyEnzymeFetcher(SourceFunctionFetcher):
    source = "expasy"
    dataset = "enzyme"
    fetch_function = staticmethod(fetch_expasy_enzyme)
    latest_version_function = staticmethod(latest_expasy_enzyme_version)
