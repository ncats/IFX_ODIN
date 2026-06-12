from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


PUBTATOR_HOMEPAGE = "https://www.ncbi.nlm.nih.gov/research/pubtator/"
PUBTATOR_URLS = [
    "https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/gene2pubtator3.gz",
]


def fetch_gene2pubtator3(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="pubtator",
        dataset="gene2pubtator3",
        urls=PUBTATOR_URLS,
        dest=dest,
        homepage=PUBTATOR_HOMEPAGE,
        timeout=timeout,
        version_description="Use the gene2pubtator3.gz Last-Modified header as version and version_date.",
    )


def latest_gene2pubtator3_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(PUBTATOR_URLS, timeout=timeout)


class PubtatorGene2Pubtator3Fetcher(SourceFunctionFetcher):
    source = "pubtator"
    dataset = "gene2pubtator3"
    fetch_function = staticmethod(fetch_gene2pubtator3)
    latest_version_function = staticmethod(latest_gene2pubtator3_version)
