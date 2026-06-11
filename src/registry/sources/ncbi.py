from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


NCBI_HOMEPAGE = "https://www.ncbi.nlm.nih.gov/gene/"
NCBI_PUBLICATION_URLS = [
    "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2pubmed.gz",
    "https://ftp.ncbi.nlm.nih.gov/gene/GeneRIF/generifs_basic.gz",
]
NCBI_GENE_SUMMARY_URLS = [
    "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_summary.gz",
]


def fetch_publications(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="ncbi",
        dataset="publications",
        urls=NCBI_PUBLICATION_URLS,
        dest=dest,
        homepage=NCBI_HOMEPAGE,
        timeout=timeout,
        version_description="Use max Last-Modified across NCBI gene2pubmed and GeneRIF files.",
    )


def fetch_gene_summary(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="ncbi",
        dataset="gene_summary",
        urls=NCBI_GENE_SUMMARY_URLS,
        dest=dest,
        homepage=NCBI_HOMEPAGE,
        timeout=timeout,
        version_description="Use the NCBI gene_summary.gz Last-Modified header as version and version_date.",
    )


def latest_publications_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(NCBI_PUBLICATION_URLS, timeout=timeout)


def latest_gene_summary_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(NCBI_GENE_SUMMARY_URLS, timeout=timeout)


class NcbiPublicationsFetcher(SourceFunctionFetcher):
    source = "ncbi"
    dataset = "publications"
    fetch_function = staticmethod(fetch_publications)
    latest_version_function = staticmethod(latest_publications_version)


class NcbiGeneSummaryFetcher(SourceFunctionFetcher):
    source = "ncbi"
    dataset = "gene_summary"
    fetch_function = staticmethod(fetch_gene_summary)
    latest_version_function = staticmethod(latest_gene_summary_version)
