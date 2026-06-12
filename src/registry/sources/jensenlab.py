from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


JENSENLAB_TISSUES_URL = "https://download.jensenlab.org/human_tissue_integrated_full.tsv"
JENSENLAB_HOMEPAGE = "https://jensenlab.org/resources/proteomics/"
JENSENLAB_TISSUES_FILE_NAME = "human_tissue_integrated_full.tsv"
JENSENLAB_DISEASE_URLS = [
    "https://download.jensenlab.org/human_disease_knowledge_filtered.tsv",
    "https://download.jensenlab.org/human_disease_experiments_filtered.tsv",
    "https://download.jensenlab.org/human_disease_textmining_filtered.tsv",
]
JENSENLAB_TINX_URLS = [
    "https://download.jensenlab.org/human_textmining_mentions.tsv",
    "https://download.jensenlab.org/disease_textmining_mentions.tsv",
]
JENSENLAB_PROTEIN_COUNTS_URLS = [
    "https://download.jensenlab.org/protein_counts.tsv",
]


def fetch_tissues(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="jensenlab",
        dataset="tissues",
        urls=[JENSENLAB_TISSUES_URL],
        dest=dest,
        homepage=JENSENLAB_HOMEPAGE,
        timeout=timeout,
        version_description="Use the HTTP Last-Modified header as both version and version_date.",
    )


def fetch_diseases(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="jensenlab",
        dataset="diseases",
        urls=JENSENLAB_DISEASE_URLS,
        dest=dest,
        homepage=JENSENLAB_HOMEPAGE,
        timeout=timeout,
        version_description="Use max Last-Modified across JensenLab disease knowledge, experiments, and text-mining files.",
    )


def fetch_tinx(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="jensenlab",
        dataset="tinx",
        urls=JENSENLAB_TINX_URLS,
        dest=dest,
        homepage=JENSENLAB_HOMEPAGE,
        timeout=timeout,
        version_description="Use max Last-Modified across JensenLab protein and disease text-mining mention files.",
        compress=True,
    )


def fetch_protein_counts(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="jensenlab",
        dataset="protein_counts",
        urls=JENSENLAB_PROTEIN_COUNTS_URLS,
        dest=dest,
        homepage=JENSENLAB_HOMEPAGE,
        timeout=timeout,
        version_description="Use the JensenLab protein_counts.tsv Last-Modified header as version and version_date.",
    )


def latest_tissues_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls([JENSENLAB_TISSUES_URL], timeout=timeout)


def latest_diseases_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(JENSENLAB_DISEASE_URLS, timeout=timeout)


def latest_tinx_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(JENSENLAB_TINX_URLS, timeout=timeout)


def latest_protein_counts_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls(JENSENLAB_PROTEIN_COUNTS_URLS, timeout=timeout)


class JensenlabTissuesFetcher(SourceFunctionFetcher):
    source = "jensenlab"
    dataset = "tissues"
    fetch_function = staticmethod(fetch_tissues)
    latest_version_function = staticmethod(latest_tissues_version)


class JensenlabDiseasesFetcher(SourceFunctionFetcher):
    source = "jensenlab"
    dataset = "diseases"
    fetch_function = staticmethod(fetch_diseases)
    latest_version_function = staticmethod(latest_diseases_version)


class JensenlabTinxFetcher(SourceFunctionFetcher):
    source = "jensenlab"
    dataset = "tinx"
    fetch_function = staticmethod(fetch_tinx)
    latest_version_function = staticmethod(latest_tinx_version)


class JensenlabProteinCountsFetcher(SourceFunctionFetcher):
    source = "jensenlab"
    dataset = "protein_counts"
    fetch_function = staticmethod(fetch_protein_counts)
    latest_version_function = staticmethod(latest_protein_counts_version)
