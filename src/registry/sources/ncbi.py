from pathlib import Path
from typing import Optional

from src.registry.sources.common import register_multi_file_last_modified_snapshot


NCBI_HOMEPAGE = "https://www.ncbi.nlm.nih.gov/gene/"
NCBI_PUBLICATION_URLS = [
    "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene2pubmed.gz",
    "https://ftp.ncbi.nlm.nih.gov/gene/GeneRIF/generifs_basic.gz",
]
NCBI_GENE_SUMMARY_URLS = [
    "https://ftp.ncbi.nlm.nih.gov/gene/DATA/gene_summary.gz",
]


def register_publications(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="ncbi",
        dataset="publications",
        urls=NCBI_PUBLICATION_URLS,
        dest=dest,
        homepage=NCBI_HOMEPAGE,
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use max Last-Modified across NCBI gene2pubmed and GeneRIF files.",
    )


def register_gene_summary(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="ncbi",
        dataset="gene_summary",
        urls=NCBI_GENE_SUMMARY_URLS,
        dest=dest,
        homepage=NCBI_HOMEPAGE,
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the NCBI gene_summary.gz Last-Modified header as version and version_date.",
    )
