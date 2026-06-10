from pathlib import Path
from typing import Optional

from src.registry.sources.common import register_multi_file_last_modified_snapshot


PUBTATOR_HOMEPAGE = "https://www.ncbi.nlm.nih.gov/research/pubtator/"
PUBTATOR_URLS = [
    "https://ftp.ncbi.nlm.nih.gov/pub/lu/PubTator3/gene2pubtator3.gz",
]


def register_gene2pubtator3(
    *,
    dest: Path,
    minio_credentials: Optional[Path] = None,
    upload: bool = False,
    timeout: int = 60,
) -> Path:
    return register_multi_file_last_modified_snapshot(
        source="pubtator",
        dataset="gene2pubtator3",
        urls=PUBTATOR_URLS,
        dest=dest,
        homepage=PUBTATOR_HOMEPAGE,
        minio_credentials=minio_credentials,
        upload=upload,
        timeout=timeout,
        version_description="Use the gene2pubtator3.gz Last-Modified header as version and version_date.",
    )
