from pathlib import Path

from src.registry.download import download_url
from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.snapshot_helpers import build_downloaded_snapshot


GTEX_VERSION = "v11"
GTEX_VERSION_DATE = "2025-08-22"
GTEX_URLS = [
    "https://storage.googleapis.com/adult-gtex/bulk-gex/v11/rna-seq/GTEx_Analysis_2025-08-22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz",
    "https://storage.googleapis.com/adult-gtex/annotations/v11/metadata-files/GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt",
    "https://storage.googleapis.com/adult-gtex/annotations/v11/metadata-files/GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt",
]
GTEX_LOCAL_NAMES = [
    "GTEx_Analysis_2025_08_22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz",
    "GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt",
    "GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt",
]


def latest_gtex_version(timeout: int = 60) -> str:
    return GTEX_VERSION


def fetch_gtex(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    source = "gtex"
    dataset = "expression_v11"
    version = latest_gtex_version(timeout=timeout)
    version_date = GTEX_VERSION_DATE
    work_dir = dest / source / dataset / "pending"
    downloaded = [
        (*download_url(url, work_dir, timeout=timeout, file_name=name), url)
        for url, name in zip(GTEX_URLS, GTEX_LOCAL_NAMES)
    ]
    return build_downloaded_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage="https://gtexportal.org/",
        urls=GTEX_URLS,
        downloaded=downloaded,
        dest=dest,
        version_method={
            "type": "manual_gtex_release",
            "description": "GTEx source selection is managed manually because release selection may require human review across portal datasets.",
            "evidence": {"version": version, "version_date": version_date},
        },
    )


class GtexTissueExpressionFetcher(SourceFunctionFetcher):
    source = "gtex"
    dataset = "expression_v11"
    fetch_function = staticmethod(fetch_gtex)
    latest_version_function = staticmethod(latest_gtex_version)
