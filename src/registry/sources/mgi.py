from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


MGI_HMD_URL = "https://www.informatics.jax.org/downloads/reports/HMD_HumanPhenotype.rpt"
MGI_HOMEPAGE = "https://www.informatics.jax.org/"
MGI_HMD_FILE_NAME = "HMD_HumanPhenotype.rpt"


def fetch_hmd_human_phenotype(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="mgi",
        dataset="hmd_human_phenotype",
        urls=[MGI_HMD_URL],
        dest=dest,
        homepage=MGI_HOMEPAGE,
        timeout=timeout,
        version_description="Use the HTTP Last-Modified date as version and version_date because the source filename is stable across updates.",
    )


def latest_hmd_human_phenotype_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls([MGI_HMD_URL], timeout=timeout)


class MgiHmdHumanPhenotypeFetcher(SourceFunctionFetcher):
    source = "mgi"
    dataset = "hmd_human_phenotype"
    fetch_function = staticmethod(fetch_hmd_human_phenotype)
    latest_version_function = staticmethod(latest_hmd_human_phenotype_version)
