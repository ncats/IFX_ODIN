from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


IMPC_URL = "https://ftp.ebi.ac.uk/pub/databases/impc/all-data-releases/latest/results/genotype-phenotype-assertions-IMPC.csv.gz"
IMPC_HOMEPAGE = "https://www.mousephenotype.org/"
IMPC_FILE_NAME = "genotype-phenotype-assertions-IMPC.csv.gz"


def fetch_genotype_phenotype_assertions(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="impc",
        dataset="genotype_phenotype_assertions",
        urls=[IMPC_URL],
        dest=dest,
        homepage=IMPC_HOMEPAGE,
        timeout=timeout,
        version_description="Use the HTTP Last-Modified date as version and version_date because the source URL points at a moving latest release.",
    )


def latest_genotype_phenotype_assertions_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls([IMPC_URL], timeout=timeout)


class ImpcGenotypePhenotypeAssertionsFetcher(SourceFunctionFetcher):
    source = "impc"
    dataset = "genotype_phenotype_assertions"
    fetch_function = staticmethod(fetch_genotype_phenotype_assertions)
    latest_version_function = staticmethod(latest_genotype_phenotype_assertions_version)
