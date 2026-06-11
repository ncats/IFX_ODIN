from pathlib import Path

from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.common import latest_version_from_last_modified_urls, fetch_multi_file_last_modified_snapshot


HCOP_URL = "https://storage.googleapis.com/public-download-files/hcop/human_all_hcop_sixteen_column.txt.gz"
HCOP_HOMEPAGE = "https://www.genenames.org/tools/hcop/"
HCOP_FILE_NAME = "human_all_hcop_sixteen_column.txt.gz"


def fetch_human_all_sixteen_column(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    return fetch_multi_file_last_modified_snapshot(
        source="hcop",
        dataset="human_all_sixteen_column",
        urls=[HCOP_URL],
        dest=dest,
        homepage=HCOP_HOMEPAGE,
        timeout=timeout,
        version_description="Use the HTTP Last-Modified date as version and version_date because the source filename is stable across updates.",
    )


def latest_human_all_sixteen_column_version(timeout: int = 60) -> str:
    return latest_version_from_last_modified_urls([HCOP_URL], timeout=timeout)


class HcopHumanAllSixteenColumnFetcher(SourceFunctionFetcher):
    source = "hcop"
    dataset = "human_all_sixteen_column"
    fetch_function = staticmethod(fetch_human_all_sixteen_column)
    latest_version_function = staticmethod(latest_human_all_sixteen_column_version)
