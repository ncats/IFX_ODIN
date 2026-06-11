import gzip
import re
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.registry.download import download_url
from src.registry.fetchers import SnapshotFile, SourceFetcher, SourceSnapshot


CTD_CURATED_GENES_DISEASES_URL = "https://ctdbase.org/reports/CTD_curated_genes_diseases.tsv.gz"
CTD_HOMEPAGE = "https://ctdbase.org/"
CTD_REPORT_CREATED_RE = re.compile(
    r"([A-Z][a-z]{2} [A-Z][a-z]{2} \d{2} \d{2}:\d{2}:\d{2} [A-Z]{3,4} \d{4})$"
)


def extract_report_created(path: Path) -> tuple[str, str]:
    report_created = None
    with gzip.open(path, "rt", encoding="utf-8", errors="replace") as handle:
        for line in handle:
            if line.startswith("# Report created:"):
                report_created = line.split(":", 1)[1].strip()
                break
            if not line.startswith("#"):
                break

    if not report_created:
        raise ValueError(f"Could not find CTD report creation date in {path}")

    match = CTD_REPORT_CREATED_RE.search(report_created)
    if match is None:
        raise ValueError(f"Could not parse CTD report creation date: {report_created}")

    version_date = datetime.strptime(match.group(1), "%a %b %d %H:%M:%S %Z %Y").date().isoformat()
    return report_created, version_date


def fetch_curated_genes_diseases(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    source = "ctd"
    dataset = "curated_genes_diseases"
    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(CTD_CURATED_GENES_DISEASES_URL, work_dir, timeout=timeout)
    report_created, version_date = extract_report_created(local_path)
    version = version_date

    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage=CTD_HOMEPAGE,
        upstream_urls=[CTD_CURATED_GENES_DISEASES_URL],
        files=[SnapshotFile(local_path, metadata.get("final_url") or CTD_CURATED_GENES_DISEASES_URL, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "downloaded_file_header",
                "description": "Parse '# Report created:' from the CTD curated genes-diseases gzip header.",
                "evidence": {
                    "file": local_path.name,
                    "report_created": report_created,
                },
            }
        },
    )


class CtdCuratedGenesDiseasesFetcher(SourceFetcher):
    source = "ctd"
    dataset = "curated_genes_diseases"

    def fetch(
        self,
        *,
        dest: Path,
        timeout: int = 60,
    ) -> SourceSnapshot:
        return fetch_curated_genes_diseases(
            dest=dest,
            timeout=timeout,
        )

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        with tempfile.TemporaryDirectory() as temp_dir:
            local_path, _ = download_url(CTD_CURATED_GENES_DISEASES_URL, Path(temp_dir), timeout=timeout, verbose=False)
            _, version = extract_report_created(local_path)
            return version
