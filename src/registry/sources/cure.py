import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlencode

import requests

from src.registry.fetchers import SnapshotFile, SourceFetcher, SourceSnapshot


CURE_REPORTS_URL = "https://cure-api.ncats.io/v2/reports"
CURE_HOMEPAGE = "https://cure.ncats.io/"
CURE_REPORTS_LIMIT = 100
CURE_REPORTS_SORT = "latest"
USER_AGENT = "IFX_ODIN/ifx-registry-cure-fetcher"


def _utc_timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _version_date_from_timestamp(timestamp: str) -> str:
    return datetime.strptime(timestamp[:8], "%Y%m%d").date().isoformat()


def _local_file_mtime_date(source_file: Path) -> str:
    return datetime.fromtimestamp(source_file.stat().st_mtime).date().isoformat()


def _copy_manual_file(source_file: Path, dest: Path) -> Path:
    if not source_file.exists():
        raise FileNotFoundError(source_file)
    dest.mkdir(parents=True, exist_ok=True)
    final_path = dest / source_file.name
    if source_file.resolve() != final_path.resolve():
        shutil.copy2(source_file, final_path)
    return final_path


def _build_reports_url() -> str:
    query = urlencode({"limit": CURE_REPORTS_LIMIT, "sort": CURE_REPORTS_SORT})
    return f"{CURE_REPORTS_URL}?{query}"


def _fetch_json(url: str, *, timeout: int) -> dict[str, Any]:
    response = requests.get(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def fetch_case_reports(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    timestamp = _utc_timestamp()
    version = f"reports_{timestamp}"
    version_date = _version_date_from_timestamp(timestamp)
    dest.mkdir(parents=True, exist_ok=True)
    output_path = dest / f"{version}.jsonl"

    next_url: Optional[str] = _build_reports_url()
    total_written = 0
    page_count = 0
    expected_count: Optional[int] = None
    upstream_urls = [next_url]

    with output_path.open("w", encoding="utf-8") as output_file:
        while next_url:
            page_count += 1
            print(f"Fetching CURE reports page {page_count}: {next_url}", flush=True)
            payload = _fetch_json(next_url, timeout=timeout)

            if expected_count is None:
                count = payload.get("count")
                expected_count = count if isinstance(count, int) else None
                if expected_count is not None:
                    print(f"CURE API reports {expected_count} total reports", flush=True)

            results = payload.get("results")
            if not isinstance(results, list):
                raise ValueError("CURE reports response did not contain a list in 'results'")

            for item in results:
                output_file.write(json.dumps(item, ensure_ascii=True))
                output_file.write("\n")
                total_written += 1

            print(
                f"Wrote {len(results)} CURE reports from page {page_count} "
                f"(total so far: {total_written})",
                flush=True,
            )
            next_value = payload.get("next")
            next_url = next_value if isinstance(next_value, str) and next_value else None
            if next_url:
                upstream_urls.append(next_url)

    return SourceSnapshot(
        source="cure",
        dataset="case_reports",
        version=version,
        version_date=version_date,
        homepage=CURE_HOMEPAGE,
        upstream_urls=upstream_urls,
        files=[
            SnapshotFile(
                output_path,
                CURE_REPORTS_URL,
                "application/x-ndjson",
            )
        ],
        extra={
            "version_method": {
                "type": "export_timestamp",
                "description": "Use the UTC timestamp at the start of the CURE reports export.",
                "evidence": {
                    "timestamp": timestamp,
                    "endpoint": CURE_REPORTS_URL,
                    "limit": CURE_REPORTS_LIMIT,
                    "sort": CURE_REPORTS_SORT,
                    "page_count": page_count,
                    "expected_count": expected_count,
                    "total_written": total_written,
                },
            }
        },
    )


def fetch_curated_concepts(
    *,
    dest: Path,
    source_file: Path = Path("input_files/manual/cure/cureid_data.tsv"),
    timeout: int = 60,
) -> SourceSnapshot:
    version_date = _local_file_mtime_date(source_file)
    copied_path = _copy_manual_file(source_file, dest)
    manual_uri = f"manual://cure/curated_concepts/{source_file.name}"
    return SourceSnapshot(
        source="cure",
        dataset="curated_concepts",
        version=version_date,
        version_date=version_date,
        downloaded_by="ifx-registry-manual",
        homepage=CURE_HOMEPAGE,
        upstream_urls=[manual_uri],
        files=[SnapshotFile(copied_path, manual_uri, "text/tab-separated-values")],
        extra={
            "version_method": {
                "type": "local_file_mtime",
                "description": "Manual CURE ID curated concepts TSV; use local file modification date as the snapshot version.",
            },
            "provenance": {
                "status": "manual CURE ID curated concept export",
                "original_location": str(source_file),
            },
        },
    )


def latest_curated_concepts_version(*, timeout: int = 60) -> Optional[str]:
    return _local_file_mtime_date(Path("input_files/manual/cure/cureid_data.tsv"))


class CureCaseReportsFetcher(SourceFetcher):
    source = "cure"
    dataset = "case_reports"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_case_reports(dest=dest, timeout=timeout)

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return None


class ManualCureCuratedConceptsFetcher(SourceFetcher):
    source = "cure"
    dataset = "curated_concepts"

    def fetch(self, *, dest: Path, timeout: int = 60) -> SourceSnapshot:
        return fetch_curated_concepts(dest=dest, timeout=timeout)

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return latest_curated_concepts_version(timeout=timeout)
