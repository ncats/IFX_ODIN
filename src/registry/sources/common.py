import gzip
import shutil
from pathlib import Path
from typing import Dict, List, Optional

from src.registry.download import download_url
from src.registry.download import http_metadata
from src.registry.fetchers import SnapshotFile, SourceSnapshot


def latest_version_from_last_modified_urls(urls: List[str], *, timeout: int = 60) -> str:
    version_dates = [http_metadata(url, timeout=timeout).get("version_date") for url in urls]
    version_dates = [version_date for version_date in version_dates if version_date]
    if not version_dates:
        raise ValueError(f"Could not determine version_date from Last-Modified headers: {urls}")
    return max(version_dates)


def version_date_from_downloaded_metadata(downloaded: List[tuple]) -> str:
    version_dates = [metadata.get("version_date") for local_path, metadata, url in downloaded if metadata.get("version_date")]
    if not version_dates:
        raise ValueError("Could not determine version_date from downloaded metadata")
    return max(version_dates)


def fetch_multi_file_last_modified_snapshot(
    *,
    source: str,
    dataset: str,
    urls: List[str],
    dest: Path,
    homepage: Optional[str] = None,
    timeout: int = 60,
    version: Optional[str] = None,
    version_description: Optional[str] = None,
    compress: bool = False,
) -> SourceSnapshot:
    work_dir = dest
    work_dir.mkdir(parents=True, exist_ok=True)
    downloaded = []
    for url in urls:
        downloaded.append((*download_url(url, work_dir, timeout=timeout), url))

    try:
        version_date = version_date_from_downloaded_metadata(downloaded)
    except ValueError as exc:
        raise ValueError(f"Could not determine version_date from Last-Modified for {source}:{dataset}") from exc
    snapshot_version = version or version_date

    moved = []
    for local_path, metadata, url in downloaded:
        final_path = local_path
        if compress and not final_path.name.endswith(".gz"):
            compressed_path = final_path.with_name(f"{final_path.name}.gz")
            with final_path.open("rb") as source_handle:
                with gzip.open(compressed_path, "wb") as compressed_handle:
                    shutil.copyfileobj(source_handle, compressed_handle)
            final_path.unlink()
            final_path = compressed_path
            metadata["content_type"] = "application/gzip"
        moved.append((final_path, metadata, url))

    evidence_files: List[Dict[str, Optional[str]]] = []
    snapshot_files = []
    for final_path, metadata, url in moved:
        snapshot_files.append(SnapshotFile(final_path, metadata.get("final_url") or url, metadata.get("content_type")))
        evidence_files.append({
            "path": final_path.name,
            "url": metadata.get("final_url") or url,
            "last_modified": metadata.get("last_modified"),
        })

    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=snapshot_version,
        version_date=version_date,
        homepage=homepage,
        upstream_urls=urls,
        files=snapshot_files,
        extra={
            "version_method": {
                "type": "multi_file_max_last_modified" if len(snapshot_files) > 1 else "single_file_last_modified",
                "description": version_description or "Use the max HTTP Last-Modified date across all files as version and version_date.",
                "evidence": {
                    "files": evidence_files,
                },
            }
        },
    )
