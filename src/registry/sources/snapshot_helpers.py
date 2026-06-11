from pathlib import Path
from typing import Dict, List, Optional, Tuple

import requests

from src.registry.download import PROGRESS_INTERVAL_BYTES, format_bytes
from src.registry.fetchers import SnapshotFile, SourceSnapshot
from src.registry.manifest import (
    parse_http_date_to_iso,
)


def build_downloaded_snapshot(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    homepage: str,
    urls: List[str],
    downloaded: List[Tuple[Path, Dict[str, Optional[str]], str]],
    dest: Path,
    version_method: Dict,
) -> SourceSnapshot:
    dest.mkdir(parents=True, exist_ok=True)
    moved = []
    for local_path, metadata, url in downloaded:
        moved.append((local_path, metadata, url))

    snapshot_files = []
    for final_path, metadata, url in moved:
        snapshot_files.append(SnapshotFile(final_path, metadata.get("final_url") or url, metadata.get("content_type")))

    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage=homepage,
        upstream_urls=urls,
        files=snapshot_files,
        extra={"version_method": version_method},
    )


def download_stream_without_head(
    url: str,
    dest_dir: Path,
    file_name: str,
    timeout: int,
) -> Tuple[Path, Dict[str, Optional[str]]]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / file_name
    with requests.get(url, stream=True, allow_redirects=True, timeout=timeout) as response:
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        print(f"Downloading {file_name} ({format_bytes(content_length)}) from {response.url}", flush=True)
        downloaded_bytes = 0
        next_progress_bytes = PROGRESS_INTERVAL_BYTES
        with dest_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
                    downloaded_bytes += len(chunk)
                    if downloaded_bytes >= next_progress_bytes:
                        print(f"Downloaded {format_bytes(downloaded_bytes)} of {file_name}", flush=True)
                        next_progress_bytes += PROGRESS_INTERVAL_BYTES
        print(f"Finished {file_name} ({format_bytes(downloaded_bytes)}) -> {dest_path}", flush=True)
        return dest_path, {
            "last_modified": response.headers.get("Last-Modified"),
            "version_date": parse_http_date_to_iso(response.headers.get("Last-Modified")),
            "content_type": response.headers.get("Content-Type"),
            "content_length": response.headers.get("Content-Length"),
            "final_url": response.url,
        }
