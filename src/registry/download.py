from pathlib import Path
from typing import Dict, Optional, Tuple

import requests
from email.message import Message

from src.registry.manifest import file_name_from_url, parse_http_date_to_iso


PROGRESS_INTERVAL_BYTES = 100 * 1024 * 1024


def format_bytes(size_bytes: Optional[int | str]) -> str:
    if size_bytes is None:
        return "unknown size"
    try:
        value = float(size_bytes)
    except (TypeError, ValueError):
        return "unknown size"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(value)} {unit}"
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} TB"


def file_name_from_response(response: requests.Response, fallback_url: str) -> str:
    content_disposition = response.headers.get("Content-Disposition")
    if content_disposition:
        message = Message()
        message["content-disposition"] = content_disposition
        filename = message.get_filename()
        if filename:
            return Path(filename).name
    return file_name_from_url(response.url or fallback_url)


def http_metadata(url: str, timeout: int = 60) -> Dict[str, Optional[str]]:
    response = requests.head(url, allow_redirects=True, timeout=timeout)
    response.raise_for_status()
    return {
        "last_modified": response.headers.get("Last-Modified"),
        "version_date": parse_http_date_to_iso(response.headers.get("Last-Modified")),
        "content_type": response.headers.get("Content-Type"),
        "content_length": response.headers.get("Content-Length"),
        "final_url": response.url,
    }


def download_url(
    url: str,
    dest_dir: Path,
    timeout: int = 60,
    file_name: Optional[str] = None,
    verbose: bool = True,
) -> Tuple[Path, Dict[str, Optional[str]]]:
    dest_dir.mkdir(parents=True, exist_ok=True)
    metadata = http_metadata(url, timeout=timeout)
    with requests.get(url, stream=True, allow_redirects=True, timeout=timeout) as response:
        response.raise_for_status()
        inferred_file_name = file_name or file_name_from_response(response, metadata.get("final_url") or url)
        dest_path = dest_dir / inferred_file_name
        content_length = response.headers.get("Content-Length") or metadata.get("content_length")
        if verbose:
            print(f"Downloading {inferred_file_name} ({format_bytes(content_length)}) from {response.url}", flush=True)
        downloaded_bytes = 0
        next_progress_bytes = PROGRESS_INTERVAL_BYTES
        with dest_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
                    downloaded_bytes += len(chunk)
                    if verbose and downloaded_bytes >= next_progress_bytes:
                        print(f"Downloaded {format_bytes(downloaded_bytes)} of {inferred_file_name}", flush=True)
                        next_progress_bytes += PROGRESS_INTERVAL_BYTES
        metadata["content_type"] = response.headers.get("Content-Type") or metadata.get("content_type")
        metadata["final_url"] = response.url
        if verbose:
            print(f"Finished {inferred_file_name} ({format_bytes(downloaded_bytes)}) -> {dest_path}", flush=True)
    return dest_path, metadata
