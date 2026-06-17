import re
from pathlib import Path
from typing import Any, Dict, List

import requests

from src.registry.download import download_url
from src.registry.fetchers import SourceFunctionFetcher
from src.registry.sources.snapshot_helpers import build_downloaded_snapshot


RAMP_DB_CONTENTS_URL = "https://api.github.com/repos/ncats/RaMP-DB/contents/db"
RAMP_DB_HOMEPAGE = "https://github.com/ncats/RaMP-DB/tree/main/db"
RAMP_SQLITE_RE = re.compile(r"^RaMP_SQLite_v(?P<version>\d+(?:\.\d+)*)\.sqlite\.gz$")
GITHUB_RAW_PREFIX = "https://raw.githubusercontent.com/"
GITHUB_MEDIA_PREFIX = "https://media.githubusercontent.com/media/"


def _version_sort_key(version: str) -> tuple:
    return tuple(int(part) for part in version.split("."))


def _github_media_url(download_url: str) -> str:
    if download_url.startswith(GITHUB_RAW_PREFIX):
        return f"{GITHUB_MEDIA_PREFIX}{download_url.removeprefix(GITHUB_RAW_PREFIX)}"
    return download_url


def _github_db_entries(timeout: int = 60) -> List[Dict[str, Any]]:
    response = requests.get(RAMP_DB_CONTENTS_URL, params={"ref": "main"}, timeout=timeout)
    response.raise_for_status()
    entries = response.json()
    if not isinstance(entries, list):
        raise ValueError("RaMP-DB GitHub contents response was not a list")
    return entries


def _sqlite_release_entries(timeout: int = 60) -> List[Dict[str, Any]]:
    releases = []
    for entry in _github_db_entries(timeout=timeout):
        name = entry.get("name")
        match = RAMP_SQLITE_RE.match(str(name or ""))
        if not match:
            continue
        download_url_value = entry.get("download_url")
        if not download_url_value:
            continue
        releases.append({
            "version": match.group("version"),
            "name": name,
            "download_url": _github_media_url(download_url_value),
            "github_download_url": download_url_value,
            "html_url": entry.get("html_url"),
            "sha": entry.get("sha"),
            "size": entry.get("size"),
        })
    if not releases:
        raise ValueError("Could not find RaMP SQLite .sqlite.gz releases in RaMP-DB db directory")
    return sorted(releases, key=lambda release: _version_sort_key(release["version"]))


def latest_ramp_sqlite_database_version(timeout: int = 60) -> str:
    return _sqlite_release_entries(timeout=timeout)[-1]["version"]


def fetch_ramp_sqlite_database(
    *,
    dest: Path,
    timeout: int = 60,
) -> Path:
    source = "ramp"
    dataset = "sqlite_database"
    release = _sqlite_release_entries(timeout=timeout)[-1]
    version = release["version"]
    work_dir = dest / source / dataset / "pending"
    downloaded = [
        (
            *download_url(
                release["download_url"],
                work_dir,
                timeout=timeout,
                file_name=release["name"],
            ),
            release["download_url"],
        )
    ]
    metadata = downloaded[0][1]
    version_date = metadata.get("version_date")
    return build_downloaded_snapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage=RAMP_DB_HOMEPAGE,
        urls=[RAMP_DB_CONTENTS_URL, release["html_url"] or release["download_url"]],
        downloaded=downloaded,
        dest=dest,
        version_method={
            "type": "github_main_db_directory",
            "description": (
                "List ncats/RaMP-DB db/ on main, select the highest "
                "RaMP_SQLite_v*.sqlite.gz semantic version, and use that version token."
            ),
            "evidence": release,
        },
    )


class RampSqliteDatabaseFetcher(SourceFunctionFetcher):
    source = "ramp"
    dataset = "sqlite_database"
    fetch_function = staticmethod(fetch_ramp_sqlite_database)
    latest_version_function = staticmethod(latest_ramp_sqlite_database_version)
