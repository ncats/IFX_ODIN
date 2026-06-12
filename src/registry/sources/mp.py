from pathlib import Path
from typing import Optional
import tempfile

from src.registry.download import download_url
from src.registry.fetchers import SnapshotFile, SourceFunctionFetcher, SourceSnapshot
from src.registry.manifest import today_utc


MP_URL = "https://purl.obolibrary.org/obo/mp.obo"
MP_HOMEPAGE = "https://obofoundry.org/ontology/mp.html"
MP_FILE_NAME = "mp.obo"


def extract_obo_data_version(path: Path) -> Optional[str]:
    with path.open("r", encoding="utf-8") as handle:
        for index, line in enumerate(handle):
            if index > 200:
                break
            if line.startswith("data-version:"):
                value = line.split(":", 1)[1].strip()
                parts = [part for part in value.rstrip("/").split("/") if part]
                if len(parts) >= 2 and parts[-1] == MP_FILE_NAME:
                    return parts[-2]
                return parts[-1] if parts else value
    return None


def fetch_mp_obo(
    *,
    dest: Path,
    timeout: int = 60,
) -> SourceSnapshot:
    source = "mp"
    dataset = "ontology"
    work_dir = dest / source / dataset / "pending"
    local_path, metadata = download_url(MP_URL, work_dir, timeout=timeout)
    obo_version = extract_obo_data_version(local_path)
    version_date = obo_version or metadata.get("version_date")
    version = version_date or today_utc()

    return SourceSnapshot(
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        homepage=MP_HOMEPAGE,
        upstream_urls=[MP_URL],
        files=[SnapshotFile(local_path, metadata.get("final_url") or MP_URL, metadata.get("content_type"))],
        extra={
            "version_method": {
                "type": "obo_data_version",
                "description": "Use the OBO data-version header when present; otherwise use Last-Modified or registry download date because the source filename is stable across updates.",
                "evidence": {
                    "url": metadata.get("final_url") or MP_URL,
                    "obo_data_version": obo_version,
                    "last_modified": metadata.get("last_modified"),
                },
            }
        },
    )


def latest_mp_obo_version(timeout: int = 60) -> str:
    with tempfile.TemporaryDirectory() as temp_dir:
        local_path, metadata = download_url(MP_URL, Path(temp_dir), timeout=timeout, verbose=False)
        return extract_obo_data_version(local_path) or metadata.get("version_date") or today_utc()


class MpOntologyFetcher(SourceFunctionFetcher):
    source = "mp"
    dataset = "ontology"
    fetch_function = staticmethod(fetch_mp_obo)
    latest_version_function = staticmethod(latest_mp_obo_version)
