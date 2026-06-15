import hashlib
import mimetypes
import os
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

import yaml


MANIFEST_FILENAME = "manifest.yaml"


def today_utc() -> str:
    return date.today().isoformat()


def parse_http_date_to_iso(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    parsed = parsedate_to_datetime(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.date().isoformat()


def iso_timestamp() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def default_content_type(path: Path, fallback: str = "application/octet-stream") -> str:
    guessed, _ = mimetypes.guess_type(str(path))
    return guessed or fallback


def snapshot_id(source: str, dataset: str, version: str) -> str:
    return f"{source}:{dataset}:{version}"


def storage_prefix(source: str, dataset: str, version: str) -> str:
    return f"sources/{source}/{dataset}/{version}"


def derived_storage_prefix(source: str, dataset: str, version: str) -> str:
    return f"derived/{source}/{dataset}/{version}"


def resolver_storage_prefix(source: str, resolver: str, version: str) -> str:
    return f"resolvers/{source}/{resolver}/{version}"


def file_entry(
    local_path: Path,
    source_url: str,
    storage_uri: Optional[str] = None,
    content_type: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "path": local_path.name,
        "size_bytes": local_path.stat().st_size,
        "sha256": sha256_file(local_path),
        "content_type": content_type or default_content_type(local_path),
        "source_url": source_url,
        "storage_uri": storage_uri,
    }


def build_source_snapshot_manifest(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    download_date: Optional[str],
    upstream_urls: Iterable[str],
    files: Iterable[Dict[str, Any]],
    homepage: Optional[str] = None,
    downloaded_by: str = "ifx-registry",
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    manifest: Dict[str, Any] = {
        "kind": "source_snapshot",
        "schema_version": 1,
        "source": source,
        "dataset": dataset,
        "snapshot_id": snapshot_id(source, dataset, version),
        "version": version,
        "version_date": version_date,
        "download_date": download_date or today_utc(),
        "downloaded_by": downloaded_by,
        "created_at": iso_timestamp(),
        "upstream": {
            "homepage": homepage,
            "urls": list(upstream_urls),
        },
        "files": list(files),
    }
    if extra:
        manifest["extra"] = extra
    return manifest


def build_derived_snapshot_manifest(
    *,
    source: str,
    dataset: str,
    version: str,
    version_date: Optional[str],
    derived_from: Iterable[Dict[str, Any]],
    transform: Dict[str, Any],
    files: Iterable[Dict[str, Any]],
    build_key: Optional[str] = None,
    stats: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    manifest: Dict[str, Any] = {
        "kind": "derived_snapshot",
        "schema_version": 1,
        "source": source,
        "dataset": dataset,
        "snapshot_id": snapshot_id(source, dataset, version),
        "version": version,
        "version_date": version_date,
        "created_at": iso_timestamp(),
        "derived_from": list(derived_from),
        "transform": transform,
        "build_key": build_key,
        "files": list(files),
        "stats": stats or {},
    }
    if extra:
        manifest["extra"] = extra
    return manifest


def build_resolver_snapshot_manifest(
    *,
    source: str,
    resolver: str,
    version: str,
    definition: Dict[str, Any],
    definition_fingerprint: str,
    resolved_inputs: Dict[str, str],
    resolved_input_metadata: Dict[str, Dict[str, Any]],
    files: Iterable[Dict[str, Any]],
    build_key: Optional[str] = None,
    stats: Optional[Dict[str, Any]] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    manifest: Dict[str, Any] = {
        "kind": "resolver_snapshot",
        "schema_version": 1,
        "source": source,
        "resolver": resolver,
        "snapshot_id": snapshot_id(source, resolver, version),
        "version": version,
        "created_at": iso_timestamp(),
        "definition": definition,
        "definition_fingerprint": definition_fingerprint,
        "resolved_inputs": dict(resolved_inputs),
        "resolved_input_metadata": dict(resolved_input_metadata),
        "build_key": build_key,
        "files": list(files),
        "stats": stats or {},
    }
    if extra:
        manifest["extra"] = extra
    return manifest


def write_manifest(manifest: Dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        yaml.safe_dump(manifest, handle, sort_keys=False)


def read_manifest(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


def manifest_checksum(path: Path) -> str:
    return sha256_file(path)


def verify_manifest_files(manifest_path: Path, base_dir: Optional[Path] = None) -> None:
    manifest = read_manifest(manifest_path)
    root = base_dir or manifest_path.parent
    errors = []
    for entry in manifest.get("files", []):
        path = root / entry["path"]
        if not path.exists():
            errors.append(f"missing file: {path}")
            continue
        expected_size = entry.get("size_bytes")
        if expected_size is not None and path.stat().st_size != expected_size:
            errors.append(f"size mismatch for {path}: expected {expected_size}, got {path.stat().st_size}")
        expected_sha = entry.get("sha256")
        if expected_sha and sha256_file(path) != expected_sha:
            errors.append(f"sha256 mismatch for {path}")
    if errors:
        raise ValueError("; ".join(errors))


def file_name_from_url(url: str) -> str:
    name = url.rstrip("/").rsplit("/", 1)[-1]
    if not name:
        raise ValueError(f"Could not infer file name from URL: {url}")
    return name
