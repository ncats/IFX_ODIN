from typing import Any, Dict, List

import yaml

from src.registry.storage import MinioStorage, s3_uri


def format_size(size_bytes: int | float | None) -> str:
    if size_bytes is None:
        return ""
    size = float(size_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024 or unit == "TB":
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return ""


def list_source_snapshots(storage: MinioStorage) -> List[Dict[str, Any]]:
    manifests = []
    for key in storage.list_keys("sources/"):
        if not key.endswith("/manifest.yaml"):
            continue
        manifest = yaml.safe_load(storage.read_text(key))
        files = manifest.get("files", []) or []
        total_size_bytes = sum(
            entry.get("size_bytes", 0) or 0
            for entry in files
        )
        manifests.append({
            "snapshot_id": manifest.get("snapshot_id"),
            "source": manifest.get("source"),
            "dataset": manifest.get("dataset"),
            "version": manifest.get("version"),
            "version_date": manifest.get("version_date"),
            "download_date": manifest.get("download_date"),
            "version_method": (manifest.get("extra") or {}).get("version_method", {}),
            "files": files,
            "total_size_bytes": total_size_bytes,
            "total_size": format_size(total_size_bytes),
            "manifest_uri": s3_uri(storage.bucket, key),
        })
    return sorted(
        manifests,
        key=lambda item: (
            item.get("source") or "",
            item.get("dataset") or "",
            item.get("version") or "",
        ),
    )
