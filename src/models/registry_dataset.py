"""ODIN-owned view of an exact dataset obtained from IFX Registry."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from src.models.datasource_version_info import DatasourceVersionInfo


class RegistryDatasetKind(str, Enum):
    SOURCE = "source_snapshot"
    DERIVED = "derived_snapshot"
    EXTERNAL = "external_dataset_version"


@runtime_checkable
class RegistryDatasetMetadata(Protocol):
    """Structural boundary used by graph-provenance collection."""

    snapshot_id: str

    def to_metadata(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class RegistryDataset:
    """The small, framework-facing contract adapters receive from Registry."""

    kind: RegistryDatasetKind
    source: str
    dataset: str
    version: str
    version_date: date | None
    download_date: date | None
    snapshot_id: str
    manifest_uri: str
    manifest: Mapping[str, Any]
    local_dir: Path | None

    @classmethod
    def from_file_backed(
        cls,
        value: Any,
        kind: RegistryDatasetKind,
    ) -> RegistryDataset:
        if kind is RegistryDatasetKind.EXTERNAL:
            raise ValueError("External Registry datasets are metadata-only")
        return cls(
            kind=kind,
            source=value.source,
            dataset=value.dataset,
            version=value.version,
            version_date=_date(value.version_date),
            download_date=_date(value.download_date),
            snapshot_id=value.snapshot_id,
            manifest_uri=value.manifest_uri,
            manifest=dict(value.manifest),
            local_dir=Path(value.local_dir),
        )

    @classmethod
    def from_external(cls, value: Any) -> RegistryDataset:
        manifest = dict(value.manifest)
        registered_at = getattr(value, "registered_at", None)
        if registered_at is not None and "registered_at" not in manifest:
            manifest["registered_at"] = _iso_temporal(registered_at)
        return cls(
            kind=RegistryDatasetKind.EXTERNAL,
            source=value.source,
            dataset=value.dataset,
            version=value.version,
            version_date=_date(value.version_date),
            download_date=None,
            snapshot_id=value.snapshot_id,
            manifest_uri=value.manifest_uri,
            manifest=manifest,
            local_dir=None,
        )

    def file(self, file_name: str | None = None) -> Path:
        if self.local_dir is None:
            raise ValueError(
                f"Registry dataset {self.snapshot_id} is metadata-only and has no files"
            )
        files = self.manifest.get("files") or []
        if not isinstance(files, list):
            raise ValueError(f"Registry dataset {self.snapshot_id} has invalid file metadata")
        if file_name is None:
            if len(files) != 1:
                raise ValueError(
                    f"Dataset {self.snapshot_id} has {len(files)} files; specify file_name"
                )
            entry = files[0]
            if not isinstance(entry, dict) or not isinstance(entry.get("path"), str):
                raise ValueError(f"Registry dataset {self.snapshot_id} has invalid file metadata")
            file_name = entry["path"]
        declared = {
            entry.get("path")
            for entry in files
            if isinstance(entry, dict) and isinstance(entry.get("path"), str)
        }
        if file_name not in declared:
            raise FileNotFoundError(
                f"Dataset {self.snapshot_id} does not declare file {file_name!r}"
            )
        path = self.local_dir / file_name
        if not path.is_file():
            raise FileNotFoundError(path)
        return path

    def version_info(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=self.version,
            version_date=self.version_date,
            download_date=self.download_date,
        )

    def to_metadata(self) -> dict[str, Any]:
        metadata = {
            "kind": self.kind.value,
            "source": self.source,
            "dataset": self.dataset,
            "version": self.version,
            "version_date": _iso_date(self.version_date),
            "download_date": _iso_date(self.download_date),
            "snapshot_id": self.snapshot_id,
            "manifest_uri": self.manifest_uri,
            "files": _safe_files(self.manifest.get("files")),
        }
        if self.kind is RegistryDatasetKind.DERIVED:
            for key in (
                "derived_from",
                "producer",
                "transform",
                "validation",
                "build_key",
                "publication_fingerprint",
            ):
                if key in self.manifest:
                    metadata[key] = self.manifest[key]
        if self.kind is RegistryDatasetKind.EXTERNAL:
            for key in (
                "interface",
                "access_mode",
                "service_name",
                "registered_at",
                "observed_at",
                "documentation_url",
                "version_evidence",
            ):
                if key in self.manifest:
                    metadata[key] = self.manifest[key]
        return metadata


def _safe_files(value: object) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [
        {
            key: entry.get(key)
            for key in ("path", "sha256", "size_bytes", "storage_uri")
        }
        for entry in value
        if isinstance(entry, dict)
    ]


def _date(value: date | datetime | str | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(value[:10])
    except ValueError:
        return None


def _iso_date(value: date | None) -> str | None:
    return value.isoformat() if value is not None else None


def _iso_temporal(value: date | datetime | str) -> str:
    return value.isoformat() if isinstance(value, (date, datetime)) else str(value)
