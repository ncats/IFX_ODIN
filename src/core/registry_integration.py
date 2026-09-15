"""Final-form, non-fallback integration with the standalone IFX Registry."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol

from ifx_registry import RegistryClient

from src.models.registry_dataset import RegistryDataset, RegistryDatasetKind


class RegistryReferenceKind(str, Enum):
    SOURCE = "source_snapshot"
    DERIVED = "derived_snapshot"
    EXTERNAL = "external_dataset_version"


@dataclass(frozen=True)
class RegistryReference:
    kind: RegistryReferenceKind
    snapshot_id: str

    @classmethod
    def parse(cls, value: str | Mapping[str, Any]) -> RegistryReference:
        if isinstance(value, str):
            return cls(RegistryReferenceKind.SOURCE, _snapshot_id(value))
        if not isinstance(value, Mapping):
            raise TypeError("Registry reference must be a snapshot string or mapping")
        unknown = set(value) - {"kind", "snapshot_id"}
        if unknown:
            raise ValueError(
                "Registry reference has unsupported fields: " + ", ".join(sorted(unknown))
            )
        try:
            kind = RegistryReferenceKind(str(value["kind"]))
            snapshot_id = _snapshot_id(str(value["snapshot_id"]))
        except KeyError as error:
            raise ValueError("Registry reference requires kind and snapshot_id") from error
        return cls(kind, snapshot_id)


class _DerivedClient(Protocol):
    def materialize(self, snapshot_id: str, *, destination: str | Path) -> Any: ...


class _ExternalClient(Protocol):
    def describe(self, snapshot_id: str) -> Any: ...


class RegistryClientPort(Protocol):
    @property
    def derived(self) -> _DerivedClient: ...

    @property
    def external(self) -> _ExternalClient: ...

    def materialize(self, snapshot_id: str, *, destination: str | Path) -> Any: ...


class RegistryIntegration:
    """Resolve explicit Registry references without probing or legacy fallback."""

    def __init__(self, client: RegistryClientPort, cache_dir: str | Path):
        self._client = client
        self._cache_dir = Path(cache_dir)

    @classmethod
    def connect(cls, config: Mapping[str, Any]) -> RegistryIntegration:
        credentials = config.get("credentials")
        if credentials is not None and not isinstance(credentials, (str, Path)):
            raise TypeError("registry.credentials must be a credential YAML path")
        cache_dir = Path(config.get("cache_dir", "/var/tmp/ifx-registry-cache"))
        client = RegistryClient.connect(
            credentials,
            cache_dir=cache_dir,
            bucket=_optional_string(config.get("bucket")),
            region=_optional_string(config.get("region")),
            prefix=str(config.get("prefix", "")),
        )
        return cls(client, cache_dir)

    def resolve(self, value: str | Mapping[str, Any]) -> RegistryDataset:
        reference = RegistryReference.parse(value)
        if reference.kind is RegistryReferenceKind.SOURCE:
            materialized = self._client.materialize(
                reference.snapshot_id,
                destination=self._cache_dir,
            )
            return RegistryDataset.from_file_backed(
                materialized,
                RegistryDatasetKind.SOURCE,
            )
        if reference.kind is RegistryReferenceKind.DERIVED:
            materialized = self._client.derived.materialize(
                reference.snapshot_id,
                destination=self._cache_dir,
            )
            return RegistryDataset.from_file_backed(
                materialized,
                RegistryDatasetKind.DERIVED,
            )
        described = self._client.external.describe(reference.snapshot_id)
        return RegistryDataset.from_external(described)


def _snapshot_id(value: str) -> str:
    normalized = value.strip()
    parts = normalized.split(":")
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError(f"Registry snapshot must be source:dataset:version, got {value!r}")
    return normalized


def _optional_string(value: object) -> str | None:
    return str(value) if value is not None else None
