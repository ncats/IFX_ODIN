"""Small ODIN-facing helpers for publishing caller-produced Registry sources."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from ifx_registry import ProducerIdentity, RegistryClient, SnapshotRef


def publish_source_file(
    snapshot_id: str,
    file_path: str | Path,
    credentials_file: str | Path,
    *,
    capture_method: str,
    source_url: str | None = None,
    metadata: Mapping[str, Any] | None = None,
):
    """Publish one exact, caller-captured file through the standalone Registry."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    version = snapshot_id.rsplit(":", 1)[-1]
    try:
        version_date = date.fromisoformat(version)
    except ValueError:
        version_date = None
    file_sources = {path.name: source_url} if source_url else None
    return RegistryClient.connect(credentials_file).publish_source(
        snapshot_id,
        files={path.name: path},
        captured_at=datetime.now(timezone.utc),
        capture_method=capture_method,
        version_date=version_date,
        file_sources=file_sources,
        upstream_urls=(source_url,) if source_url else (),
        metadata=dict(metadata or {}),
    )


def publish_derived_file(
    snapshot_id: str,
    file_path: str | Path,
    credentials_file: str | Path,
    *,
    inputs: Sequence[str],
    producer_release: str,
    producer_repository: str,
    producer_revision: str,
    transform_name: str,
    validation: Mapping[str, Any],
    metadata: Mapping[str, Any] | None = None,
):
    """Publish one reusable derived file with exact, kind-qualified inputs."""
    path = Path(file_path)
    if not path.is_file():
        raise FileNotFoundError(path)
    version = snapshot_id.rsplit(":", 1)[-1]
    try:
        version_date = date.fromisoformat(version)
    except ValueError:
        version_date = None
    refs = tuple(_snapshot_ref(value) for value in inputs)
    producer = ProducerIdentity(
        name="ifx_harmonizers",
        release=producer_release,
        code_repository=producer_repository,
        code_revision=producer_revision,
    )
    return RegistryClient.connect(credentials_file).derived.publish(
        snapshot_id,
        files={path.name: path},
        inputs=refs,
        producer=producer,
        transform={"name": transform_name, "version": producer_release},
        validation=dict(validation),
        version_date=version_date,
        metadata=dict(metadata or {}),
    )


def _snapshot_ref(value: str) -> SnapshotRef:
    try:
        kind, snapshot_id = value.split("=", 1)
    except ValueError as error:
        raise ValueError(
            "Derived input must be kind=source:dataset:version"
        ) from error
    constructors = {
        "source": SnapshotRef.source,
        "derived": SnapshotRef.derived,
        "external": SnapshotRef.external,
    }
    try:
        constructor = constructors[kind]
    except KeyError as error:
        raise ValueError("Derived input kind must be source, derived, or external") from error
    return constructor(snapshot_id)
