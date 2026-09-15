from __future__ import annotations

from datetime import date
from pathlib import Path

from src.models.registry_dataset import RegistryDataset, RegistryDatasetKind


def registry_dataset(
    directory: Path,
    *file_names: str,
    source: str = "test",
    dataset: str = "fixture",
    version: str = "test",
    version_date: date | None = None,
    download_date: date | None = None,
) -> RegistryDataset:
    """Build the same small dataset contract that production adapters receive."""
    snapshot_id = f"{source}:{dataset}:{version}"
    return RegistryDataset(
        kind=RegistryDatasetKind.SOURCE,
        source=source,
        dataset=dataset,
        version=version,
        version_date=version_date,
        download_date=download_date,
        snapshot_id=snapshot_id,
        manifest_uri=f"memory://{snapshot_id}/manifest.yaml",
        manifest={
            "kind": RegistryDatasetKind.SOURCE.value,
            "files": [{"path": file_name} for file_name in file_names],
        },
        local_dir=directory,
    )
