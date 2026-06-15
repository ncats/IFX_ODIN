from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from src.models.datasource_version_info import DatasourceVersionInfo


@dataclass
class SnapshotFile:
    path: Path
    source_url: str
    content_type: Optional[str] = None


@dataclass
class SourceSnapshot:
    source: str
    dataset: str
    version: str
    version_date: Optional[str]
    homepage: Optional[str]
    upstream_urls: List[str]
    files: List[SnapshotFile]
    extra: Dict = field(default_factory=dict)
    downloaded_by: str = "ifx-registry"


@dataclass
class ExternalSourceRegistration:
    source: str
    dataset: str
    version: str
    version_date: Optional[str]
    connection: Dict[str, Any]
    access: Dict[str, Any]
    extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ArtifactFile:
    path: Path
    content_type: Optional[str] = None


@dataclass
class ResolvedDependency:
    source: str
    dataset: str
    version: str
    snapshot_id: str
    manifest_uri: str
    manifest: Dict[str, Any]
    local_dir: Optional[Path] = None

    def file(self, file_name: str) -> Path:
        if self.local_dir is None:
            raise ValueError(f"Dependency {self.snapshot_id} has not been staged locally")
        path = self.local_dir / file_name
        if not path.exists():
            raise FileNotFoundError(path)
        return path


@dataclass
class MaterializedDataset:
    source: str
    dataset: str
    version: str
    version_date: Optional[str]
    download_date: Optional[str]
    snapshot_id: str
    manifest_uri: str
    manifest: Dict[str, Any]
    local_dir: Path
    resolver_inputs: Dict[str, "MaterializedDataset"] = field(default_factory=dict)

    def file(self, file_name: Optional[str] = None) -> Path:
        files = self.manifest.get("files", []) or []
        if file_name is None:
            if len(files) != 1:
                raise ValueError(
                    f"Dataset {self.snapshot_id} has {len(files)} files; specify file_name"
                )
            file_name = files[0]["path"]
        path = self.local_dir / file_name
        if not path.exists():
            raise FileNotFoundError(path)
        return path

    def version_info(self) -> DatasourceVersionInfo:
        return DatasourceVersionInfo(
            version=self.version,
            version_date=self._parse_date(self.version_date),
            download_date=self._parse_date(self.download_date),
        )

    def to_metadata(self) -> Dict[str, Any]:
        metadata = {
            "kind": self.manifest.get("kind"),
            "source": self.source,
            "dataset": self.dataset,
            "version": self.version,
            "version_date": self.version_date,
            "download_date": self.download_date,
            "snapshot_id": self.snapshot_id,
            "manifest_uri": self.manifest_uri,
            "local_dir": str(self.local_dir),
            "files": [
                {
                    "path": entry.get("path"),
                    "sha256": entry.get("sha256"),
                    "size_bytes": entry.get("size_bytes"),
                    "storage_uri": entry.get("storage_uri"),
                }
                for entry in self.manifest.get("files", []) or []
            ],
        }
        if self.resolver_inputs:
            metadata["resolver_inputs"] = {
                name: dataset.to_metadata()
                for name, dataset in self.resolver_inputs.items()
            }
        return metadata

    @staticmethod
    def _parse_date(value: Optional[str]) -> Optional[date]:
        if not value:
            return None
        text = str(value).strip()
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            pass
        for date_format in ("%d-%B-%Y", "%d-%B-%y", "%d-%b-%Y", "%d-%b-%y"):
            try:
                return datetime.strptime(text, date_format).date()
            except ValueError:
                pass
        return None


@dataclass
class DerivedArtifact:
    source: str
    dataset: str
    version: str
    version_date: Optional[str]
    derived_from: List[Dict[str, Any]]
    transform: Dict[str, Any]
    files: List[ArtifactFile]
    build_key: Optional[str] = None
    stats: Dict[str, Any] = field(default_factory=dict)
    extra: Dict[str, Any] = field(default_factory=dict)


class SourceFetcher(ABC):
    source: str
    dataset: str

    @abstractmethod
    def fetch(
        self,
        *,
        dest: Path,
        timeout: int = 60,
    ) -> SourceSnapshot:
        raise NotImplementedError

    @abstractmethod
    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        raise NotImplementedError


class SourceFunctionFetcher(SourceFetcher):
    source: str
    dataset: str
    fetch_function: Callable
    latest_version_function: Callable

    def fetch(
        self,
        *,
        dest: Path,
        timeout: int = 60,
    ) -> SourceSnapshot:
        return self.fetch_function(
            dest=dest,
            timeout=timeout,
        )

    def get_latest_version(self, *, timeout: int = 60) -> Optional[str]:
        return self.latest_version_function(timeout=timeout)


class ExternalSourceProvider(ABC):
    source: str
    dataset: str

    @abstractmethod
    def build_registration(self, *, config: Dict[str, Any]) -> ExternalSourceRegistration:
        raise NotImplementedError


class DerivedArtifactBuilder(ABC):
    source: str
    dataset: str

    @abstractmethod
    def build(
        self,
        *,
        config: Dict[str, Any],
        dependencies: List[ResolvedDependency],
        dest: Path,
        version: str,
    ) -> DerivedArtifact:
        raise NotImplementedError
