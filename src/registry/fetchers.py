from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


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
