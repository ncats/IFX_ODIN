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
