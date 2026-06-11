from dataclasses import dataclass, field
from datetime import date
from copy import deepcopy
import importlib
from pathlib import Path
import shutil
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.registry.fetchers import ExternalSourceProvider, ExternalSourceRegistration, SourceFetcher, SourceSnapshot
from src.registry.manifest import (
    MANIFEST_FILENAME,
    build_source_snapshot_manifest,
    file_entry,
    iso_timestamp,
    manifest_checksum,
    read_manifest,
    storage_prefix,
    today_utc,
    verify_manifest_files,
    write_manifest,
)
from src.registry.storage import DEFAULT_REGISTRY_BUCKET, MinioStorage, load_minio_credentials, s3_uri
from src.shared.db_credentials import DBCredentials


REGISTRY_SOURCES_CONFIG = Path(__file__).parents[1] / "registry" / "registry_sources.yaml"
RegistryEntry = Dict[str, Any]
RegistryKey = Tuple[str, str]
VersionedRegistryKey = Tuple[str, str, str]
DEFAULT_FETCH_TIMEOUT = 60


@dataclass
class _RegistryIndex:
    source_snapshots: List[RegistryEntry]
    external_sources: List[RegistryEntry]
    source_by_dataset: Dict[RegistryKey, List[RegistryEntry]] = field(init=False)
    source_by_version: Dict[VersionedRegistryKey, RegistryEntry] = field(init=False)
    external_by_dataset: Dict[RegistryKey, List[RegistryEntry]] = field(init=False)
    external_by_version: Dict[VersionedRegistryKey, RegistryEntry] = field(init=False)

    def __post_init__(self) -> None:
        self.source_by_dataset, self.source_by_version = self._index_entries(self.source_snapshots)
        self.external_by_dataset, self.external_by_version = self._index_entries(self.external_sources)

    @staticmethod
    def _index_entries(
        entries: List[RegistryEntry],
    ) -> Tuple[Dict[RegistryKey, List[RegistryEntry]], Dict[VersionedRegistryKey, RegistryEntry]]:
        by_dataset: Dict[RegistryKey, List[RegistryEntry]] = {}
        by_version: Dict[VersionedRegistryKey, RegistryEntry] = {}
        for entry in entries:
            source = entry.get("source")
            dataset = entry.get("dataset")
            version = entry.get("version")
            if not source or not dataset:
                continue
            by_dataset.setdefault((source, dataset), []).append(entry)
            if version:
                by_version[(source, dataset, version)] = entry
        return by_dataset, by_version

    def list_sources(self) -> List[str]:
        return sorted({
            *[source for source, _ in self.source_by_dataset],
            *[source for source, _ in self.external_by_dataset],
        })

    def list_datasets(self, source: str) -> List[str]:
        return sorted({
            *[dataset for entry_source, dataset in self.source_by_dataset if entry_source == source],
            *[dataset for entry_source, dataset in self.external_by_dataset if entry_source == source],
        })

    def list_downloads(self, source: str) -> List[str]:
        return sorted(dataset for entry_source, dataset in self.source_by_dataset if entry_source == source)

    def list_external_datasets(self, source: str) -> List[str]:
        return sorted(dataset for entry_source, dataset in self.external_by_dataset if entry_source == source)

    def list_versions(self, source: str, dataset: str) -> List[str]:
        versions = {
            entry.get("version")
            for entry in self.source_by_dataset.get((source, dataset), [])
            if entry.get("version")
        }
        versions.update(
            entry.get("version")
            for entry in self.external_by_dataset.get((source, dataset), [])
            if entry.get("version")
        )
        return sorted(versions)


class DataRegistry:
    """ODIN-facing interface for registered data sources and artifacts."""

    def __init__(
        self,
        storage: Optional[MinioStorage] = None,
        *,
        sources_config_path: Path = REGISTRY_SOURCES_CONFIG,
    ):
        self.storage = storage
        self.sources_config_path = sources_config_path
        self._source_snapshots: Optional[List[RegistryEntry]] = None
        self._external_sources: Optional[List[RegistryEntry]] = None
        self._index: Optional[_RegistryIndex] = None
        self._source_index: Optional[_RegistryIndex] = None
        self._external_index: Optional[_RegistryIndex] = None
        self._sources_config: Optional[dict] = None

    @classmethod
    def local(
        cls,
        *,
        sources_config_path: Path = REGISTRY_SOURCES_CONFIG,
    ) -> "DataRegistry":
        return cls(storage=None, sources_config_path=sources_config_path)

    @classmethod
    def from_minio_credentials(
        cls,
        credentials_path: str | Path,
        *,
        bucket: Optional[str] = DEFAULT_REGISTRY_BUCKET,
        use_internal_url: bool = False,
        connect_timeout: int = 5,
        read_timeout: int = 30,
    ) -> "DataRegistry":
        credentials_path = Path(credentials_path)
        credentials = load_minio_credentials(credentials_path)
        storage = MinioStorage(
            credentials=credentials,
            bucket=bucket,
            use_internal_url=use_internal_url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        return cls(storage)

    @classmethod
    def from_credentials(
        cls,
        credentials: DBCredentials,
        *,
        bucket: Optional[str] = DEFAULT_REGISTRY_BUCKET,
        use_internal_url: bool = False,
        connect_timeout: int = 5,
        read_timeout: int = 30,
    ) -> "DataRegistry":
        storage = MinioStorage(
            credentials=credentials,
            bucket=bucket,
            use_internal_url=use_internal_url,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
        )
        return cls(storage)

    def list_source_snapshots(self) -> List[RegistryEntry]:
        self._require_storage()
        if self._source_snapshots is None:
            self._source_snapshots = self._list_source_snapshots()
        return self._source_snapshots

    def list_external_sources(self) -> List[RegistryEntry]:
        self._require_storage()
        if self._external_sources is None:
            self._external_sources = self._list_external_sources()
        return self._external_sources

    def refresh_catalog(self) -> None:
        self._require_storage()
        self._source_snapshots = self._list_source_snapshots()
        self._external_sources = self._list_external_sources()
        self._index = _RegistryIndex(self._source_snapshots, self._external_sources)
        self._source_index = _RegistryIndex(self._source_snapshots, [])
        self._external_index = _RegistryIndex([], self._external_sources)

    @staticmethod
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

    def _list_source_snapshots(self) -> List[RegistryEntry]:
        manifests = []
        for key in self.storage.list_keys("sources/"):
            if not key.endswith(f"/{MANIFEST_FILENAME}"):
                continue
            manifest = yaml.safe_load(self.storage.read_text(key))
            files = deepcopy(manifest.get("files", []) or [])
            for entry in files:
                entry["size"] = self.format_size(entry.get("size_bytes"))
            total_size_bytes = sum(entry.get("size_bytes", 0) or 0 for entry in files)
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
                "total_size": self.format_size(total_size_bytes),
                "manifest_uri": s3_uri(self.storage.bucket, key),
                "manifest": manifest,
                "manifest_yaml": yaml.safe_dump(manifest, sort_keys=False),
            })
        return self._sort_registry_entries(manifests)

    def _list_external_sources(self) -> List[RegistryEntry]:
        manifests = []
        for key in self.storage.list_keys("external/"):
            if not key.endswith(f"/{MANIFEST_FILENAME}"):
                continue
            manifest = yaml.safe_load(self.storage.read_text(key))
            manifests.append({
                "registration_id": manifest.get("registration_id"),
                "source": manifest.get("source"),
                "dataset": manifest.get("dataset"),
                "version": manifest.get("version"),
                "version_date": manifest.get("version_date"),
                "registered_date": manifest.get("registered_date"),
                "connection_type": (manifest.get("connection") or {}).get("type"),
                "connection": manifest.get("connection") or {},
                "access": manifest.get("access") or {},
                "version_method": (manifest.get("extra") or {}).get("version_method", {}),
                "manifest_uri": s3_uri(self.storage.bucket, key),
                "manifest": manifest,
                "manifest_yaml": yaml.safe_dump(manifest, sort_keys=False),
            })
        return self._sort_registry_entries(manifests)

    @staticmethod
    def _sort_registry_entries(entries: List[RegistryEntry]) -> List[RegistryEntry]:
        return sorted(
            entries,
            key=lambda item: (
                item.get("source") or "",
                item.get("dataset") or "",
                item.get("version") or "",
            ),
        )

    def fetch_dataset(
        self,
        source: str,
        dataset: str,
        *,
        dest: str | Path,
        timeout: Optional[int] = None,
    ) -> Path:
        dataset_config = self.get_dataset_config(source, dataset)
        fetch_config = dataset_config.get("fetch") or {}

        fetcher = self._load_fetcher(source, dataset, fetch_config)
        cache_root = Path(dest)
        work_dir = cache_root / "_registry_work" / source / dataset
        if work_dir.exists():
            shutil.rmtree(work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        snapshot = fetcher.fetch(
            dest=work_dir,
            timeout=timeout if timeout is not None else DEFAULT_FETCH_TIMEOUT,
        )
        if not isinstance(snapshot, SourceSnapshot):
            raise TypeError(f"Fetcher for {source}/{dataset} returned {type(snapshot).__name__}; expected SourceSnapshot")
        return self.write_snapshot(snapshot, dest=cache_root)

    def write_snapshot(self, snapshot: SourceSnapshot, *, dest: str | Path) -> Path:
        final_dir = Path(dest) / snapshot.source / snapshot.dataset / snapshot.version
        final_dir.mkdir(parents=True, exist_ok=True)

        entries = []
        for source_file in snapshot.files:
            local_path = Path(source_file.path)
            final_path = final_dir / local_path.name
            if local_path.resolve() != final_path.resolve():
                shutil.move(str(local_path), final_path)
            entries.append(
                file_entry(
                    local_path=final_path,
                    source_url=source_file.source_url,
                    storage_uri=None,
                    content_type=source_file.content_type,
                )
            )

        manifest = build_source_snapshot_manifest(
            source=snapshot.source,
            dataset=snapshot.dataset,
            version=snapshot.version,
            version_date=snapshot.version_date,
            download_date=None,
            homepage=snapshot.homepage,
            upstream_urls=snapshot.upstream_urls,
            files=entries,
            downloaded_by=snapshot.downloaded_by,
            extra=snapshot.extra,
        )
        manifest_path = final_dir / MANIFEST_FILENAME
        write_manifest(manifest, manifest_path)
        print(f"Wrote {manifest_path}")
        print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
        return manifest_path

    def upload_snapshot(self, manifest_path: str | Path) -> List[str]:
        self._require_storage()
        manifest_path = Path(manifest_path)
        verify_manifest_files(manifest_path)
        manifest = read_manifest(manifest_path)
        source = manifest["source"]
        dataset = manifest["dataset"]
        version = manifest["version"]
        prefix = storage_prefix(source, dataset, version)
        uploaded = []
        for entry in manifest.get("files", []) or []:
            local_path = manifest_path.parent / entry["path"]
            key = f"{prefix}/{entry['path']}"
            entry["storage_uri"] = self.storage.upload_file(local_path, key, entry.get("content_type"))
            uploaded.append(entry["storage_uri"])
        manifest["manifest_uri"] = s3_uri(self.storage.bucket, f"{prefix}/{MANIFEST_FILENAME}")
        write_manifest(manifest, manifest_path)
        uploaded_manifest_uri = self.storage.upload_file(
            manifest_path,
            f"{prefix}/{MANIFEST_FILENAME}",
            "application/x-yaml",
        )
        uploaded.append(uploaded_manifest_uri)
        self.refresh_catalog()
        return uploaded

    def refresh_dataset(
        self,
        source: str,
        dataset: str,
        *,
        dest: str | Path,
        timeout: Optional[int] = None,
    ) -> Path:
        manifest_path = self.fetch_dataset(source, dataset, dest=dest, timeout=timeout)
        self.upload_snapshot(manifest_path)
        return manifest_path

    def register_external_source(
        self,
        source: str,
        dataset: str,
        *,
        dest: str | Path,
        upload: bool = True,
    ) -> Path:
        registration = self.build_external_registration(source, dataset)
        manifest_path = self.write_external_registration(registration, dest=dest)
        if upload:
            self.upload_external_registration(manifest_path)
        return manifest_path

    def register_external_sources(
        self,
        *,
        dest: str | Path,
        upload: bool = True,
    ) -> List[Path]:
        paths = []
        for source, source_config in sorted((self._load_sources_config().get("sources") or {}).items()):
            for dataset, dataset_config in sorted((source_config.get("datasets") or {}).items()):
                if "external" not in dataset_config:
                    continue
                paths.append(self.register_external_source(source, dataset, dest=dest, upload=upload))
        return paths

    def check_external_registrations(self) -> List[RegistryEntry]:
        self._require_storage()
        statuses: List[RegistryEntry] = []
        for source, source_config in sorted((self._load_sources_config().get("sources") or {}).items()):
            for dataset, dataset_config in sorted((source_config.get("datasets") or {}).items()):
                external_config = dataset_config.get("external") or {}
                if not external_config:
                    continue
                registered_versions = self.list_versions(source, dataset)
                status: RegistryEntry = {
                    "source": source,
                    "dataset": dataset,
                    "latest_version": None,
                    "latest_version_date": None,
                    "registered_versions": registered_versions,
                    "latest_registered_version": registered_versions[-1] if registered_versions else None,
                    "days_since_last_update": self._days_since_version(registered_versions[-1]) if registered_versions else None,
                    "is_latest_registered": False,
                    "error": None,
                }
                try:
                    registration = self.build_external_registration(source, dataset)
                    status["latest_version"] = registration.version
                    status["latest_version_date"] = registration.version_date
                    status["is_latest_registered"] = registration.version in registered_versions
                except Exception as exc:
                    status["error"] = str(exc)
                statuses.append(status)
        return statuses

    def sync_external_sources(
        self,
        *,
        dest: Optional[str | Path] = None,
        min_days_since_last_update: int = 0,
        dry_run: bool = True,
    ) -> List[RegistryEntry]:
        """
        Register missing or stale external source pointers.

        With dry_run=True, this returns the external registrations that would
        be written/uploaded without mutating MinIO.
        """
        if not dry_run and dest is None:
            raise ValueError("dest is required when dry_run=False")
        results: List[RegistryEntry] = []
        for status in self.check_external_registrations():
            reason = self._sync_candidate_reason(
                status,
                min_days_since_last_update=min_days_since_last_update,
            )
            if reason is None:
                continue
            result = dict(status)
            result["sync_reason"] = reason
            result["sync_action"] = "would_register" if dry_run else "register"
            result["manifest_path"] = None
            result["sync_error"] = None
            if not dry_run:
                try:
                    print(
                        f"Registering external source {status['source']}/{status['dataset']} "
                        f"(reason: {reason})",
                        flush=True,
                    )
                    manifest_path = self.register_external_source(
                        status["source"],
                        status["dataset"],
                        dest=dest,
                        upload=True,
                    )
                    result["sync_action"] = "registered"
                    result["manifest_path"] = str(manifest_path)
                    print(
                        f"Registered external source {status['source']}/{status['dataset']} -> {manifest_path}",
                        flush=True,
                    )
                except Exception as exc:
                    result["sync_action"] = "failed"
                    result["sync_error"] = str(exc)
                    print(
                        f"Failed registering external source {status['source']}/{status['dataset']}: {exc}",
                        flush=True,
                    )
            results.append(result)
        return results

    def build_external_registration(self, source: str, dataset: str) -> ExternalSourceRegistration:
        dataset_config = self.get_dataset_config(source, dataset)
        external_config = dataset_config.get("external") or {}
        provider = self._load_external_provider(source, dataset, external_config)
        registration = provider.build_registration(config=external_config)
        if not isinstance(registration, ExternalSourceRegistration):
            raise TypeError(
                f"External provider for {source}/{dataset} returned {type(registration).__name__}; "
                "expected ExternalSourceRegistration"
            )
        return registration

    def write_external_registration(
        self,
        registration: ExternalSourceRegistration,
        *,
        dest: str | Path,
    ) -> Path:
        manifest = self._build_external_manifest(registration)
        final_dir = Path(dest) / registration.source / registration.dataset / registration.version
        manifest_path = final_dir / MANIFEST_FILENAME
        write_manifest(manifest, manifest_path)
        print(f"Wrote {manifest_path}")
        print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
        return manifest_path

    def upload_external_registration(self, manifest_path: str | Path) -> List[str]:
        self._require_storage()
        manifest_path = Path(manifest_path)
        manifest = read_manifest(manifest_path)
        source = manifest["source"]
        dataset = manifest["dataset"]
        version = manifest["version"]
        prefix = self.external_storage_prefix(source, dataset, version)
        manifest["manifest_uri"] = s3_uri(self.storage.bucket, f"{prefix}/{MANIFEST_FILENAME}")
        write_manifest(manifest, manifest_path)
        uploaded_manifest_uri = self.storage.upload_file(
            manifest_path,
            f"{prefix}/{MANIFEST_FILENAME}",
            "application/x-yaml",
        )
        self.refresh_catalog()
        return [uploaded_manifest_uri]

    @staticmethod
    def external_storage_prefix(source: str, dataset: str, version: str) -> str:
        return f"external/{source}/{dataset}/{version}"

    @staticmethod
    def _build_external_manifest(registration: ExternalSourceRegistration) -> RegistryEntry:
        return {
            "kind": "external_source_registration",
            "schema_version": 1,
            "source": registration.source,
            "dataset": registration.dataset,
            "registration_id": f"{registration.source}:{registration.dataset}:{registration.version}",
            "version": registration.version,
            "version_date": registration.version_date,
            "registered_date": today_utc(),
            "created_at": iso_timestamp(),
            "connection": registration.connection,
            "access": registration.access,
            "extra": registration.extra,
        }

    def get_latest_version(self, source: str, dataset: str, *, timeout: Optional[int] = None) -> Optional[str]:
        dataset_config = self.get_dataset_config(source, dataset)
        fetch_config = dataset_config.get("fetch") or {}
        fetcher = self._load_fetcher(source, dataset, fetch_config)
        if not hasattr(fetcher, "get_latest_version"):
            raise TypeError(f"Configured registry fetcher for {source}/{dataset} does not implement get_latest_version")
        return fetcher.get_latest_version(timeout=timeout if timeout is not None else DEFAULT_FETCH_TIMEOUT)

    def is_latest_registered(self, source: str, dataset: str, *, timeout: Optional[int] = None) -> Optional[bool]:
        latest_version = self.get_latest_version(source, dataset, timeout=timeout)
        if latest_version is None:
            return None
        return latest_version in self.list_versions(source, dataset)

    def check_all_latest_registered(self, *, timeout: Optional[int] = None) -> List[RegistryEntry]:
        self._require_storage()
        statuses: List[RegistryEntry] = []
        for source, source_config in sorted((self._load_sources_config().get("sources") or {}).items()):
            for dataset, dataset_config in sorted((source_config.get("datasets") or {}).items()):
                fetch_config = dataset_config.get("fetch") or {}
                if not fetch_config:
                    continue
                version_strategy = fetch_config.get("version_strategy")
                dataset_timeout = timeout if timeout is not None else DEFAULT_FETCH_TIMEOUT
                registered_versions = self.list_versions(source, dataset)
                status: RegistryEntry = {
                    "source": source,
                    "dataset": dataset,
                    "version_strategy": version_strategy,
                    "latest_version": None,
                    "registered_versions": registered_versions,
                    "latest_registered_version": registered_versions[-1] if registered_versions else None,
                    "days_since_last_update": self._days_since_version(registered_versions[-1]) if registered_versions else None,
                    "is_latest_registered": None,
                    "error": None,
                }
                try:
                    latest_version = self.get_latest_version(source, dataset, timeout=dataset_timeout)
                    status["latest_version"] = latest_version
                    status["is_latest_registered"] = latest_version in registered_versions if latest_version else False
                except Exception as exc:
                    status["error"] = str(exc)
                statuses.append(status)
        return statuses

    def sync_latest_snapshots(
        self,
        *,
        dest: Optional[str | Path] = None,
        min_days_since_last_update: int = 0,
        dry_run: bool = True,
        timeout: Optional[int] = None,
    ) -> List[RegistryEntry]:
        """
        Fetch and upload missing or stale source snapshots.

        With dry_run=True, this returns the datasets that would be refreshed
        without downloading or uploading anything.
        """
        if not dry_run and dest is None:
            raise ValueError("dest is required when dry_run=False")
        candidates = self._sync_latest_snapshot_candidates(
            min_days_since_last_update=min_days_since_last_update,
            timeout=timeout,
        )
        results: List[RegistryEntry] = []
        for candidate in candidates:
            result = dict(candidate)
            result["sync_action"] = "would_refresh" if dry_run else "refresh"
            result["manifest_path"] = None
            result["sync_error"] = None
            if not dry_run:
                try:
                    print(
                        f"Refreshing {candidate['source']}/{candidate['dataset']} "
                        f"(reason: {candidate['sync_reason']})",
                        flush=True,
                    )
                    manifest_path = self.refresh_dataset(
                        candidate["source"],
                        candidate["dataset"],
                        dest=dest,
                        timeout=timeout,
                    )
                    result["sync_action"] = "refreshed"
                    result["manifest_path"] = str(manifest_path)
                    print(
                        f"Refreshed {candidate['source']}/{candidate['dataset']} -> {manifest_path}",
                        flush=True,
                    )
                except Exception as exc:
                    result["sync_action"] = "failed"
                    result["sync_error"] = str(exc)
                    print(
                        f"Failed refreshing {candidate['source']}/{candidate['dataset']}: {exc}",
                        flush=True,
                    )
            results.append(result)
        return results

    def _sync_latest_snapshot_candidates(
        self,
        *,
        min_days_since_last_update: int,
        timeout: Optional[int],
    ) -> List[RegistryEntry]:
        candidates: List[RegistryEntry] = []
        for status in self.check_all_latest_registered(timeout=timeout):
            reason = self._sync_candidate_reason(
                status,
                min_days_since_last_update=min_days_since_last_update,
            )
            if reason is None:
                continue
            candidate = dict(status)
            candidate["sync_reason"] = reason
            candidates.append(candidate)
        return candidates

    @staticmethod
    def _days_since_version(version: str) -> Optional[int]:
        try:
            parsed = date.fromisoformat(str(version)[:10])
        except ValueError:
            return None
        return (date.today() - parsed).days

    @staticmethod
    def _sync_candidate_reason(
        status: RegistryEntry,
        *,
        min_days_since_last_update: int,
    ) -> Optional[str]:
        if status.get("error"):
            return None
        if not status.get("registered_versions"):
            return "missing"
        if status.get("is_latest_registered") is not False:
            return None

        days_since_last_update = status.get("days_since_last_update")
        if days_since_last_update is not None and days_since_last_update < min_days_since_last_update:
            return None
        return "not_latest"

    def _load_fetcher(self, source: str, dataset: str, fetch_config: dict):
        module_name = fetch_config.get("module")
        if not module_name:
            raise ValueError(f"Missing fetch module for {source}/{dataset}")
        module = importlib.import_module(module_name)

        class_name = fetch_config.get("class")
        if not class_name:
            raise ValueError(f"Missing fetcher class for {source}/{dataset}")
        fetcher = getattr(module, class_name)()
        if not isinstance(fetcher, SourceFetcher):
            raise TypeError(f"Fetcher {class_name} must implement SourceFetcher")
        if getattr(fetcher, "source", None) != source or getattr(fetcher, "dataset", None) != dataset:
            raise ValueError(f"Fetcher {class_name} does not match configured dataset {source}/{dataset}")
        return fetcher

    def _load_external_provider(self, source: str, dataset: str, external_config: dict) -> ExternalSourceProvider:
        module_name = external_config.get("module")
        if not module_name:
            raise ValueError(f"Missing external source module for {source}/{dataset}")
        module = importlib.import_module(module_name)

        class_name = external_config.get("class")
        if not class_name:
            raise ValueError(f"Missing external source class for {source}/{dataset}")
        provider = getattr(module, class_name)()
        if not isinstance(provider, ExternalSourceProvider):
            raise TypeError(f"External source provider {class_name} must implement ExternalSourceProvider")
        if getattr(provider, "source", None) != source or getattr(provider, "dataset", None) != dataset:
            raise ValueError(f"External source provider {class_name} does not match configured dataset {source}/{dataset}")
        return provider

    def get_dataset_config(self, source: str, dataset: str) -> dict:
        source_config = (self._load_sources_config().get("sources") or {}).get(source)
        if not source_config:
            raise LookupError(f"No registry source configured for {source}")
        dataset_config = (source_config.get("datasets") or {}).get(dataset)
        if not dataset_config:
            raise LookupError(f"No registry dataset configured for {source}/{dataset}")
        return dataset_config

    def _load_sources_config(self) -> dict:
        if self._sources_config is None:
            with self.sources_config_path.open("r", encoding="utf-8") as handle:
                self._sources_config = yaml.safe_load(handle) or {}
        return self._sources_config

    def _require_storage(self) -> None:
        if self.storage is None:
            raise ValueError("This DataRegistry instance is not connected to MinIO storage")

    def _catalog_index(self) -> _RegistryIndex:
        if self._index is None:
            self._index = _RegistryIndex(self.list_source_snapshots(), self.list_external_sources())
        return self._index

    def _source_catalog_index(self) -> _RegistryIndex:
        if self._source_index is None:
            self._source_index = _RegistryIndex(self.list_source_snapshots(), [])
        return self._source_index

    def _external_catalog_index(self) -> _RegistryIndex:
        if self._external_index is None:
            self._external_index = _RegistryIndex([], self.list_external_sources())
        return self._external_index

    def list_sources(self) -> List[str]:
        return self._catalog_index().list_sources()

    def list_datasets(self, source: str) -> List[str]:
        return self._catalog_index().list_datasets(source)

    def list_downloads(self, source: str) -> List[str]:
        return self._catalog_index().list_downloads(source)

    def list_external_datasets(self, source: str) -> List[str]:
        return self._catalog_index().list_external_datasets(source)

    def list_versions(self, source: str, dataset: str) -> List[str]:
        return self._catalog_index().list_versions(source, dataset)

    def list_files(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> List[RegistryEntry]:
        try:
            return self.get_source_snapshot(source, dataset, version).get("files", [])
        except LookupError:
            if self.is_external_source(source, dataset, version):
                return []
            raise

    def is_source_snapshot(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> bool:
        index = self._source_catalog_index()
        if version is not None:
            return (source, dataset, version) in index.source_by_version
        return (source, dataset) in index.source_by_dataset

    def is_external_source(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> bool:
        index = self._external_catalog_index()
        if version is not None:
            return (source, dataset, version) in index.external_by_version
        return (source, dataset) in index.external_by_dataset

    def get_source_snapshot(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> RegistryEntry:
        index = self._source_catalog_index()
        matches = self._matches(
            by_dataset=index.source_by_dataset,
            by_version=index.source_by_version,
            source=source,
            dataset=dataset,
            version=version,
        )
        return self._single_match(
            matches,
            kind="source snapshot",
            source=source,
            dataset=dataset,
            version=version,
        )

    def get_external_source(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> RegistryEntry:
        index = self._external_catalog_index()
        matches = self._matches(
            by_dataset=index.external_by_dataset,
            by_version=index.external_by_version,
            source=source,
            dataset=dataset,
            version=version,
        )
        return self._single_match(
            matches,
            kind="external source",
            source=source,
            dataset=dataset,
            version=version,
        )

    @staticmethod
    def _single_match(
        matches: List[RegistryEntry],
        *,
        kind: str,
        source: str,
        dataset: str,
        version: Optional[str],
    ) -> RegistryEntry:
        label = f"{kind} {source}/{dataset}"
        if version is not None:
            label = f"{label}/{version}"
        if not matches:
            raise LookupError(f"No registered {label}")
        if len(matches) > 1:
            versions = ", ".join(sorted(str(match.get("version")) for match in matches))
            raise ValueError(f"Multiple registered {label} entries match; specify version. Versions: {versions}")
        return matches[0]

    @staticmethod
    def _matches(
        *,
        by_dataset: Dict[RegistryKey, List[RegistryEntry]],
        by_version: Dict[VersionedRegistryKey, RegistryEntry],
        source: str,
        dataset: str,
        version: Optional[str],
    ) -> List[RegistryEntry]:
        if version is not None:
            match = by_version.get((source, dataset, version))
            return [match] if match else []
        return by_dataset.get((source, dataset), [])
