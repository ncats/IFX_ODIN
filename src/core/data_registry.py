from dataclasses import dataclass, field
from datetime import date
from copy import deepcopy
import hashlib
import importlib
import json
from pathlib import Path
import shutil
from typing import Any, Dict, List, Optional, Tuple

import yaml

from src.registry.fetchers import (
    DerivedArtifact,
    DerivedArtifactBuilder,
    ExternalSourceProvider,
    ExternalSourceRegistration,
    MaterializedDataset,
    ResolvedDependency,
    SourceFetcher,
    SourceSnapshot,
)
from src.registry.manifest import (
    MANIFEST_FILENAME,
    build_derived_snapshot_manifest,
    build_resolver_snapshot_manifest,
    build_source_snapshot_manifest,
    derived_storage_prefix,
    file_entry,
    iso_timestamp,
    manifest_checksum,
    read_manifest,
    resolver_storage_prefix,
    storage_prefix,
    today_utc,
    verify_manifest_files,
    write_manifest,
)
from src.registry.storage import DEFAULT_REGISTRY_BUCKET, MinioStorage, load_minio_credentials, s3_uri
from src.shared.db_credentials import DBCredentials


REGISTRY_SOURCES_CONFIG = Path(__file__).parents[1] / "registry" / "registry_sources.yaml"
REGISTRY_RESOLVERS_CONFIG = Path(__file__).parents[1] / "registry" / "registry_resolvers.yaml"
REPO_ROOT = Path(__file__).parents[2]
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
        resolvers_config_path: Path = REGISTRY_RESOLVERS_CONFIG,
    ):
        self.storage = storage
        self.sources_config_path = sources_config_path
        self.resolvers_config_path = resolvers_config_path
        self._source_snapshots: Optional[List[RegistryEntry]] = None
        self._external_sources: Optional[List[RegistryEntry]] = None
        self._derived_artifacts: Optional[List[RegistryEntry]] = None
        self._resolver_snapshots: Optional[List[RegistryEntry]] = None
        self._index: Optional[_RegistryIndex] = None
        self._source_index: Optional[_RegistryIndex] = None
        self._external_index: Optional[_RegistryIndex] = None
        self._sources_config: Optional[dict] = None
        self._resolvers_config: Optional[dict] = None

    @classmethod
    def local(
        cls,
        *,
        sources_config_path: Path = REGISTRY_SOURCES_CONFIG,
        resolvers_config_path: Path = REGISTRY_RESOLVERS_CONFIG,
    ) -> "DataRegistry":
        return cls(
            storage=None,
            sources_config_path=sources_config_path,
            resolvers_config_path=resolvers_config_path,
        )

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

    def list_derived_artifacts(self) -> List[RegistryEntry]:
        self._require_storage()
        if self._derived_artifacts is None:
            self._derived_artifacts = self._list_derived_artifacts()
        return self._derived_artifacts

    def list_resolver_snapshots(self) -> List[RegistryEntry]:
        self._require_storage()
        if self._resolver_snapshots is None:
            self._resolver_snapshots = self._list_resolver_snapshots()
        return self._resolver_snapshots

    def refresh_catalog(self) -> None:
        self._require_storage()
        self._source_snapshots = self._list_source_snapshots()
        self._external_sources = self._list_external_sources()
        self._derived_artifacts = self._list_derived_artifacts()
        self._resolver_snapshots = self._list_resolver_snapshots()
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

    def _list_derived_artifacts(self) -> List[RegistryEntry]:
        manifests = []
        for key in self.storage.list_keys("derived/"):
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
                "created_at": manifest.get("created_at"),
                "derived_from": manifest.get("derived_from") or [],
                "transform": manifest.get("transform") or {},
                "build_key": manifest.get("build_key"),
                "files": files,
                "stats": manifest.get("stats") or {},
                "total_size_bytes": total_size_bytes,
                "total_size": self.format_size(total_size_bytes),
                "manifest_uri": s3_uri(self.storage.bucket, key),
                "manifest": manifest,
                "manifest_yaml": yaml.safe_dump(manifest, sort_keys=False),
            })
        return self._sort_registry_entries(manifests)

    def _list_resolver_snapshots(self) -> List[RegistryEntry]:
        manifests = []
        for key in self.storage.list_keys("resolvers/"):
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
                "resolver": manifest.get("resolver"),
                "dataset": manifest.get("resolver"),
                "version": manifest.get("version"),
                "created_at": manifest.get("created_at"),
                "definition": manifest.get("definition") or {},
                "accepted_types": (manifest.get("definition") or {}).get("accepted_types") or [],
                "definition_fingerprint": manifest.get("definition_fingerprint"),
                "resolved_inputs": manifest.get("resolved_inputs") or {},
                "resolved_input_metadata": manifest.get("resolved_input_metadata") or {},
                "build_key": manifest.get("build_key"),
                "files": files,
                "stats": manifest.get("stats") or {},
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
            registration_id = manifest.get("registration_id")
            manifests.append({
                "registration_id": registration_id,
                "snapshot_id": registration_id,
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

    def materialize_source_snapshot(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
        *,
        dest: str | Path,
    ) -> MaterializedDataset:
        self._require_storage()
        snapshot = self.get_source_snapshot(source, dataset, version)
        local_dir = Path(dest) / source / dataset / snapshot["version"]
        local_dir.mkdir(parents=True, exist_ok=True)
        prefix = storage_prefix(source, dataset, snapshot["version"])
        for entry in snapshot.get("files", []) or []:
            local_path = local_dir / entry["path"]
            if self._local_file_matches_entry(local_path, entry):
                print(f"Using cached registry file {local_path}", flush=True)
                continue
            key = self._storage_key_for_entry(entry, prefix)
            print(f"Downloading registry file {key} -> {local_path}", flush=True)
            self.storage.download_file(key, local_path)
        return MaterializedDataset(
            source=source,
            dataset=dataset,
            version=snapshot["version"],
            version_date=snapshot.get("version_date"),
            download_date=snapshot.get("download_date"),
            snapshot_id=snapshot["snapshot_id"],
            manifest_uri=snapshot["manifest_uri"],
            manifest=snapshot["manifest"],
            local_dir=local_dir,
        )

    def materialize_derived_artifact(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
        *,
        dest: str | Path,
    ) -> MaterializedDataset:
        self._require_storage()
        artifact = self.get_derived_artifact(source, dataset, version)
        local_dir = Path(dest) / source / dataset / artifact["version"]
        local_dir.mkdir(parents=True, exist_ok=True)
        prefix = derived_storage_prefix(source, dataset, artifact["version"])
        for entry in artifact.get("files", []) or []:
            local_path = local_dir / entry["path"]
            if self._local_file_matches_entry(local_path, entry):
                print(f"Using cached registry file {local_path}", flush=True)
                continue
            key = self._storage_key_for_entry(entry, prefix)
            print(f"Downloading registry file {key} -> {local_path}", flush=True)
            self.storage.download_file(key, local_path)
        return MaterializedDataset(
            source=source,
            dataset=dataset,
            version=artifact["version"],
            version_date=artifact.get("version_date"),
            download_date=artifact.get("created_at"),
            snapshot_id=artifact["snapshot_id"],
            manifest_uri=artifact["manifest_uri"],
            manifest=artifact["manifest"],
            local_dir=local_dir,
        )

    def materialize_resolver_snapshot(
        self,
        source: str,
        resolver: str,
        version: Optional[str] = None,
        *,
        dest: str | Path,
    ) -> MaterializedDataset:
        self._require_storage()
        snapshot = self.get_resolver_snapshot(source, resolver, version)
        local_dir = Path(dest) / source / resolver / snapshot["version"]
        local_dir.mkdir(parents=True, exist_ok=True)
        prefix = resolver_storage_prefix(source, resolver, snapshot["version"])
        for entry in snapshot.get("files", []) or []:
            local_path = local_dir / entry["path"]
            if self._local_file_matches_entry(local_path, entry):
                print(f"Using cached registry file {local_path}", flush=True)
                continue
            key = self._storage_key_for_entry(entry, prefix)
            print(f"Downloading registry file {key} -> {local_path}", flush=True)
            self.storage.download_file(key, local_path)
        resolver_inputs = {}
        for input_name, resolved_ref in (snapshot.get("resolved_inputs") or {}).items():
            input_source, input_dataset, input_version = self._parse_registry_ref(resolved_ref)
            resolver_inputs[input_name] = self._materialize_registry_ref(
                input_source,
                input_dataset,
                input_version,
                dest=dest,
            )
        return MaterializedDataset(
            source=source,
            dataset=resolver,
            version=snapshot["version"],
            version_date=None,
            download_date=snapshot.get("created_at"),
            snapshot_id=snapshot["snapshot_id"],
            manifest_uri=snapshot["manifest_uri"],
            manifest=snapshot["manifest"],
            local_dir=local_dir,
            resolver_inputs=resolver_inputs,
        )

    def materialize_external_source(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
        *,
        dest: str | Path,
    ) -> MaterializedDataset:
        self._require_storage()
        registration = self.get_external_source(source, dataset, version)
        local_dir = Path(dest) / source / dataset / registration["version"]
        local_dir.mkdir(parents=True, exist_ok=True)
        return MaterializedDataset(
            source=source,
            dataset=dataset,
            version=registration["version"],
            version_date=registration.get("version_date"),
            download_date=registration.get("registered_date"),
            snapshot_id=registration["registration_id"],
            manifest_uri=registration["manifest_uri"],
            manifest=registration["manifest"],
            local_dir=local_dir,
        )

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

    def build_derived_artifact(
        self,
        source: str,
        dataset: str,
        *,
        dest: str | Path,
    ) -> Path:
        dataset_config = self.get_dataset_config(source, dataset)
        derived_config = dataset_config.get("derived") or {}
        builder = self._load_derived_builder(source, dataset, derived_config)
        cache_root = Path(dest)
        version = self._derived_version(derived_config)
        work_dir = cache_root / "_registry_work" / "derived" / source / dataset / version
        work_dir.mkdir(parents=True, exist_ok=True)
        transform = self._derived_transform_metadata(derived_config)
        dependencies = self._resolve_derived_dependencies(
            derived_config,
            dest=cache_root,
            stage_files=True,
        )
        build_key = self._derived_build_key_from_dependencies(dependencies, transform)
        builder_config = deepcopy(derived_config)
        builder_config["transform"] = transform
        artifact = builder.build(
            config=builder_config,
            dependencies=dependencies,
            dest=work_dir / "output",
            version=version,
        )
        if not isinstance(artifact, DerivedArtifact):
            raise TypeError(f"Derived builder for {source}/{dataset} returned {type(artifact).__name__}; expected DerivedArtifact")
        artifact.build_key = artifact.build_key or build_key
        return self.write_derived_artifact(artifact, dest=cache_root)

    def write_derived_artifact(self, artifact: DerivedArtifact, *, dest: str | Path) -> Path:
        final_dir = Path(dest) / artifact.source / artifact.dataset / artifact.version
        final_dir.mkdir(parents=True, exist_ok=True)

        entries = []
        for artifact_file in artifact.files:
            local_path = Path(artifact_file.path)
            final_path = final_dir / local_path.name
            if local_path.resolve() != final_path.resolve():
                shutil.move(str(local_path), final_path)
            entries.append(
                file_entry(
                    local_path=final_path,
                    source_url=f"derived://{artifact.source}/{artifact.dataset}/{artifact.version}/{final_path.name}",
                    storage_uri=None,
                    content_type=artifact_file.content_type,
                )
            )

        manifest = build_derived_snapshot_manifest(
            source=artifact.source,
            dataset=artifact.dataset,
            version=artifact.version,
            version_date=artifact.version_date,
            derived_from=artifact.derived_from,
            transform=artifact.transform,
            build_key=artifact.build_key,
            files=entries,
            stats=artifact.stats,
            extra=artifact.extra,
        )
        manifest_path = final_dir / MANIFEST_FILENAME
        write_manifest(manifest, manifest_path)
        print(f"Wrote {manifest_path}")
        print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
        return manifest_path

    def upload_derived_artifact(self, manifest_path: str | Path) -> List[str]:
        self._require_storage()
        manifest_path = Path(manifest_path)
        verify_manifest_files(manifest_path)
        manifest = read_manifest(manifest_path)
        source = manifest["source"]
        dataset = manifest["dataset"]
        version = manifest["version"]
        prefix = derived_storage_prefix(source, dataset, version)
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

    def register_derived_artifact(
        self,
        source: str,
        dataset: str,
        *,
        dest: str | Path,
        upload: bool = True,
    ) -> Path:
        manifest_path = self.build_derived_artifact(source, dataset, dest=dest)
        if upload:
            self.upload_derived_artifact(manifest_path)
        return manifest_path

    def check_derived_artifacts(self) -> List[RegistryEntry]:
        self._require_storage()
        statuses: List[RegistryEntry] = []
        for source, source_config in sorted((self._load_sources_config().get("sources") or {}).items()):
            for dataset, dataset_config in sorted((source_config.get("datasets") or {}).items()):
                derived_config = dataset_config.get("derived") or {}
                if not derived_config:
                    continue
                registered_versions = self.list_derived_versions(source, dataset)
                status: RegistryEntry = {
                    "source": source,
                    "dataset": dataset,
                    "latest_version": None,
                    "latest_build_key": None,
                    "registered_versions": registered_versions,
                    "registered_build_keys": [],
                    "latest_registered_version": registered_versions[-1] if registered_versions else None,
                    "days_since_last_update": self._days_since_version(registered_versions[-1]) if registered_versions else None,
                    "is_latest_registered": False,
                    "error": None,
                }
                try:
                    status["latest_version"] = self._derived_version(derived_config)
                    status["latest_build_key"] = self._derived_build_key(derived_config)
                    registered_artifacts = [
                        entry
                        for entry in self.list_derived_artifacts()
                        if entry.get("source") == source and entry.get("dataset") == dataset
                    ]
                    status["registered_build_keys"] = [
                        entry.get("build_key")
                        for entry in registered_artifacts
                        if entry.get("build_key")
                    ]
                    status["is_latest_registered"] = any(
                        entry.get("version") == status["latest_version"]
                        and entry.get("build_key") == status["latest_build_key"]
                        for entry in registered_artifacts
                    )
                except Exception as exc:
                    status["error"] = str(exc)
                statuses.append(status)
        return statuses

    def sync_derived_artifacts(
        self,
        *,
        dest: Optional[str | Path] = None,
        min_days_since_last_update: int = 0,
        dry_run: bool = True,
    ) -> List[RegistryEntry]:
        if not dry_run and dest is None:
            raise ValueError("dest is required when dry_run=False")
        results: List[RegistryEntry] = []
        for status in self.check_derived_artifacts():
            reason = self._sync_candidate_reason(
                status,
                min_days_since_last_update=min_days_since_last_update,
            )
            if reason is None:
                continue
            result = dict(status)
            result["sync_reason"] = reason
            result["sync_action"] = "would_build" if dry_run else "build"
            result["manifest_path"] = None
            result["sync_error"] = None
            result["dependency_cache"] = None
            if dest is not None:
                try:
                    dataset_config = self.get_dataset_config(status["source"], status["dataset"])
                    result["dependency_cache"] = self._derived_dependency_cache_status(
                        dataset_config.get("derived") or {},
                        dest=Path(dest),
                    )
                except Exception as exc:
                    result["dependency_cache_error"] = str(exc)
            if not dry_run:
                try:
                    print(
                        f"Building derived artifact {status['source']}/{status['dataset']} "
                        f"(reason: {reason})",
                        flush=True,
                    )
                    manifest_path = self.register_derived_artifact(
                        status["source"],
                        status["dataset"],
                        dest=dest,
                        upload=True,
                    )
                    result["sync_action"] = "built"
                    result["manifest_path"] = str(manifest_path)
                    print(
                        f"Built derived artifact {status['source']}/{status['dataset']} -> {manifest_path}",
                        flush=True,
                    )
                except Exception as exc:
                    result["sync_action"] = "failed"
                    result["sync_error"] = str(exc)
                    print(
                        f"Failed building derived artifact {status['source']}/{status['dataset']}: {exc}",
                        flush=True,
                    )
            results.append(result)
        return results

    def build_resolver_snapshot_manifest(
        self,
        source: str,
        resolver: str,
    ) -> RegistryEntry:
        definition = self._resolver_definition_metadata(source, resolver)
        resolved_inputs, resolved_input_metadata = self._resolve_resolver_inputs(definition)
        definition_fingerprint = self._resolver_definition_fingerprint(definition)
        build_key = self._resolver_build_key(
            definition_fingerprint=definition_fingerprint,
            resolved_input_metadata=resolved_input_metadata,
        )
        version = f"deps-{build_key[:12]}"
        return build_resolver_snapshot_manifest(
            source=source,
            resolver=resolver,
            version=version,
            definition=definition,
            definition_fingerprint=definition_fingerprint,
            resolved_inputs=resolved_inputs,
            resolved_input_metadata=resolved_input_metadata,
            files=[],
            build_key=build_key,
        )

    def write_resolver_snapshot(self, manifest: RegistryEntry, *, dest: str | Path) -> Path:
        final_dir = Path(dest) / manifest["source"] / manifest["resolver"] / manifest["version"]
        final_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = final_dir / MANIFEST_FILENAME
        write_manifest(manifest, manifest_path)
        print(f"Wrote {manifest_path}")
        print(f"Manifest sha256: {manifest_checksum(manifest_path)}")
        return manifest_path

    def upload_resolver_snapshot(self, manifest_path: str | Path) -> List[str]:
        self._require_storage()
        manifest_path = Path(manifest_path)
        verify_manifest_files(manifest_path)
        manifest = read_manifest(manifest_path)
        source = manifest["source"]
        resolver = manifest["resolver"]
        version = manifest["version"]
        prefix = resolver_storage_prefix(source, resolver, version)
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

    def register_resolver_snapshot(
        self,
        source: str,
        resolver: str,
        *,
        dest: str | Path,
        upload: bool = True,
    ) -> Path:
        manifest = self.build_resolver_snapshot_manifest(source, resolver)
        manifest_path = self.write_resolver_snapshot(manifest, dest=dest)
        if upload:
            self.upload_resolver_snapshot(manifest_path)
        return manifest_path

    def check_resolvers(self) -> List[RegistryEntry]:
        self._require_storage()
        statuses: List[RegistryEntry] = []
        for source, resolver, _config in self._resolver_definition_entries():
            registered_versions = self.list_resolver_versions(source, resolver)
            status: RegistryEntry = {
                "source": source,
                "resolver": resolver,
                "latest_version": None,
                "latest_build_key": None,
                "registered_versions": registered_versions,
                "registered_build_keys": [],
                "latest_registered_version": registered_versions[-1] if registered_versions else None,
                "days_since_last_update": self._days_since_version(registered_versions[-1]) if registered_versions else None,
                "is_latest_registered": False,
                "error": None,
            }
            try:
                manifest = self.build_resolver_snapshot_manifest(source, resolver)
                status["latest_version"] = manifest["version"]
                status["latest_build_key"] = manifest["build_key"]
                registered_snapshots = [
                    entry
                    for entry in self.list_resolver_snapshots()
                    if entry.get("source") == source and entry.get("resolver") == resolver
                ]
                status["registered_build_keys"] = [
                    entry.get("build_key")
                    for entry in registered_snapshots
                    if entry.get("build_key")
                ]
                status["is_latest_registered"] = any(
                    entry.get("version") == status["latest_version"]
                    and entry.get("build_key") == status["latest_build_key"]
                    for entry in registered_snapshots
                )
            except Exception as exc:
                status["error"] = str(exc)
            statuses.append(status)
        return statuses

    def sync_resolvers(
        self,
        *,
        dest: Optional[str | Path] = None,
        min_days_since_last_update: int = 0,
        dry_run: bool = True,
    ) -> List[RegistryEntry]:
        if not dry_run and dest is None:
            raise ValueError("dest is required when dry_run=False")
        results: List[RegistryEntry] = []
        for status in self.check_resolvers():
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
                        f"Registering resolver snapshot {status['source']}/{status['resolver']} "
                        f"(reason: {reason})",
                        flush=True,
                    )
                    manifest_path = self.register_resolver_snapshot(
                        status["source"],
                        status["resolver"],
                        dest=dest,
                        upload=True,
                    )
                    result["sync_action"] = "registered"
                    result["manifest_path"] = str(manifest_path)
                    print(
                        f"Registered resolver snapshot {status['source']}/{status['resolver']} -> {manifest_path}",
                        flush=True,
                    )
                except Exception as exc:
                    result["sync_action"] = "failed"
                    result["sync_error"] = str(exc)
                    print(
                        f"Failed registering resolver snapshot {status['source']}/{status['resolver']}: {exc}",
                        flush=True,
                    )
            results.append(result)
        return results

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

    def _load_derived_builder(self, source: str, dataset: str, derived_config: dict) -> DerivedArtifactBuilder:
        module_name = derived_config.get("module")
        if not module_name:
            raise ValueError(f"Missing derived artifact module for {source}/{dataset}")
        module = importlib.import_module(module_name)

        class_name = derived_config.get("class")
        if not class_name:
            raise ValueError(f"Missing derived artifact class for {source}/{dataset}")
        builder = getattr(module, class_name)()
        if not isinstance(builder, DerivedArtifactBuilder):
            raise TypeError(f"Derived artifact builder {class_name} must implement DerivedArtifactBuilder")
        if getattr(builder, "source", None) != source or getattr(builder, "dataset", None) != dataset:
            raise ValueError(f"Derived artifact builder {class_name} does not match configured dataset {source}/{dataset}")
        return builder

    def _resolve_derived_dependencies(
        self,
        derived_config: dict,
        *,
        dest: Optional[Path] = None,
        stage_files: bool = False,
    ) -> List[ResolvedDependency]:
        self._require_storage()
        dependencies = []
        for dependency_config in derived_config.get("dependencies") or []:
            source = dependency_config["source"]
            dataset = dependency_config["dataset"]
            version = dependency_config.get("version")
            snapshot = self.get_source_snapshot(source, dataset, version)
            dependency_dir = None
            if stage_files:
                if dest is None:
                    raise ValueError("dest is required when stage_files=True")
                dependency_dir = dest / source / dataset / snapshot["version"]
                dependency_dir.mkdir(parents=True, exist_ok=True)
                prefix = storage_prefix(source, dataset, snapshot["version"])
                for entry in snapshot.get("files", []) or []:
                    local_path = dependency_dir / entry["path"]
                    if self._local_file_matches_entry(local_path, entry):
                        print(f"Using cached dependency file {local_path}", flush=True)
                        continue
                    key = self._storage_key_for_entry(entry, prefix)
                    self.storage.download_file(key, local_path)
            dependencies.append(
                ResolvedDependency(
                    source=source,
                    dataset=dataset,
                    version=snapshot["version"],
                    snapshot_id=snapshot["snapshot_id"],
                    manifest_uri=snapshot["manifest_uri"],
                    manifest=snapshot["manifest"],
                    local_dir=dependency_dir,
                )
            )
        return dependencies

    def _derived_dependency_cache_status(
        self,
        derived_config: dict,
        *,
        dest: Path,
    ) -> List[RegistryEntry]:
        entries = []
        for dependency in self._resolve_derived_dependencies(derived_config, stage_files=False):
            dependency_dir = dest / dependency.source / dependency.dataset / dependency.version
            for file_entry_ in dependency.manifest.get("files", []) or []:
                local_path = dependency_dir / file_entry_["path"]
                cached = self._local_file_matches_entry(local_path, file_entry_)
                entries.append({
                    "source": dependency.source,
                    "dataset": dependency.dataset,
                    "version": dependency.version,
                    "snapshot_id": dependency.snapshot_id,
                    "path": file_entry_["path"],
                    "local_path": str(local_path),
                    "cached": cached,
                    "would_download": not cached,
                    "size_bytes": file_entry_.get("size_bytes"),
                    "sha256": file_entry_.get("sha256"),
                })
        return entries

    def _derived_version(self, derived_config: dict) -> str:
        dependencies = self._resolve_derived_dependencies(derived_config, stage_files=False)
        transform = self._derived_transform_metadata(derived_config)
        if len(dependencies) == 1:
            return str(dependencies[0].version)
        fingerprint = self._derived_fingerprint(dependencies, transform)
        return f"deps-{fingerprint}"

    def _derived_build_key(self, derived_config: dict) -> str:
        dependencies = self._resolve_derived_dependencies(derived_config, stage_files=False)
        transform = self._derived_transform_metadata(derived_config)
        return self._derived_build_key_from_dependencies(dependencies, transform)

    @staticmethod
    def _derived_build_key_from_dependencies(
        dependencies: List[ResolvedDependency],
        transform: Dict[str, Any],
    ) -> str:
        return DataRegistry._derived_fingerprint(dependencies, transform, length=None)

    @staticmethod
    def _derived_transform_metadata(derived_config: dict) -> Dict[str, Any]:
        transform = deepcopy(derived_config.get("transform") or {})
        code_ref = transform.get("code_ref")
        if code_ref:
            code_path = Path(code_ref)
            if not code_path.is_absolute():
                code_path = REPO_ROOT / code_path
            transform["code_sha256"] = manifest_checksum(code_path)
        return transform

    @staticmethod
    def _derived_fingerprint(
        dependencies: List[ResolvedDependency],
        transform: Dict[str, Any],
        *,
        length: Optional[int] = 12,
    ) -> str:
        payload = {
            "dependencies": sorted(dependency.snapshot_id for dependency in dependencies),
            "transform": transform,
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        digest = hashlib.sha256(encoded).hexdigest()
        if length is None:
            return digest
        return digest[:length]

    def _resolver_definition_entries(self) -> List[Tuple[str, str, RegistryEntry]]:
        entries = []
        for source, source_config in sorted((self._load_resolvers_config().get("resolvers") or {}).items()):
            for resolver, resolver_config in sorted((source_config or {}).items()):
                entries.append((source, resolver, resolver_config or {}))
        return entries

    def get_resolver_definition_config(self, source: str, resolver: str) -> RegistryEntry:
        source_config = (self._load_resolvers_config().get("resolvers") or {}).get(source)
        if not source_config:
            raise LookupError(f"No registry resolver source configured for {source}")
        resolver_config = (source_config or {}).get(resolver)
        if not resolver_config:
            raise LookupError(f"No registry resolver configured for {source}/{resolver}")
        return resolver_config

    def _resolver_definition_metadata(self, source: str, resolver: str) -> RegistryEntry:
        definition = deepcopy(self.get_resolver_definition_config(source, resolver))
        definition["source"] = source
        definition["resolver"] = resolver
        definition["accepted_types"] = self._normalize_accepted_types(definition.get("accepted_types"))
        code_ref = definition.get("import")
        if code_ref:
            code_path = Path(code_ref)
            if not code_path.is_absolute():
                code_path = REPO_ROOT / code_path
            definition["code_sha256"] = manifest_checksum(code_path)
        return definition

    def _resolve_resolver_inputs(
        self,
        definition: RegistryEntry,
    ) -> Tuple[Dict[str, str], Dict[str, Dict[str, Any]]]:
        resolved_inputs: Dict[str, str] = {}
        resolved_input_metadata: Dict[str, Dict[str, Any]] = {}
        for input_name, logical_ref in (definition.get("inputs") or {}).items():
            source, dataset = self._parse_logical_registry_ref(logical_ref)
            snapshot = self._latest_source_snapshot(source, dataset)
            resolved_ref = f"{source}:{dataset}:{snapshot['version']}"
            resolved_inputs[input_name] = resolved_ref
            resolved_input_metadata[input_name] = {
                "source": source,
                "dataset": dataset,
                "version": snapshot["version"],
                "snapshot_id": snapshot["snapshot_id"],
                "manifest_uri": snapshot["manifest_uri"],
            }
        return resolved_inputs, resolved_input_metadata

    @staticmethod
    def _parse_logical_registry_ref(ref: str) -> Tuple[str, str]:
        parts = str(ref).split(":")
        if len(parts) != 2:
            raise ValueError(f"Registry resolver input must be source:dataset, got {ref!r}")
        return parts[0], parts[1]

    @staticmethod
    def _parse_registry_ref(ref: str) -> Tuple[str, str, str]:
        parts = str(ref).split(":")
        if len(parts) != 3:
            raise ValueError(f"Registry ref must be source:dataset:version, got {ref!r}")
        return parts[0], parts[1], parts[2]

    def _materialize_registry_ref(
        self,
        source: str,
        dataset: str,
        version: str,
        *,
        dest: str | Path,
    ) -> MaterializedDataset:
        try:
            return self.materialize_source_snapshot(source, dataset, version, dest=dest)
        except LookupError:
            try:
                return self.materialize_derived_artifact(source, dataset, version, dest=dest)
            except LookupError:
                return self.materialize_external_source(source, dataset, version, dest=dest)

    def _latest_source_snapshot(self, source: str, dataset: str) -> RegistryEntry:
        versions = [
            entry.get("version")
            for entry in self.list_source_snapshots()
            if entry.get("source") == source and entry.get("dataset") == dataset and entry.get("version")
        ]
        if not versions:
            raise LookupError(f"No registered source snapshot {source}/{dataset}")
        return self.get_source_snapshot(source, dataset, sorted(versions)[-1])

    @staticmethod
    def _resolver_definition_fingerprint(definition: RegistryEntry) -> str:
        fingerprint_definition = deepcopy(definition)
        fingerprint_definition.pop("type_sensitive", None)
        encoded = json.dumps(fingerprint_definition, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _normalize_accepted_types(accepted_types: Any) -> List[str]:
        if accepted_types is None:
            return []
        if not isinstance(accepted_types, list):
            raise ValueError("Resolver accepted_types must be a list")
        normalized = []
        for node_type in accepted_types:
            if not isinstance(node_type, str) or not node_type.strip():
                raise ValueError("Resolver accepted_types entries must be non-empty strings")
            normalized.append(node_type.strip())
        return sorted(dict.fromkeys(normalized))

    @staticmethod
    def _resolver_build_key(
        *,
        definition_fingerprint: str,
        resolved_input_metadata: Dict[str, Dict[str, Any]],
    ) -> str:
        payload = {
            "definition_fingerprint": definition_fingerprint,
            "resolved_inputs": {
                input_name: metadata.get("snapshot_id")
                for input_name, metadata in sorted(resolved_input_metadata.items())
            },
        }
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _local_file_matches_entry(local_path: Path, entry: RegistryEntry) -> bool:
        if not local_path.exists():
            return False
        expected_size = entry.get("size_bytes")
        if expected_size is not None and local_path.stat().st_size != expected_size:
            return False
        expected_sha256 = entry.get("sha256")
        if expected_sha256 and manifest_checksum(local_path) != expected_sha256:
            return False
        return True

    def _storage_key_for_entry(self, entry: RegistryEntry, default_prefix: str) -> str:
        storage_uri = entry.get("storage_uri")
        if storage_uri:
            expected_prefix = f"s3://{self.storage.bucket}/"
            if storage_uri.startswith(expected_prefix):
                return storage_uri[len(expected_prefix):]
        return f"{default_prefix}/{entry['path']}"

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

    def _load_resolvers_config(self) -> dict:
        if self._resolvers_config is None:
            with self.resolvers_config_path.open("r", encoding="utf-8") as handle:
                self._resolvers_config = yaml.safe_load(handle) or {}
        return self._resolvers_config

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

    def list_derived_versions(self, source: str, dataset: str) -> List[str]:
        return sorted(
            entry.get("version")
            for entry in self.list_derived_artifacts()
            if entry.get("source") == source and entry.get("dataset") == dataset and entry.get("version")
        )

    def list_resolver_versions(self, source: str, resolver: str) -> List[str]:
        return sorted(
            entry.get("version")
            for entry in self.list_resolver_snapshots()
            if entry.get("source") == source and entry.get("resolver") == resolver and entry.get("version")
        )

    def get_derived_artifact(
        self,
        source: str,
        dataset: str,
        version: Optional[str] = None,
    ) -> RegistryEntry:
        matches = [
            entry
            for entry in self.list_derived_artifacts()
            if entry.get("source") == source
            and entry.get("dataset") == dataset
            and (version is None or entry.get("version") == version)
        ]
        return self._single_match(
            matches,
            kind="derived artifact",
            source=source,
            dataset=dataset,
            version=version,
        )

    def get_resolver_snapshot(
        self,
        source: str,
        resolver: str,
        version: Optional[str] = None,
    ) -> RegistryEntry:
        matches = [
            entry
            for entry in self.list_resolver_snapshots()
            if entry.get("source") == source
            and entry.get("resolver") == resolver
            and (version is None or entry.get("version") == version)
        ]
        return self._single_match(
            matches,
            kind="resolver snapshot",
            source=source,
            dataset=resolver,
            version=version,
        )

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
