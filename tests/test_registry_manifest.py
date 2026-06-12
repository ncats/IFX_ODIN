import gzip
from datetime import date
from types import SimpleNamespace
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from src.registry.manifest import (
    build_source_snapshot_manifest,
    file_entry,
    parse_http_date_to_iso,
    read_manifest,
    snapshot_id,
    verify_manifest_files,
    write_manifest,
)
from src.registry.fetchers import ExternalSourceProvider, ExternalSourceRegistration, SnapshotFile, SourceFetcher, SourceSnapshot
from src.registry.storage import DEFAULT_REGISTRY_BUCKET, MinioStorage
from src.registry.sources.ctd import extract_report_created
from src.registry.sources.hcop import HCOP_FILE_NAME
from src.registry.sources.impc import IMPC_FILE_NAME
from src.registry.sources.jensenlab import JENSENLAB_DISEASE_URLS, JENSENLAB_TINX_URLS, JENSENLAB_TISSUES_FILE_NAME
from src.registry.sources.mgi import MGI_HMD_FILE_NAME
from src.registry.sources.mp import MP_FILE_NAME, extract_obo_data_version
from src.registry.sources.ncbi import NCBI_GENE_SUMMARY_URLS, NCBI_PUBLICATION_URLS
from src.registry.sources.external_sources import _version_token
from src.registry.sources.bioplex import latest_bioplex_version
from src.registry.sources.gtex import latest_gtex_version
from src.registry.sources.pathway_sources import latest_panther_version
from src.registry.sources.string import latest_string_version
from src.shared.db_credentials import DBCredentials
from src.core.data_registry import DataRegistry


def test_parse_http_date_to_iso():
    assert parse_http_date_to_iso("Wed, 10 Jun 2026 12:34:56 GMT") == "2026-06-10"


def test_source_snapshot_manifest_roundtrip(tmp_path: Path):
    data_path = tmp_path / "example.tsv"
    data_path.write_text("id\tname\n1\talpha\n", encoding="utf-8")
    entry = file_entry(
        local_path=data_path,
        source_url="https://example.org/example.tsv",
        storage_uri="s3://ifx-registry/sources/example/example/1/example.tsv",
    )
    manifest = build_source_snapshot_manifest(
        source="example",
        dataset="example",
        version="1",
        version_date="2026-06-10",
        download_date="2026-06-10",
        upstream_urls=["https://example.org/example.tsv"],
        files=[entry],
    )
    assert manifest["snapshot_id"] == snapshot_id("example", "example", "1")
    manifest_path = tmp_path / "manifest.yaml"
    write_manifest(manifest, manifest_path)
    verify_manifest_files(manifest_path)


def test_verify_manifest_files_catches_modified_file(tmp_path: Path):
    data_path = tmp_path / "example.tsv"
    data_path.write_text("original\n", encoding="utf-8")
    manifest = build_source_snapshot_manifest(
        source="example",
        dataset="example",
        version="1",
        version_date=None,
        download_date="2026-06-10",
        upstream_urls=["https://example.org/example.tsv"],
        files=[file_entry(data_path, "https://example.org/example.tsv")],
    )
    manifest_path = tmp_path / "manifest.yaml"
    write_manifest(manifest, manifest_path)
    data_path.write_text("changed\n", encoding="utf-8")

    with pytest.raises(ValueError, match="sha256 mismatch"):
        verify_manifest_files(manifest_path)


def test_minio_storage_defaults_to_registry_bucket():
    credentials = DBCredentials(url="http://localhost:9000", user="user", password="password", schema="odin-data")
    storage = MinioStorage(credentials)

    assert storage.bucket == DEFAULT_REGISTRY_BUCKET


def test_list_source_snapshots_from_manifest_keys():
    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            assert prefix == "sources/"
            return [
                "sources/ctd/curated_genes_diseases/2026-05-28/CTD_curated_genes_diseases.tsv.gz",
                "sources/ctd/curated_genes_diseases/2026-05-28/manifest.yaml",
            ]

        def read_text(self, key):
            assert key == "sources/ctd/curated_genes_diseases/2026-05-28/manifest.yaml"
            return """
kind: source_snapshot
schema_version: 1
source: ctd
dataset: curated_genes_diseases
snapshot_id: ctd:curated_genes_diseases:2026-05-28
version: '2026-05-28'
version_date: '2026-05-28'
download_date: '2026-06-10'
files:
  - path: CTD_curated_genes_diseases.tsv.gz
    size_bytes: 1536
"""

    snapshots = DataRegistry(FakeStorage()).list_source_snapshots()

    assert len(snapshots) == 1
    assert snapshots[0]["snapshot_id"] == "ctd:curated_genes_diseases:2026-05-28"
    assert snapshots[0]["total_size_bytes"] == 1536
    assert snapshots[0]["total_size"] == "1.5 KB"
    assert snapshots[0]["manifest_uri"] == "s3://ifx-registry/sources/ctd/curated_genes_diseases/2026-05-28/manifest.yaml"


def test_list_external_registrations_from_manifest_keys():
    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            assert prefix == "external/"
            return [
                "external/chembl/activity_database/chembl36/manifest.yaml",
            ]

        def read_text(self, key):
            assert key == "external/chembl/activity_database/chembl36/manifest.yaml"
            return """
kind: external_source_registration
schema_version: 1
source: chembl
dataset: activity_database
registration_id: chembl:activity_database:chembl36
version: chembl36
registered_date: '2026-06-10'
connection:
  type: mysql
  host: chembl.ncats.io
  schema: chembl36
access:
  mode: query
  interface: sql
  database_type: mysql
extra:
  version_method:
    type: database_schema
"""

    registrations = DataRegistry(FakeStorage()).list_external_sources()

    assert len(registrations) == 1
    assert registrations[0]["registration_id"] == "chembl:activity_database:chembl36"
    assert registrations[0]["connection_type"] == "mysql"
    assert registrations[0]["access"]["mode"] == "query"
    assert registrations[0]["manifest_uri"] == "s3://ifx-registry/external/chembl/activity_database/chembl36/manifest.yaml"


def test_data_registry_lists_and_gets_registered_sources():
    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return [
                    "sources/surechembl/patent_discovery/2026-06-01/manifest.yaml",
                    "sources/uniprot/human_proteome/2026_03/manifest.yaml",
                ]
            if prefix == "external/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            if key == "sources/surechembl/patent_discovery/2026-06-01/manifest.yaml":
                return """
kind: source_snapshot
schema_version: 1
source: surechembl
dataset: patent_discovery
snapshot_id: surechembl:patent_discovery:2026-06-01
version: '2026-06-01'
version_date: '2026-06-01'
download_date: '2026-06-10'
files:
  - path: patents.parquet
    size_bytes: 2048
"""
            if key == "sources/uniprot/human_proteome/2026_03/manifest.yaml":
                return """
kind: source_snapshot
schema_version: 1
source: uniprot
dataset: human_proteome
snapshot_id: uniprot:human_proteome:2026_03
version: '2026_03'
version_date: '2026-05-28'
download_date: '2026-06-10'
files: []
"""
            raise AssertionError(key)

    registry = DataRegistry(FakeStorage())

    snapshots = registry.list_source_snapshots()
    snapshot = registry.get_source_snapshot("surechembl", "patent_discovery")

    assert len(snapshots) == 2
    assert snapshot["snapshot_id"] == "surechembl:patent_discovery:2026-06-01"
    assert snapshot["total_size"] == "2.0 KB"


def test_data_registry_gets_external_source_registration():
    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "external/":
                return ["external/chembl/activity_database/chembl36/manifest.yaml"]
            if prefix == "sources/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            assert key == "external/chembl/activity_database/chembl36/manifest.yaml"
            return """
kind: external_source_registration
schema_version: 1
source: chembl
dataset: activity_database
registration_id: chembl:activity_database:chembl36
version: chembl36
registered_date: '2026-06-10'
connection:
  type: mysql
  host: chembl.ncats.io
access:
  mode: query
  interface: sql
"""

    registry = DataRegistry(FakeStorage())
    registration = registry.get_external_source("chembl", "activity_database", "chembl36")

    assert registration["registration_id"] == "chembl:activity_database:chembl36"
    assert registration["connection_type"] == "mysql"


def test_data_registry_lists_sources_datasets_and_versions():
    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return [
                    "sources/surechembl/patent_discovery/2026-06-01/manifest.yaml",
                    "sources/uniprot/human_proteome/2026_03/manifest.yaml",
                ]
            if prefix == "external/":
                return [
                    "external/chembl/activity_database/chembl36/manifest.yaml",
                    "external/uniprot/sparql_endpoint/current/manifest.yaml",
                ]
            raise AssertionError(prefix)

        def read_text(self, key):
            manifests = {
                "sources/surechembl/patent_discovery/2026-06-01/manifest.yaml": """
kind: source_snapshot
schema_version: 1
source: surechembl
dataset: patent_discovery
snapshot_id: surechembl:patent_discovery:2026-06-01
version: '2026-06-01'
files: []
""",
                "sources/uniprot/human_proteome/2026_03/manifest.yaml": """
kind: source_snapshot
schema_version: 1
source: uniprot
dataset: human_proteome
snapshot_id: uniprot:human_proteome:2026_03
version: '2026_03'
files: []
""",
                "external/chembl/activity_database/chembl36/manifest.yaml": """
kind: external_source_registration
schema_version: 1
source: chembl
dataset: activity_database
registration_id: chembl:activity_database:chembl36
version: chembl36
connection:
  type: mysql
""",
                "external/uniprot/sparql_endpoint/current/manifest.yaml": """
kind: external_source_registration
schema_version: 1
source: uniprot
dataset: sparql_endpoint
registration_id: uniprot:sparql_endpoint:current
version: current
connection:
  type: sparql
""",
            }
            return manifests[key]

    registry = DataRegistry(FakeStorage())

    assert registry.list_sources() == ["chembl", "surechembl", "uniprot"]
    assert registry.list_datasets("uniprot") == ["human_proteome", "sparql_endpoint"]
    assert registry.list_downloads("uniprot") == ["human_proteome"]
    assert registry.list_external_datasets("uniprot") == ["sparql_endpoint"]
    assert registry.list_versions("surechembl", "patent_discovery") == ["2026-06-01"]
    assert registry.list_versions("chembl", "activity_database") == ["chembl36"]
    assert registry.is_source_snapshot("surechembl", "patent_discovery", "2026-06-01")
    assert not registry.is_source_snapshot("chembl", "activity_database", "chembl36")
    assert registry.is_external_source("chembl", "activity_database", "chembl36")
    assert not registry.is_external_source("surechembl", "patent_discovery", "2026-06-01")
    assert registry.list_files("surechembl", "patent_discovery", "2026-06-01") == []
    assert registry.list_files("chembl", "activity_database", "chembl36") == []


def test_data_registry_requires_version_for_ambiguous_lookup():
    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            assert prefix == "sources/"
            return [
                "sources/uniprot/human_proteome/2026_02/manifest.yaml",
                "sources/uniprot/human_proteome/2026_03/manifest.yaml",
            ]

        def read_text(self, key):
            version = key.split("/")[-2]
            return f"""
kind: source_snapshot
schema_version: 1
source: uniprot
dataset: human_proteome
snapshot_id: uniprot:human_proteome:{version}
version: '{version}'
files: []
"""

    registry = DataRegistry(FakeStorage())

    with pytest.raises(ValueError, match="specify version"):
        registry.get_source_snapshot("uniprot", "human_proteome")

    snapshot = registry.get_source_snapshot("uniprot", "human_proteome", "2026_03")
    assert snapshot["version"] == "2026_03"


def test_data_registry_caches_catalog_reads_until_refresh():
    class FakeStorage:
        bucket = "ifx-registry"

        def __init__(self):
            self.calls = {"sources/": 0, "external/": 0, "derived/": 0}

        def list_keys(self, prefix):
            self.calls[prefix] += 1
            if prefix == "sources/":
                return ["sources/surechembl/patent_discovery/2026-06-01/manifest.yaml"]
            if prefix == "external/":
                return ["external/chembl/activity_database/chembl36/manifest.yaml"]
            if prefix == "derived/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            if key == "sources/surechembl/patent_discovery/2026-06-01/manifest.yaml":
                return """
kind: source_snapshot
schema_version: 1
source: surechembl
dataset: patent_discovery
snapshot_id: surechembl:patent_discovery:2026-06-01
version: '2026-06-01'
files: []
"""
            if key == "external/chembl/activity_database/chembl36/manifest.yaml":
                return """
kind: external_source_registration
schema_version: 1
source: chembl
dataset: activity_database
registration_id: chembl:activity_database:chembl36
version: chembl36
connection:
  type: mysql
"""
            raise AssertionError(key)

    storage = FakeStorage()
    registry = DataRegistry(storage)

    registry.list_sources()
    registry.list_sources()
    registry.list_datasets("surechembl")
    registry.list_versions("chembl", "activity_database")
    registry.list_files("surechembl", "patent_discovery", "2026-06-01")

    assert storage.calls == {"sources/": 1, "external/": 1, "derived/": 0}

    registry.refresh_catalog()

    assert storage.calls == {"sources/": 2, "external/": 2, "derived/": 1}


def test_data_registry_fetches_configured_fetcher_class(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      dataset:
        fetch:
          module: fake_fetcher_module
          class: ExampleFetcher
""",
        encoding="utf-8",
    )
    calls = []
    class ExampleFetcher(SourceFetcher):
        source = "example"
        dataset = "dataset"

        def fetch(self, *, dest, timeout):
            calls.append({
                "dest": dest,
                "timeout": timeout,
            })
            data_path = Path(dest) / "example.tsv"
            data_path.write_text("id\n1\n", encoding="utf-8")
            return SourceSnapshot(
                source="example",
                dataset="dataset",
                version="v1",
                version_date=None,
                homepage=None,
                upstream_urls=["https://example.org/example.tsv"],
                files=[SnapshotFile(data_path, "https://example.org/example.tsv", "text/tab-separated-values")],
            )

        def get_latest_version(self, *, timeout):
            return "v1"

    def fake_import_module(module_name):
        assert module_name == "fake_fetcher_module"
        return SimpleNamespace(ExampleFetcher=ExampleFetcher)

    monkeypatch.setattr("src.core.data_registry.importlib.import_module", fake_import_module)
    registry = DataRegistry(FakeStorageForRegistry(), sources_config_path=config_path)

    result = registry.fetch_dataset("example", "dataset", dest=tmp_path / "cache")

    assert result == tmp_path / "cache" / "example" / "dataset" / "v1" / "manifest.yaml"
    assert calls == [{
        "dest": tmp_path / "cache" / "_registry_work" / "example" / "dataset",
        "timeout": 60,
    }]


def test_data_registry_fetcher_class_must_match_dataset(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      dataset:
        fetch:
          module: fake_fetcher_module
          class: ExampleFetcher
""",
        encoding="utf-8",
    )

    class ExampleFetcher(SourceFetcher):
        source = "other"
        dataset = "dataset"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            return "v1"

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(ExampleFetcher=ExampleFetcher),
    )
    registry = DataRegistry(FakeStorageForRegistry(), sources_config_path=config_path)

    with pytest.raises(ValueError, match="does not match"):
        registry.fetch_dataset("example", "dataset", dest=tmp_path / "cache")


def test_data_registry_reports_whether_latest_version_is_registered(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      dataset:
        fetch:
          module: fake_fetcher_module
          class: ExampleFetcher
""",
        encoding="utf-8",
    )

    class ExampleFetcher(SourceFetcher):
        source = "example"
        dataset = "dataset"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            assert timeout == 60
            return "v2"

    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return ["sources/example/dataset/v1/manifest.yaml"]
            if prefix == "external/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            return """
kind: source_snapshot
schema_version: 1
source: example
dataset: dataset
snapshot_id: example:dataset:v1
version: v1
files: []
"""

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(ExampleFetcher=ExampleFetcher),
    )
    registry = DataRegistry(FakeStorage(), sources_config_path=config_path)

    assert registry.get_latest_version("example", "dataset") == "v2"
    assert registry.is_latest_registered("example", "dataset") is False


def test_data_registry_checks_all_latest_registered(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      registered:
        fetch:
          module: fake_fetcher_module
          class: RegisteredFetcher
          version_strategy: api_listcache_id
      failing:
        fetch:
          module: fake_fetcher_module
          class: FailingFetcher
""",
        encoding="utf-8",
    )

    class RegisteredFetcher(SourceFetcher):
        source = "example"
        dataset = "registered"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            assert timeout == 60
            return "v1"

    class FailingFetcher(SourceFetcher):
        source = "example"
        dataset = "failing"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            raise RuntimeError("probe failed")

    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return ["sources/example/registered/v1/manifest.yaml"]
            if prefix == "external/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            return """
kind: source_snapshot
schema_version: 1
source: example
dataset: registered
snapshot_id: example:registered:v1
version: v1
files: []
"""

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(RegisteredFetcher=RegisteredFetcher, FailingFetcher=FailingFetcher),
    )
    registry = DataRegistry(FakeStorage(), sources_config_path=config_path)

    statuses = registry.check_all_latest_registered()

    assert statuses == [
        {
            "source": "example",
            "dataset": "failing",
            "version_strategy": None,
            "latest_version": None,
            "registered_versions": [],
            "latest_registered_version": None,
            "days_since_last_update": None,
            "is_latest_registered": None,
            "error": "probe failed",
        },
        {
            "source": "example",
            "dataset": "registered",
            "version_strategy": "api_listcache_id",
            "latest_version": "v1",
            "registered_versions": ["v1"],
            "latest_registered_version": "v1",
            "days_since_last_update": None,
            "is_latest_registered": True,
            "error": None,
        },
    ]


def test_data_registry_reports_metadata_version_mismatch(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      dated:
        fetch:
          module: fake_fetcher_module
          class: DatedFetcher
          version_strategy: single_file_last_modified
""",
        encoding="utf-8",
    )

    class DatedFetcher(SourceFetcher):
        source = "example"
        dataset = "dated"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            return "2026-06-11"

    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return ["sources/example/dated/2026-06-10/manifest.yaml"]
            if prefix == "external/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            return """
kind: source_snapshot
schema_version: 1
source: example
dataset: dated
snapshot_id: example:dated:2026-06-10
version: '2026-06-10'
files: []
"""

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(DatedFetcher=DatedFetcher),
    )
    registry = DataRegistry(FakeStorage(), sources_config_path=config_path)

    class FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 6, 12)

    monkeypatch.setattr("src.core.data_registry.date", FrozenDate)

    assert registry.check_all_latest_registered() == [{
        "source": "example",
        "dataset": "dated",
        "version_strategy": "single_file_last_modified",
        "latest_version": "2026-06-11",
        "registered_versions": ["2026-06-10"],
        "latest_registered_version": "2026-06-10",
        "days_since_last_update": 2,
        "is_latest_registered": False,
        "error": None,
    }]


def test_data_registry_reports_unknown_latest_as_not_registered(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      dated:
        fetch:
          module: fake_fetcher_module
          class: DatedFetcher
          version_strategy: download_date
""",
        encoding="utf-8",
    )

    class DatedFetcher(SourceFetcher):
        source = "example"
        dataset = "dated"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            return None

    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return ["sources/example/dated/2026-06-10/manifest.yaml"]
            if prefix == "external/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            return """
kind: source_snapshot
schema_version: 1
source: example
dataset: dated
snapshot_id: example:dated:2026-06-10
version: '2026-06-10'
files: []
"""

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(DatedFetcher=DatedFetcher),
    )
    registry = DataRegistry(FakeStorage(), sources_config_path=config_path)

    class FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 6, 12)

    monkeypatch.setattr("src.core.data_registry.date", FrozenDate)

    assert registry.check_all_latest_registered() == [{
        "source": "example",
        "dataset": "dated",
        "version_strategy": "download_date",
        "latest_version": None,
        "registered_versions": ["2026-06-10"],
        "latest_registered_version": "2026-06-10",
        "days_since_last_update": 2,
        "is_latest_registered": False,
        "error": None,
    }]


def test_data_registry_sync_dry_run_reports_missing_and_stale_datasets(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      missing:
        fetch:
          module: fake_fetcher_module
          class: MissingFetcher
          version_strategy: api_listcache_id
      stale:
        fetch:
          module: fake_fetcher_module
          class: StaleFetcher
          version_strategy: single_file_last_modified
      too_recent:
        fetch:
          module: fake_fetcher_module
          class: TooRecentFetcher
          version_strategy: single_file_last_modified
""",
        encoding="utf-8",
    )

    class MissingFetcher(SourceFetcher):
        source = "example"
        dataset = "missing"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            return "v1"

    class StaleFetcher(SourceFetcher):
        source = "example"
        dataset = "stale"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            return "2026-06-11"

    class TooRecentFetcher(SourceFetcher):
        source = "example"
        dataset = "too_recent"

        def fetch(self, *, dest, timeout):
            return tmp_path / "manifest.yaml"

        def get_latest_version(self, *, timeout):
            return "2026-06-11"

    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return [
                    "sources/example/stale/2026-06-01/manifest.yaml",
                    "sources/example/too_recent/2026-06-10/manifest.yaml",
                ]
            if prefix == "external/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            if key == "sources/example/stale/2026-06-01/manifest.yaml":
                return """
kind: source_snapshot
schema_version: 1
source: example
dataset: stale
snapshot_id: example:stale:2026-06-01
version: '2026-06-01'
files: []
"""
            if key == "sources/example/too_recent/2026-06-10/manifest.yaml":
                return """
kind: source_snapshot
schema_version: 1
source: example
dataset: too_recent
snapshot_id: example:too_recent:2026-06-10
version: '2026-06-10'
files: []
"""
            raise AssertionError(key)

    class FrozenDate(date):
        @classmethod
        def today(cls):
            return cls(2026, 6, 12)

    monkeypatch.setattr("src.core.data_registry.date", FrozenDate)
    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(
            MissingFetcher=MissingFetcher,
            StaleFetcher=StaleFetcher,
            TooRecentFetcher=TooRecentFetcher,
        ),
    )
    registry = DataRegistry(FakeStorage(), sources_config_path=config_path)

    plan = registry.sync_latest_snapshots(min_days_since_last_update=7)

    assert [(entry["dataset"], entry["sync_reason"]) for entry in plan] == [
        ("missing", "missing"),
        ("stale", "not_latest"),
    ]
    assert [entry["sync_action"] for entry in plan] == ["would_refresh", "would_refresh"]
    assert plan[1]["days_since_last_update"] == 11


def test_data_registry_sync_requires_dest_when_not_dry_run():
    registry = DataRegistry(FakeStorageForRegistry())

    with pytest.raises(ValueError, match="dest is required"):
        registry.sync_latest_snapshots(dry_run=False)


def test_data_registry_sync_latest_snapshots_refreshes_selected_dataset(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      dataset:
        fetch:
          module: fake_fetcher_module
          class: ExampleFetcher
          version_strategy: api_listcache_id
""",
        encoding="utf-8",
    )
    manifest_path = tmp_path / "cache" / "example" / "dataset" / "v1" / "manifest.yaml"

    class ExampleFetcher(SourceFetcher):
        source = "example"
        dataset = "dataset"

        def fetch(self, *, dest, timeout):
            data_path = Path(dest) / "example.tsv"
            data_path.write_text("id\n1\n", encoding="utf-8")
            return SourceSnapshot(
                source="example",
                dataset="dataset",
                version="v1",
                version_date=None,
                homepage=None,
                upstream_urls=["https://example.org/example.tsv"],
                files=[SnapshotFile(data_path, "https://example.org/example.tsv", "text/tab-separated-values")],
            )

        def get_latest_version(self, *, timeout):
            return "v1"

    class UploadStorage(FakeStorageForRegistry):
        def __init__(self):
            self.uploaded = []

        def upload_file(self, local_path, key, content_type=None):
            self.uploaded.append(key)
            return f"s3://ifx-registry/{key}"

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(ExampleFetcher=ExampleFetcher),
    )
    storage = UploadStorage()
    registry = DataRegistry(storage, sources_config_path=config_path)

    result = registry.sync_latest_snapshots(dest=tmp_path / "cache", dry_run=False)

    assert result[0]["sync_action"] == "refreshed"
    assert result[0]["manifest_path"] == str(manifest_path)
    assert storage.uploaded == [
        "sources/example/dataset/v1/example.tsv",
        "sources/example/dataset/v1/manifest.yaml",
    ]


def test_data_registry_sync_latest_snapshots_handles_manual_input_files(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  antibodypedia:
    datasets:
      scraped_results:
        fetch:
          module: src.registry.sources.manual_pharos
          class: ManualAntibodypediaScrapedResultsFetcher
          version_strategy: filename_timestamp
""",
        encoding="utf-8",
    )
    source_dir = tmp_path / "input_files" / "manual" / "antibodypedia"
    source_dir.mkdir(parents=True)
    source_file = source_dir / "antibodypedia_scraped_results_2025-06-27_12-32.csv"
    source_file.write_text("target,antibody\nA1,B1\n", encoding="utf-8")

    class UploadStorage(FakeStorageForRegistry):
        def __init__(self):
            self.uploaded = {}

        def list_keys(self, prefix):
            if prefix == "sources/":
                return sorted(self.uploaded)
            if prefix == "external/":
                return []
            if prefix == "derived/":
                return []
            raise AssertionError(prefix)

        def upload_file(self, local_path, key, content_type=None):
            path = Path(local_path)
            self.uploaded[key] = path.read_bytes()
            return f"s3://ifx-registry/{key}"

        def read_text(self, key):
            return self.uploaded[key].decode("utf-8")

    monkeypatch.chdir(tmp_path)
    storage = UploadStorage()
    registry = DataRegistry(storage, sources_config_path=config_path)

    plan = registry.sync_latest_snapshots()

    assert plan[0]["source"] == "antibodypedia"
    assert plan[0]["dataset"] == "scraped_results"
    assert plan[0]["latest_version"] == "2025-06-27_12-32"
    assert plan[0]["sync_reason"] == "missing"

    result = registry.sync_latest_snapshots(dest=tmp_path / "cache", dry_run=False)

    assert result[0]["sync_action"] == "refreshed"
    assert sorted(storage.uploaded) == [
        "sources/antibodypedia/scraped_results/2025-06-27_12-32/antibodypedia_scraped_results_2025-06-27_12-32.csv",
        "sources/antibodypedia/scraped_results/2025-06-27_12-32/manifest.yaml",
    ]
    assert registry.is_latest_registered("antibodypedia", "scraped_results") is True


def test_source_latest_version_helpers_parse_source_metadata(monkeypatch):
    class FakeResponse:
        def __init__(self, text="", json_payload=None):
            self.text = text
            self._json_payload = json_payload

        def raise_for_status(self):
            pass

        def json(self):
            return self._json_payload

    def fake_get(url, **kwargs):
        if url == "https://bioplex.hms.harvard.edu/":
            return FakeResponse("BioPlex 2.0 and BioPlex 3.0")
        if url == "https://stringdb-downloads.org/download/":
            return FakeResponse('<a href="protein.links.v11.5/">old</a><a href="protein.links.v12.0/">new</a>')
        if url == "https://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/":
            return FakeResponse('<a href="PTHR18.0_human">old</a><a href="PTHR19.0_human">new</a>')
        raise AssertionError(url)

    monkeypatch.setattr("src.registry.sources.bioplex.requests.get", fake_get)
    monkeypatch.setattr("src.registry.sources.string.requests.get", fake_get)
    monkeypatch.setattr("src.registry.sources.pathway_sources.requests.get", fake_get)

    assert latest_bioplex_version() == "3.0"
    assert latest_string_version() == "12.0"
    assert latest_gtex_version() == "v11"
    assert latest_panther_version() == "19.0"


def test_data_registry_upload_snapshot_writes_storage_uris(tmp_path: Path):
    data_path = tmp_path / "example.tsv"
    data_path.write_text("id\tname\n1\talpha\n", encoding="utf-8")
    manifest = build_source_snapshot_manifest(
        source="example",
        dataset="dataset",
        version="v1",
        version_date="2026-06-11",
        download_date="2026-06-11",
        upstream_urls=["https://example.org/example.tsv"],
        files=[file_entry(data_path, "https://example.org/example.tsv")],
    )
    manifest_path = tmp_path / "manifest.yaml"
    write_manifest(manifest, manifest_path)

    class UploadStorage:
        bucket = "ifx-registry"

        def __init__(self):
            self.uploaded = []

        def upload_file(self, local_path, key, content_type=None):
            self.uploaded.append((Path(local_path).name, key, content_type))
            return f"s3://ifx-registry/{key}"

        def list_keys(self, prefix):
            return []

    storage = UploadStorage()
    registry = DataRegistry(storage)

    uploaded = registry.upload_snapshot(manifest_path)
    updated_manifest = read_manifest(manifest_path)

    assert uploaded == [
        "s3://ifx-registry/sources/example/dataset/v1/example.tsv",
        "s3://ifx-registry/sources/example/dataset/v1/manifest.yaml",
    ]
    assert updated_manifest["files"][0]["storage_uri"] == "s3://ifx-registry/sources/example/dataset/v1/example.tsv"
    assert updated_manifest["manifest_uri"] == "s3://ifx-registry/sources/example/dataset/v1/manifest.yaml"
    assert storage.uploaded == [
        ("example.tsv", "sources/example/dataset/v1/example.tsv", "text/tab-separated-values"),
        ("manifest.yaml", "sources/example/dataset/v1/manifest.yaml", "application/x-yaml"),
    ]


def test_data_registry_local_instance_cannot_upload(tmp_path: Path):
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text("files: []\n", encoding="utf-8")
    registry = DataRegistry.local()

    with pytest.raises(ValueError, match="not connected to MinIO"):
        registry.upload_snapshot(manifest_path)


def test_data_registry_registers_external_source_from_config(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    credentials_path = tmp_path / "credentials.yaml"
    credentials_path.write_text("url: example.org\nschema: example_schema\n", encoding="utf-8")
    config_path.write_text(
        f"""
sources:
  example_external:
    datasets:
      database:
        external:
          module: fake_external_module
          class: ExampleExternalSource
          credentials: {credentials_path}
""",
        encoding="utf-8",
    )
    calls = []

    class ExampleExternalSource(ExternalSourceProvider):
        source = "example_external"
        dataset = "database"

        def build_registration(self, *, config):
            calls.append(config)
            return ExternalSourceRegistration(
                source=self.source,
                dataset=self.dataset,
                version="v1",
                version_date="2026-06-11",
                connection={
                    "type": "mysql",
                    "host": "example.org",
                    "schema": "example_schema",
                    "credential_ref": str(config["credentials"]),
                },
                access={
                    "mode": "query",
                    "interface": "sql",
                    "database_type": "mysql",
                },
                extra={
                    "version_method": {
                        "type": "test_probe",
                    },
                },
            )

    class UploadStorage(FakeStorageForRegistry):
        def __init__(self):
            self.uploaded = []

        def upload_file(self, local_path, key, content_type=None):
            self.uploaded.append((Path(local_path).name, key, content_type))
            return f"s3://ifx-registry/{key}"

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(ExampleExternalSource=ExampleExternalSource),
    )
    storage = UploadStorage()
    registry = DataRegistry(storage, sources_config_path=config_path)

    manifest_path = registry.register_external_source(
        "example_external",
        "database",
        dest=tmp_path / "cache",
    )
    manifest = read_manifest(manifest_path)

    assert calls == [{
        "module": "fake_external_module",
        "class": "ExampleExternalSource",
        "credentials": str(credentials_path),
    }]
    assert manifest_path == tmp_path / "cache" / "example_external" / "database" / "v1" / "manifest.yaml"
    assert manifest["kind"] == "external_source_registration"
    assert manifest["registration_id"] == "example_external:database:v1"
    assert manifest["manifest_uri"] == "s3://ifx-registry/external/example_external/database/v1/manifest.yaml"
    assert storage.uploaded == [
        ("manifest.yaml", "external/example_external/database/v1/manifest.yaml", "application/x-yaml"),
    ]


def test_data_registry_latest_checks_skip_external_sources(tmp_path: Path):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example_external:
    datasets:
      database:
        external:
          module: fake_external_module
          class: ExampleExternalSource
          credentials: src/use_cases/secrets/example.yaml
""",
        encoding="utf-8",
    )
    registry = DataRegistry(FakeStorageForRegistry(), sources_config_path=config_path)

    assert registry.check_all_latest_registered() == []


def test_data_registry_checks_external_registrations(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example_external:
    datasets:
      registered:
        external:
          module: fake_external_module
          class: RegisteredExternalSource
          credentials: src/use_cases/secrets/example.yaml
      missing:
        external:
          module: fake_external_module
          class: MissingExternalSource
          credentials: src/use_cases/secrets/example.yaml
""",
        encoding="utf-8",
    )

    class RegisteredExternalSource(ExternalSourceProvider):
        source = "example_external"
        dataset = "registered"

        def build_registration(self, *, config):
            return ExternalSourceRegistration(
                source=self.source,
                dataset=self.dataset,
                version="v1",
                version_date="2026-06-11",
                connection={},
                access={},
            )

    class MissingExternalSource(ExternalSourceProvider):
        source = "example_external"
        dataset = "missing"

        def build_registration(self, *, config):
            return ExternalSourceRegistration(
                source=self.source,
                dataset=self.dataset,
                version="v2",
                version_date="2026-06-11",
                connection={},
                access={},
            )

    class FakeStorage:
        bucket = "ifx-registry"

        def list_keys(self, prefix):
            if prefix == "sources/":
                return []
            if prefix == "external/":
                return ["external/example_external/registered/v1/manifest.yaml"]
            raise AssertionError(prefix)

        def read_text(self, key):
            return """
kind: external_source_registration
schema_version: 1
source: example_external
dataset: registered
registration_id: example_external:registered:v1
version: v1
connection: {}
access: {}
"""

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(
            RegisteredExternalSource=RegisteredExternalSource,
            MissingExternalSource=MissingExternalSource,
        ),
    )
    registry = DataRegistry(FakeStorage(), sources_config_path=config_path)

    statuses = registry.check_external_registrations()

    assert [(entry["dataset"], entry["latest_version"], entry["is_latest_registered"]) for entry in statuses] == [
        ("missing", "v2", False),
        ("registered", "v1", True),
    ]


def test_data_registry_sync_external_sources_dry_run_and_registers(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example_external:
    datasets:
      database:
        external:
          module: fake_external_module
          class: ExampleExternalSource
          credentials: src/use_cases/secrets/example.yaml
""",
        encoding="utf-8",
    )

    class ExampleExternalSource(ExternalSourceProvider):
        source = "example_external"
        dataset = "database"

        def build_registration(self, *, config):
            return ExternalSourceRegistration(
                source=self.source,
                dataset=self.dataset,
                version="v1",
                version_date="2026-06-11",
                connection={"type": "mysql"},
                access={"mode": "query"},
            )

    class UploadStorage(FakeStorageForRegistry):
        def __init__(self):
            self.uploaded = {}

        def list_keys(self, prefix):
            if prefix == "sources/":
                return []
            if prefix == "external/":
                return sorted(self.uploaded)
            if prefix == "derived/":
                return []
            raise AssertionError(prefix)

        def upload_file(self, local_path, key, content_type=None):
            self.uploaded[key] = Path(local_path).read_text(encoding="utf-8")
            return f"s3://ifx-registry/{key}"

        def read_text(self, key):
            return self.uploaded[key]

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(ExampleExternalSource=ExampleExternalSource),
    )
    storage = UploadStorage()
    registry = DataRegistry(storage, sources_config_path=config_path)

    dry_run = registry.sync_external_sources()

    assert [(entry["dataset"], entry["sync_reason"], entry["sync_action"]) for entry in dry_run] == [
        ("database", "missing", "would_register"),
    ]

    result = registry.sync_external_sources(dest=tmp_path / "cache", dry_run=False)

    assert result[0]["sync_action"] == "registered"
    assert result[0]["manifest_path"] == str(tmp_path / "cache" / "example_external" / "database" / "v1" / "manifest.yaml")
    assert sorted(storage.uploaded) == [
        "external/example_external/database/v1/manifest.yaml",
    ]
    assert registry.is_external_source("example_external", "database", "v1")


def test_data_registry_sync_derived_artifact_builds_from_source_snapshot(tmp_path: Path):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  surechembl:
    datasets:
      patent_family_mentions:
        derived:
          module: src.registry.derived.surechembl
          class: SurechemblPatentFamilyMentionsBuilder
          dependencies:
            - source: surechembl
              dataset: patent_discovery
          output:
            file_name: protein_patent_family_mentions.parquet
          transform:
            name: surechembl_patent_family_mentions
            version: 1
            code_ref: src/registry/derived/surechembl.py
""",
        encoding="utf-8",
    )
    source_dir = tmp_path / "source_files"
    source_dir.mkdir()
    patents_path = source_dir / "patents.parquet"
    entities_path = source_dir / "biomedical_entities.parquet"
    locations_path = source_dir / "biomedical_locations.parquet"
    pq.write_table(
        pa.Table.from_pylist([
            {"id": 10, "publication_date": date(2020, 1, 1), "family_id": 100},
            {"id": 11, "publication_date": date(2021, 1, 1), "family_id": 200},
            {"id": 12, "publication_date": date(2022, 1, 1), "family_id": 300},
        ]),
        patents_path,
    )
    pq.write_table(
        pa.Table.from_pylist([
            {"id": 1, "entity_type_id": 1, "resolved_form": "HGNC:1"},
            {"id": 2, "entity_type_id": 1, "resolved_form": "P04637"},
            {"id": 3, "entity_type_id": 2, "resolved_form": "HGNC:2"},
        ]),
        entities_path,
    )
    pq.write_table(
        pa.Table.from_pylist([
            {"patent_id": 10, "entity_id": 1},
            {"patent_id": 11, "entity_id": 2},
            {"patent_id": 12, "entity_id": 3},
        ]),
        locations_path,
    )

    source_keys = {
        "sources/surechembl/patent_discovery/2026-06-01/patents.parquet": patents_path,
        "sources/surechembl/patent_discovery/2026-06-01/biomedical_entities.parquet": entities_path,
        "sources/surechembl/patent_discovery/2026-06-01/biomedical_locations.parquet": locations_path,
    }

    class DerivedStorage(FakeStorageForRegistry):
        def __init__(self):
            self.uploaded = {}

        def list_keys(self, prefix):
            if prefix == "sources/":
                return ["sources/surechembl/patent_discovery/2026-06-01/manifest.yaml"]
            if prefix == "derived/":
                return sorted(self.uploaded)
            if prefix == "external/":
                return []
            raise AssertionError(prefix)

        def read_text(self, key):
            if key == "sources/surechembl/patent_discovery/2026-06-01/manifest.yaml":
                return """
kind: source_snapshot
schema_version: 1
source: surechembl
dataset: patent_discovery
snapshot_id: surechembl:patent_discovery:2026-06-01
version: '2026-06-01'
version_date: '2026-06-01'
files:
  - path: patents.parquet
    storage_uri: s3://ifx-registry/sources/surechembl/patent_discovery/2026-06-01/patents.parquet
  - path: biomedical_entities.parquet
    storage_uri: s3://ifx-registry/sources/surechembl/patent_discovery/2026-06-01/biomedical_entities.parquet
  - path: biomedical_locations.parquet
    storage_uri: s3://ifx-registry/sources/surechembl/patent_discovery/2026-06-01/biomedical_locations.parquet
"""
            return self.uploaded[key].decode("utf-8")

        def download_file(self, key, local_path):
            local_path.parent.mkdir(parents=True, exist_ok=True)
            local_path.write_bytes(source_keys[key].read_bytes())
            return local_path

        def upload_file(self, local_path, key, content_type=None):
            self.uploaded[key] = Path(local_path).read_bytes()
            return f"s3://ifx-registry/{key}"

    storage = DerivedStorage()
    registry = DataRegistry(storage, sources_config_path=config_path)

    plan = registry.sync_derived_artifacts()

    assert [(entry["dataset"], entry["latest_version"], entry["sync_action"]) for entry in plan] == [
        ("patent_family_mentions", "2026-06-01", "would_build"),
    ]
    assert len(plan[0]["latest_build_key"]) == 64
    assert plan[0]["dependency_cache"] is None

    cached_dependency_dir = tmp_path / "cache" / "surechembl" / "patent_discovery" / "2026-06-01"
    cached_dependency_dir.mkdir(parents=True)
    (cached_dependency_dir / "patents.parquet").write_bytes(patents_path.read_bytes())
    plan_with_cache = registry.sync_derived_artifacts(dest=tmp_path / "cache")

    assert [
        (entry["path"], entry["cached"], entry["would_download"])
        for entry in plan_with_cache[0]["dependency_cache"]
    ] == [
        ("patents.parquet", True, False),
        ("biomedical_entities.parquet", False, True),
        ("biomedical_locations.parquet", False, True),
    ]

    result = registry.sync_derived_artifacts(dest=tmp_path / "cache", dry_run=False)

    assert result[0]["sync_action"] == "built"
    assert (tmp_path / "cache" / "surechembl" / "patent_discovery" / "2026-06-01" / "patents.parquet").exists()
    assert sorted(storage.uploaded) == [
        "derived/surechembl/patent_family_mentions/2026-06-01/manifest.yaml",
        "derived/surechembl/patent_family_mentions/2026-06-01/protein_patent_family_mentions.parquet",
    ]
    output_path = tmp_path / "cache" / "surechembl" / "patent_family_mentions" / "2026-06-01" / "protein_patent_family_mentions.parquet"
    output_rows = pq.read_table(output_path).to_pylist()
    assert output_rows == [
        {
            "protein_id": "HGNC:1",
            "patent_family_mentions": ["2020:100"],
            "patent_identifier_sources": ["HGNC"],
        },
        {
            "protein_id": "UniProtKB:P04637",
            "patent_family_mentions": ["2021:200"],
            "patent_identifier_sources": ["UniProtKB"],
        },
    ]
    manifest = read_manifest(tmp_path / "cache" / "surechembl" / "patent_family_mentions" / "2026-06-01" / "manifest.yaml")
    assert manifest["kind"] == "derived_snapshot"
    assert manifest["derived_from"] == [{
        "snapshot_id": "surechembl:patent_discovery:2026-06-01",
        "manifest_uri": "s3://ifx-registry/sources/surechembl/patent_discovery/2026-06-01/manifest.yaml",
    }]
    assert len(manifest["build_key"]) == 64
    assert manifest["build_key"] == result[0]["latest_build_key"]
    assert manifest["transform"]["code_ref"] == "src/registry/derived/surechembl.py"
    assert len(manifest["transform"]["code_sha256"]) == 64
    assert manifest["stats"]["row_count"] == 2

    registry.refresh_catalog()
    post_build_status = registry.check_derived_artifacts()
    assert post_build_status[0]["is_latest_registered"] is True
    assert post_build_status[0]["latest_build_key"] == manifest["build_key"]


def test_data_registry_refresh_fetches_then_uploads(tmp_path: Path, monkeypatch):
    config_path = tmp_path / "registry_sources.yaml"
    config_path.write_text(
        """
sources:
  example:
    datasets:
      dataset:
        fetch:
          module: fake_fetcher_module
          class: ExampleFetcher
""",
        encoding="utf-8",
    )
    manifest_path = tmp_path / "cache" / "example" / "dataset" / "v1" / "manifest.yaml"
    calls = []

    class ExampleFetcher(SourceFetcher):
        source = "example"
        dataset = "dataset"

        def fetch(self, *, dest, timeout):
            calls.append((dest, timeout))
            data_path = Path(dest) / "example.tsv"
            data_path.write_text("id\n1\n", encoding="utf-8")
            return SourceSnapshot(
                source="example",
                dataset="dataset",
                version="v1",
                version_date=None,
                homepage=None,
                upstream_urls=["https://example.org/example.tsv"],
                files=[SnapshotFile(data_path, "https://example.org/example.tsv", "text/tab-separated-values")],
            )

        def get_latest_version(self, *, timeout):
            return "v1"

    class UploadStorage(FakeStorageForRegistry):
        def upload_file(self, local_path, key, content_type=None):
            return f"s3://ifx-registry/{key}"

    monkeypatch.setattr(
        "src.core.data_registry.importlib.import_module",
        lambda module_name: SimpleNamespace(ExampleFetcher=ExampleFetcher),
    )
    registry = DataRegistry(UploadStorage(), sources_config_path=config_path)

    result = registry.refresh_dataset("example", "dataset", dest=tmp_path / "cache", timeout=30)

    assert result == manifest_path
    assert calls == [(tmp_path / "cache" / "_registry_work" / "example" / "dataset", 30)]


class FakeStorageForRegistry:
    bucket = "ifx-registry"

    def list_keys(self, prefix):
        return []


def test_extract_ctd_report_created(tmp_path: Path):
    fixture_path = tmp_path / "ctd.tsv.gz"
    with gzip.open(fixture_path, "wt", encoding="utf-8") as handle:
        handle.write("# Report created: Thu Jun 06 11:03:20 EDT 2024\n")
        handle.write("GeneSymbol\tGeneID\tDiseaseName\tDiseaseID\tDirectEvidence\tOmimIDs\tPubMedIDs\n")

    report_created, version_date = extract_report_created(fixture_path)

    assert report_created == "Thu Jun 06 11:03:20 EDT 2024"
    assert version_date == "2024-06-06"


def test_hcop_source_file_name_is_expected():
    assert HCOP_FILE_NAME == "human_all_hcop_sixteen_column.txt.gz"


def test_single_file_source_file_names_are_expected():
    assert IMPC_FILE_NAME == "genotype-phenotype-assertions-IMPC.csv.gz"
    assert JENSENLAB_TISSUES_FILE_NAME == "human_tissue_integrated_full.tsv"
    assert MGI_HMD_FILE_NAME == "HMD_HumanPhenotype.rpt"
    assert MP_FILE_NAME == "mp.obo"


def test_extract_obo_data_version(tmp_path: Path):
    obo_path = tmp_path / "mp.obo"
    obo_path.write_text(
        "format-version: 1.2\n"
        "data-version: releases/2026-05-01/mp.obo\n"
        "\n"
        "[Term]\n",
        encoding="utf-8",
    )

    assert extract_obo_data_version(obo_path) == "2026-05-01"


def test_external_registration_version_token_is_path_safe():
    assert _version_token("2026-06-11 03:15:00") == "2026-06-11T03-15-00"
    assert _version_token("DrugCentral/2026") == "DrugCentral-2026"


def test_multi_file_source_url_sets_match_expected_counts():
    assert len(JENSENLAB_DISEASE_URLS) == 3
    assert len(JENSENLAB_TINX_URLS) == 2
    assert len(NCBI_PUBLICATION_URLS) == 2
    assert len(NCBI_GENE_SUMMARY_URLS) == 1
