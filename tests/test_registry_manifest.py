import gzip
from pathlib import Path

import pytest

from src.registry.manifest import (
    build_source_snapshot_manifest,
    file_entry,
    parse_http_date_to_iso,
    snapshot_id,
    verify_manifest_files,
    write_manifest,
)
from src.registry.catalog import list_source_snapshots
from src.registry.storage import DEFAULT_REGISTRY_BUCKET, MinioStorage
from src.registry.sources.ctd import extract_report_created
from src.registry.sources.hcop import HCOP_FILE_NAME
from src.registry.sources.impc import IMPC_FILE_NAME
from src.registry.sources.jensenlab import JENSENLAB_DISEASE_URLS, JENSENLAB_TINX_URLS, JENSENLAB_TISSUES_FILE_NAME
from src.registry.sources.mgi import MGI_HMD_FILE_NAME
from src.registry.sources.mp import MP_FILE_NAME
from src.registry.sources.ncbi import NCBI_GENE_SUMMARY_URLS, NCBI_PUBLICATION_URLS
from src.shared.db_credentials import DBCredentials


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

    snapshots = list_source_snapshots(FakeStorage())

    assert len(snapshots) == 1
    assert snapshots[0]["snapshot_id"] == "ctd:curated_genes_diseases:2026-05-28"
    assert snapshots[0]["total_size_bytes"] == 1536
    assert snapshots[0]["total_size"] == "1.5 KB"
    assert snapshots[0]["manifest_uri"] == "s3://ifx-registry/sources/ctd/curated_genes_diseases/2026-05-28/manifest.yaml"


def test_extract_ctd_report_created(tmp_path: Path):
    fixture_path = tmp_path / "ctd.tsv.gz"
    with gzip.open(fixture_path, "wt", encoding="utf-8") as handle:
        handle.write("# Report created: Thu Jun 06 11:03:20 EDT 2024\n")
        handle.write("GeneSymbol\tGeneID\tDiseaseName\tDiseaseID\tDirectEvidence\tOmimIDs\tPubMedIDs\n")

    report_created, version_date = extract_report_created(fixture_path)

    assert report_created == "Thu Jun 06 11:03:20 EDT 2024"
    assert version_date == "2024-06-06"


def test_hcop_version_is_existing_file_name():
    assert HCOP_FILE_NAME == "human_all_hcop_sixteen_column.txt.gz"


def test_single_file_source_versions_are_existing_file_names():
    assert IMPC_FILE_NAME == "genotype-phenotype-assertions-IMPC.csv.gz"
    assert JENSENLAB_TISSUES_FILE_NAME == "human_tissue_integrated_full.tsv"
    assert MGI_HMD_FILE_NAME == "HMD_HumanPhenotype.rpt"
    assert MP_FILE_NAME == "mp.obo"


def test_multi_file_source_url_sets_match_expected_counts():
    assert len(JENSENLAB_DISEASE_URLS) == 3
    assert len(JENSENLAB_TINX_URLS) == 2
    assert len(NCBI_PUBLICATION_URLS) == 2
    assert len(NCBI_GENE_SUMMARY_URLS) == 1
