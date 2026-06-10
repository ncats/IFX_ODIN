# IFX Registry Playbook

Use this workflow when adding, refreshing, browsing, or consuming datasets in the IFX Datasource Registry.

The registry stores versioned source snapshots and manifests in MinIO. It is not just a GUI over a bucket: it is the shared contract for reproducible ETL inputs, local build caches, future derived artifacts, and resolver artifacts.

## Core Ideas

- MinIO stores immutable files and manifests.
- Source snapshots live under `sources/<source>/<dataset>/<version>/`.
- Derived artifacts live under `derived/<source>/<dataset>/<version>/`.
- The default bucket is `ifx-registry`.
- Builds should download files into a local cache, then adapters read local paths.
- Local caches are disposable. Do not treat them as source-of-truth.
- Do not change existing Snakefiles or adapters until the registry path is ready and validated.

## Snapshot Model

Use this hierarchy:

- **source**: upstream provider or logical source, for example `jensenlab`, `ncbi`, `surechembl`
- **dataset**: one download or logical file set from that source, for example `tissues`, `gene_summary`, `patent_discovery`
- **snapshot/version**: immutable dated or release-specific capture, for example `2026-06-01`
- **files**: one or more stored files in that snapshot

Example:

```text
sources/jensenlab/tinx/2026-05-31/
  human_textmining_mentions.tsv.gz
  disease_textmining_mentions.tsv.gz
  manifest.yaml
```

## Source Snapshot Manifest

Recommended fields:

```yaml
kind: source_snapshot
schema_version: 1
source: jensenlab
dataset: tinx
snapshot_id: jensenlab:tinx:2026-05-31
version: "2026-05-31"
version_date: "2026-05-31"
download_date: "2026-06-10"
downloaded_by: ifx-registry
created_at: "2026-06-10T00:00:00+00:00"
upstream:
  homepage: https://jensenlab.org/resources/proteomics/
  urls:
    - https://download.jensenlab.org/human_textmining_mentions.tsv
files:
  - path: human_textmining_mentions.tsv.gz
    size_bytes: 123
    sha256: ...
    content_type: application/gzip
    source_url: https://download.jensenlab.org/human_textmining_mentions.tsv
    storage_uri: s3://ifx-registry/sources/jensenlab/tinx/2026-05-31/human_textmining_mentions.tsv.gz
extra:
  version_method:
    type: multi_file_max_last_modified
    description: Use max Last-Modified across source files.
```

## Version Strategy

Prefer version evidence in this order:

1. Official release/version endpoint or filename.
2. Dated release directory.
3. Embedded ontology or metadata version.
4. HTTP `Last-Modified`.
5. Download date, only when the source exposes no better stable version.

Capture the selected strategy in `extra.version_method`.

Examples:

- UniProt: `x-uniprot-release` and `x-uniprot-release-date` headers.
- TIGA: latest `YYYYMMDD` directory plus file `Last-Modified`.
- Disease Ontology: embedded `owl#versionInfo`.
- SureChEMBL: newest dated `bulk_data/YYYY-MM-DD/` directory.
- WikiPathways: date embedded in current filename.

## Adding A New Source Snapshot

1. Read the relevant existing download rule, design doc, or source documentation.
2. Identify the exact upstream files and whether the current adapter uses all of them.
3. Inspect real payload shape when the source is new or uncertain.
4. Decide `source`, `dataset`, and `version` names.
5. Implement a source-specific registry command under `src/registry/sources/`.
6. Add a CLI command in `src/registry/cli.py`.
7. Download to a local cache under `/private/tmp/ifx-registry-cache` or another disposable cache.
8. Write `manifest.yaml` next to the cached files.
9. Upload files and manifest to `s3://ifx-registry/sources/...`.
10. Verify the MinIO catalog and QA Browser entry.
11. Ask the user before purging local cache files.

Do not purge anything under `input_files` unless explicitly instructed.

## Multi-File Datasets

Register related files as one dataset snapshot when the adapter consumes them together or when the files share one version contract.

Examples:

- `ncbi/publications`: `gene2pubmed.gz`, `generifs_basic.gz`
- `tiga/gene_trait`: stats and provenance TSVs
- `reactome/pathways`: GMT, relations, UniProt mapping, interactions
- `surechembl/patent_discovery`: five parquet files

The QA Browser should show the dataset as one row with a file count. Add an expandable file list later if needed.

## Compression

Store text-heavy large files compressed when reasonable.

- Prefer preserving upstream compression when it already exists.
- For large plain TSV files, gzip before upload when adapters can stream gzip directly.
- Record stored compressed file checksums and sizes.
- Keep `source_url` pointing to the original upstream file.

TINX is the current example: the upstream TSVs are stored as `.tsv.gz`.

## Local Cache

Registry workflows should download to a local cache first unless direct-to-MinIO streaming is explicitly implemented for that source.

Cache rules:

- It is safe to delete after upload and verification.
- It should not be under `input_files`.
- It should preserve enough local files to inspect failures.
- Ask the user before purging.

For very large sources, a future direct-to-MinIO multipart mode is acceptable if it still computes size and checksum for the manifest.

## QA Browser

The QA Browser should be a read-only view over registry contents.

Current expected language:

- Page title: `IFX Datasource Registry`
- Group first by source.
- Then show datasets under each source.
- Show snapshots horizontally in tables.
- Top stats should show:
  - source count
  - dataset count
  - total size

Avoid hard-coded snapshot lists in the UI. Snapshot data should come from MinIO manifests.

## External Database Sources

Some sources are queried from external databases rather than downloaded files, for example ChEMBL or DrugCentral.

For these, register metadata rather than pretending MinIO owns the raw source:

- source/database name
- host/schema/table set, if appropriate
- query or extraction code reference
- upstream release/version if available
- row counts and timestamps
- optional derived export files if created

If reproducibility or speed requires it, create a derived export and register that under `derived/...`.

## Derived Artifacts

Use `playbooks/derived_artifact_playbook.md` for derived datasets.

Examples:

- SureChEMBL raw parquet -> adapter-facing patent-family parquet.
- Target graph manual TSV set -> curated registry artifact.
- Resolver SQLite files -> resolver/derived snapshots.

Derived artifacts must declare exact source snapshot dependencies.

## Resolver Artifacts

Resolver SQLite artifacts can be stored in MinIO later.

Treat them as resolver or derived snapshots with:

- resolver type
- supported namespaces
- dependency source snapshots
- config/code reference
- row counts and coverage stats

This lets ETL builds pin both source data and resolver state.

## Validation Checklist

Before considering a source snapshot registered:

- Manifest exists in MinIO.
- All listed files have `size_bytes`, `sha256`, and `storage_uri`.
- Source/version/date metadata is present or explicitly documented as unavailable.
- QA Browser shows the source, dataset, snapshot, file count, and size.
- Local manifest verification passes where applicable.
- Focused registry tests pass:

```bash
.venv/bin/python -m pytest tests/test_registry_manifest.py
```

## Current Registry Coverage

As of the initial Pharos registry pass, every `download_*` rule in `workflows/pharos.Snakefile` has a corresponding source snapshot in `ifx-registry`.

Keep this section high-level. Use the QA Browser or MinIO catalog as the source of truth for the live list.
