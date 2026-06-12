# Derived Artifact Playbook

Use this workflow when creating an IFX registry artifact that is derived from one or more registered source snapshots, resolver snapshots, database snapshots, or curated local files.

For raw source downloads, registry browsing, cache rules, and source snapshot manifests, start with `playbooks/ifx_registry_playbook.md`.

Derived artifacts are adapter-facing or build-facing products. They should make ETL faster, smaller, or more stable without replacing the raw source snapshot as the reproducibility anchor.

## When To Use

- A raw source is too large or expensive for every ETL run.
- An adapter repeatedly performs the same deterministic preprocessing.
- A build needs a stable curated file set, such as target graph TSVs.
- A resolver SQLite, DuckDB, parquet, or cache file should be shared through MinIO.
- A source is external/database-backed and the build consumes a reproducible export.

## Principles

- Keep the raw source snapshot in the registry when possible.
- Register derived artifacts separately from raw source snapshots.
- Every derived artifact must declare its inputs with exact snapshot IDs.
- The transform must be deterministic and documented.
- The adapter should consume the smallest artifact that preserves needed semantics.
- Do not hide source decisions inside adapter code when they can be captured in the derived manifest.
- Prefer immutable versioned derived artifacts over mutable `latest` paths.

## Manifest Shape

Recommended manifest fields:

```yaml
kind: derived_snapshot
schema_version: 1
source: surechembl
dataset: patent_family_mentions
snapshot_id: surechembl:patent_family_mentions:2026-06-01
version: "2026-06-01"
version_date: "2026-06-01"
download_date: "2026-06-10"
created_at: "2026-06-10T00:00:00+00:00"
derived_from:
  - snapshot_id: surechembl:patent_discovery:2026-06-01
    manifest_uri: s3://ifx-registry/sources/surechembl/patent_discovery/2026-06-01/manifest.yaml
transform:
  name: surechembl_patent_family_mentions
  version: 1
  code_ref: src/registry/derived/surechembl.py
  code_sha256: ...
build_key: ...
files:
  - path: protein_patent_family_mentions.parquet
    size_bytes: 123
    sha256: ...
    content_type: application/vnd.apache.parquet
    storage_uri: s3://ifx-registry/derived/surechembl/patent_family_mentions/2026-06-01/protein_patent_family_mentions.parquet
stats:
  row_count: 0
  source_counts: {}
```

Use `sources/...` for raw source snapshots and `derived/...` for derived snapshots.

## Workflow

1. Identify the raw source snapshot or snapshots.
   - Read their manifests from MinIO.
   - Record exact `snapshot_id` and `manifest_uri`.
   - Confirm the input files exist and checksums are present.

2. Define the adapter-facing output.
   - List columns and types.
   - Confirm this output preserves all fields the adapter needs.
   - Decide whether output should be parquet, SQLite, DuckDB, TSV, or another format.

3. Profile the raw payload before writing the transform.
   - Confirm required columns exist.
   - Check row counts and cardinalities.
   - For large files, inspect schemas and row-group counts first.

4. Write a deterministic transform.
   - Put reusable transforms under `src/registry/derived/`.
   - Prefer streaming or chunked reads for large inputs.
   - Do not require adapters to recompute expensive joins or filters.
   - Persist enough stats to validate the transform.

5. Write the derived output and manifest locally.
   - Use a temp/cache directory, not `input_files`.
   - Verify local output checksums and sizes.
   - Record transform metadata and raw input dependencies in the manifest.

6. Upload derived output to MinIO.
   - Store under `derived/<source>/<dataset>/<version>/`.
   - Upload output files and `manifest.yaml`.
   - Do not overwrite an existing version unless the user explicitly approves replacement.

7. Validate the derived artifact.
   - Confirm the manifest is visible in the registry catalog or browser.
   - Compare row counts and representative records against the raw source.
   - For adapter-facing artifacts, run or prepare focused adapter tests.
   - Let the user run full Snakemake/ETL builds unless they explicitly delegate them.

8. Update documentation.
   - Record source-specific transform decisions under `designs/`.
   - If the pattern is reusable, update this playbook.

## SureChEMBL Patent Families

Initial derived artifact target:

- Raw input: `surechembl:patent_discovery:2026-06-01`
- Derived dataset: `surechembl:patent_family_mentions:2026-06-01`
- Output file: `protein_patent_family_mentions.parquet`

Recommended columns:

- `protein_id`: `HGNC:...` or `UniProtKB:...`
- `patent_family_mentions`: list of compact `YYYY:FAMILY_ID` tokens
- `patent_identifier_sources`: list containing `HGNC` and/or `UniProtKB`

The transform should materialize what `SureChEMBLPatentFamilyAdapter` currently computes from:

- `biomedical_entities.parquet`
- `biomedical_locations.parquet`
- `patents.parquet`

The adapter can later be simplified to read the derived parquet directly.

Configuration lives in `src/registry/registry_sources.yaml` as a `derived:` dataset:

```yaml
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
```

Check or build via `DataRegistry`:

```python
from src.core.data_registry import DataRegistry

registry = DataRegistry.from_minio_credentials("src/use_cases/secrets/ifxdev_minio.yaml")
plan = registry.sync_derived_artifacts(dry_run=True)
results = registry.sync_derived_artifacts(dest="/tmp/ifx-registry-cache", dry_run=False)
```

## Target Graph TSVs

Initial registry target:

- Source/dataset suggestion: `target_graph/core_ids/<version>`
- Files:
  - `gene_ids.tsv`
  - `transcript_ids.tsv`
  - `protein_ids.tsv`
  - `disease_ids.tsv`
  - relevant mapping files, for example `uniprotkb_mapping_20260507.csv`

Treat these as curated/manual registry artifacts unless they are generated by a reproducible transform. If generated, register them as derived snapshots and declare their upstream dependencies.

## Resolver Artifacts

Resolver SQLite artifacts should be registered as derived or resolver snapshots, not raw source snapshots.

Recommended fields:

- resolver name and entity type
- supported identifier namespaces
- artifact type, for example `sqlite`
- dependencies on source snapshots
- config path/hash
- transform code reference
- row counts and namespace coverage stats

These artifacts can later back a resolver API, but MinIO storage alone is enough for initial shared build reproducibility.
