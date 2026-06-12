# IFX Registry Codex Workflow

## Goal

Use Codex as the guided interface for registering source dataset snapshots.
Codex should help with the judgment-heavy metadata questions, then use
`DataRegistry` to download files, write `manifest.yaml`, verify checksums, and
upload to MinIO.

## Workflow

1. Identify the source and dataset.
   - Ask whether the user means one logical dataset or several datasets under
     the same source release.
   - For sources like UniProt, separate release from dataset/file set.

2. Inspect the upstream source.
   - Confirm the exact URL or release page.
   - Prefer official release/version metadata.
   - Use `Last-Modified` only when no better version date exists.

3. Draft snapshot metadata.
   - `source`
   - `dataset`
   - `version`
   - `version_date`
   - `download_date`
   - homepage/source URLs
   - expected file list

4. Run the registry workflow through `DataRegistry`.

   ```python
   from src.core.data_registry import DataRegistry

   registry = DataRegistry.from_minio_credentials(
       "src/use_cases/secrets/ifxdev_minio.yaml"
   )
   registry.refresh_dataset(
       "ctd",
       "curated_genes_diseases",
       dest="/private/tmp/ifx-registry-cache",
   )
   ```

5. Verify the local cache with registry manifest helpers when needed.

   ```python
   from src.registry.manifest import verify_manifest_files

   verify_manifest_files(
       "/private/tmp/ifx-registry-cache/ctd/curated_genes_diseases/2026-05-28/manifest.yaml"
   )
   ```

6. Report the result.
   - local cache path
   - `snapshot_id`
   - manifest checksum
   - MinIO prefix
   - any caveats about version/date inference

7. Review uploaded snapshots and ask before cache cleanup.
   - List the snapshots and MinIO prefixes that uploaded successfully.
   - Ask the user before deleting temporary registry cache files.
   - Only purge temporary registry cache paths such as
     `/private/tmp/ifx-registry-cache/...`.
   - Never delete or modify files under `input_files/` as part of registry cache
     cleanup.

## Source-Specific Fetchers

Most Pharos sources need source-specific download logic. That logic should move
out of ad-hoc shell snippets and into registry source modules implementing the
fetcher interface used by `DataRegistry`.

Each downloader should own:

- exact source URLs or release-page discovery
- how to derive `version`
- how to derive `version_date`
- whether multiple files belong to one dataset snapshot
- per-file headers and checksums
- any source-specific evidence used to justify the version fields

Examples from `workflows/pharos.Snakefile`:

| Source | Version strategy |
| --- | --- |
| UniProt | Read `x-uniprot-release` and `x-uniprot-release-date` response headers from a small API request. |
| CTD | Download the file, parse `# Report created:` from the gzip header, and use that as both version and version date. |
| JensenLab disease files | Download several files, read each `Last-Modified`, and use the max date as the dataset snapshot date. |
| TIGA | Discover the latest dated release directory, then use max `Last-Modified` across the primary files. |
| PathwayCommons | Download one GMT file, but parse version/date from the companion `datasources.txt` header. |
| WikiPathways | Scrape the current GMT directory for the dated Homo sapiens file and derive version/date from the filename. |

The manifest replaces the old `*_version.tsv` sidecar. Do not generate
compatibility TSVs. Adapters should be updated to read registry manifests
directly.

## Manifest Version Evidence

Source manifests should include enough information to explain how the version
fields were produced. Suggested extension:

```yaml
version: 2026_02
version_date: 2026-02-26
download_date: 2026-06-10
version_method:
  type: response_header
  description: Read UniProt release headers from a size=1 API request.
  evidence:
    url: https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=json&size=1&query=accession:P04637
    headers:
      x-uniprot-release: 2026_02
      x-uniprot-release-date: 2026-02-26
```

For multi-file datasets:

```yaml
version_method:
  type: max_last_modified
  description: Use max Last-Modified across all files in the dataset snapshot.
  evidence:
    files:
      - path: human_disease_knowledge_filtered.tsv
        last_modified: Tue, 09 Jun 2026 12:00:00 GMT
      - path: human_disease_experiments_filtered.tsv
        last_modified: Mon, 08 Jun 2026 12:00:00 GMT
```

This gives future downloads a recipe to repeat, and when an upstream source
changes its release surface, the change is localized to that downloader module
and documented in the next manifest.

## Current Source Snapshots

Registered source snapshots currently include:

```text
snapshot_id: ctd:curated_genes_diseases:2026-05-28
MinIO prefix: s3://ifx-registry/sources/ctd/curated_genes_diseases/2026-05-28/

snapshot_id: hcop:human_all_sixteen_column:human_all_hcop_sixteen_column.txt.gz
MinIO prefix: s3://ifx-registry/sources/hcop/human_all_sixteen_column/human_all_hcop_sixteen_column.txt.gz/
```

## Boundaries

Codex should not treat the local cache as authoritative. The cache is disposable
and should be recreated from the MinIO snapshot when needed.

Do not create build manifests yet. This workflow only registers source dataset
snapshots.
