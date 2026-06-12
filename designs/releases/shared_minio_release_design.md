# IFX Data Source Registry Design

## Current Design State

As of 2026-06-12, the shared MinIO idea has become the IFX Data Source
Registry. The registry is an ODIN core service layer, not just a bucket browser.
It stores reproducible source snapshots, external source registrations, and
derived artifacts in MinIO, while `DataRegistry` is the Python interface that
knows how to list, check, fetch, sync, and materialize those registry entries.

The registry is intentionally modest: it is not lakeFS, not a full data lake
query engine, and not a replacement for ETL configuration. It is the
authoritative store for dataset bytes and manifests that ETL builds can pin and
cache locally.

## Goals

- Keep one shared, versioned copy of source input files and derived build inputs.
- Preserve enough metadata to understand where bytes came from and how versions
  were determined.
- Let local builds use disposable caches without treating those caches as
  authoritative.
- Support sources that are downloaded files, manual local files, external
  databases/APIs, and deterministic derived products.
- Give adapters and future build workflows a stable way to point at registered
  datasets.

## Core Components

| Component | Responsibility |
| --- | --- |
| MinIO bucket `ifx-registry` | Object storage for manifests and files. |
| `src/core/data_registry.py` | ODIN-facing API for registry operations. |
| `src/registry/registry_sources.yaml` | Declarative catalog of configured source, external, and derived datasets. |
| `src/registry/sources/` | Source-specific fetchers and external source providers. |
| `src/registry/derived/` | Deterministic builders for derived artifacts. |
| QA Browser registry page | Read-only view of raw snapshots, derived artifacts, and external source registrations. |
| `playbooks/ifx_registry_playbook.md` | Operational workflow for source snapshots and external sources. |
| `playbooks/derived_artifact_playbook.md` | Operational workflow for derived artifacts. |

## Storage Namespaces

Registry objects use three top-level MinIO namespaces:

```text
sources/<source>/<dataset>/<version>/
external/<source>/<dataset>/<version>/
derived/<source>/<dataset>/<version>/
```

Examples:

```text
sources/surechembl/patent_discovery/2026-06-01/
external/chembl/activity_database/chembl36/
derived/surechembl/patent_family_mentions/2026-06-01/
```

Each namespace entry contains a `manifest.yaml`. File-backed entries also store
the referenced data files next to the manifest.

## Registry Entry Types

### Source Snapshots

Source snapshots represent immutable downloaded or manual file sets. A snapshot
has:

- `source`, for example `ncbi`, `jensenlab`, `surechembl`
- `dataset`, for example `publications`, `tissues`, `patent_discovery`
- `version`, preferably an upstream release, dated directory, ontology
  `data-version`, or HTTP metadata date
- `files`, each with size, SHA256, content type, source URL, and storage URI
- `extra.version_method`, which documents how the version was discovered

`DataRegistry.sync_latest_snapshots(...)` checks configured fetchers, downloads
missing or stale datasets into a local cache, writes manifests, and uploads
files/manifests to MinIO.

### External Source Registrations

External registrations represent sources where MinIO does not own raw bytes,
such as ChEMBL or DrugCentral databases. The registry stores a manifest with:

- source/dataset/version
- non-secret connection metadata
- credential reference path
- access mode/interface
- version method evidence

`DataRegistry.sync_external_sources(...)` can check and write these manifests so
external sources are visible in the same registry catalog as file snapshots.

### Derived Artifacts

Derived artifacts are deterministic products built from registered dependencies.
They live under `derived/...` and declare exact inputs in `derived_from`.

The first concrete artifact is:

```text
derived/surechembl/patent_family_mentions/<version>/protein_patent_family_mentions.parquet
```

Its dependency is the raw source snapshot:

```text
sources/surechembl/patent_discovery/<version>/
```

Derived manifests include:

- dependency snapshot IDs and manifest URIs
- transform metadata
- watched transform code reference and `code_sha256`
- `build_key`, a hash of dependency snapshot IDs plus transform metadata
- output files and validation stats

The derived artifact `version` remains human-readable and input-oriented. For a
single dependency, it is currently the dependency version, for example
`2026-06-01`. The `build_key` is the stricter invalidation signal. This means a
code change can mark a derived artifact stale even if the upstream input version
is unchanged.

## `DataRegistry` API Shape

`DataRegistry` is the intended public interface for registry operations inside
ODIN code and ad hoc working scripts.

Current high-level operations:

```python
registry = DataRegistry.from_minio_credentials(
    "src/use_cases/secrets/ifxdev_minio.yaml"
)

registry.list_source_snapshots()
registry.list_external_sources()
registry.list_derived_artifacts()

registry.check_all_latest_registered()
registry.sync_latest_snapshots(dest="/tmp/ifx-registry-cache", dry_run=True)
registry.sync_latest_snapshots(dest="/tmp/ifx-registry-cache", dry_run=False)

registry.check_external_registrations()
registry.sync_external_sources(dest="/tmp/ifx-registry-cache", dry_run=False)

registry.check_derived_artifacts()
registry.sync_derived_artifacts(dest="/tmp/ifx-registry-cache", dry_run=True)
registry.sync_derived_artifacts(dest="/tmp/ifx-registry-cache", dry_run=False)
```

The CLI is no longer the primary abstraction. It can stay thin or be removed
where `DataRegistry` covers the behavior directly.

## Local Cache Model

The registry is authoritative. Local files are caches.

Expected build flow:

1. Registry manifest exists in MinIO.
2. `DataRegistry` materializes needed files into a local cache.
3. ETL/prep code reads local paths for speed.
4. The cache can be deleted and recreated from MinIO.

Current default cache examples:

```text
/tmp/ifx-registry-cache
/private/tmp/ifx-registry-cache
```

Dependency staging for derived artifacts uses the source snapshot cache layout:

```text
<cache>/<source>/<dataset>/<version>/
```

Derived outputs are built under:

```text
<cache>/_registry_work/derived/<source>/<dataset>/<version>/output/
```

and then copied/moved into:

```text
<cache>/<source>/<dataset>/<version>/
```

## QA Browser

The QA Browser registry page is a read-only MinIO-backed catalog. It should not
hard-code source lists.

Current display:

- top summary row with source, dataset, derived artifact, external source, and
  total size counts
- registered source snapshots grouped by source and dataset
- derived artifacts grouped by source and dataset
- external sources listed separately
- expandable file, dependency, access, and manifest details

## Current Limitations And Next Decisions

- `sync_reason` for derived artifacts is still coarse: `missing` or
  `not_latest`. The build key can detect staleness, but the status does not yet
  distinguish dependency changes from transform-code changes.
- Build-level manifests are still deferred. ETL YAMLs may eventually point at
  registered datasets, but the registry does not yet define a full ETL input
  set.
- Adapters have not all been moved to consume registry manifests or derived
  artifacts. The likely first adapter migration is SureChEMBL patent family
  mentions.
- After the Pharos/target_graph adapter and resolver migration is complete,
  remove temporary constructor compatibility for replaced `file_path`,
  `file_paths`, and `version_file_path` arguments. Registry-backed components
  should expose the `data_source` inputs they actually use rather than carrying
  both old local-file and new registry contracts indefinitely.
- Resolver SQLite artifacts are planned but not yet modeled as a separate
  registry kind.
- Direct-to-MinIO streaming for very large sources remains optional future work.
  The current path downloads to a local cache first.
- The registry does not expose a record-level API such as `get_record` or
  `get_batch`; that remains a future service layer if needed.

## Historical Investigation

The notes below are the original investigation that motivated the registry
design. They distinguish raw source data from Jessica's derived resolver
artifacts and helped define why the first implementation should start with
source snapshots and manifests rather than a full data lake.

## Goal

Before designing shared MinIO storage, inventory Jessica's source inputs and the derived artifact layers produced by `entity_resolvers`.

## Jessica Pipeline Artifact Layers

This table distinguishes Jessica's producer layers from the subset currently consumed by the `build_pharos.py` path.

| Domain | Raw sources Jessica pulls | First derived layer | Harmonized / merged layer | Final canonical artifacts | Used In `build_pharos.py` |
| --- | --- | --- | --- | --- | --- |
| TARGETS | Ensembl, NCBI Gene, HGNC, RefSeq, UniProt, NodeNorm/Babel | cleaned source tables such as `ensembl_data_with_isoforms.csv`, `ncbi_gene_info.csv`, `hgnc_complete_set.csv`, `refseq_transformed.csv`, `uniprotkb_mapping.csv`, `uniprotkb_info.csv`, `nodenorm_genes.csv`, `nodenorm_proteins.csv` | `gene_mapping_provenance.csv`, `transcript_mapping_provenance.csv`, `protein_provenance_mapping.csv` | `gene_ids.tsv`, `transcript_ids.tsv`, `protein_ids.tsv` | `gene_ids.tsv`, `transcript_ids.tsv`, `protein_ids.tsv`, `uniprotkb_mapping.csv` |
| DISEASES | MONDO, DOID, MedGen, Orphanet, OMIM, UMLS, NodeNorm disease, Jensen disease associations | cleaned source tables such as `mondo_ids.csv`, `doid.csv`, `medgen_id_mappings.csv`, `orphanet_disease_ids.csv`, `orphanet_gene_associations.csv`, `OMIM_diseases.csv`, `umls_diseases.csv`, `nodenorm_disease.csv`, `jensen_disease_gene_associations.csv` | `disease_mapping_provenance.csv`, `disease_name_clusters.tsv` | no single final disease ID file is obvious from config; current visible merged outputs are provenance- and cluster-oriented | none directly from `entity_resolvers` |
| DRUGS | GSRS public dump | `gsrs_drug_ids.tsv` | no separate merge layer visible in current config | `gsrs_drug_ids.tsv` | none directly |
| GO | GO OBO, GOA human GAF, NCBI `gene2go` | `go_obo_dataframe.csv`, `goa.csv`, `gene2go.csv` | no explicit cross-source merge file visible; GO transform also depends on target gene/protein mappings | `GOterms.csv` plus resolved GO edge outputs under `cleaned/resolved_edges/` | none directly from `entity_resolvers` |
| PPI | STRING human PPI download | filtered / transformed STRING table | no separate merge layer visible in current config | `string_ppi.csv` | none directly |
| PHENOTYPES | HPO OBO, `phenotype.hpoa`, `genes_to_phenotype.txt`, `phenotype_to_genes.txt` | cleaned phenotype edge tables | no separate merge layer visible in current config | `hpoa.tsv`, `hpo_ids.tsv`, plus resolved phenotype edge outputs | none directly |
| PATHWAYS | Pathway Commons, Panther, Reactome, WikiPathways | cleaned source tables such as `pathwaycommons_uniprot.csv`, `pathwaycommons_hgnc.csv`, `pathwaycommons_pathways.csv`, `panther2uniprot.csv`, `reactome_pathways.csv`, `reactome_to_uniprot.csv`, `wikipathways_human_pathways.tsv` | `pathway_provenance.tsv` | `pathway_ids.tsv` | none directly from `entity_resolvers` |

## Consumer Note

| Consumer | What it mainly uses |
| --- | --- |
| IFX_ODIN `src/id_resolvers` and graph builds | Mostly the final canonical artifacts, not the full raw or intermediate producer layers |

## First Implementation Scope

Start with a small source registry tool, not a full build registry.

The first milestone should only:

- download source files from known upstream locations
- write one `manifest.yaml` for each downloaded dataset snapshot
- upload the source files and manifest to shared MinIO
- verify local or uploaded files by checksum
- materialize downloaded files into a local build cache when needed

Defer build-level manifests until the source-snapshot workflow is working. A
future build manifest can decide which source snapshots and derived artifacts
belong in a specific ETL run, but that is a separate layer from recording what a
downloaded dataset is.

## Source Snapshot Manifest

Each downloaded dataset snapshot should have a manifest that says what the
dataset is, where it came from, when it was downloaded, and how to verify the
bytes.

Suggested minimal shape:

```yaml
kind: source_snapshot
schema_version: 1
source: uniprot
dataset: uniprotkb
snapshot_id: uniprotkb:2026_02
version: 2026_02
version_date: 2026-02-26
download_date: 2026-06-10
downloaded_by: ifx-registry
upstream:
  homepage: https://www.uniprot.org/
  urls:
    - https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.xml.gz
files:
  - path: uniprot_sprot.xml.gz
    size_bytes: 123456789
    sha256: "<sha256>"
    content_type: application/gzip
    source_url: https://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/uniprot_sprot.xml.gz
    storage_uri: s3://ifx-registry/sources/uniprot/uniprotkb/2026_02/uniprot_sprot.xml.gz
```

Manifest rules:

- `snapshot_id` is the stable identifier consumers should record.
- `version` is the source's release string when available.
- `version_date` is the official release date when available, otherwise a
  documented proxy such as `Last-Modified`.
- `download_date` is when IFX retrieved the bytes.
- `version_method` should describe how `version` and `version_date` were
  derived, including enough evidence to repeat or debug the derivation.
- every file entry must include `sha256`, `size_bytes`, `source_url`, and
  `storage_uri`.
- the manifest itself should be uploaded next to the files and should also be
  checksumed.
- Do not store or generate separate `*_version.tsv` files. Adapters should read
  registry manifests directly.

## Registry Tool Responsibilities

MinIO should remain the object store. The IFX-specific tool should own source
knowledge and metadata quality.

Initial command shape:

```bash
ifx-registry source fetch uniprot --version 2026_02
ifx-registry source fetch hgnc --latest
ifx-registry source inspect uniprotkb:2026_02
ifx-registry source materialize uniprotkb:2026_02 --dest /data/odin/cache/uniprotkb_2026_02
ifx-registry cache verify /data/odin/cache/uniprotkb_2026_02
```

For each supported source, the tool should know how to:

- discover the requested or latest version
- download the source files
- derive `version_date` from official metadata or a documented fallback
- record the version derivation method and evidence in the manifest
- compute checksums and file sizes
- write the source snapshot manifest
- upload files and manifests to MinIO
- recreate a local cache from MinIO without treating the cache as authoritative

## Local Build Cache

Builds should read large files from local disk for performance. The registry is
authoritative, while the local cache is disposable.

Expected flow:

1. source snapshot is downloaded and registered in MinIO
2. a developer or build server materializes the snapshot into a local cache
3. ETL adapters read local files
4. ETL metadata records the `snapshot_id` and manifest checksum used
5. old local caches can be deleted and recreated from MinIO

This avoids treating local build inputs as another source of truth.

## Explicitly Deferred

Do not solve these in the first pass:

- build manifests that define complete ETL input sets
- compatibility checks across multiple source snapshots
- derived artifact manifests for resolver outputs
- a record-level `get_record` / `get_batch` API
- a web UI over the registry
- lakeFS-style object-tree branching and commits

Those are likely useful later, but the first useful boundary is reliable source
downloads plus manifests.

## Usage Basis

`Used In build_pharos.py` means the artifact is part of the current Pharos build path through:

- [pharos.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/pharos/pharos.yaml)
- [build_pharos.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/pharos/build_pharos.py)

That column is intentionally narrower than "used somewhere in ODIN."

## Files Reviewed

- [entity_resolvers/src/workflows/targets.Snakefile](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/workflows/targets.Snakefile)
- [entity_resolvers/src/code/main.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/main.py)
- [entity_resolvers/config/targets_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/targets_config.yaml)
- [entity_resolvers/config/diseases_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/diseases_config.yaml)
- [entity_resolvers/config/drugs_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/drugs_config.yaml)
- [entity_resolvers/config/GO_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/GO_config.yaml)
- [entity_resolvers/config/ppi_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/ppi_config.yaml)
- [entity_resolvers/config/phenotypes_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/phenotypes_config.yaml)
- [entity_resolvers/config/pathways_config.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/config/pathways_config.yaml)
- [entity_resolvers/src/code/publicdata/target_data/gene_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/target_data/gene_ids.py)
- [entity_resolvers/src/code/publicdata/target_data/transcript_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/target_data/transcript_ids.py)
- [entity_resolvers/src/code/publicdata/target_data/protein_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/target_data/protein_ids.py)
- [entity_resolvers/src/code/publicdata/pathway_data/pathways_merge.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/pathway_data/pathways_merge.py)
- [entity_resolvers/src/code/publicdata/pathway_data/pathway_ids.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/entity_resolvers/src/code/publicdata/pathway_data/pathway_ids.py)
