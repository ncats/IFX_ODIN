# Shared Data Investigation

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
