# TIN-X Ingest Design

## Goal
Add a first-pass TIN-X ingest derived from the raw Jensen mention files, while keeping the current IFX_ODIN disease resolution model intact and skipping article-rank / PMID ranking support.

## Source Files

Primary inputs:
- `https://download.jensenlab.org/human_textmining_mentions.tsv`
- `https://download.jensenlab.org/disease_textmining_mentions.tsv`

## Version Strategy

- Download the two Jensen mention files into `input_files/auto/jensenlab/`.
- Record a shared `tinx_version.tsv`.
- Use the latest `Last-Modified` date across the two source URLs as `version_date`.
- Leave `version` empty unless Jensen exposes a stable release string later.

## Observed Payload Shape

Both source files are headerless TSVs with two columns:
1. source entity identifier
2. space-delimited PMID list

Observed examples:
- `human_textmining_mentions.tsv`
  - non-protein entries exist, e.g. `18S_rRNA`
  - accepted target rows use `ENSP...`
- `disease_textmining_mentions.tsv`
  - legacy loader and TCRD outputs indicate accepted rows use `DOID:*`

## Legacy Behavior

The old UNM flow computed TIN-X from the raw Jensen mention files, then loaded:
- `tinx_novelty`
- `tinx_disease`
- `tinx_importance`
- `tinx_articlerank`

For `pharos319`, only the aggregate score tables survive in the snapshot currently used for parity:
- `tinx_novelty`
- `tinx_disease`
- `tinx_importance`

`tinx_articlerank` was present in older TCRD histories but is intentionally out of scope for this first pass.

## Current Mapping

- `Protein.novelty`
  - computed from Jensen protein mention PMIDs
  - stored on the canonical `Protein` node as a list
  - collapsed to `min(novelty)` when exporting to MySQL
- `Disease.novelty`
  - computed from Jensen disease mention PMIDs
  - stored on the canonical resolved `Disease` node as a list
  - collapsed to `min(novelty)` when exporting to MySQL
- `TIN-X` importance
  - computed from shared protein/disease PMIDs
  - no longer materialized in the Pharos graph
  - loaded directly into TCRD/MySQL from source files in `tcrd.yaml`
  - canonical disease/protein IDs still depend on resolver behavior matching the graph build

MySQL shape:
- no dedicated `tinx_novelty`
- no dedicated `tinx_disease`
- protein novelty goes on canonical `protein.novelty`
- disease novelty goes on canonical `ncats_disease.novelty`
- `tinx_importance` remains as the source-specific association table
- `tinx_importance` is keyed by canonical disease identity plus protein identity
- original source `DOID` is retained in `tinx_importance.doid` as provenance only

Short-term operational constraint:
- `src/use_cases/pharos/tcrd.yaml` must keep its `Disease` resolver behavior aligned with `src/use_cases/pharos/pharos.yaml`
- otherwise the direct TIN-X importance loader can drift from the canonical disease IDs used by the graph build

## Scoring Rules

Use the legacy TIN-X aggregate formulas:

- protein novelty:
  - `1 / sum(1 / targets_in_pmid)` over PMIDs mentioning the protein
- disease novelty:
  - `1 / sum(1 / diseases_in_pmid)` over PMIDs mentioning the disease
- protein-disease importance:
  - `sum(1 / (targets_in_pmid * diseases_in_pmid))` over shared PMIDs

## Filtering

- keep only protein rows with `ENSP...` identifiers
- keep only disease rows with `DOID:*`
- skip article-rank and PMID-level support outputs in the first pass
- do not enrich diseases from Disease Ontology inside the TIN-X adapter; ontology metadata should come from the regular disease ingest

## Config Wiring

Current wiring:
- `src/use_cases/working.yaml`
- `src/use_cases/working_mysql.yaml`
- `src/use_cases/pharos/target_graph.yaml`
- `src/use_cases/pharos/pharos.yaml`
- `src/use_cases/pharos/tcrd.yaml`

## Validation Outcome

Validated in the working path:
- graph stores TIN-X protein novelty on `Protein.novelty`
- graph stores TIN-X disease novelty on `Disease.novelty`
- MySQL stores novelty on canonical `protein.novelty` and `ncats_disease.novelty`
- MySQL stores TIN-X association scores plus source `DOID` in `tinx_importance`

Notable downstream behavior:
- `DiseaseAdapter(associated_only: true)` means the MySQL path only exports diseases that participate in exported associations
- unresolved TIN-X diseases can remain canonical `DOID:*` diseases; resolved ones typically become `MONDO:*`
