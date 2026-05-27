# Disease ID Resolver Design

## Purpose

Use `disease_ids.tsv` as the disease identifier resolver for Pharos disease nodes and disease edge endpoints.

Adapters continue to emit the source disease IDs they actually receive. Cross-identifier reconciliation happens in `DiseaseIdResolver`.

## Source File

- Local working path: `input_files/manual/target_graph/disease_ids.tsv`
- Original handoff path for discovery: `/Users/kelleherkj/Downloads/disease_ids.tsv`
- Format: tab-separated values with one header row
- Rows profiled: 202,629

Important columns:

- `standard_id`: canonical disease ID for the row
- `nn_curie`: prior NodeNorm canonical ID when available
- `*_xref`: alternate identifiers, including MONDO, DOID, GARD, OMIM, Orphanet, UMLS, MedGen, and others
- `is_rare`: rare disease flag for future rare disease badge work

## Canonical ID Policy

Use `standard_id` as the canonical `Disease.id`.

Discovery showed that rows with a MONDO mapping already use MONDO as `standard_id`:

- rows with `mondo_xref`: 28,165
- rows with `mondo_xref` and MONDO-prefixed `standard_id`: 28,165
- rows with `mondo_xref` and non-MONDO `standard_id`: 0

This keeps MONDO as the standard disease identifier when MONDO exists, while allowing non-MONDO canonical IDs for diseases outside MONDO coverage instead of leaving every source ID unmatched.

## Resolver Behavior

For each TSV row:

- `standard_id` resolves to itself with highest priority.
- `nn_curie` resolves to `standard_id`.
- Every populated `*_xref` value resolves to `standard_id`.
- Prefix case variants are indexed for lookup robustness.
- GARD numeric IDs are indexed in both padded and unpadded forms, for example `GARD:1` and `GARD:0000001`.

Multi-matches are possible for broad xrefs such as OMIM. Resolver configs use `multi_match_behavior: All` so a source association to a broad disease identifier fans out to every curated canonical match.

Matches are ordered deterministically:

1. exact `standard_id`
2. `nn_curie`
3. xref
4. canonical ID lexical order as a tie-breaker

Unmatched IDs are allowed, matching the current Disease resolver behavior.

## Configuration

The resolver is configured in:

- `src/use_cases/working.yaml`
- `src/use_cases/pharos/pharos.yaml`
- `src/use_cases/pharos/target_graph.yaml`
- `src/use_cases/pharos/tcrd.yaml`

The Pharos graph configs include MONDO disease nodes and hierarchy, DO disease nodes and hierarchy, and the RDAS rare disease flag adapter. TCRD export uses the same disease resolver path for source-file disease associations so disease IDs do not drift between graph build and MySQL export.

## RDAS Rare Disease Flag

`RDASRareDiseaseAdapter` pages the RDAS diseases GraphQL API and emits one `Disease` node per GARD disease:

- `Disease.id`: source GARD ID normalized losslessly to padded `GARD:0000001` syntax
- `Disease.name`: RDAS `GardName`
- `Disease.rare_disease`: `True`

The adapter does not map GARD to MONDO or any other identifier family. The configured `DiseaseIdResolver` resolves those emitted GARD IDs into the canonical disease space.

The working validation produced 12,039 rare disease nodes. Most resolved to canonical MONDO IDs through `disease_ids.tsv`; the remaining records stayed as canonical GARD IDs when no MONDO mapping was available.

## MySQL Export

`Disease.rare_disease` maps to `ncats_disease.gard_rare` in the TCRD SQL converter.

For graph-to-MySQL export, `working_mysql.yaml` reads already-resolved `Disease` and `ProteinDiseaseEdge` records from `test_pharos`, so it does not configure a Disease resolver. This avoids re-resolving canonical graph Disease IDs and accidentally fanning out historical MONDO aliases during export.

`DiseaseAdapter` is configured with `associated_only: false` for TCRD export. This intentionally allows disease pages for diseases without linked target rows. It also preserves RDAS rare disease metadata and diseases required by direct source-file MySQL loads, especially TIN-X, which no longer depends on graph materialized `TINXImportanceEdge` records.

## Deferred Work

- Decide whether legacy `mondo_parent`, `do_parent`, and `ancestry_do` MySQL tables remain compatibility outputs or are replaced by a canonical `disease_parent` hierarchy.
