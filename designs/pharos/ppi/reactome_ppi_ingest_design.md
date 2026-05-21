# Reactome PPI Ingest Design

## Status

Implemented, validated in the working graph and working MySQL paths, and promoted to `pharos.yaml` / `target_graph.yaml`.

## Goal

Add a first-pass Reactome-derived protein-protein interaction ingest for Pharos.

## Source Choice

Use the official human Reactome tab-delimited interaction file:

- `reactome.homo_sapiens.interactions.tab-delimited.txt`

Rationale:

- This is the current official human interaction export on the Reactome download site.
- It matches the old TCRD loader input format.
- It includes interaction type and context/PMID fields that can map naturally into the current PPI model and TCRD export.

## Source URLs

- Download docs: `https://reactome.org/download-data?id=62&ml=1`
- Directory index: `https://reactome.org/download/current/interactors/`
- Human tab-delimited file: `https://reactome.org/download/current/interactors/reactome.homo_sapiens.interactions.tab-delimited.txt`

## Version Strategy

- Use the Reactome database version recorded in `input_files/auto/reactome/reactome_version.tsv`
- Use the PPI file `Last-Modified` header as `version_date`
- Let adapter-side `download_date` come from file mtime unless we later decide to persist it explicitly

## Documented File Shape

Reactome documents the tab-delimited human interaction file as:

1. interactor 1 protein ID
2. interactor 1 Ensembl gene ID(s)
3. interactor 1 Entrez Gene ID(s)
4. interactor 2 protein ID
5. interactor 2 Ensembl gene ID(s)
6. interactor 2 Entrez Gene ID(s)
7. interaction type
8. interaction context
9. PubMed IDs

## Legacy Comparison

The old UNM TCRD loader used the same human tab-delimited Reactome interaction file and:

- required both interactors to have UniProt IDs
- populated `interaction_type`
- skipped duplicate interaction rows
- skipped self-pairs

This should be treated as a comparison point only; current behavior should still be validated against the real file after download.

## Observed File Profile

Observed counts from the downloaded file:

- total rows: `123,895`
- rows where both interactors are UniProt proteins: `83,545`
- filtered non-protein rows: `40,350`
- protein self-pairs: `7,677`
- duplicate unordered protein-pair-plus-type rows: `57,386`
- distinct unordered protein-pair-plus-type combinations: `26,159`

Observed payload behavior:

- non-protein rows include identifiers such as `ChEBI:*`
- almost every protein-protein row has PubMed references
- every row has a Reactome context string like `reactome:R-HSA-...`
- current interaction types include values such as:
  - `physical association`
  - `enzymatic reaction`
  - `cleavage reaction`
  - `dephosphorylation reaction`

## Implemented Mapping

Current first-pass graph mapping:

- emit `PPIEdge`
- keep only rows where both interactors are UniProt IDs
- skip self-pairs
- canonicalize unordered protein pairs
- dedupe repeated source rows by unordered pair plus interaction type
- preserve:
  - `interaction_type` as a graph list field
  - `contexts` as a graph list field
  - `pmids` as a graph list field
- do not populate adapter-level `sources`; the ETL framework stamps canonical datasource/version metadata

## Legacy Downstream Comparison

Direct inspection of `pharos319.ncats_ppi` showed:

- `StringDB` populated only `score`
- `BioPlex` populated only `p_int`, `p_ni`, and `p_wrong`
- `Reactome` rows left `evidence`, `interaction_type`, `score`, `p_int`, `p_ni`, and `p_wrong` empty

## Current Downstream Mapping

Current IFX_ODIN downstream decision:

- keep Reactome `pmids`, `contexts`, and `interaction_type` in the graph
- map `pmids` to `ncats_ppi.evidence` as pipe-delimited PMIDs
- map `interaction_type` to `ncats_ppi.interaction_type`
- keep `contexts` graph-only for now

## Validation Summary

Validated outcomes:

- Reactome-backed graph edges landed with non-empty `pmids`, `contexts`, and `interaction_type`
- Reactome merged cleanly with both BioPlex and STRING on shared canonical pairs
- downstream `ncats_ppi` rows now carry Reactome PMIDs in `evidence` and Reactome interaction types in `interaction_type`
- promoted into:
  - `src/use_cases/pharos/pharos.yaml`
  - `src/use_cases/pharos/target_graph.yaml`

## Open Follow-Ups

- decide whether Reactome context should eventually have its own dedicated downstream column or lookup table
