# BioPlex PPI Ingest Design

## Status

Implemented, validated in the working graph and working MySQL paths, and promoted to `pharos.yaml` / `target_graph.yaml`.

## Goal

Add a first-pass BioPlex protein-protein interaction ingest for Pharos.

## Source Choice

Use the official undirected BioPlex 3.0 interaction releases from the BioPlex download page:

- `BioPlex_293T_Network_10K_Dec_2019.tsv`
- `BioPlex_HCT116_Network_5.5K_Dec_2019.tsv`

Rationale:

- These are the current official-release network files exposed on the BioPlex site.
- They match the current graph `PPIEdge` model better than the directed bait-prey files.
- They avoid the noisier unfiltered candidate-interaction lists.

## Source URLs

- Landing page: `https://bioplex.hms.harvard.edu/interactions.php`
- Data index: `https://bioplex.hms.harvard.edu/data/`
- 293T release: `https://bioplex.hms.harvard.edu/data/BioPlex_293T_Network_10K_Dec_2019.tsv`
- HCT116 release: `https://bioplex.hms.harvard.edu/data/BioPlex_HCT116_Network_5.5K_Dec_2019.tsv`

## Version Strategy

- Use BioPlex release label `3.0` as the dataset version.
- Capture per-file `Last-Modified` dates into `input_files/auto/bioplex/bioplex_version.tsv`.
- Let adapter-side `download_date` come from file mtime unless we later decide to persist it explicitly.

Observed current `/data` filenames include a December 2019 stamp even though the site still presents them as the current BioPlex 3.0 official releases.

## Observed File Shape

Current BioPlex 3.0 files have the same shape as the legacy TCRD BioPlex loader expected:

- `GeneA`
- `GeneB`
- `UniprotA`
- `UniprotB`
- `SymbolA`
- `SymbolB`
- `pW`
- `pNI`
- `pInt`

Observed counts:

- `BioPlex_293T_Network_10K_Dec_2019.tsv`: `118,162` rows
- `BioPlex_HCT116_Network_5.5K_Dec_2019.tsv`: `70,966` rows

Observed payload details:

- no self-pairs in either file
- gene IDs are numeric Entrez Gene identifiers
- isoform-suffixed UniProt accessions are common
- `UniprotA` can be the literal string `UNKNOWN`
  - `293T`: `4,950` rows
  - `HCT116`: `1,688` rows
- `UniprotB` did not contain `UNKNOWN` in the profiled files
- `pInt` values range from about `0.75` to `1.0`

## Implemented Mapping

Current graph mapping:

- emit `PPIEdge`
- use UniProt accessions as the primary emitted identifier family for endpoint proteins
- fall back to `NCBIGene` when BioPlex reports `UniprotA='UNKNOWN'`
- preserve BioPlex confidence-style fields into:
  - `p_wrong`
  - `p_ni`
  - `p_int`

Implementation choice:

- configure one adapter instance per file so provenance distinguishes `293T` versus `HCT116` via the version string
- do not populate adapter-level `sources`; the ETL framework stamps canonical datasource/version metadata

## Validation Summary

Validated outcomes:

- merged graph edges can carry multiple `p_int` values when the same canonical pair is supported by both BioPlex cell lines
- `UNKNOWN` UniProt rows resolve through `NCBIGene:*` fallback when the reviewed target graph contains the mapped protein
- downstream `ncats_ppi` exports BioPlex rows with scalar `p_int`, `p_ni`, and `p_wrong` using `max(...)` collapse for merged graph lists
- promoted into:
  - `src/use_cases/pharos/pharos.yaml`
  - `src/use_cases/pharos/target_graph.yaml`

Open follow-up questions:

- whether cell-line provenance should eventually be carried in a dedicated edge field instead of only in provenance / sources
