# Reactome Nearest Tclin Feasibility Design

## Goal

Evaluate whether the legacy KEGG-derived `kegg_distance` and
`kegg_nearest_tclin` features in Pharos/TCRD can be rebuilt from a non-KEGG
pathway source, with Reactome as the leading candidate.

This document records discovery findings only. No ingest or conversion changes
have been implemented for this feature.

## Motivation

Legacy Pharos 3.19 contains two KEGG-generated pathway distance tables:

- `kegg_distance`: directed shortest-path distance between two proteins.
- `kegg_nearest_tclin`: nearest upstream and downstream Tclin targets derived
  from `kegg_distance`.

KEGG licensing now makes this an undesirable dependency for the current Pharos
build. Reactome is attractive because the repository already downloads and
ingests Reactome pathway content, and Reactome database-derived files are
licensed under CC0 according to the Reactome license page:
`https://reactome.org/license`.

## Legacy KEGG Algorithm

Historical TCRD loader references:

- `https://github.com/unmtransinfo/TCRD/blob/master/loaders/KEGG_Graph.py`
- `https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-KEGGDistances.py`
- `https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-KEGGNearestTclins.py`

The legacy process was:

1. Parse each KEGG KGML pathway into a NetworkX directed graph.
2. Convert KEGG group entries into bidirectional complete subgraphs.
3. Convert KEGG relations into directed graph edges.
4. Compute all directed shortest path lengths per pathway.
5. Collapse duplicate ordered protein pairs across pathways by keeping the
   minimum observed distance.
6. Map KEGG human gene IDs to TCRD protein IDs.
7. Insert ordered protein-protein distances into `kegg_distance`.
8. For each non-Tclin target, query nearest upstream and downstream Tclin
   proteins from `kegg_distance` and insert those rows into
   `kegg_nearest_tclin`.

Observed orientation in `pharos319`:

- downstream nearest Tclin maps as `protein_id -> Tclin protein`.
- upstream nearest Tclin maps as `Tclin protein -> protein_id`.
- `kegg_nearest_tclin.tclin_id` stores `target.id`, not `protein.id`.

## Legacy pharos319 Table Profiles

### kegg_distance

Schema:

- `id`
- `pid1` -> `protein.id`
- `pid2` -> `protein.id`
- `distance`

Profile:

- rows: 208,238
- distinct `pid1`: 4,323
- distinct `pid2`: 4,071
- distinct ordered pairs: 208,238
- duplicate ordered pairs: 0
- reciprocal unordered pair groups: 35,501

Distance distribution:

| distance | rows |
| --- | ---: |
| 1 | 57,868 |
| 2 | 54,891 |
| 3 | 33,806 |
| 4 | 22,026 |
| 5 | 16,772 |
| 6 | 11,122 |
| 7 | 5,798 |
| 8 | 2,646 |
| 9 | 2,244 |
| 10 | 533 |
| 11 | 427 |
| 12 | 55 |
| 13 | 19 |
| 14 | 12 |
| 15 | 11 |
| 16 | 8 |

### kegg_nearest_tclin

Schema:

- `id`
- `protein_id` -> `protein.id`
- `tclin_id` -> `target.id`
- `direction`: `upstream` or `downstream`
- `distance`

Profile:

- rows: 15,911
- distinct source proteins: 2,574
- distinct Tclin targets referenced: 403
- distinct protein/Tclin pairs: 14,335
- all referenced nearest targets have `target.tdl = 'Tclin'`
- no duplicate `(protein_id, tclin_id, direction)` groups
- 1,576 protein/Tclin pairs appear in both directions

Direction profile:

| direction | rows | min distance | max distance | avg distance |
| --- | ---: | ---: | ---: | ---: |
| upstream | 7,563 | 1 | 8 | 1.6807 |
| downstream | 8,348 | 1 | 7 | 1.5073 |

Source target TDL profile:

| source TDL | rows | source proteins |
| --- | ---: | ---: |
| Tbio | 10,676 | 1,658 |
| Tchem | 4,423 | 756 |
| Tdark | 454 | 112 |
| Tclin | 358 | 48 |

Overlap with `kegg_distance`:

- downstream nearest rows match `kegg_distance` in the forward orientation
  (`protein_id -> Tclin protein`) for 8,348 / 8,348 rows.
- upstream nearest rows match `kegg_distance` in the reverse orientation
  (`Tclin protein -> protein_id`) for 7,563 / 7,563 rows.
- distances match exactly in those expected orientations.

## Reactome Inputs Already Present

The repository already downloads Reactome inputs in `workflows/pharos.Snakefile`:

- `input_files/auto/reactome/ReactomePathways.gmt.zip`
- `input_files/auto/reactome/ReactomePathwaysRelation.txt`
- `input_files/auto/reactome/UniProt2Reactome_All_Levels.txt`
- `input_files/auto/reactome/reactome.homo_sapiens.interactions.tab-delimited.txt`
- `input_files/auto/reactome/reactome_version.tsv`

Existing Reactome adapters:

- `src/input_adapters/reactome/reactome_pathways.py`
- `src/input_adapters/reactome/reactome_ppi.py`

Current Reactome pathway ingest emits:

- `Pathway` nodes
- `PathwayParentEdge` edges
- `ProteinPathwayEdge` edges

Current Reactome PPI ingest emits `PPIEdge` records. It intentionally sorts
protein pairs before emitting, which makes the graph undirected for PPI
purposes. That behavior is not suitable for upstream/downstream distance
calculation; a distance calculation must read the raw ordered interaction rows
or use a dedicated adapter/computation path.

## Reactome Interaction File Profile

Local file profiled:

`input_files/auto/reactome/reactome.homo_sapiens.interactions.tab-delimited.txt`

Observed profile:

- rows: 123,895
- self rows: 7,771
- distinct UniProt tokens: 9,700
- distinct ordered pairs: 47,752
- distinct unordered pairs: 41,176
- unordered pair groups with reciprocal ordered rows: 6,576

Interaction type distribution:

| interaction type | rows |
| --- | ---: |
| physical association | 112,584 |
| enzymatic reaction | 7,885 |
| cleavage reaction | 1,615 |
| dephosphorylation reaction | 615 |
| oxidoreductase activity electron transfer reaction | 418 |
| glycosylation reaction | 158 |
| phospholipase reaction | 145 |
| acetylation reaction | 99 |
| isomerase reaction | 98 |

Reactome documents these interaction files as electronically inferred from
complexes and reactions. The interaction meaning is broad: two proteins may
occur in the same complex or in the same reaction. This makes the file useful as
a protein relationship network, but weaker as a directional biological flow
network.

Relevant Reactome documentation:

- `https://reactome.org/download-data`
- `https://reactome.org/download-data?id=62&ml=1`
- `https://reactome.org/license`

## Feasibility Assessment

Reactome can support a KEGG-like distance feature mechanically, but the
semantics are not identical.

What is feasible in a first pass:

- Build a directed graph from raw ordered Reactome interaction rows.
- Exclude self-pairs.
- Compute shortest directed paths over UniProt IDs.
- Resolve UniProt IDs to target proteins using the normal resolver/export path.
- Derive nearest upstream and downstream Tclin rows from the computed distance
  table using the same orientation as legacy:
  - downstream: source protein -> Tclin protein
  - upstream: Tclin protein -> source protein

Key risks:

- Most Reactome interaction rows are `physical association`, which does not
  imply pathway flow direction.
- Existing `ReactomePPIAdapter` destroys row order by sorting protein pairs, so
  it cannot be reused directly for directed distance.
- Reactome interaction context is a pathway/reaction-like identifier, but the
  flat interaction file does not expose the full Reactome reaction graph
  structure needed to distinguish inputs, outputs, catalysts, regulators, and
  complex membership.
- Counts and distances should not be expected to match KEGG. Reactome has
  broader protein coverage in the local interaction file than legacy KEGG, but
  a different relationship model.

## Recommendation

Treat Reactome nearest Tclin as a larger modeling effort, not a quick table
replacement.

For a minimal implementation, create Reactome-specific downstream tables or
graph-derived exports named for Reactome semantics, for example
`reactome_distance` and `reactome_nearest_tclin`, rather than repopulating
`kegg_distance` or `kegg_nearest_tclin`.

For a biologically stronger implementation, use Reactome reaction graph data
instead of the flat PPI interaction file. That effort should model at least:

- reaction inputs
- reaction outputs
- catalysts
- positive and negative regulators
- complex membership
- pathway/reaction context

The first validation milestone should be an offline prototype that computes
Reactome distances and reports:

- total distance rows
- distance distribution
- number of proteins with upstream/downstream Tclin hits
- nearest Tclin row counts by source target TDL
- representative source protein -> Tclin paths
- comparison against legacy `pharos319.kegg_distance` and
  `pharos319.kegg_nearest_tclin` distributions

Only after that prototype should model classes, converters, or MySQL output
tables be added.
