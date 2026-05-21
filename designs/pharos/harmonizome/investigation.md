# Harmonizome Pharos Metrics Design

Date: 2026-05-21

## Goal

Preserve the old Pharos Harmonizome-style target breadth metrics without opening a separate justification thread for each graph-native metric.

Old Pharos used Harmonizome as a target-facing summary layer, not as a source-native graph ingest. The relevant legacy loaders are:
- [`load-Harmonizome.py`](https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-Harmonizome.py)
- [`load-HGramCDFs.py`](https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-HGramCDFs.py)

## Decision

Do not design 30+ new graph-derived metrics for the first pass.

Use the legacy Harmonizome table shape and CDF calculation as the compatibility target. There are two viable implementation paths:

1. **Preferred if time allows:** rebuild `gene_attribute_type`, `gene_attribute`, and `hgram_cdf` from Harmonizome 3.0.
2. **Fallback:** map/copy the existing Harmonizome 2.0-derived rows from `pharos319`.

Both paths preserve the Pharos-facing behavior and avoid having to justify each metric independently.

## Legacy Table Shape

Old `pharos319` contained:
- `gene_attribute_type`: 113 rows
- `gene_attribute`: 65,549,760 rows
- `hgram_cdf`: 1,167,880 rows

`gene_attribute_type` identifies one Harmonizome dataset.

`gene_attribute` stores one matched target association:
- `protein_id`
- `gat_id`
- `name`: gene set / attribute name
- `value`: Harmonizome `thresholdValue`

`hgram_cdf` stores one derived metric row:
- `protein_id`
- `type`: `gene_attribute_type.name`
- `attr_count`: number of gene attributes for that protein and type
- `attr_cdf`: Gaussian CDF over per-protein counts for that type

## Harmonizome 3.0 Rebuild Plan

Harmonizome 3.0 still exposes the same basic concepts:
- dataset/resource metadata
- gene sets
- gene associations
- `thresholdValue`
- optional `standardizedValue`
- gene payloads with `symbol` and often `ncbiEntrezGeneId`

Use a Pharos compatibility pipeline rather than a general IFX graph ingest.

### 1. Build the Pharos protein map

Create lookup maps from the current Pharos protein table:
- primary: `protein.geneid -> protein.id`
- fallback: `protein.preferred_symbol` / `protein.sym -> protein.id`

Prefer Entrez Gene ID matches when Harmonizome provides `ncbiEntrezGeneId`. Use symbol matching only as fallback and record ambiguous or mismatched cases.

### 2. Load dataset metadata

For each selected non-archived Harmonizome 3.0 dataset, create one `gene_attribute_type` row.

Map metadata as closely as possible:
- `name` = dataset name
- `association` = Harmonizome association text, when available
- `description` = resource/dataset description
- `resource_group` = Harmonizome category, constrained to the existing enum
- `measurement` = Harmonizome measurement, when available
- `attribute_group` = Harmonizome attribute/category grouping, when available
- `attribute_type` = Harmonizome attribute type
- `pubmed_ids` = pipe-delimited PMIDs
- `url` = resource URL

If a 3.0 category does not fit the old enum, document the mapping and use the closest legacy value rather than changing the Pharos schema in the first pass.

### 3. Load gene attributes

Process dataset/gene-set associations, not gene-centric pages, when possible.

For each association:
1. identify the Harmonizome gene
2. map it to `protein.id`
3. insert one `gene_attribute` row with:
   - `protein_id`
   - `gat_id`
   - `name` = gene set / attribute name
   - `value` = `thresholdValue`

Store only rows that map to Pharos proteins. Keep `standardizedValue` out of the compatibility tables unless a later UI requirement needs it.

### 4. Calculate CDF rows

For each `gene_attribute_type.name`:

```text
attr_count(protein, type) =
  count(gene_attribute rows for that protein and dataset type)
```

Compute the same Gaussian CDF used by `load-HGramCDFs.py`:

```text
attr_cdf = 0.5 * (1 + erf((attr_count - mean) / (std * sqrt(2))))
```

Where `mean` and `std` are calculated from the non-zero `attr_count` values observed for that dataset type. The legacy loader did not emit rows for proteins with no gene attributes for a type.

If `std == 0`, skip the type or emit a documented constant value. Do not silently invent a distribution.

## Fallback: Copy From `pharos319`

If the Harmonizome 3.0 rebuild is too slow or produces difficult-to-explain drift, copy/map the old rows from `pharos319`:
- `gene_attribute_type`
- `gene_attribute`
- `hgram_cdf`

The main mapping requirement is translating old `protein_id` values to the current Pharos protein IDs. Prefer stable identifiers in this order:
1. UniProt accession
2. Entrez Gene ID
3. approved/preferred symbol

This preserves old Harmonizome 2.0 semantics exactly, at the cost of not refreshing the source.

## Validation

For either path, validate:
- row counts by table
- number of dataset types
- top dataset types by `gene_attribute` row count
- distinct protein coverage
- `hgram_cdf.type` values all match `gene_attribute_type.name`
- `attr_count` equals grouped `gene_attribute` counts for sampled proteins/types
- `attr_cdf` recomputes from the documented Gaussian formula for sampled types
- unmapped gene/protein counts are reported

For Harmonizome 3.0, also compare against old `pharos319`:
- dataset names added, removed, or renamed
- per-type row count deltas
- protein coverage deltas
- categories/attribute types that needed legacy enum mapping

## Current Recommendation

Run a short spike on the Harmonizome 3.0 rebuild using a small set of representative datasets. If the 3.0 API/download path is stable and the validation deltas are explainable, proceed with the rebuild.

If the spike shows heavy API friction or large unexplained drift, use the `pharos319` copy path for the release and document Harmonizome as a legacy compatibility metric.

## Artifacts

- [sources.csv](/Users/kelleherkj/IdeaProjects/IFX_ODIN/designs/pharos/harmonizome/sources.csv): Harmonizome 3.0 catalog snapshot collected during discovery.
