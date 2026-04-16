# Harmonizome Investigation Notes

Date: 2026-04-15

## Goal

Investigate whether the current Harmonizome release should be ingested into IFX_ODIN / Pharos, and if so, what the right model boundary is.

This started from the `New Concepts` note in [TCRD_TODO.md](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/pharos/TCRD_TODO.md:69).

## High-Level Conclusion

Do not start by ingesting Harmonizome wholesale.

The old Pharos/TCRD use of Harmonizome was mostly a generic summary-attribute layer over targets, not a source-native graph ingest. After inspecting the current Harmonizome catalog, API, and the old `pharos319` landing tables, the cleaner direction is likely:

1. preserve this investigation as reference
2. avoid a full Harmonizome ingest for now
3. compute analogous high-level summary metrics directly from the IFX_ODIN graph

Harmonizome may still be useful later as a source of selected refreshed datasets, but it does not currently look like the best path for a first-pass new Pharos concept.

## What Old Pharos Did

Historical loader:
- [`load-Harmonizome.py`](https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-Harmonizome.py)

Observed legacy behavior:
- one `gene_attribute_type` row per Harmonizome dataset
- one `gene_attribute` row per matched `protein_id` and gene set
- provenance recorded for:
  - `gene_attribute`
  - `gene_attribute_type`
  - `hgram_cdf`

Important implication:
- old Pharos did not use Harmonizome as a native disease/pathway/tissue/PPI graph source
- it used Harmonizome as a target-facing summary layer

## What Landed In `pharos319`

Read-only inspection of `pharos319` showed:

- `gene_attribute_type`: `113` rows
- `gene_attribute`: `65,549,760` rows
- `hgram_cdf`: `1,167,880` rows

Representative `gene_attribute_type.name` values:
- `GTEx Tissue Gene Expression Profiles`
- `DISEASES Text-mining Gene-Disease Assocation Evidence Scores`
- `Virus MINT Protein-Viral Protein Interactions`
- `Reactome Pathways`

Representative `gene_attribute` pattern:
- `protein_id`
- `gat_id`
- `name` = gene set name
- `value` = threshold-like integer value

Representative `hgram_cdf` pattern:
- `protein_id`
- `type` = dataset name
- `attr_count`
- `attr_cdf`

`hgram_cdf.type` matched `gene_attribute_type.name` directly for most legacy dataset types.

Interpretation:
- `gene_attribute_type` identified the dataset
- `gene_attribute` stored per-target membership / score against attributes within that dataset
- `hgram_cdf` stored derived summary statistics over those rows

## Current Harmonizome Surface

Current public release:
- Harmonizome 3.0

Verified current sources:
- [About](https://maayanlab.cloud/Harmonizome/about)
- [What's New](https://maayanlab.cloud/Harmonizome/whatsNew)
- [Download](https://maayanlab.cloud/Harmonizome/download)
- [Documentation](https://maayanlab.cloud/Harmonizome/documentation)

Key observations:
- The catalog is mixed.
  - some refreshed datasets are clearly current (`2023`-`2026`)
  - many legacy datasets are still present
  - some old entries are explicitly archived
- The download page currently exposes more datasets than the legacy downloader script.
  - live catalog extracted into [sources.csv](/Users/kelleherkj/IdeaProjects/IFX_ODIN/designs/harmonizome/sources.csv)
  - legacy `harmonizomedownloader.py` only hard-codes `129` datasets
- The current site still uses the same abstract model:
  - dataset/resource metadata
  - generic `Category`
  - generic `Attribute`
  - gene-attribute matrices / edge lists / gene set libraries

## Attribute Vocabulary

The current `Attribute` vocabulary from the download page is extremely close to the old `gene_attribute_type.attribute_type` vocabulary in old Pharos.

Main result:
- this appears to be the same conceptual field, carried forward with minor drift

Observed current-only additions:
- `cell type`
- `glycan`

Important nuance:
- `Attribute` is not unique to one `Category`
- the same attribute values can appear under multiple categories

Examples from [sources.csv](/Users/kelleherkj/IdeaProjects/IFX_ODIN/designs/harmonizome/sources.csv):
- `tissue`: `proteomics`, `structural or functional annotations`, `transcriptomics`
- `cell line`: `disease or phenotype associations`, `genomics`, `proteomics`, `transcriptomics`
- `microRNA`: `genomics`, `physical interactions`
- `protein complex`: `proteomics`, `structural or functional annotations`

Interpretation:
- `Dataset` is still the primary identity
- `Category` and `Attribute` are dataset metadata, not stable graph concept keys by themselves

## Current API Findings

Useful endpoints from the docs:
- `GET /api/1.0/gene`
- `GET /api/1.0/gene/<symbol>`
- `GET /api/1.0/gene/<symbol>?showAssociations=true`

Live fetch inspected:
- `GET https://maayanlab.cloud/Harmonizome/api/1.0/gene/DRD2?showAssociations=true`

Observed gene payload fields:
- `symbol`
- `synonyms`
- `name`
- `description`
- `ncbiEntrezGeneId`
- `ncbiEntrezGeneUrl`
- `proteins`
- `hgncRootFamilies`
- `associations`

Observed association payload fields:
- `geneSet.name`
- `geneSet.href`
- `thresholdValue`
- `standardizedValue`

Important identifier result:
- the live payload includes `ncbiEntrezGeneId`
- when ingesting, the intended identifier policy should be:
  - use `NCBIGene:` when available
  - otherwise use `Symbol:`

Important shape result:
- `geneSet.name` looks like:
  - `<attribute value>/<dataset name>`

Examples:
- `697/Achilles Cell Line Gene Essentiality Profiles`
- `nucleus accumbens, right/Allen Brain Atlas Adult Human Brain Tissue Gene Expression Profiles`

Interpretation:
- the API is gene-centric
- the API likely gives enough to recover:
  - dataset name
  - attribute name
  - association sign / standardized score
- but a full API crawl would still be expensive and slow

## Gene vs Protein

Current Harmonizome is still fundamentally gene-first.

Evidence:
- gene pages and API expose `ncbiEntrezGeneId`
- associations are attached to gene entities
- old loader mapped those gene associations onto `protein_id` in TCRD

Recommended semantic boundary:
- if Harmonizome were ingested, target graph should treat it as gene-level first
- protein-facing Pharos/TCRD behavior would be a later side-lift

## Modeling Options Considered

### Option A: Fully model attributes as nodes

Potential shape:
- `GeneAttributeType`
- `GeneAttribute`
- `GeneGeneAttributeEdge`

Rejected for first pass because:
- too much graph expansion
- old Pharos mostly treated attribute names as payload, not as reusable graph concepts

### Option B: Dataset node plus edge details

Potential shape:
- `GeneAttributeType` node per dataset
- `GeneGeneAttributeEdge` from `Gene` to `GeneAttributeType`
- `details` containing per-attribute entries

This was the best ingest-style fit we found.

Potential detail fields:
- `attribute_name`
- `gene_set_name`
- `gene_set_href`
- `threshold_value`
- `standardized_value`

Benefits:
- much closer to old `gene_attribute`
- much less graph bloat
- preserves dataset identity cleanly

### Option C: Do not ingest Harmonizome; compute our own summary metrics

This is the direction that currently looks best.

Reasoning:
- the real value old Pharos got from Harmonizome was high-level summary statistics
- IFX_ODIN already has a richer graph than old TCRD in many concept areas
- we can compute summary metrics directly from our own graph instead of importing Ma'ayan Lab's summary layer

Examples of graph-derived summaries we could compute ourselves:
- disease counts / percentiles
- pathway counts / percentiles
- tissue/expression breadth
- GO annotation breadth
- ligand / MoA breadth
- publication / GeneRIF metrics
- future phenotype or PPI breadth if those concepts are added

## `attr_count` / `attr_cdf`

Important clarification from the legacy schema:
- `attr_count` and `attr_cdf` are derived summary values
- they are not the raw association payload

Likely intended meaning:
- `attr_count` = number of associated attributes for one dataset type for one protein
- `attr_cdf` = empirical cumulative distribution value over those counts for that dataset type

This is not simple min-max scaling.

If we ever reproduce this behavior:
- compute from graph associations in post-processing
- not during raw adapter ingest

## Current Recommendation

Do not implement Harmonizome ingest yet.

Instead:
- treat this investigation as closed for now
- if we revisit, start from a graph-derived summary-metrics design
- only pull specific Harmonizome datasets later if there is a concrete gap our graph does not already cover

If Harmonizome is revisited later, the most defensible ingest boundary would be:
- `GeneAttributeType` node per dataset
- `GeneGeneAttributeEdge` with per-attribute `details`
- optional side-lift to protein-facing export for Pharos

## Artifacts Produced

- [sources.csv](/Users/kelleherkj/IdeaProjects/IFX_ODIN/designs/harmonizome/sources.csv)
- this note

## Open Questions If Revisited

- Which graph-derived summary metrics would actually be most useful in Pharos UX?
- Should those summaries live only in post-processing / MySQL export, or also as graph fields?
- Are there any current Harmonizome datasets that still fill genuine gaps after accounting for direct sources already present in IFX_ODIN?
