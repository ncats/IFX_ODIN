# Ortholog And Mouse Phenotype Design

## Goal

Add a graph-first ortholog and mouse phenotype path that:

- keeps source semantics clean in the graph
- supports Pharos protein-centric traversal through explicit side-lifted edges
- projects compatible `ortholog`, `nhprotein`, and `phenotype` rows into `pharos400`

## Sources

### HCOP

- file: `human_all_hcop_sixteen_column.txt.gz`
- role: human gene to non-human gene orthology
- graph scope: full legacy Pharos species allowlist

Accepted species:

- `9598`
- `9544`
- `10090`
- `10116`
- `9615`
- `9796`
- `9913`
- `9823`
- `13616`
- `9258`
- `9031`
- `28377`
- `8364`
- `7955`
- `6239`
- `7227`
- `4932`

### IMPC

- file: `genotype-phenotype-assertions-IMPC.csv.gz`
- role: mouse gene to mouse phenotype assertions
- key source ID: `marker_accession_id` (`MGI:...`)

### JAX/MGI

- file: `HMD_HumanPhenotype.rpt`
- role: human gene to mouse phenotype assertions
- key human ID: human Entrez Gene ID
- key phenotype IDs: comma-delimited `MP:...` list

Observed current payload shape:

- headerless
- tab-delimited
- 6 columns
- human symbol
- human Entrez Gene ID
- mouse symbol
- mouse MGI marker ID
- comma-delimited MP term IDs
- trailing empty column

### MP Ontology

- file: `mp.obo`
- role: authoritative `MP:... -> name` term labels for `MousePhenotype`

## Graph Model

### Nodes

#### `OrthologGene`

Non-human ortholog anchor.

Important fields:

- `id`
- `species`
- `symbol`
- `name`
- `source_primary_id`
- `source_db_id`
- `entrez_gene_id`
- `ensembl_gene_id`
- `xref`

#### `MousePhenotype`

MP ontology term.

Fields:

- `id`
- `name`

### Source-Truth Edges

#### `GeneOrthologGeneEdge`

From:

- human `Gene`

To:

- non-human `OrthologGene`

Fields:

- `species`
- `support_sources`
- `source_db_ids`
- `ortholog_symbols`
- `ortholog_names`

This is the authoritative HCOP edge in `target_graph`.

#### `OrthologGeneMousePhenotypeEdge`

From:

- `OrthologGene`

To:

- `MousePhenotype`

Payload:

- `details`

Current IMPC detail fields:

- `source`
- `source_id`
- `top_level_term_id`
- `top_level_term_name`
- `trait`
- `p_value`
- `percentage_change`
- `effect_size`
- `procedure_name`
- `parameter_name`
- `gp_assoc`
- `statistical_method`
- `sex`

#### `GeneMousePhenotypeEdge`

From:

- human `Gene`

To:

- `MousePhenotype`

Payload:

- `details`

Current HMD/JAX detail fields:

- `source`
- `source_id`

### Pharos-Facing Side-Lifted Edges

These are materialized in the Pharos graph through explicit edge remapping.

#### `ProteinOrthologGeneEdge`

Protein-facing side-lift of `GeneOrthologGeneEdge`.

#### `ProteinMousePhenotypeEdge`

Protein-facing side-lift of `GeneMousePhenotypeEdge`.

Notes:

- side-lifting is explicit, not generic
- edge remapping is hardcoded in `InputAdapter._canonicalize_relationship_class()`
- `OrthologGeneMousePhenotypeEdge` remains unchanged because IMPC is already anchored on the mouse side

## Resolver Strategy

### `HCOPOrthologGeneResolver`

Purpose:

- canonicalize non-human ortholog gene IDs through Translator Node Normalizer
- restrict `OrthologGene` resolution to the HCOP-defined ortholog universe

Behavior:

1. preload accepted HCOP ortholog-side IDs at startup
2. canonicalize them through Node Normalizer
3. store the resulting canonical IDs as the allowed `OrthologGene` set
4. only resolve incoming ortholog IDs that fall inside that set

This resolver is skip-only by contract.

### Human-Side Resolution

#### `target_graph.yaml`

- human ortholog and HMD/JAX edges resolve to `Gene`
- HCOP remains gene-native
- HMD/JAX remains gene-native

#### `pharos.yaml`

- human ortholog and HMD/JAX edges resolve to `Protein`
- explicit edge remapping produces:
  - `ProteinOrthologGeneEdge`
  - `ProteinMousePhenotypeEdge`

## Promoted Graph Scope

### `target_graph.yaml`

Promoted path:

- `HCOPOrthologAdapter`
- `MPPhenotypeAdapter`
- `IMPCPhenotypeAdapter`
- `HMDHumanPhenotypeAdapter`

Semantics:

- HCOP: `Gene -> OrthologGene`
- IMPC: `OrthologGene -> MousePhenotype`
- HMD/JAX: `Gene -> MousePhenotype`

### `pharos.yaml`

Promoted path:

- same sources and same HCOP resolver
- human-side edges side-lift to `Protein`

Semantics:

- HCOP: `Protein -> OrthologGene`
- IMPC: `OrthologGene -> MousePhenotype`
- HMD/JAX: `Protein -> MousePhenotype`

## MySQL Projection

### `OrthologGene -> nhprotein`

`OrthologGene` is projected to `nhprotein` as a compatibility join layer.

Populate:

- `sym`
- `name`
- `description`
- `species`
- `taxid`
- `geneid`
- `uniprot` only when a real non-human UniProt xref exists

Important note:

- this is gene-derived compatibility output
- it is not intended to reproduce old UniProt-derived `nhprotein` semantics exactly

### `ProteinOrthologGeneEdge -> ortholog`

Export rule:

- apply the legacy support rule in the converter, not in the graph ingest
- support set considered: `{Inparanoid, OMA, EggNOG}`
- require at least `2`
- emit deterministic `sources` ordering:
  - `Inparanoid, OMA, EggNOG`
  - `Inparanoid, OMA`
  - `Inparanoid, EggNOG`
  - `OMA, EggNOG`

Populate:

- `protein_id`
- `taxid`
- `species`
- `db_id`
- `geneid`
- `symbol`
- `name`
- `mod_url`
- `sources`

This table should stay as close as practical to `pharos319.ortholog`.

### IMPC: `OrthologGeneMousePhenotypeEdge -> phenotype`

Export rule:

- one MySQL row per IMPC detail entry per qualifying linked human protein
- linked proteins come from `ProteinOrthologGeneEdge`
- apply the same legacy HCOP `2-of-3` support rule before emitting the row
- if no linked human ortholog passes the rule, skip the row

Populate:

- `ptype = 'IMPC'`
- `protein_id`
- `nhprotein_id`
- `term_id`
- `term_name`
- legacy-compatible IMPC phenotype fields from the detail payload

### JAX/MGI: `ProteinMousePhenotypeEdge -> phenotype`

Export rule:

- direct protein-facing projection
- one MySQL row per detail entry
- no `nhprotein_id`

Populate:

- `ptype = 'JAX/MGI Human Ortholog Phenotype'`
- `protein_id`
- `nhprotein_id = NULL`
- `term_id`
- `term_name`

This matches the legacy JAX/MGI phenotype shape much better than projecting genotype-level mouse phenotype assertions.

## Current Validation Outcome

Validated end-to-end on the current promoted mouse phenotype path:

- full HCOP species allowlist works in graph ingest
- IMPC phenotype rows project through `nhprotein` and filtered ortholog links
- HMD/JAX phenotype rows project directly to `protein_id` with `nhprotein_id = NULL`
- MP ontology fills `MousePhenotype.name` completely for the loaded phenotype node set

Observed final MySQL phenotype counts in `pharos400_working`:

- `IMPC`: `59,812`
- `JAX/MGI Human Ortholog Phenotype`: `85,133`

Observed JAX/MGI MySQL shape:

- `protein_id` populated on all rows
- `nhprotein_id` populated on none
- `term_id` and `term_name` populated from MP-backed `MousePhenotype`
