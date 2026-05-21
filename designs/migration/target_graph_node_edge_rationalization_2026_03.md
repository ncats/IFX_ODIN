# Target Graph Node And Edge Rationalization, March 2026

## Goal

Make `target_graph` emit node and edge types that match source-truth biology and
identifier level, while still allowing Pharos to present protein-facing views.

This design is now based on source audits, resolver-cache checks, and paired
comparison builds of:

- Pharos-style graph: `TCRDTargetResolver(reviewed_only=True)`
- target-graph-style graph: `TargetGraph*Resolver`

## Core Rule

`Node type must match the semantic level of the source identifier.`

- gene ID -> `Gene`
- transcript ID -> `Transcript`
- protein ID -> `Protein`

Do not use `Protein` as a convenience alias for “target-like thing.”

## Why This Matters

`target_graph` resolves `Gene`, `Transcript`, and `Protein` independently.
If an adapter emits `Protein(id="ENSEMBL:ENSG...")`, the protein resolver may
skip it, drop edges, or fan out in ways that hide the source’s real meaning.

Pharos can still project gene-level facts into protein space later. That
projection should be explicit and resolver-driven, not baked into the ingest
type incorrectly.

## Final Decisions By Source

### Change To Gene-Anchored

- `CTDGeneDiseaseAdapter`
  - source IDs are `NCBIGene:*`
  - now emits `GeneDiseaseEdge`
- `WikiPathways...`
  - source IDs are `NCBIGene:*`
  - now emits `GenePathwayEdge`
- `PathwayCommons...`
  - source IDs are symbol-level gene identifiers
  - now emits `GenePathwayEdge`
- `GTExExpressionAdapter`
  - source IDs are `ENSG`
  - now emits `GeneTissueExpressionEdge`
- `HPARnaExpressionAdapter`
  - source IDs are `ENSG`
  - now emits `GeneTissueExpressionEdge`
- `HPAProteinExpressionAdapter`
  - source IDs are still `ENSG` even though the assay is IHC protein expression
  - now emits `GeneTissueExpressionEdge`

### Keep Protein-Anchored

- `HPMExpressionAdapter`
  - source IDs are overwhelmingly RefSeq protein accessions (`NP_*`, `XP_*`)
  - stays `ProteinTissueExpressionEdge`
- `JensenLabTissuesExpressionAdapter`
  - raw file is messy, but adapter-kept rows are all `ENSP`
  - stays `ProteinTissueExpressionEdge`

### Already Fine / Not In Scope

- target-graph structural adapters:
  - `GeneNodeAdapter`
  - `TranscriptNodeAdapter`
  - `ProteinNodeAdapter`
  - `GeneTranscriptEdgeAdapter`
  - `TranscriptProteinEdgeAdapter`
  - `GeneProteinEdgeAdapter`
  - `IsoformProteinEdgeAdapter`
- direct protein-centric sources:
  - UniProt protein and disease adapters
  - GO protein-term edges
  - IUPHAR / ChEMBL / DrugCentral protein-ligand edges
  - Reactome protein-pathway edges
  - `AntibodyCountAdapter`
  - `IDGFamilyAdapter`
  - `TotalPMScoreAdapter`

## Evidence From Comparison Builds

### Gene-Anchored Sources

- `CTD`
  - target-graph retained gene-level facts correctly
  - Pharos side-lifted the subset with protein target paths
- `WikiPathways`
  - target-graph retained more source memberships than protein projection
- `PathwayCommons`
  - symbol matching to proteins was high, but gene-anchored output still
    preserved more source-truth memberships
- `GTEx`
  - strongest example
  - target-graph retained far more gene-tissue associations than Pharos protein
    projection
- `HPA RNA`
  - source is clearly gene-level
  - bounded comparison slice projected fully, but full resolver audit still shows
    TCRD loss due to reviewed-only target projection
- `HPA Protein`
  - source is also gene-level
  - bounded comparison showed modest Pharos loss from reviewed-only projection

### Protein-Anchored Sources

- `HPM`
  - target-graph protein resolver coverage is high
  - Pharos drops many rows because many protein mappings are unreviewed-only
- `JensenLab`
  - adapter-kept IDs are protein-level `ENSP`
  - Pharos loss is also dominated by reviewed-only projection

## Resolver Findings

### CTD

- `TargetGraphGeneResolver` matched gene IDs
- `TargetGraphProteinResolver` did not
- `TCRDTargetResolver` matched only the subset that rolls up to reviewed targets

Conclusion:
- CTD is genuinely gene-level and must remain gene-anchored in `target_graph`

### HPM / JensenLab

- `TargetGraphProteinResolver` matched strongly
- `TCRDTargetResolver` was narrower because it only exposes reviewed targets

Conclusion:
- these are protein-level sources; loss on the Pharos side is expected target
  projection behavior, not an ingest typing bug

## Gene Tau / Calculated Properties

Gene-level expression sources previously computed tau while emitting protein
patches. After moving them to gene-anchored output:

- `Gene` now carries `calculated_properties`
- restored:
  - `gtex_tau`
  - `gtex_tau_male`
  - `gtex_tau_female`
  - `hpa_rna_tau`
  - `hpa_ihc_tau`

These are now emitted on `Gene` node patches in `target_graph`.

## Cross-Type Node Coercion For Pharos

Pharos still needs some gene-level patches to land on canonical protein nodes.
To support that safely:

- node-side cross-type coercion is now allowed only when populated fields on the
  emitted node are also valid fields on the canonical class
- compatible fields are copied during retype
- incompatible cross-type patches are still rejected with a warning

This was verified with GTEx:

- Pharos `Protein.calculated_properties.gtex_tau*`
- target-graph `Gene.calculated_properties.gtex_tau*`

matched exactly for all overlapping resolved entities in the comparison build.

## Comparison Harness Notes

The reusable comparison YAMLs were tightened to reduce harness drift:

- Pharos side includes `TissueResolver`
- target-graph side includes both gene and protein resolvers
- `clean_edges=False` is used so partial comparison builds do not delete their
  own edges

Important limitation:

- the current compare harness must still be pointed at a source-appropriate
  resolver setup
  - gene sources compare best with target-graph gene resolution
  - protein sources compare best with target-graph protein resolution

## Final Recommendation

Proceed with full `pharos` and `target_graph` builds using:

- gene-anchored output for gene-level sources
- protein-anchored output for true protein-level sources
- explicit Pharos-side projection rather than mis-typing ingest records

The adapter set audited in this work is now considered ready from a semantic
typing and resolver-behavior standpoint.
