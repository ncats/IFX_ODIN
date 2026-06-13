# PHIPSTER Legacy-Lift Design

## Goal

Populate PHIPSTER content in both the Pharos graph and `pharos400` by lifting
the existing legacy `pharos319` tables into explicit graph concepts, then
exporting those concepts back into the MySQL viral tables.

This is intentionally a legacy-lift implementation, not a raw-source ingest.

## Final Scope

Implemented:

- lift PHIPSTER content from `pharos319`
- represent PHIPSTER in the graph with dedicated viral models
- export those graph models back into:
  - `virus`
  - `viral_protein`
  - `viral_ppi`
- validate graph and MySQL output against legacy `pharos319`

Deferred:

- rebuilding PHIPSTER from the current public archive
- replacing the legacy-lift path with a fresh-source ingest

## Source Strategy

Operational source of truth for this implementation:

- `pharos319`

Reason:

- the public PHIPSTER download does not expose all metadata needed to recreate
  legacy `virus` and `viral_protein` content
- legacy `pharos319` already contains the complete populated PHIPSTER tables
- no newer public PHIPSTER release with full companion metadata was identified

Version strategy:

- current registry migration stamps this adapter with the shared
  `legacy_pharos:pharos319_mysql:pharos319` source snapshot, matching the other
  datasets lifted from old Pharos
- future work should split lifted pharos319 datasets back into their real source
  identities and version them more precisely; PHIPSTER's source-specific version
  could use the publication date proxy, `2019-09-05`

## Graph Model

Dedicated graph concepts:

- `Virus`
- `ViralProtein`
- `VirusViralProteinEdge`
- `ViralPPIEdge`
- `ViralPPIDetail`

Final relationship directions:

- `ViralProtein -> Virus`
- `Protein -> ViralProtein`

Important modeling rule:

- PHIPSTER row-level edge payload is stored in `ViralPPIEdge.details`
- this preserves multiple legacy PHIPSTER rows that collapse onto the same
  canonical `Protein -> ViralProtein` edge after resolver normalization

## Field Mapping

### Virus

Graph node mirrors the legacy `virus` table fields needed for downstream export:

- `nucleic1`
- `nucleic2`
- `order`
- `family`
- `subfamily`
- `genus`
- `species`
- `name`

Graph ID is a stable virus taxon-based identifier.

### ViralProtein

Graph node mirrors the legacy `viral_protein` payload:

- `name`
- `ncbi`

Graph ID uses the stable legacy PHIPSTER viral protein key:

- `PHIPSTER.ViralProtein:<legacy_id>`

Deliberately omitted from the node:

- `source_id`
- `virus_source_id`

Reason:

- both were redundant with graph identity or graph structure

### ViralPPIEdge

Conceptual graph edge:

- `Protein -> ViralProtein`

Row-level detail payload:

- `source`
- `source_protein_id`
- `final_lr`
- `pdb_ids`
- `high_confidence`

`pdb_ids` is modeled as `List[str]` in the graph.

## Export Strategy

`TCRDOutputConverter` reconstructs legacy tables from graph content:

- `Virus -> virus`
- `ViralProtein -> viral_protein`
- `ViralPPIEdge.details -> viral_ppi`

Important export rule:

- one `viral_ppi` row is emitted per `ViralPPIDetail`

This preserves legacy row multiplicity even when multiple source host UniProts
collapse onto one canonical `Protein -> ViralProtein` graph edge.

`viral_protein.virus_id` remains nullable and is reconstructed from
`VirusViralProteinEdge`.

## Validation Results

### Legacy Reference (`pharos319`)

- `virus`: `1001`
- `viral_protein`: `12499`
- `viral_ppi`: `282528`
- `viral_protein.virus_id is null`: `5036`

### Final Graph (`test_pharos`)

- `Virus`: `1001`
- `ViralProtein`: `12499`
- `VirusViralProteinEdge`: `7463`
- `ViralPPIEdge`: `274047`
- total `ViralPPIDetail` records: `279977`

Interpretation:

- node counts match legacy
- `7463` virus links exactly matches legacy non-null `viral_protein.virus_id`
- PHIPSTER multiplicity is preserved in `details`

Observed detail multiplicity confirms the model change is necessary:

- `273530` edges have `1` detail
- `517` edges have more than `1` detail
- max observed multiplicity is `35` details on one canonical edge

### Final MySQL (`pharos400_working`)

- `virus`: `1001`
- `viral_protein`: `12499`
- `viral_ppi`: `279977`
- `viral_protein.virus_id is null`: `5036`
- all `viral_ppi.dataSource = 'P-HIPSTer'`

Interpretation:

- `virus` matches legacy exactly
- `viral_protein` matches legacy exactly
- `viral_ppi` matches the graph `detail_total` exactly
- MySQL export is no longer losing PHIPSTER rows beyond what the graph already
  lost upstream

## Remaining Gap To Legacy

Remaining gap:

- legacy `viral_ppi`: `282528`
- final graph / MySQL: `279977`
- difference: `2551`

Current interpretation:

- this remaining loss is not a MySQL conversion problem
- it is not the old edge-overwrite problem
- it is consistent with resolver/canonicalization policy on the human protein
  side

In subset validation, the remaining misses were explained by UniProt IDs that
exist in `TargetGraphProteinResolver.sqlite` but not in
`TCRDTargetResolver.sqlite`, which indicates a target-resolution policy gap
rather than a PHIPSTER structural mapping issue.

## Acceptance

PHIPSTER ingest is accepted in its current form as:

- a legacy-lift implementation from `pharos319`
- a graph representation with explicit viral concepts
- a MySQL round-trip that preserves legacy viral tables and row multiplicity as
  far as the current TCRD target resolver allows

Future revisit trigger:

- PHIPSTER publishes a verifiable updated release with complete metadata needed
  for a true source ingest
