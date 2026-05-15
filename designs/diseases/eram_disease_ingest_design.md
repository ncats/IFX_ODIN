# ERAM Disease Association Ingest Design

## Status

Implemented as a legacy Pharos carry-forward and promoted from `working.yaml` into:

- `src/use_cases/pharos/pharos.yaml`
- `src/use_cases/pharos/target_graph.yaml`

No ERAM-specific changes were needed in `src/use_cases/pharos/tcrd.yaml`.

## Decision

Do not build a fresh ingest from the public ERAM downloads.

Use the already transformed legacy `pharos319.disease` rows with `dtype='eRAM'` as the source of truth for ERAM coverage in the current Pharos graph and downstream MySQL export.

Why:

- the public ERAM files appear stale and partially malformed
- the legacy Pharos rows already contain the shape we want
- the legacy rows already carry stable `DOID:*` disease IDs
- the legacy rows already carry the embedded ERAM provenance/source list

## Source Shape

Legacy source table: `pharos319.disease`

Relevant legacy fields:

- `dtype = 'eRAM'`
- `protein_id`
- `did` as `DOID:*`
- `ncats_name`
- `source` as a pipe-delimited ERAM provenance/source list

Representative legacy counts observed during implementation:

- `14660` total ERAM rows
- `14428` distinct `(protein_id, did)` pairs
- `1380` distinct `did`
- `5139` distinct `protein_id`

## Graph Mapping

Adapter: `src/input_adapters/pharos_mysql/eram_disease_adapter.py`

The adapter emits:

- `Disease`
  - `id = did`
  - `name = ncats_name`
- `ProteinDiseaseEdge`
  - one edge per `(UniProtKB, DOID)` pair
  - one or more `DiseaseAssociationDetail` entries as needed after graph merge

ERAM detail payload:

- `source = "eRAM"`
- `source_id = DOID`
- `original_sources = [...]`

`original_sources` is the deduped list parsed from the legacy pipe-delimited `disease.source` field.

ERAM intentionally does not use:

- `evidence_terms`
- `evidence_codes`
- legacy `mondoid` as the primary graph disease ID

## MySQL Mapping

The existing TCRD converter path is reused.

ERAM-specific downstream expectations:

- `disease.dtype = 'eRAM'`
- `disease.did = detail.source_id`
- `disease.source = '|'.join(detail.original_sources)`
- `disease.evidence` remains driven by actual evidence fields, which ERAM does not populate here

## Validation Summary

Working graph (`test_pharos`) checks showed:

- ERAM `ProteinDiseaseEdge` rows landed with `detail.source = 'eRAM'`
- `detail.source_id` preserved the legacy `DOID:*`
- `detail.original_sources` preserved the legacy embedded source list
- most disease endpoints resolved to canonical `MONDO:*`, while the source DOID remained on the detail payload

Working MySQL (`pharos400_working`) checks showed:

- `disease.dtype = 'eRAM'`
- `disease.did` populated from the source DOID
- `disease.source` populated from `original_sources`
- `disease.evidence` remained null, which is expected for this ERAM shape

## Known Caveats

- Legacy ERAM UniProt accessions do not all exist as direct lookup aliases in the current `TCRDTargetResolver` cache.
- Most of the dropped legacy accessions correspond to proteins that still exist in the current Pharos protein universe under a different canonical accession.
- A small number of legacy ERAM proteins appear absent from the current canonical Pharos protein set entirely.
- Because graph node scalar merge is last-write-wins, ERAM disease names can still overwrite other disease node names if they resolve onto the same canonical disease node and ERAM is loaded later.

## Scope Kept Out

- no ERAM raw-file download workflow
- no reconstruction of the historical normalization pipeline from public ERAM files
- no new `tcrd.yaml` direct-load adapter
- no special-case legacy protein remapping beyond the current Pharos resolver behavior
