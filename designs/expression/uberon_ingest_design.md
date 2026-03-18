# UBERON Ingest Design

## Scope

- Source: `input_files/auto/uberon/uberon.obo`
- Ingest in `src/use_cases/working.yaml` only (until validated)
- Entities in scope:
  - `Tissue` nodes from `UBERON:*`
  - `TissueParentEdge` from `is_a` between `UBERON:*` terms

## Adapter Responsibilities

- `UberonAdapter` emits both tissues and parent edges.
- Output order is type-grouped: node batches first, then edge batches.
- Keep canonical node IDs as `UBERON:*`.
- Extract:
  - `name`
  - `definition` (human-readable text from OBO `def`)
  - `synonyms` (exact/related/broad/narrow)
- Derive datasource metadata from OBO header (`data-version`) and adapter `download_date`.

## Resolver Responsibilities

- `TissueResolver` reads `uberon.obo` directly.
- Filters xrefs by allowlisted ontology prefixes.
- Ontology prefix checks are case-insensitive (lowercase normalization).
- Resolver enriches `equivalent_ids`; adapters do not parse/store xrefs.

## Validation

- Confirm `name`, `def`, synonym fields, and `is_a` are present in downloaded payload.
- Run ingest via `src/use_cases/working.yaml`.
- Verify stable node/edge counts and sample term correctness.

## Promotion Criteria

Promote to `src/use_cases/pharos/target_graph.yaml` after:
- payload validation,
- ingest sanity checks,
- acceptable graph quality from spot-checks.
