# Arango Large-Node Merge Strategy

## Problem

Some adapters update `Protein` nodes with relatively small patches, but the current Arango write path merges by:

1. fetching the full existing documents from Arango
2. merging the incoming records client-side
3. writing the full merged documents back with `insert_many(..., overwrite=True)`

For large `Protein` documents, this creates two practical issues:

- fetches move much more data than the current adapter actually needs
- batched writes can become large enough to fail unless adapter `batch_size` is reduced

This has led to a workaround where some protein-facing adapters use smaller batches such as `1000`.

## Current Behavior

In `src/output_adapters/arango_output_adapter.py`:

- `get_existing_nodes()` uses `collection.get_many(keys)` and returns full documents
- `RecordMerger.merge_records()` merges against those full records
- `collection.insert_many(..., overwrite=True)` rewrites the full merged records

This means the cost of an update is driven by total document size, not just the changed fields contributed by the current adapter.

## Observed Tradeoff

- Smaller adapter batches reduce request size and can avoid insert failures
- But pushing this concern into each adapter is not a good long-term design
- Server-side Arango merge/upsert was previously explored, but in practice it appeared slow, likely because it required document-at-a-time updates

## Recommended Framework Fix

Keep client-side merge, but make the framework more selective and payload-aware.

### 1. Fetch only fields needed for the current merge

Instead of reading full existing documents, derive the required field set from the incoming records and fetch only:

- `id`
- fields present in the current adapter payload
- merge metadata fields needed to preserve prior history, such as:
  - `creation`
  - `updates`
  - `resolved_ids`
  - any other framework-maintained merge fields

For example, a PubMed score adapter update should not need the full `Protein` payload if it only contributes:

- `pm_score`
- `pm_score_by_year`
- merge metadata

Preferred implementation direction:

- replace `collection.get_many(keys)` for node merge reads with an AQL query that uses `KEEP(doc, ...)`
- derive the field list from `obj_list` inside `store()`

### 2. Split writes by payload size in the output adapter

Keep adapter `batch_size` focused on logical source batching, but add a second layer of output chunking before `insert_many()`.

Possible controls:

- `max_docs_per_write`
- or preferably `max_payload_bytes`

This should happen in the Arango output adapter rather than in each individual input adapter.

### 3. Preserve correct merge semantics

The framework fix should preserve current behavior for:

- list merging
- dict merging
- `resolved_ids`
- `creation` / `updates`
- overwrite semantics for scalar fields under `KeepLast`

## Short-Term Guidance

Until the framework fix is implemented:

- smaller `batch_size` values for large protein-node adapters are an acceptable workaround
- but they should be treated as operational tuning, not the long-term merge strategy

## Out Of Scope For Current Publication-Score Ingest

This design note is intentionally separate from the PubMed / PubTator score ingest work.

For the current publication-score ingest:

- continue using the existing source-backed adapter path
- do not block the ingest on a framework refactor
- revisit the framework change as a separate follow-up
