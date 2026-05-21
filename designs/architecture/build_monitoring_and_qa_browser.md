# Build Monitoring And QA Browser

## Goal

Provide enough live build visibility and recovery support to make long Arango
graph builds operationally manageable.

For now, the implemented scope is:

- resumable ETL runs
- per-adapter checkpoint state persisted during the run
- incremental `collection_schemas` persistence so metadata survives crashes
- QA browser visibility into current build progress and recent completion state

That addresses the immediate pain point. We do not currently need a larger
control-plane design.

---

## What We Implemented

### 1. Adapter checkpoints in `metadata_store`

`ArangoOutputAdapter` now writes an `etl_checkpoint` document keyed by build
`run_id` (currently the YAML path passed through `BuildGraphFromYaml`).

Each adapter entry records:

- `status`: `running`, `completed`, or `failed`
- `adapter_position`
- `adapter_total`
- `records_written`
- `started_at`
- `completed_at`
- `failed_at`
- `error_message`

This state is written during ETL, not only at the end.

### 2. Incremental `collection_schemas`

`collection_schemas` is now flushed after each successful adapter instead of
being written only in final post-processing.

This matters because partial builds are common in practice. If an ETL crashes
midway, the schema metadata for completed adapters should still be durable.

Current behavior:

- completed adapters persist their schema contributions immediately
- the currently running or failed adapter is not marked complete
- final post-processing may still rewrite metadata, but it is no longer the
  only moment where schema metadata becomes durable

### 3. Resumable ETL

`ETL.do_etl()` now supports `resume=True`.

Resume behavior:

- completed adapters are skipped
- the first incomplete adapter is rerun
- later adapters run normally

This is exposed from `BuildGraphFromYaml`, and `src/use_cases/gramp/build_gramp.py`
now supports:

- normal rebuild
- `--resume`

Operationally, this replaces the old habit of manually commenting out early
adapters in YAML to restart from the middle of a long build.

### 4. QA browser build status page

The QA browser now has a dedicated build status page:

- `/db/<db_name>/build-status`

It reads the latest `etl_checkpoint` document and shows:

- per-adapter status
- adapter order
- records written
- elapsed time per adapter
- overall progress counts

It also derives a top-level build state:

- `running`
- `failed`
- `post_processing`
- `completed`
- `partial`
- `unknown`

The top-level state combines checkpoint data with `etl_metadata` so the page can
distinguish:

- adapter execution still running
- adapters done but post-processing/cleanup still running
- final ETL metadata written, meaning the build is complete

The page auto-refreshes while a build is active.

---

## Why This Design

The earlier design discussion separated:

- graph provenance
- build control / run state

That split is still conceptually true, but for current operational needs we do
not need an external control-plane database yet.

The implemented design keeps build progress state in the target graph database's
`metadata_store`, because that is:

- simple
- already available to the QA browser
- sufficient for resume and monitoring of the builds we are actually running

This is enough for current use cases:

- "Where did the build fail?"
- "Can I resume it?"
- "Which adapter is running now?"
- "Why does the QA page say 100% adapters but the build still is not done?"

---

## Current Metadata Model

### `etl_checkpoint`

Stored in `metadata_store` as:

- `_key`: `etl_checkpoint__<safe_run_id>`
- `type`: `etl_checkpoint`
- `run_id`
- `last_updated`
- `adapters`: object keyed by adapter name

### `etl_metadata`

Still written at the end of post-processing and used as a final completion
signal.

Current role:

- runtime/environment metadata
- last completed ETL run timestamp
- useful to distinguish `post_processing` from `completed`

### `collection_schemas`

Still the schema metadata used by:

- QA browser collection introspection
- `arango_to_mysql`
- `arango_to_rdf`

Important operational rule:

- `collection_schemas` now survives partial builds better than before
- but it still only reflects adapters that have successfully completed and
  flushed metadata

That is acceptable for the resume model we now have.

---

## Resume Semantics

The intended semantics are:

- rerunning a completed adapter should generally be avoided
- resume should skip completed adapters
- the first incomplete adapter is rerun in full

This assumes adapter writes are deterministic enough that rerunning the first
incomplete adapter is safe. That is the practical model we want; we are not
trying to resume inside an adapter batch.

For `gramp`, this already proved useful in practice.

---

## QA Browser Experience

Current QA browser behavior:

- main DB dashboard links to the build status page via the Tools card
- build status page shows adapter table and overall state
- page auto-refreshes while active

This is enough for current monitoring needs. A separate global run-history page
or admin trigger UI is not required right now.

---

## What We Deliberately Did Not Build

Not in current scope:

- external control-plane database for build definitions/runs/events
- sync comparison between "built from" and "configured now"
- admin-triggered builds from the QA browser
- append-only event timeline storage
- a full provenance snapshot of resolved YAML files and hashes

Those may still be useful later, but they are not necessary to solve the main
operational problem we had.

---

## Remaining Gaps

These are the main remaining limitations:

1. Checkpoint state is stored in the target graph DB.
   If the DB is completely unavailable, live monitoring is unavailable too.

2. `collection_schemas` still depends on successful adapter completion.
   It no longer waits until the end of the run, but it is not a live DB
   inventory.

3. `etl_metadata` is still a lightweight final marker, not a full provenance
   snapshot.

4. Resume safety depends on adapter idempotence.
   That is acceptable for now, but it should stay an explicit assumption.

---

## Recommended Near-Term Direction

Do not expand this design much further until there is a concrete need.

For now, the right operational baseline is:

- use checkpointed resume instead of manual YAML commenting
- trust the QA browser build status page for current progress
- treat `etl_metadata` as the final completion marker
- continue improving adapter determinism where resume safety matters

If we later need more, the next logical addition would be richer provenance in
`etl_metadata`, not a whole new control plane.
