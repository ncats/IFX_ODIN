# Pharos PubMed Mirror Design, May 2026

## Goal

Re-establish a local PubMed article mirror for Pharos as a small standalone ETL that:

- runs outside the main IFX_ODIN graph ingest framework
- loads PubMed baseline XML plus updatefiles from NCBI
- maintains a local MySQL `pubmed` table
- preserves article metadata needed by Pharos features:
  - PMID
  - title
  - journal
  - publication date
  - derived publication year
  - authors
  - abstract
- handles both record updates and deleted PMIDs correctly

This mirror is shared infrastructure. PubTator publication statistics are only one downstream consumer.

## Why This Sits Outside ODIN

This job is not graph-native ETL.

Its responsibilities are:

- mirroring an external publication corpus
- replaying update archives
- validating checksums
- applying upserts
- applying deletions
- maintaining local operational state

Those concerns are operational and storage-oriented, not node/edge-oriented.

Keeping the PubMed mirror outside the ODIN adapter framework gives us:

- a smaller code path
- a simpler execution model
- independent scheduling
- a stable local relational store that multiple Pharos features can query

ODIN should consume this mirror, not own the mirroring process.

## Source Of Truth

Official source:

- PubMed baseline XML archives
- PubMed daily update XML archives

Official landing pages:

- `https://pubmed.ncbi.nlm.nih.gov/download/`
- `https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/`
- `https://ftp.ncbi.nlm.nih.gov/pubmed/updatefiles/`

## Prior Art

We previously had a separate ETL in `ncats/pharos_pubmed` that:

- downloaded PubMed baseline archives
- downloaded daily update archives
- parsed article metadata and abstracts
- upserted into MySQL
- deleted withdrawn PMIDs

That earlier implementation established the right system boundary.

This design intentionally keeps that boundary:

- standalone mirror job
- local MySQL store
- downstream read consumers

But we will modernize it slightly:

- add `pub_year`
- keep code flatter and cleaner
- make state tracking explicit
- avoid Airflow-specific coupling in the core implementation

## Scope

### In Scope

- one standalone PubMed mirror under `scripts/pubmed_mirror/`
- MySQL target storage
- full rebuild from baseline archives
- recurring incremental updates from updatefiles
- checksum verification
- record upserts
- deleted PMID handling
- reusable local query surface for downstream consumers

### Out Of Scope

- representing publications as graph nodes
- full text search
- abstract NLP or annotation
- pushing PubMed XML fetch logic into ODIN input adapters
- supporting multiple storage backends in the first pass

## Design Principles

The implementation should follow simple, explicit design rules:

- one reason to change per module
- pure parsing code separated from I/O
- explicit naming over cleverness
- small functions with narrow responsibilities
- deterministic state transitions
- idempotent update behavior
- operational state stored explicitly, not inferred from side effects

Concretely:

- XML parsing should not know about MySQL
- MySQL writing should not know XML element paths
- archive discovery should not know article normalization rules
- CLI orchestration should compose the pieces, not contain business logic

## Execution Model

The mirror has two modes:

### 1. Rebuild

Use for first load or full recovery.

Behavior:

- ensure schema exists
- discover baseline archives
- download missing archives and checksums
- verify checksums
- parse every archive in sorted order
- upsert article rows
- apply deleted PMID records if present
- mark each archive as processed

Important clarification:

- `rebuild` should process the PubMed baseline archive set
- `rebuild` should not also replay the updatefiles in the same command by default

Reason:

- baseline rebuild and incremental catch-up are operationally different steps
- keeping them separate makes restarts, debugging, and validation easier
- after `rebuild`, run `update` once to catch up from baseline coverage to current state

### 2. Update

Use for recurring weekly runs.

Behavior:

- discover update archives
- download new archives and checksums
- skip archives already marked processed
- verify checksums
- parse each new archive in sorted order
- upsert changed and new article rows
- delete withdrawn PMIDs
- mark each archive as processed

Although NCBI publishes updatefiles daily, a weekly run is sufficient for current Pharos needs unless a later consumer requires tighter freshness.

## Database Schema

We will reuse the existing `ncats_pubmed.pubmed` table shape as the starting point and add `pub_year`, but the new mirror should live in a new schema.

### `pubmed`

Proposed columns:

- `id INT NOT NULL`
- `title TEXT NOT NULL`
- `journal TEXT NULL`
- `date VARCHAR(10) NULL`
- `pub_year SMALLINT NULL`
- `authors TEXT NULL`
- `abstract TEXT NULL`
- `fetch_date DATETIME NULL`
- `source_file VARCHAR(255) NULL`

Primary key:

- `PRIMARY KEY (id)`

Indexes:

- `KEY pubmed_year_idx (pub_year, id)`
- `KEY pubmed_date_idx (date, id)`

Notes:

- `id` remains the PMID primary key for compatibility with prior usage
- `date` stays source-shaped for compatibility and easy debugging
- `pub_year` is materialized because many downstream consumers want a cheap year join
- `source_file` helps trace provenance during debugging and replay verification

## State Tracking

The mirror needs explicit operational state separate from article content.

I do not want to reuse the old sentinel-file approach as the primary state model.

Instead, create a small MySQL state table:

### `pubmed_mirror_file_state`

Columns:

- `archive_name VARCHAR(255) NOT NULL`
- `archive_group VARCHAR(32) NOT NULL`
  - `baseline`
  - `update`
- `remote_last_modified DATETIME NULL`
- `md5 VARCHAR(64) NULL`
- `downloaded_at DATETIME NULL`
- `processed_at DATETIME NULL`
- `status VARCHAR(32) NOT NULL`
  - `downloaded`
  - `processed`
  - `checksum_failed`
  - `failed`
- `error_message TEXT NULL`

Primary key:

- `PRIMARY KEY (archive_name)`

This gives us:

- resumability
- operational visibility
- explicit failure tracking
- one place to answer "what ran?"

## Article Update Semantics

PubMed update archives are not append-only from the perspective of article fields.

Records can:

- appear for the first time
- reappear with changed metadata
- be deleted

So the storage contract must be:

- upsert article rows by PMID
- replace mutable fields with the latest parsed values
- delete rows for withdrawn PMIDs

This is not a "load once and only insert new PMIDs" workflow.

## Parsing Rules

The parser should extract the following fields:

- PMID
- title
- journal title
- abstract text
- author string
- publication date
- derived publication year

Date behavior:

- prefer article journal publication date when available
- fall back to PubMed history date when needed
- preserve source-shaped date strings:
  - `YYYY`
  - `YYYY-MM`
  - `YYYY-MM-DD`
- derive `pub_year` from the chosen date

Abstract behavior:

- preserve article abstract text as delivered
- strip or normalize minimal inline XML markup only as needed to keep parsing stable
- do not perform semantic rewriting or sentence cleanup

Author behavior:

- preserve a readable flattened string
- keep group authors when present

## Code Layout

The standalone code should live in:

```text
scripts/pubmed_mirror/
  README.md
  schema.sql
  main.py
  config.py
  source_client.py
  parser.py
  repository.py
  state_repository.py
  service.py
```

Responsibilities:

### `main.py`

- parse CLI arguments
- construct config
- invoke service commands

### `config.py`

- load database connection details
- load local working directory
- define remote URLs
- keep environment-specific wiring out of business logic

### `source_client.py`

- list remote archives
- download archive files
- download checksum files
- fetch remote metadata like last-modified where available
- verify MD5 checksums

### `parser.py`

- parse raw PubMed XML into article records
- parse deleted PMIDs
- remain pure and database-agnostic

### `repository.py`

- create schema if needed
- bulk upsert `pubmed` rows
- bulk delete withdrawn PMIDs
- read lightweight stats for status reporting

### `state_repository.py`

- read and write `pubmed_mirror_file_state`
- answer which archives are already processed
- record failures cleanly

### `service.py`

- orchestrate rebuild and update workflows
- keep sequencing readable
- apply retry boundaries at the operation level, not inside parsing code

## CLI

Recommended commands:

- `python scripts/pubmed_mirror/main.py init`
- `python scripts/pubmed_mirror/main.py rebuild`
- `python scripts/pubmed_mirror/main.py update`
- `python scripts/pubmed_mirror/main.py status`

### `init`

- create required tables only
- validate connectivity
- do not download archives
- do not populate article rows

### `rebuild`

- process the PubMed baseline archives from scratch
- download baseline archives and checksums as needed
- truncate or recreate mirror content before reload
- do not process updatefiles by default

### `update`

- process new PubMed update archives only
- download update archives and checksums as needed
- skip already-processed archives
- this is the command intended for weekly cron

### `status`

- show article count
- show processed archive count
- show latest processed archive
- show max `pub_year`
- show failure count

## MySQL Behavior

Use bulk operations deliberately.

Requirements:

- batch upserts
- batch deletes
- explicit transaction boundaries per archive

Preferred write behavior:

- process one archive
- write all row changes for that archive
- commit
- then mark archive as processed

That ordering ensures:

- an archive is never marked complete before its writes land
- partial failures stay restartable

## Failure Model

Expected failures include:

- download interruption
- checksum mismatch
- malformed XML in a specific archive
- transient MySQL write failure

Handling rules:

- never mark an archive `processed` unless the archive transaction succeeded
- record `checksum_failed` explicitly
- record `failed` plus error message for operational debugging
- allow rerun without manual cleanup for ordinary transient failures

## Download Behavior

Yes, the mirror should download and manage the PubMed archives itself.

That is part of its contract.

Expected behavior:

- `rebuild` downloads baseline `.gz` archives plus checksum files as needed
- `update` downloads updatefile `.gz` archives plus checksum files as needed
- already-downloaded files may be reused if checksum and state information remain valid

The mirror should keep a local working directory under `scripts/pubmed_mirror/` configuration, for example:

- `baseline/`
- `updatefiles/`
- optional logs or temporary working files

This keeps network fetch concerns inside the mirror and avoids making ODIN or cron scripts manage archive-by-archive downloads.

## Recommended Operator Flow

First-time setup:

1. Run `init`
2. Run `rebuild`
3. Run `update`
4. Validate row counts and sample records
5. Enable weekly cron for `update`

Normal ongoing operation:

1. Weekly cron runs `update`
2. Operators inspect `status` if a run fails or looks stale

Recovery operation:

1. Run `rebuild`
2. Run `update`

## Cron Setup

The mirror should document a simple cron entry for weekly updates.

Assumptions:

- code lives in the IFX_ODIN checkout
- a Python virtualenv exists
- database credentials are supplied through environment variables or a config file read by `config.py`

Example wrapper script:

```bash
#!/bin/zsh
set -euo pipefail

cd /Users/kelleherkj/IdeaProjects/IFX_ODIN
source .venv/bin/activate

export PYTHONPATH=/Users/kelleherkj/IdeaProjects/IFX_ODIN

python scripts/pubmed_mirror/main.py update
```

Example cron entry for every Sunday at 03:15:

```cron
15 3 * * 0 /Users/kelleherkj/IdeaProjects/IFX_ODIN/scripts/pubmed_mirror/run_weekly_update.sh >> /Users/kelleherkj/IdeaProjects/IFX_ODIN/scripts/pubmed_mirror/cron.log 2>&1
```

Recommended companion checks:

- keep `status` available for manual verification
- log stdout and stderr from cron
- prefer one small wrapper script over embedding environment setup directly into cron

If cron proves too limited later, the same wrapper can be moved to `launchd` or another scheduler without changing the mirror code.

## Downstream Usage

The mirror exists to support multiple consumers.

Examples:

- PubTator `PMID -> pub_year` joins
- Pharos article title/abstract lookups
- publication statistics grouped by year
- downstream export or UI enrichment paths

ODIN-side consumers should query this store rather than re-fetching PubMed XML or re-deriving year metadata ad hoc.

## First-Pass Recommendation

Build the first version as:

- standalone Python under `scripts/pubmed_mirror/`
- MySQL-backed
- baseline rebuild plus weekly incremental update
- compatible with the old `ncats_pubmed.pubmed` table shape while loading into `ifx_pubmed.pubmed`
- with one additive schema improvement:
  - `pub_year`

Do not:

- force this into the ODIN input adapter model
- add extra storage backends
- add full text indexing yet
- overgeneralize for non-PubMed literature sources

## Validation Plan

Before trusting the mirror operationally, validate:

1. Rebuild loads baseline files successfully into MySQL
2. Update mode replays new updatefiles idempotently
3. Deleted PMID records remove rows from `pubmed`
4. Sample rows preserve title, abstract, journal, authors, and date correctly
5. `pub_year` is populated correctly from representative year-only, year-month, and year-month-day records
6. Downstream year-join queries for PubTator are fast enough with the new index

## Open Questions

- whether any downstream readers still assume the old `ncats_pubmed` schema name rather than the new `ifx_pubmed` schema
- whether `source_file` is useful enough to keep permanently
- whether weekly cadence is sufficient once real consumers are hooked up
- whether some downstream Pharos readers expect exact old column names or older nullability assumptions

## Recommendation

Proceed with a small standalone MySQL mirror in `scripts/pubmed_mirror/`.

That is the simplest architecture that correctly handles:

- article metadata
- abstract storage
- update replay
- deleted PMIDs
- shared reuse across Pharos features

It preserves the proven `pharos_pubmed` system boundary while keeping the implementation clean, direct, and easier to maintain.
