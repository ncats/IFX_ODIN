# TIN-X Importance Performance Design

## Status

- Implemented
- Validated on `working_mysql.yaml`

## Goal

Reduce TIN-X importance wall-clock time in the direct MySQL load while keeping:

- TIN-X importance out of the main graph
- current score semantics
- current disease and protein resolver behavior

The implementation target is:

- `src/input_adapters/jensenlab/tinx.py`

The current validation path is:

- `src/use_cases/working_mysql.yaml`

## Final Design

`TINXImportanceFileAdapter` now uses a sqlite-backed staged aggregation flow:

1. stream `human_textmining_mentions.tsv` into `protein_mentions(protein_id, pmid)`
2. stream `disease_textmining_mentions.tsv` into `disease_mentions(doid, pmid)`
3. build:
   - `pmid_protein_count(pmid, n)`
   - `pmid_disease_count(pmid, n)`
4. build `tinx_importance_stage(doid, protein_id, importance)` in disease batches
5. create an index on `(doid, protein_id)`
6. stream sorted stage rows into `TINXImportanceEdge` batches for the normal resolver and MySQL path

The importance score remains:

```sql
SUM(1.0 / (ppc.n * dpc.n))
```

over shared PMIDs for each `(doid, protein_id)` pair.

## Why This Shape

The old implementation aggregated one disease at a time, which repeated the same large join pattern thousands of times.

The final implementation keeps the heavy work inside sqlite but changes the execution shape:

- aggregation is set-oriented within each disease batch
- intermediate stage rows are committed batch-by-batch
- stage build progress is observable during long runs
- final row streaming is ordered and simple

This avoids the worst repeated per-disease query overhead while also giving better operational visibility than one giant uncommitted sqlite statement.

## Logging And Operability

The adapter now logs:

- timing for protein load
- timing for disease load
- timing for PMID count-table build
- per-batch progress during importance-stage construction
- heartbeat-style progress during stage-table index creation
- timing for stage streaming

Stage-build logs include:

- batch number
- processed diseases / total diseases
- cumulative stage-row count
- rows added in the current batch
- elapsed seconds

## Batch Sizes

Current adapter tuning:

- sqlite mention-load batch: `100_000`
- sqlite stage disease batch: `500`
- emitted `TINXImportanceEdge` batch size: `100_000`

These values are implementation settings, not product semantics.

## Validation Outcome

Observed on the working MySQL validation path:

- disease mention rows loaded: `213,352,891`
- diseases processed: `10,131`
- final stage rows: `20,516,075`
- stage build time: about `1,866.8s`

Downstream MySQL result checked in `pharos400_working`:

- `tinx_importance` rows inserted: `104,642`
- no `tinx_importance` rows were found where `protein.novelty` was null
- no `tinx_importance` rows were found where `ncats_disease.novelty` was null

## Known Caveat

`max_pairs` still limits emitted edges, but it does not make the expensive stage build cheap by itself. For narrow validation runs, pair it with `max_diseases`.

## Recommendation

This is the preferred TIN-X importance implementation for the current direct-to-MySQL path.

It preserves current semantics, finishes in a reasonable runtime on the refreshed Jensen files, and provides enough progress visibility to operate safely on large builds.
