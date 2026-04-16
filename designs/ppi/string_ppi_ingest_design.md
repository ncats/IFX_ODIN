# STRING PPI Ingest Design

## Status

Implemented and validated in the working graph and working MySQL paths.

This first pass covers **STRING human protein-protein interactions** only.
BioPlex and Reactome PPI remain follow-up sources.

## Scope

Implemented source:

- STRING human PPI

Explicitly deferred:

- BioPlex PPI
- Reactome PPI
- source-specific `interaction_type` / `evidence` population

## Files Added / Changed

### New / updated ingest code

- `src/input_adapters/string/string_ppi.py`
- `src/models/ppi.py`
- `workflows/pharos.Snakefile`
- `src/use_cases/working.yaml`

### New / updated downstream code

- `src/input_adapters/pharos_arango/tcrd/ppi.py`
- `src/output_adapters/sql_converters/tcrd.py`
- `src/use_cases/working_mysql.yaml`

### Tests

- `tests/test_string_ppi.py`
- `tests/test_ppi_record_merging.py`
- `tests/test_tcrd_output_converter.py`

## Source Inputs

Implemented download target:

- `https://stringdb-downloads.org/download/protein.links.v12.0/9606.protein.links.v12.0.txt.gz`

Stored under:

- `input_files/auto/string/9606.protein.links.v12.0.txt.gz`
- `input_files/auto/string/string_version.tsv`

Version strategy:

- `version`: hardcoded as `12.0` in the Snakemake rule
- `version_date`: derived from HTTP `Last-Modified`
- `download_date`: adapter file mtime unless explicitly present in version TSV

## Raw File Profiling

Observed file shape:

- header: `protein1 protein2 combined_score`
- IDs are `9606.ENSP...`
- no self-pairs were observed in the raw file

Observed counts from the local v12.0 file:

- total rows: `13,715,404`
- rows kept at `score >= 400`: `1,858,944`
- rows filtered out below `400`: `11,856,460`
- percent filtered out by the cutoff: `86.45%`

## Legacy Comparison

Old TCRD loader reviewed:

- `https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-STRINGDB.py`

Confirmed old behavior:

- input file: `9606.protein.links.v11.0.txt`
- strips `9606.` and works with bare `ENSP...`
- inserts `ppitype='STRINGDB'`
- inserts scalar `score=<combined_score>`
- skips unmapped proteins
- skips self-pairs
- does **not** apply a score cutoff during load

Confirmed old IFX_ODIN / Pharos readback behavior:

- `src/input_adapters/pharos_mysql/ppi_adapter.py` keeps only `StringDB` rows with
  `score >= 400`
- it filters `protein_id < other_id`, which implies `ncats_ppi` stores reciprocal
  rows and the graph readback collapses them to one undirected edge

## Implemented Graph Mapping

`StringPPIAdapter` emits:

- `PPIEdge`
  - `start_node`: `Protein(id="ENSEMBL:ENSP...")`
  - `end_node`: `Protein(id="ENSEMBL:ENSP...")`
  - `sources`: STRING provenance list
  - `score`: list-valued, emitted as `[combined_score]`

Implementation choices:

- `score_cutoff` is an adapter parameter with default `400`
- rows below the cutoff are discarded before they enter the graph
- self-pairs are discarded before they enter the graph
- `max_rows` is supported for bounded validation runs and counts **kept emitted
  edges**, not scanned raw lines

Intentionally not populated for STRING first pass:

- `p_int`
- `p_ni`
- `p_wrong`
- `interaction_type`
- `evidence`

Reason:

- the selected STRING file only provides `combined_score`
- preserving guessed mappings into legacy fields would be speculative

## Resolver Path

STRING emits `ENSEMBL:ENSP...` protein IDs and relies on `tcrd_targets` for
canonicalization into reviewed target-graph proteins.

Observed partial-run behavior with:

- `score_cutoff: 400`
- `max_rows: 10000`
- `collapse_reviewed_targets: true`

Results:

- adapter emitted `10,000` edges
- `test_pharos` stored `9,425` `PPIEdge` docs
- the difference was mostly resolver drop-off from unresolved reviewed-target
  coverage, not adapter parsing or cutoff logic

Spot checks performed during validation:

- no adapter-emitted self-loops
- no graph self-loop `PPIEdge` docs
- graph `PPIEdge` score range: `400..999`

## Graph Merge Behavior

`PPIEdge.score` is now list-valued.

Reason:

- some final graph edges aggregate multiple STRING rows after target resolution and
  reviewed-target collapse
- a scalar `score` was being overwritten under `KeepLast`
- a list-valued `score` preserves all merged values in the graph

This was validated with a dedicated record-merging test.

## Downstream MySQL Mapping

`TCRDOutputConverter` now exports `PPIEdge` to `ncats_ppi`.

Implemented behavior:

- export reciprocal rows for parity with `pharos319`
- normalize graph source label `STRING` to legacy `StringDB` in `ppitypes`
- write `score = max(score_list)` for the SQL row

Why `max(score)`:

- the graph preserves all merged STRING scores
- the legacy SQL schema stores only one scalar `score`
- `max(score)` is a conservative first-pass collapse rule

## Validation Results

### Local test coverage

Executed:

- `pytest tests/test_string_ppi.py`
- `pytest tests/test_string_ppi.py tests/test_ppi_record_merging.py`
- `pytest tests/test_tcrd_output_converter.py`

Result:

- all targeted tests passed

### Partial graph validation

Working graph setup used:

- reviewed UniProt proteins
- `StringPPIAdapter(score_cutoff=400, max_rows=10000)`
- database: `test_pharos`

Observed graph contents:

- `Protein`: `20,332`
- `PPIEdge`: `9,425`
- all `PPIEdge` rows carried STRING provenance
- score range in graph: `400..999`
- no graph self-loops

### Working MySQL validation

`working_mysql.yaml` was reduced to:

- `ProteinAdapter`
- `ProteinPPIAdapter`

Observed downstream contents in `pharos400_working`:

- `ncats_ppi` rows: `18,850`
- score range: `400..999`

Important note:

- `18,850` rows corresponds to reciprocal export of `9,425` graph edges
- however, not every unordered protein pair appears exactly twice
- some unordered pairs appear four times because multiple graph `PPIEdge` docs
  collapse to the same canonical protein pair after reviewed-target resolution,
  and each graph edge is then exported in both directions

This is currently acceptable for parity-oriented validation, but it is a follow-up
cleanup topic if we later want the downstream SQL path to deduplicate canonical
pairs before export.

## Open Follow-Ups

- Profile whether STRING `.protein.links.full...` is worth revisiting for richer
  channel-specific fields
- Add Reactome PPI ingest
  - populate `interaction_type`
  - decide whether the Reactome evidence/context column should map to `evidence`
- Add BioPlex PPI ingest
- Decide whether downstream `ncats_ppi` export should collapse duplicate canonical
  pairs before reciprocal row generation, or continue to preserve one SQL row pair
  per graph edge
