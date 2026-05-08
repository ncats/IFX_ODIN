# SureChEMBL Patent Ingest

## Goal

Add a modern patent signal to Pharos using current SureChEMBL biomedical annotations instead of the frozen 2017 legacy EBI patent-count file.

## Source

SureChEMBL bulk data:

- https://chembl.gitbook.io/surechembl/downloads/bulk-data
- https://ftp.ebi.ac.uk/pub/databases/chembl/SureChEMBL/bulk_data/

Downloaded by:

- [workflows/pharos.Snakefile](/Users/kelleherkj/IdeaProjects/IFX_ODIN/workflows/pharos.Snakefile:474)

Files used:

- `input_files/auto/surechembl/patents.parquet`
- `input_files/auto/surechembl/biomedical_entities.parquet`
- `input_files/auto/surechembl/biomedical_locations.parquet`
- `input_files/auto/surechembl/surechembl_version.tsv`

Current snapshot size is about `7.5G` across the patent-focused discovery subset.

## Chosen Signal

We landed on:

- patent **family** membership, not document membership

Reasons:

- `family_id` coverage is strong in current SureChEMBL data
- family is the cleaner invention-level signal
- document count is mostly an amplified jurisdiction/publication-stage version of the same underlying signal
- keeping only family membership reduces node payload size

Out of scope for this first pass:

- document-level patent membership
- mention-count weighting from `biomedical_locations.count`
- section-specific metrics such as claims-only counts
- patent-index style fractional weighting

## Model

Implemented on `Protein`:

- `patent_family_mentions: List[str]`
- `patent_identifier_sources: List[str]`

Field shape:

- each `patent_family_mentions` entry is a compact token `YYYY:FAMILY_ID`
- example: `2014:70611452`

This shape was chosen because the current graph merger already dedupes top-level scalar lists correctly.

## Adapter Behavior

Adapter:

- [src/input_adapters/surechembl/patent_families.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/input_adapters/surechembl/patent_families.py:1)

Behavior:

- emits `Protein` nodes only
- keeps `HGNC:...` ids as-is
- normalizes bare UniProt accessions to `UniProtKB:...`
- skips blank or unrecognized `resolved_form` values
- filters patent metadata to:
  - `family_id > 0`
  - `1950 <= publication_year <= current_year`

Scan order:

1. load target-like rows from `biomedical_entities`
2. scan `biomedical_locations` to collect relevant `patent_id`s
3. scan `patents.parquet` only for that relevant patent subset
4. scan `biomedical_locations` again to aggregate final family membership

Aggregation:

- each target is fully aggregated before emit
- final `Protein` nodes are chunked only after aggregation
- this avoids repeated partial updates to the same node from the adapter

Optional smoke-test knob:

- `max_location_batches`

## Resolver Fit

Relevant resolver path:

- `TCRDTargetResolver`

Observed overlap from discovery:

- SureChEMBL `HGNC:` aliases: `37,723`
- SureChEMBL UniProt-like aliases: `102,536`
- canonical proteins reached through resolver union: about `19,799`

The source is strongly gene-dominated, with UniProt-form hits adding supplemental coverage.

## Downstream Export

SQL converter:

- [src/output_adapters/sql_converters/tcrd.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/output_adapters/sql_converters/tcrd.py:51)

Export behavior:

- `patent_family_mentions` becomes yearly `patent_count` rows
- aggregate unique family total becomes `tdl_info.itype = 'SureChEMBL Patent Family Count'`

## Config Promotion

Implemented in:

- working graph config: [src/use_cases/working.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/working.yaml:1)
- Pharos graph config: [src/use_cases/pharos/pharos.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/pharos/pharos.yaml:1)
- target graph config: [src/use_cases/pharos/target_graph.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/pharos/target_graph.yaml:1)

Ordering choice:

- SureChEMBL and publication-heavy adapters are placed near the end of the Pharos YAML input-adapter lists
- this keeps heavier `Protein` node writes later in the build

## Validation Status

Targeted tests pass:

```bash
pytest -q tests/test_from_dict.py tests/test_record_merger.py tests/test_tcrd_output_converter.py
```

Smoke-test validation completed:

- capped `working.yaml` build into `test_pharos`
- capped `working_mysql.yaml` export into `pharos400_working`

Observed capped results:

- graph `Protein` docs: `20,332`
- graph proteins with `patent_family_mentions`: `5,231`
- MySQL `patent_count` rows: `47,012`
- MySQL proteins with yearly patent rows: `7,035`
- MySQL `tdl_info` rows with `SureChEMBL Patent Family Count`: `7,035`

These smoke-test results are not representative of the full corpus year range because `max_location_batches` was enabled.

## Remaining Validation

Still needed:

1. run uncapped graph build
2. run uncapped MySQL export
3. confirm final year distribution and payload sizes on hot targets
