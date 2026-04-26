# Pharos Publication Statistics: PubMed Score

## Source

- Current source file: `input_files/auto/jensenlab/protein_counts.tsv`
- Download rule: `workflows/pharos.Snakefile` `download_pm_scores`
- Current graph adapter: `src/input_adapters/jensenlab/total_pmscore.py`

## Observed Payload Shape

- Row format is `ENSP<TAB>year<TAB>score`
- Current local file profile:
  - `513813` total rows
  - `19460` distinct ENSP identifiers
  - no blank years
  - no duplicate `(ENSP, year)` rows
  - year range `1901` to `2027`
- The source currently includes at least one future year row (`2027`), so the adapter should preserve source year values verbatim rather than filtering to `<= current_year`.

## Mapping Decision

- Keep the scalar aggregate on `Protein.pm_score`
- Add the time series on `Protein.pm_score_by_year`
- Derive `pm_score` by summing the yearly `score` values for each ENSP, preserving the current adapter behavior
- Preserve the yearly observations as source-native ordered records rather than collapsing to a dict

## First-Pass Scope

- Implement `pm_score_by_year` only for JensenLab PubMed Score
- Do not add separate publication nodes or edges
- Remove the dead legacy MySQL stop-gap adapter for PubMed score once the source-backed adapter remains authoritative

## Implementation Landed

### Graph model

- Added `Protein.pm_score_by_year`
- Added a small `YearScore` structure to represent `{year, score}` observations
- Kept `Protein.pm_score` as the aggregate scalar already used by Pharos/TCRD logic

### Source-backed adapter

- Updated `src/input_adapters/jensenlab/total_pmscore.py`
- The adapter now:
  - reads the existing `protein_counts.tsv`
  - derives `pm_score` by summing yearly scores per ENSP
  - emits `pm_score_by_year` from the source-native yearly rows
  - sorts the yearly observations before emit

### Working graph wiring

- Added the source-backed PubMed score adapter to `src/use_cases/working.yaml`
- No new Snakemake rule was needed because the existing `download_pm_scores` rule and `protein_counts.tsv` path were already in place

### Legacy cleanup

- Deleted the old MySQL stop-gap adapter file:
  - `src/input_adapters/pharos_mysql/ab_count_adapter.py`

### MySQL conversion

- Extended the TCRD converter so graph `Protein.pm_score_by_year` also exports to MySQL `pmscore`
- Existing scalar export to `tdl_info` (`itype='JensenLab PubMed Score'`) remains in place

## Validation Outcomes

### Graph (`test_pharos`)

Validated directly in Arango:

- `Protein` count: `20332`
- proteins with `pm_score`: `17980`
- proteins with `pm_score_by_year`: `17980`
- proteins with both: `17980`

Representative documents contain both:

- `pm_score`
- `pm_score_by_year`

### MySQL (`pharos400_working`)

Validated directly in MySQL:

- `pmscore` rows: `483012`
- distinct proteins in `pmscore`: `17980`
- `tdl_info` rows with `itype='JensenLab PubMed Score'`: `17980`

Sample `pmscore` rows matched the graph-side yearly series for the same protein.

## Follow-Up Notes

- `pm_score` is intentionally the aggregate rollup across yearly rows, matching the prior source-backed behavior and old TCRD semantics
- `pm_score_by_year` is now the graph-side source of truth for the yearly series
- PubTator was investigated separately and intentionally deferred until local PMID-year infrastructure is in better shape
