# Expression Atlas Disease Association Ingest Design

## Status

Discovery only. No code changes yet.

Current recommendation: punt for now. The old TCRD/Pharos pipeline depended on a bulk Atlas export model that no longer appears to exist in the same form. A fresh ingest is still possible, but it now looks like a large per-experiment harvesting project rather than a straightforward source adapter.

## Goal

Understand how old TCRD loaded Expression Atlas disease associations, compare that workflow with the current Expression Atlas download surface, and decide whether a modern ingest is practical.

## Historical TCRD Workflow

Steve Mathias used a two-step process:

1. Download a bulk Atlas archive:
   - `ftp://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/experiments/atlas-latest-data.tar.gz`
2. Run `R/exp-atlas_process.R` to produce:
   - `disease_assoc_human_do_uniq.tsv`
3. Load that TSV with:
   - `load-ExpressionAtlas-Diseases.py`

### Old preprocessing logic

The R script:

- read `contrastdetails.tsv`
- kept contrasts where:
  - reference disease label was `normal` or `healthy`
  - test disease label was not `normal` or `healthy`
- scanned per-experiment `-analytics.tsv` files
- kept rows with:
  - `p-value <= 0.05`
  - `abs(log2foldchange) > 1`
- mapped disease names to DOID through a local `doid.dict.tsv`
- dropped rows without a DOID match
- kept only human Ensembl gene IDs matching `ENSG0`
- deduped to unique `(Gene ID, DOID)` rows

Final output columns were:

- `Gene ID`
- `DOID`
- `Gene Name`
- `log2foldchange`
- `p-value`
- `disease`
- `experiment_id`
- `contrast_id`

### Old loader behavior

`load-ExpressionAtlas-Diseases.py`:

- loaded `disease_assoc_human_do_uniq.tsv`
- resolved targets by symbol first, then by ENSG xref
- inserted into TCRD `disease` with:
  - `dtype = 'Expression Atlas'`
  - `protein_id`
  - `name = disease label`
  - `did = DOID`
  - `log2foldchange`
  - `pvalue`

It did not preserve `experiment_id` or `contrast_id` in the final inserted `disease` row.

## Historical Output In `pharos319`

Read-only inspection of `pharos319` shows:

- `disease` rows with `dtype='Expression Atlas'`: `159846`

Sample rows contain:

- `protein_id`
- `did` as `DOID:*`
- `name`
- `log2foldchange`
- `pvalue`

## Current Expression Atlas Download Surface

Current official docs:

- `https://www.ebi.ac.uk/gxa/download`

The current download page says:

- data is available for every dataset individually via FTP
- differential results can be downloaded from each experiment page
- for differential experiments, an `analytics` download contains all statistics for that experiment

### FTP findings

Current FTP root:

- `https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/`

Key observation:

- there is no obvious modern equivalent of the old monolithic `atlas-latest-data.tar.gz`
- instead, there is an `experiments/` directory containing many per-experiment folders

Current experiment root:

- `https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/experiments/`

This is a large directory of experiment IDs such as:

- `E-MTAB-*`
- `E-GEOD-*`
- `E-CURD-*`

### Sample per-experiment contents

Example:

- `https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/experiments/E-GEOD-60424/`

Observed files include:

- `E-GEOD-60424-analytics.tsv`
- `E-GEOD-60424-analysis-methods.tsv`
- `E-GEOD-60424.idf.txt`
- `E-GEOD-60424-configuration.xml`
- additional contrast-specific outputs and plots

This suggests that the current raw material still exists, but only as per-experiment files.

## Main Discovery Conclusion

The old loader depended on two pieces that are no longer obviously available as a single bulk package:

- a global bulk experiment archive
- a global contrast metadata table (`contrastdetails.tsv`) used to identify disease-vs-healthy contrasts

The modern Atlas surface appears to require:

- enumerating experiments one by one
- deciding which are human differential disease studies
- reading per-experiment `analytics.tsv` plus metadata files
- reconstructing the disease-vs-healthy filtering logic ourselves

So this is no longer a small adapter task. It is a bulk-harvesting and curation project.

## Risks

- no obvious current single-file bulk export
- likely need to crawl many experiment directories
- likely need to reconstruct the old `contrastdetails.tsv` logic from per-experiment metadata
- disease normalization to DOID would still need to be rebuilt
- very large scope relative to other disease-association sources

## Recommendation

Punt for now.

If revisited later, treat it as a dedicated project with these phases:

1. bulk experiment discovery from the FTP `experiments/` tree
2. filtering to human differential disease studies
3. per-experiment analytics parsing
4. reconstruction of disease-vs-healthy contrast selection
5. disease-name normalization to DOID or another current canonical disease model

This does not look like a good “next ingest” candidate compared with sources that still provide cleaner bulk disease-association exports.

## Sources

- Old build notes:
  - `https://raw.githubusercontent.com/unmtransinfo/TCRD/master/doc/TCRD_Build_Notes.txt`
- Old preprocessing script:
  - `https://raw.githubusercontent.com/unmtransinfo/TCRD/master/R/exp-atlas_process.R`
- Old loader:
  - `https://raw.githubusercontent.com/unmtransinfo/TCRD/master/loaders/load-ExpressionAtlas-Diseases.py`
- Current download page:
  - `https://www.ebi.ac.uk/gxa/download`
- Current FTP root:
  - `https://ftp.ebi.ac.uk/pub/databases/microarray/data/atlas/`
