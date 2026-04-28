# TIGA Ingest Design

## Goal

Add a first-pass TIGA ingest for Pharos that preserves raw GWAS trait space, keeps TIGA as the source of the association metrics, and supports a best-effort projection from GWAS traits into canonical disease space when resolver-backed normalization succeeds.

## Source Files

Primary ingest inputs:
- `https://unmtid-dbs.net/download/TIGA/latest/tiga_gene-trait_stats.tsv`
- `https://unmtid-dbs.net/download/TIGA/latest/tiga_gene-trait_provenance.tsv`

Useful secondary references:
- `https://unmtid-dbs.net/download/TIGA/`
- `https://unmtid-shinyapps.net/tiga/`
- `https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-TIGA.py`

## Version Strategy

- Download the two primary TSVs into `input_files/auto/tiga/`.
- Persist a `tiga_version.tsv` sidecar during download.
- Capture:
  - `version`: the latest dated release directory under `https://unmtid-dbs.net/download/TIGA/`
  - `version_date`: the max `Last-Modified` date across the two primary files
  - `download_date`: the UTC date when the files are fetched

Observed during discovery on April 28, 2026:
- `latest/` pointed to a `20260120` release directory
- both files had `Last-Modified: 2026-02-28`

Observed in the downloaded local sidecar:
- `version`: `20260120`
- `version_date`: `2026-02-28`
- `download_date`: `2026-04-28`

## Observed Payload Shape

### `tiga_gene-trait_stats.tsv`

Header:

`ensemblId	efoId	trait	n_study	n_snp	n_snpw	geneNtrait	geneNstudy	traitNgene	traitNstudy	pvalue_mlog_median	pvalue_mlog_max	or_median	n_beta	study_N_mean	rcras	geneSymbol	TDL	geneFamily	geneIdgList	geneName	meanRank	meanRankScore`

Notes:
- current file is plain TSV, not gzipped
- current file includes `pvalue_mlog_max`, which the old TCRD loader did not read
- the source identifier column is still named `efoId`, but current values are not limited to `EFO_*`

Discovery profile for the April 2026 payload:
- rows: `761,797`
- distinct genes: `18,902`
- distinct trait IDs: `12,291`

Local file profile:
- line count on disk: `761,798` including header
- distinct `(ensemblId, efoId)` association keys: `753,581`
- duplicate `(ensemblId, efoId)` rows: `8,216`
- sampled duplicates inspected so far are exact duplicate rows, not conflicting metric rows

Field coverage / distributions:
- `TDL` distribution:
  - `Tbio`: `508,418`
  - `Tdark`: `125,438`
  - `Tchem`: `97,296`
  - `Tclin`: `30,645`
- `geneIdgList`:
  - `FALSE`: `751,003`
  - `TRUE`: `10,794`
- `or_median`:
  - `NA`: `670,306`
  - populated: `91,491`
- `pvalue_mlog_median == pvalue_mlog_max`: `479,069`
- `pvalue_mlog_median != pvalue_mlog_max`: `282,728`
- `study_N_mean` parsed as numeric for every row inspected

Most common `geneFamily` values:
- `NA`: `441,878`
- `Enzyme`: `166,234`
- `TF`: `56,657`
- `Kinase`: `27,500`
- `Transporter`: `21,822`
- `Epigenetic`: `12,572`
- `IC`: `12,325`
- `GPCR`: `11,672`

Identifier prefix distribution:
- `EFO`: `539,715`
- `OBA`: `190,351`
- `MONDO`: `23,395`
- `HP`: `7,085`
- `GO`: `815`
- `PATO`: `300`
- `NCIT`: `33`
- `Orphanet`: `74`
- `MP`: `25`
- `GSSO`: `4`

Representative non-EFO examples:
- `OBA_0003277` — platelet volume
- `OBA_VT0005416` — blood protein amount
- `MONDO_*`
- `HP:*` encoded as `HP_*`

### `tiga_gene-trait_provenance.tsv`

Header:

`ensemblId	TRAIT_URI	STUDY_ACCESSION	PUBMEDID	efoId`

Discovery profile for the April 2026 payload:
- rows: `1,558,387`
- distinct studies: `42,274`
- distinct PMIDs: `4,026`

Local file profile:
- line count on disk: `1,558,388` including header
- distinct `(ensemblId, efoId)` association keys: `753,581`
- key coverage matches the stats file exactly:
  - stats-only keys: `0`
  - provenance-only keys: `0`
  - shared keys: `753,581`

Provenance cardinality per association key:
- `508,630` associations have exactly `1` provenance row
- `115,561` have `2`
- `50,957` have `3`
- `25,509` have `4`
- `14,691` have `5`
- long tail continues upward; the highest duplicate-key counts observed so far reach `6` in sampled rows and much larger provenance-row counts for some keys

Most reused study accessions across association rows:
- `GCST90245848`: `12,844`
- `GCST90565843`: `7,639`
- `GCST90105038`: `5,646`
- `GCST90565837`: `4,774`
- `GCST001762`: `3,762`

Identifier prefix distribution:
- `EFO`: `1,110,770`
- `OBA`: `383,427`
- `MONDO`: `51,244`
- `HP`: `11,428`
- `GO`: `1,080`
- `PATO`: `300`
- `NCIT`: `35`
- `Orphanet`: `74`
- `MP`: `25`
- `GSSO`: `4`

## Legacy Comparison

Legacy TCRD already had:
- `tiga`
- `tiga_provenance`
- `JensenLab Experiment TIGA` rows in `disease`

Observed in `pharos319`:
- `tiga`: `745,611` rows
- `tiga_provenance`: `721,164` rows
- distinct proteins in `tiga`: `18,005`
- distinct trait IDs in `tiga`: `2,247`

Important modeling finding:
- only `128,263` legacy `tiga` rows had `ncats_disease_id`
- `617,348` legacy `tiga` rows had `ncats_disease_id IS NULL`

Implication:
- TIGA is not a disease-only source
- treating it primarily as `ProteinDiseaseEdge` would discard most of the source's native semantics
- the existing JensenLab experiments ingest preserves a disease-oriented TIGA slice, but not the full GWAS metric table or the study-level provenance table

## Implemented Graph Model

The working graph now uses three TIGA-related graph structures:

- `GwasTrait`
- `ProteinGwasTraitEdge`
- `GwasTraitDiseaseEdge`

### `GwasTrait`

- keyed by the raw TIGA `efoId` value exactly as delivered by the source, for example `EFO_0007990` or `OBA_VT0005416`
- stores the source trait label in `name`
- stores `trait_uri` from the provenance file when available

### `ProteinGwasTraitEdge`

- connects the resolved protein endpoint to the raw `GwasTrait`
- carries the TIGA GWAS association payload in `details`
- each `details` entry is a `GwasAssociationDetail`
- each detail contains the grouped TIGA metrics plus nested `provenance_details`

This follows the existing Pharos pattern used by `ProteinDiseaseEdge` and `ProteinLigandEdge`, where source-specific association payload lives in `details` rather than being flattened into top-level edge fields.

### `GwasTraitDiseaseEdge`

- projects raw GWAS trait space into canonical disease space
- starts at `GwasTrait`
- ends at `Disease`
- seeds the disease endpoint from the same raw trait ID converted into CURIE form, for example `EFO_0007990` -> `EFO:0007990`
- relies on the configured `Disease` resolver to normalize the projected disease endpoint
- unresolved projected edges are intentionally dropped by resolver behavior

This keeps the raw TIGA trait identity intact while still allowing a resolver-mediated bridge into canonical `Disease` nodes when available.

### Why this boundary

- `GwasTrait` is a real reusable graph concept
- the TIGA GWAS metrics are association-level payload, not trait-level payload
- disease linkage is a best-effort normalization, not the primary identity model
- the stats file contains exact duplicate `(ensemblId, efoId)` rows, so the ingest should dedupe repeated detail payload on the same protein-trait edge rather than reify association rows as standalone nodes

## TCRD Export Plan

From `ProteinGwasTraitEdge` plus its `details` entries:

- emit one `tiga` row per edge detail
- emit one `tiga_provenance` row per provenance detail nested under an edge detail
- populate `tiga.ncats_disease_id` only when the trait participates in a surviving `GwasTraitDiseaseEdge` and the working MySQL build also includes `DiseaseAdapter`

Important constraint:

- `ncats_disease_id` remains best-effort enrichment, not the primary identity model
- most TIGA traits are not diseases
- legacy `pharos319` also showed that only a minority of TIGA rows linked to `ncats_disease`

## Working Validation Results

### Graph build

Validated in `test_pharos` using the capped `max_rows: 5000` working build.

Observed counts:

- `GwasTrait`: `2,380`
- `ProteinGwasTraitEdge`: `5,000`
- `GwasTraitDiseaseEdge`: `163`
- `Disease`: `26,257`

Observed structural behavior:

- every sampled `ProteinGwasTraitEdge` had exactly one TIGA `details` entry
- the important multiplicity is in `details[0].provenance_details`, not in repeated top-level edge details
- the disease projection is highly selective:
  - `163` distinct traits survived as `GwasTraitDiseaseEdge`
  - all surviving projected disease endpoints normalized to `MONDO:*`
  - no projected edges survived as raw `EFO:*`, `HP:*`, or `OBA:*`

Observed protein-trait coverage for the disease projection:

- `163` distinct GWAS traits participate in a surviving disease projection
- `346` of the `5,000` `ProteinGwasTraitEdge` rows point to one of those traits

Interpretation:

- the resolver-mediated projection is conservative, which is appropriate for TIGA
- most TIGA traits remain raw GWAS traits only
- the graph now supports a principled bridge from GWAS traits into canonical disease space without collapsing the entire source into a disease model

### Working MySQL build

Validated after the disease projection change:

- `tiga`: `5,000` rows
- `tiga_provenance`: `9,931` rows
- `346` `tiga` rows have `ncats_disease_id`
- `163` distinct `efoid` values have `ncats_disease_id`
- duplicated `(protein_id, efoid)` pairs in `tiga`: `0`

Observed row population:

- unresolved measurement and non-disease traits remain with `ncats_disease_id = NULL`
- resolvable disease-like traits now populate `ncats_disease_id`
- sample resolved mappings looked sensible, for example:
  - `EFO_0006788` `anxiety disorder` -> `ncats_disease_id 109`
  - `EFO_0000178` `gastric carcinoma` -> `ncats_disease_id 35`
  - `EFO_0000275` `atrial fibrillation` -> `ncats_disease_id 40`
  - `MONDO_0005148` `type 2 diabetes mellitus` -> `ncats_disease_id 69`

Sample unresolved rows also looked appropriate, including:

- blood-cell measurements
- protein abundance measurements
- OBA traits
- generic quantitative lab phenotypes such as `hematocrit`

Sample rows showed expected population of:
  - `protein_id`
  - `ensg`
  - `efoid`
  - `trait`
  - `n_study`
  - `n_snp`
  - `pvalue_mlog_max`
  - `meanRankScore`
  - `study_N_mean`

Interpretation:

- the best-effort disease projection now reaches MySQL cleanly
- the coverage matches the graph-side disease projection exactly:
  - `163` resolvable traits
  - `346` affected protein-trait rows
- no duplication was introduced in the working build by disease resolution

## Follow-up

- decide whether to keep disease linkage as export-time best-effort enrichment only, or also surface it more directly in downstream Pharos use cases
- consider whether adapter dependency contracts should become explicit framework metadata, since the TIGA disease projection depends on both `Disease` nodes and a `Disease` resolver being present in the build
