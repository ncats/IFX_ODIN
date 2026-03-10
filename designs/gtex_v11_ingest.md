# GTEx V11 Ingest Design (Pharos / Target Graph)

## Summary
This ingest updates GTEx expression support to V11 and emits protein–
tissue expression data as a single canonical `ProteinTissueExpressionEdge`
per `(Protein, Tissue)` pair. GTEx evidence is stored as `ExpressionDetail`
entries nested in the edge, one per sex cohort (all / male / female).

The `ExpressionDetail` model is shared across all expression sources
(GTEx, HPA Protein, HPA RNA, HPM Protein, JensenLab TISSUES). GTEx
uses `number_value` for median TPM and `source_rank` for the
normalised tissue rank.

## Scope (First Pass)

### In scope
- GTEx V11 bulk RNA-seq inputs:
  - gene TPM matrix
  - sample attributes
  - subject phenotypes
- GTEx evidence stored as `ExpressionDetail` entries with:
  - `source` = `"GTEx"`
  - `tissue` (tissue name string)
  - `uberon_id`
  - `sex` (`None` = all cohorts combined, `"male"`, `"female"`)
  - `number_value` (median TPM for that cohort)
  - `source_rank` (normalised rank, 0.0–1.0, within cohort)
- Legacy-consistent sample and subject filtering.
- Datasource metadata:
  - `version`
  - `version_date`
  - `download_date` (derived by adapter from input file mtimes)

### Out of scope
- Cross-source harmonized expression scoring.
- New ontology normalization policy.
- Broad model/resolver refactors beyond GTEx ingest needs.
- Additional GTEx modalities.

## Inputs

### Files (V11)
- `GTEx_Analysis_2025-08-22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz`
- `GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt`
- `GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt`
- `gtex_version.tsv` with:
  - header: `version\tversion_date`
  - value: `GTEx Analysis Version 11\t2025-08-22`

### Download locations
- `https://storage.googleapis.com/adult-gtex/bulk-gex/v11/rna-seq/
  GTEx_Analysis_2025-08-22_v11_RNASeQCv2.4.3_gene_tpm.gct.gz`
- `https://storage.googleapis.com/adult-gtex/annotations/v11/
  metadata-files/GTEx_Analysis_v11_Annotations_SampleAttributesDS.txt`
- `https://storage.googleapis.com/adult-gtex/annotations/v11/
  metadata-files/GTEx_Analysis_v11_Annotations_SubjectPhenotypesDS.txt`

## Workflow Wiring
- Snakefile: `workflows/pharos.Snakefile`
- Rule: `download_gtex`
- Output directory: `input_files/auto/gtex/`
- Trial config target: `src/use_cases/working.yaml`
- Promotion target after validation: `src/use_cases/pharos/
  target_graph.yaml`

## Validated Payload Assumptions
- Matrix sample IDs are expected to match sample metadata IDs.
- Matrix gene identifier column is `Name` with Ensembl version
  suffix; ingest strips suffix (e.g., `ENSG....N` → `ENSG...`).
- Sample metadata includes tissue name (`SMTSD`/`SMTS`), UBERON ID
  (`SMUBRID`), and autolysis score (`SMATSSCR`).
- Subject metadata includes sex (`SEX`, coded `1`/`2`) and
  death-hardy score (`DTHHRDY`).

## Filtering Rules (Legacy-Compatible)
Both filters are applied up front in `_load_samples`. Any sample
failing either rule is excluded from all cohorts (all, male, female).

1. Sample must map to sample metadata with tissue fields present.
2. Sample's subject must map to subject metadata with a known sex
   (`SEX` = `1` or `2`).
3. **Subject filter:** exclude samples where `DTHHRDY > 2`
   (traumatic/sudden death excluded; ventilator and slow death
   included).
4. **Autolysis filter:** exclude samples where `SMATSSCR >= 2`
   (moderate or severe autolysis excluded; none or mild included).
   `SMATSSCR` is a 0–3 categorical score.

## Aggregation Rules

For each gene and tissue, eligible samples are split by sex:

- `sex=None` (all): median TPM across all eligible samples
  (male + female combined).
- `sex="male"`: median TPM across eligible male samples only.
- `sex="female"`: median TPM across eligible female samples only.

`sex=None` is always emitted. `sex="male"` and `sex="female"` are
only emitted when at least one eligible sample of that sex exists
for the tissue.

Note: because `sex=None` is the union of male and female cohorts,
a tissue with only female samples will have identical `number_value`
for `sex=None` and `sex="female"`, but their `source_rank` values
may differ because the rank pools are different (the all-cohort pool
includes tissues where male samples shift the combined median).

## Ranking Rules

For each gene, tissues are ranked within each sex cohort independently
using the legacy-compatible algorithm:

1. Compute average-method rank for each tissue's median TPM within
   the cohort (lowest TPM = rank 1, ties share the average of their
   ranks).
2. Divide each rank by `n` (number of tissues in the cohort).
3. Min-max normalise to `[0.0, 1.0]`:
   `(rank/n - min) / (max - min)`

Result: lowest-expressed tissue = `0.0`, highest = `1.0`.
Edge case: if all medians are zero, all ranks are `0.0`.
Edge case: if all medians are equal (range = 0), ranks are returned
pre-normalisation (all equal).

Ranks are per-gene across tissues (not per-tissue across genes).
Ranks are per-cohort: the `source_rank` for `sex=None` and
`sex="female"` for the same tissue will differ when other tissues
have male samples that shift the combined cohort ordering.

## Graph Mapping Decisions

### Canonical edge strategy
- One `ProteinTissueExpressionEdge` per `(Protein, Tissue)` pair.
- GTEx sex cohorts are modeled as separate `ExpressionDetail` entries
  within that edge, not as separate edges.

### ExpressionDetail model
`ExpressionDetail` is shared across all expression sources. Fields
used by GTEx:

| Field | GTEx usage |
|---|---|
| `source` | `"GTEx"` |
| `tissue` | tissue name string |
| `uberon_id` | UBERON ontology ID |
| `sex` | `None` / `"male"` / `"female"` |
| `number_value` | median TPM |
| `source_rank` | normalised rank (0.0–1.0) |

Fields used by other sources (`qual_value`, `expressed`, `evidence`,
`oid`) are null for GTEx.

### Identifier strategy
- Ensembl gene IDs stripped of version suffix become the protein
  node identifier (resolver maps ENSEMBL → IFXProtein).
- Tissue node ID is UBERON ID where available, falling back to
  tissue name string.
- Cross-ontology normalization remains resolver responsibility.

## Datasource Metadata Strategy
- `version` and `version_date` come from `gtex_version.tsv`.
- `download_date` is derived from the minimum mtime of all four
  input files.
- `DatasourceVersionInfo` is populated using named parameters.

## Validation Plan
- Run ingest in `src/use_cases/working.yaml`.
- Validate:
  - successful parse of all three V11 inputs
  - expected non-zero expression relationship output
  - no duplicate expression edges for same `(Protein, Tissue)` pair
  - up to three `ExpressionDetail` entries per edge (all/male/female)
  - `source_rank` values in `[0.0, 1.0]`; exactly one tissue per
    gene per cohort has `source_rank = 0.0` and one has `1.0`
  - filtering rules measurably applied (samples removed by
    `DTHHRDY > 2` and `SMATSSCR >= 2` filters)
  - provenance metadata present (`version`, `version_date`,
    `download_date`)

## Promotion Criteria
Promote to `src/use_cases/pharos/target_graph.yaml` only when:
- V11 download + parse is stable.
- Output counts are directionally consistent with V11 breadth.
- All three sex cohort details present where expected.
- Legacy-compatible filtering confirmed in output QA.
