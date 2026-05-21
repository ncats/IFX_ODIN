# DrugCentral Indications Summary

Date: 2026-04-14

## Goal

Add DrugCentral indications to the Pharos working graph and validate the TCRD MySQL conversion path.

## Scope Chosen

- Source: current DrugCentral PostgreSQL `omop_relationship_doid_view`
- Relationship scope: `indication` only
- Protein gating: only drugs with human target activity in `act_table_full`
- Graph outputs:
  - `Disease`
  - `ProteinDiseaseEdge`
- Working config only:
  - graph in `src/use_cases/working.yaml`
  - MySQL in `src/use_cases/working_mysql.yaml`

Reference counts from discovery:

- Raw source counts:
  - current DrugCentral `indication` rows: `12047`
  - current indication structures: `2723`
  - current indication rows with `UMLS`: `9562`
- Legacy downstream count:
  - `pharos319.disease` rows with `dtype='DrugCentral Indication'`: `13919`
- Working graph count:
  - `test_pharos.ProteinDiseaseEdge` rows: `41663`
- Initial working MySQL count:
  - `pharos400_working.disease` rows with `dtype='DrugCentral Indication'`: `62140`

Note:

- the raw DrugCentral source count is not directly comparable to the MySQL `disease` table count
- the source count is pre-expansion
- the MySQL `disease` count is post-expansion across protein targets

Deferred:

- contraindication / off-label use
- approval metadata
- full Pharos config promotion

## Key Design Decisions

- Use `UMLS` as the primary source disease ID.
  - Node Normalizer coverage was stronger for `UMLS` than `SNOMEDCT`.
  - `SNOMEDCT` rows were already paired with `UMLS`, so it was not needed as a fallback.
- Do not rely on DrugCentral `DOID` as a fallback.
  - It did not add useful extra coverage once `UMLS` / `SNOMEDCT` were considered.
- Preserve text-only indication concepts.
  - If a row has `UMLS`, emit `Disease.id = UMLS:<cui>`.
  - If a row has no `UMLS`, emit a stable local ID `DrugCentral:INDICATION:<hash>`.
- Preserve source metadata in edge details:
  - `drug_name`
  - `snomed_id`
  - `doid`

Resolver investigation summary:

- We checked the current Node Normalizer integration used by Pharos / target_graph.
- `UMLS` resolved at a higher rate than `SNOMEDCT` for DrugCentral indication IDs.
- Where both existed, `UMLS` and `SNOMEDCT` usually normalized to the same concept, but not always.
- That was enough to justify a deterministic rule:
  - use `UMLS` as the emitted disease ID
  - keep `SNOMEDCT` as metadata
  - avoid mixing the two at adapter time

## Legacy Comparison

Legacy `pharos319` DrugCentral indications:

- lived in `disease` with `dtype = 'DrugCentral Indication'`
- populated `name` and `drug_name`
- sometimes populated `did`
- often had no discrete disease ID at all

This mattered for first-pass design because legacy Pharos did preserve text-only indication names downstream.

Additional legacy comparison:

- among the Pharos disease association sources reviewed in `pharos319`, DrugCentral was the only one with true name-only disease association rows that had neither `did` nor `mondoid`

## What Was Implemented

Code/config changes:

- new DrugCentral indication adapter
- `DiseaseAssociationDetail` extended for DrugCentral metadata
- `working.yaml` updated to ingest DrugCentral indications into `test_pharos`
- `working_mysql.yaml` updated to read disease data from `test_pharos`
- TCRD converter updated so:
  - `disease.did = detail.source_id`
  - `disease.drug_name = detail.drug_name`

## Graph Validation

Validation of `test_pharos` showed:

- DrugCentral indication diseases were loaded
- many `UMLS` diseases normalized to `MONDO`
- text-only indication concepts were preserved as local `DrugCentral:INDICATION:*` disease nodes

Representative outcomes:

- `Anesthesia for cesarean section` preserved
- `Local anesthesia` preserved
- `Alcoholism` normalized to `MONDO`
- `Metastatic Breast Carcinoma` preserved as a local DrugCentral indication concept

## MySQL Validation

Initial `pharos400_working` run showed the main converter issues:

- `disease.drug_name` was not being populated
- local graph IDs were leaking into `disease.did`

Those converter issues were patched.

## Remaining Follow-Up

- Recheck `pharos400_working` after the latest converter rerun:
  - `disease.did`
  - `disease.drug_name`
  - `disease.mondoid`
  - `ncats_disease` preservation of text-only names
- Compare this pattern with other disease association ingests such as CTD to decide whether the local-ID strategy should remain DrugCentral-specific or become a broader convention.
