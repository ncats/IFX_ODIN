# Drug Source Update Report v1.4.0

This report is generated from per-source metadata after the drug harmonizer run.
It distinguishes real source refreshes from intentionally skipped optional layers.

## Summary

- Metadata rows: 20
- Download artifacts changed: 2
- Download artifacts unchanged/skipped: 7

## Status Counts

- `local_file`: 1
- `transformed`: 10
- `unchanged`: 6
- `unchanged_valid_local_zip`: 1
- `updated`: 2

## Source Rows

| Source | Artifact | Version | Source date | ODIN completed | Status | Changed | Records | Note |
|---|---|---:|---:|---:|---|---:|---:|---|
| ChEBI | download | 2026-09-09 | 2026-09-09 | 2026-09-24T13:35:28+00:00 | unchanged | FALSE |  | raw file exists and source version/remote headers are unchanged |
| ChEBI | transform | 2026-09-09 | 2026-09-09 | 2026-09-24T13:36:25+00:00 | transformed |  | 210997 |  |
| ChEMBL | download | ChEMBL_37 | 2026-05-01 | 2026-09-24T13:35:48+00:00 | unchanged | FALSE | 16784 | ChEMBL source version and query URLs are unchanged |
| ChEMBL | transform | ChEMBL_37 |  | 2026-09-24T13:38:16+00:00 | transformed |  | 2905804 |  |
| NodeNorm Chemical | transform | api_current |  | 2026-09-24T13:50:45+00:00 | transformed |  | 1304056 | NodeNorm is validation/canonicalization evidence, not an asserting drug source. |
| DrugCentral | download | 2021_09_01 | 2021-09-01 | 2026-09-24T13:35:28+00:00 | unchanged | FALSE |  | static DrugCentral raw files already match configured source_version and URLs |
| DrugCentral temporary pre-release overlay | download | temp_2026-08-21 |  | 2026-09-24T13:36:30+00:00 | local_file | TRUE |  |  |
| DrugCentral temporary pre-release overlay | transform | temp_2026-08-21 |  | 2026-09-24T13:36:32+00:00 | updated |  | 20265 |  |
| DrugCentral | transform | 2021_09_01 | 2021-09-01 | 2026-09-24T13:36:30+00:00 | transformed |  | 18400 |  |
| GSRS | download | 2026-08-06 | 2026-08-06 | 2026-09-24T13:35:07+00:00 | unchanged | FALSE |  |  |
| GSRS | transform | 2026-08-06 | 2026-08-06 | 2026-09-24T13:36:12+00:00 | transformed |  | 177121 |  |
| NCATS Inxight Drugs | transform | api_current |  | 2026-09-24T13:39:21+00:00 | transformed |  | 99607 |  |
| IUPHAR/BPS Guide to PHARMACOLOGY | download | 2026-09-16 | 2026-09-16 | 2026-09-24T13:35:53+00:00 | unchanged | FALSE |  | IUPHAR raw files already match configured source_version and URLs |
| IUPHAR/BPS Guide to PHARMACOLOGY | transform | 2026-09-16 | 2026-09-16 | 2026-09-24T13:38:26+00:00 | transformed |  | 22862 |  |
| PubChem | download | pug_current |  | 2026-09-24T13:39:38+00:00 | unchanged | FALSE | 132191 | existing PubChem raw file covers all current source CIDs except known bad CIDs |
| PubChem | transform | pug_current |  | 2026-09-24T13:39:43+00:00 | transformed |  | 132191 |  |
| RxNorm Current Prescribable Content | download | 2026-08-03 | 2026-08-03 | 2026-09-24T13:35:53+00:00 | unchanged_valid_local_zip | FALSE |  |  |
| RxNorm | transform | 2026-08-03 | 2026-08-03 | 2026-09-24T13:38:34+00:00 | transformed |  | 44096 |  |
| UniChem | download | current |  | 2026-09-24T13:45:11+00:00 | updated | TRUE | 5000 |  |
| UniChem | transform | current |  | 2026-09-24T13:45:12+00:00 | transformed |  | 5000 |  |
