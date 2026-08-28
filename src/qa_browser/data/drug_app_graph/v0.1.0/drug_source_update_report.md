# Drug Source Update Report v0.1.0

This report is generated from per-source metadata after the drug harmonizer run.
It distinguishes real source refreshes from intentionally skipped optional layers.

## Summary

- Metadata rows: 16
- Download artifacts changed: 0
- Download artifacts unchanged/skipped: 7

## Status Counts

- `disabled`: 1
- `transformed`: 8
- `unchanged`: 5
- `xref_only`: 2

## Source Rows

| Source | Artifact | Version | Status | Changed | Records | Note |
|---|---|---:|---|---:|---:|---|
| ChEBI | download | 2026-08-14 | unchanged | FALSE |  | raw file exists and source version/remote headers are unchanged |
| ChEBI | transform | 2026-08-14 | transformed |  | 210889 |  |
| ChEMBL | download | ChEMBL_37 | unchanged | FALSE |  | ChEMBL source version and query URL are unchanged |
| ChEMBL | transform | ChEMBL_37 | transformed |  | 4225 |  |
| NodeNorm Chemical | transform | api_current | transformed |  | 412089 | NodeNorm is validation/canonicalization evidence, not an asserting drug source. |
| DrugCentral | download | 2021_09_01 | unchanged | FALSE |  | static DrugCentral raw files already match configured source_version and URLs |
| DrugCentral | transform | 2021_09_01 | transformed |  | 23477 |  |
| GSRS | download | 2026-08-06 | unchanged | FALSE |  |  |
| GSRS | transform | 2026-08-06 | transformed |  | 177121 |  |
| NCATS Inxight Drugs | transform | api_current | transformed |  | 99607 |  |
| PubChem | download | pug_current | unchanged | FALSE | 49986 | existing PubChem raw file covers all current source CIDs except known bad CIDs |
| PubChem | transform | pug_current | transformed |  | 49986 |  |
| RxNorm Current Prescribable Content | download | 2026-08-03 | xref_only | FALSE |  | RxNorm configured as xref-only: full CPC ZIP was not downloaded. RXCUI identifiers asserted by PubChem, ChEMBL, ChEBI, DrugCentral, GSRS/Inxight, or UniChem remain in drug xrefs. |
| RxNorm | transform | 2026-08-03 | xref_only |  | 0 | Full RxNorm product context is disabled for this release. RXCUI identifiers found in other source xrefs are retained by drug_merge. |
| UniChem | download | not_run | disabled | FALSE |  | UniChem API has no batch endpoint; use source mapping files for reproducible large-scale enrichment. |
| UniChem | transform | not_run | transformed |  | 0 |  |
