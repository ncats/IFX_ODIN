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

| Source | Artifact | Version | Source date | ODIN completed | Status | Changed | Records | Note |
|---|---|---:|---:|---:|---|---:|---:|---|
| ChEBI | download | 2026-08-14 | 2026-08-14 | 2026-08-27T20:46:17+00:00 | unchanged | FALSE |  | raw file exists and source version/remote headers are unchanged |
| ChEBI | transform | 2026-08-14 | 2026-08-14 | 2026-08-27T20:47:18+00:00 | transformed |  | 210889 |  |
| ChEMBL | download | ChEMBL_37 | 2026-05-01 | 2026-08-27T20:46:45+00:00 | unchanged | FALSE |  | ChEMBL source version and query URL are unchanged |
| ChEMBL | transform | ChEMBL_37 |  | 2026-08-27T20:47:21+00:00 | transformed |  | 4225 |  |
| NodeNorm Chemical | transform | api_current |  | 2026-08-27T20:50:16+00:00 | transformed |  | 412089 | NodeNorm is validation/canonicalization evidence, not an asserting drug source. |
| DrugCentral | download | 2021_09_01 | 2021-09-01 | 2026-08-27T20:46:18+00:00 | unchanged | FALSE |  | static DrugCentral raw files already match configured source_version and URLs |
| DrugCentral | transform | 2021_09_01 | 2021-09-01 | 2026-08-27T20:47:21+00:00 | transformed |  | 23477 |  |
| GSRS | download | 2026-08-06 | 2026-08-06 | 2026-08-27T20:46:16+00:00 | unchanged | FALSE |  |  |
| GSRS | transform | 2026-08-06 | 2026-08-06 | 2026-08-27T20:47:05+00:00 | transformed |  | 177121 |  |
| NCATS Inxight Drugs | transform | api_current |  | 2026-08-27T20:48:10+00:00 | transformed |  | 99607 |  |
| PubChem | download | pug_current |  | 2026-08-27T20:48:13+00:00 | unchanged | FALSE | 49986 | existing PubChem raw file covers all current source CIDs except known bad CIDs |
| PubChem | transform | pug_current |  | 2026-08-27T20:48:14+00:00 | transformed |  | 49986 |  |
| RxNorm Current Prescribable Content | download | 2026-08-03 | 2026-08-03 | 2026-08-27T20:46:45+00:00 | xref_only | FALSE |  | RxNorm configured as xref-only: full CPC ZIP was not downloaded. RXCUI identifiers asserted by PubChem, ChEMBL, ChEBI, DrugCentral, GSRS/Inxight, or UniChem remain in drug xrefs. |
| RxNorm | transform | 2026-08-03 | 2026-08-03 | 2026-08-27T20:47:21+00:00 | xref_only |  | 0 | Full RxNorm product context is disabled for this release. RXCUI identifiers found in other source xrefs are retained by drug_merge. |
| UniChem | download | not_run |  | 2026-08-27T20:48:14+00:00 | disabled | FALSE |  | UniChem API has no batch endpoint; use source mapping files for reproducible large-scale enrichment. |
| UniChem | transform | not_run |  | 2026-08-27T20:48:14+00:00 | transformed |  | 0 |  |
