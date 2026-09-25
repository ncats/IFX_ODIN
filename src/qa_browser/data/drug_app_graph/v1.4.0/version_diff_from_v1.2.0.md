# Drug App Graph Diff

Baseline: v1.2.0
Current: v1.4.0
Generated: 2026-09-24T14:59:04.664867+00:00
Comparison scope: complete

## Summary

| Metric | Value |
|---|---:|
| drugs_old | 1304102 |
| drugs_new | 1304056 |
| drugs_added | 0 |
| drugs_removed | 46 |
| drugs_retained | 1304056 |
| edges_old | 1924334 |
| edges_new | 1957067 |
| edges_added | 351128 |
| edges_removed | 318395 |
| standard_name_changes | 0 |
| evidence_tier_changes | 76 |
| approval_status_changes | 0 |
| biolink_category_changes | 0 |
| inchikey_changes | 0 |
| source_version_changes | 1 |

## Why identities are no longer current

46 prior IFXDrug identities are no longer current; this does not necessarily mean 46 chemicals disappeared.

41 lost all qualifying support after source quality filtering, while 0 consolidated by exact source identifiers and 5 have a current equivalent indicated by non-asserting NodeNorm validation. The filtering evidence covers 65 ChEMBL activity records carrying the potential-duplicate flag.

| Reason | Count |
|---|---:|
| equivalent_current_identity_retained | 5 |
| source_quality_filter_exclusion | 41 |

### Interpretation notes

- Trusted-source successors and NodeNorm validation-only equivalents are reported separately; names, CAS values, and fuzzy structure matching are not used.
- A changed IFXDrug ID can indicate identity-component consolidation and should not be interpreted as chemical deletion.
- The rejection checksum pins the active pipeline-run evidence used for this diff; retain the generated diff artifact for reproducibility.

## Output Sections

- added_drugs
- removed_drugs
- standard_name_changes
- evidence_tier_changes
- approval_status_changes
- biolink_category_changes
- inchikey_changes
- edges_added_examples
- edges_removed_examples
- source_version_changes

Large row-level sections are capped by limits.max_rows_per_section in the JSON file.
