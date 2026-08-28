# Drug Harmonizer QC Scenario Log

Release: v0.1.0

This file is generated from `drug_scenarios.yaml` and the current divergence registry.

## Multiple InChIKeys in one harmonized component

- Scenario ID: `multiple_inchikey_values`
- Current rows: 420
- Default decision: needs_expert_review
- Severity: high
- Rationale: InChIKey is used as a structural identity bridge. More than one InChIKey in a merged component can be legitimate for mixtures/salts but is a strong signal that the component needs chemical review.
- Reviewer action: Confirm whether the component represents one active ingredient/substance, a salt/hydrate/isomer family, or an accidental merge.

## NodeNorm returned a non-chemical preferred type

- Scenario ID: `nodenorm_non_chemical_type`
- Current rows: 0
- Default decision: needs_expert_review
- Severity: medium
- Rationale: NodeNorm is used only as a validator/canonicalizer. A non-chemical canonical type should not silently replace the source-standard drug or chemical identifier.
- Reviewer action: Inspect the candidate CURIEs and source-standard ID. Keep the local source-standard ID if NodeNorm appears to normalize to the wrong entity.

## NodeNorm did not normalize the preferred candidate

- Scenario ID: `nodenorm_not_normalized`
- Current rows: 0
- Default decision: accepted_source_standard
- Severity: info
- Rationale: Source-standard IDs remain valid even when NodeNorm lacks coverage. This is tracked for transparency but is not automatically a human-review item.
- Reviewer action: No action required unless the row also has source-standard or structural conflicts.

## NodeNorm preferred CURIE differs from source-standard primary ID

- Scenario ID: `nodenorm_preferred_curie`
- Current rows: 0
- Default decision: accept_nodenorm_preferred_curie
- Severity: info
- Rationale: The source-standard primary ID is retained, while primary_id is set to the NodeNorm-preferred CURIE when NodeNorm resolves the candidate as an equivalent identifier.
- Reviewer action: No action required unless the preferred CURIE is clinically or chemically broader/narrower than the source-standard entity.

## NodeNorm query failed for the preferred candidate

- Scenario ID: `nodenorm_query_failed`
- Current rows: 0
- Default decision: retry_or_review
- Severity: low
- Rationale: A transient NodeNorm failure is not an evidence conflict, but it means the row has not been externally canonicalized in this release.
- Reviewer action: Retry the NodeNorm enrichment after service recovery. Manually review only if the row remains unresolved across reruns.

## Multiple IDs from the same source inside one harmonized component

- Scenario ID: `source_standard_conflict`
- Current rows: 4,988
- Default decision: needs_expert_review
- Severity: high
- Rationale: A source-standard identifier should normally appear once per harmonized substance/component. Multiple values from the same source may indicate an over-merged structure, salt/mixture ambiguity, stereochemistry issue, or product-vs-ingredient leakage.
- Reviewer action: Check the source labels, structural keys, and cross-source xrefs. Split the component or select the correct source-standard identifier when the merge is too broad.
