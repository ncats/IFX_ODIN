# CURE Rasopathies TSV Reconstruction Checklist

## Purpose

This checklist tracks whether the JSONL-derived rasopathies graph contains enough information to
account for the curated Translator-style TSV facts in `cureid_data.tsv`.

The goal is not to regenerate the TSV exactly. The graph is intentionally richer and more
case-report oriented. The goal is that every curated TSV fact can either be reconstructed from graph
content or explicitly marked as deferred.

Reference files:

- Raw-ish case-report source: `input_files/manual/cure/reports.jsonl`
- Curated association/concept source: `input_files/manual/cure/cureid_data.tsv`
- Equivalent external copy:
  `/Users/kelleherkj/IdeaProjects/translator-ingests/data/cureid/2025-12-17/source_data/cureid_data.tsv`

The two TSV paths above were byte-for-byte identical when checked during this review.

## Current TSV Shape

- `240` association rows across `11` rasopathies reports.
- `5` curated disease concepts.
- `19` drug labels / curated drug IDs.
- `6` gene labels.
- `10` sequence variant labels, currently without final variant CURIEs.
- `140` disease-to-phenotype rows.
- `63` drug treatment rows.
- `7` adverse event rows.
- `10` gene-to-disease rows.
- `10` gene-to-variant rows.
- `10` variant-to-disease rows.

## Checklist

- `[x]` Case report identity
  - Graph has all `11` rasopathies case reports.
  - TSV has `11` distinct `report_id` values.

- `[x]` Patient anchor
  - Graph emits `11` `Patient` nodes and `11` `CaseReportPatientEdge` edges.
  - Graph emits `11` `PatientClinicalContextEdge` edges, so clinical clinical_context hangs from the
    patient rather than directly from the report.
  - This includes reports with empty demographics, so downstream case-scoped data has a stable
    patient anchor.

- `[x]` Disease / condition concepts
  - TSV has `5` disease concepts.
  - These are the `5` curated `Condition` nodes emitted from the TSV concept adapter:
    - `MONDO:0015280` cardiofaciocutaneous syndrome
    - `MONDO:0018997` Noonan syndrome
    - `MONDO:0007893` Noonan syndrome with multiple lentigines
    - `MONDO:0009026` Costello syndrome
    - `ORPHA:544254` SYNGAP1-related developmental and epileptic encephalopathy
  - JSONL emits `11` `ClinicalContextConditionEdge` records from report disease labels; resolver
    normalization collapses those report-level disease mentions to the same `5` concepts.

- `[x]` ClinicalContext findings needed for TSV `has_phenotype_of`
  - JSONL adapter emits `125` raw clinical_context `Finding` nodes through `ClinicalContextFindingEdge`.
  - Each `Finding` points to a raw phenotype endpoint through `FindingPhenotypeEdge`.
  - The CURE label resolver expands those clinical_context finding phenotype endpoints to `141`
    resolved `Finding -> Phenotype` links.
  - Four resolved links collapse to duplicate `(ClinicalContext, Phenotype)` pairs when summarized at
    the report-concept level, yielding `137` unique report-to-phenotype concept relationships.
  - The TSV has `140` `has_phenotype_of` rows, but only `137` unique `(report_id, phenotype CURIE)`
    pairs.
  - Traversing `ClinicalContext -> Finding -> Phenotype` can reconstruct those same `137` unique TSV
    `(report_id, phenotype CURIE)` pairs.

- `[x]` Perinatal / fetal phenotype context
  - JSONL adapter emits `9` perinatal `Finding` nodes through `PerinatalContextFindingEdge`.
  - Each perinatal `Finding` points to a raw phenotype endpoint through `FindingPhenotypeEdge`.
  - These are additional fetal/perinatal findings from JSONL, not part of the TSV
    `has_phenotype_of` parity target.
  - Current unresolved source labels:
    - `Polyhydramnios` (`4`)
    - `Increased fetal nuchal translucency (NT), edema/ cystic hydroga`
    - `Bilateral Hydronephrosis`
    - `Fetal pleural effusion`
    - `Hypertrophic Cardiomyopathy (septal thickness 9 mm, thoracic index 0.5)`
    - `Fetal cardiac anomaly`

- `[x]` Curated phenotype concept nodes
  - TSV concept adapter emits `107` curated `Phenotype` nodes.
  - These include TSV `PhenotypicFeature` and `AdverseEvent` concept nodes.

- `[x]` Drug concepts
  - TSV has `19` unique drug labels / curated drug IDs.
  - JSONL contains drug data in `report.drugs` and `report.treatments[].treatment_drug`.
  - The TSV concept adapter emits `19` curated `Drug` nodes.
  - The JSONL adapter emits `23` source treatment drug mentions through `DrugTreatment -> Drug`.
  - The CURE label resolver normalizes those treatment drug endpoints to the same `19` curated drug
    concepts.

- `[x]` Drug treatment response facts
  - TSV has `63` `applied_to_treat` rows.
  - JSONL contains richer treatment blocks, including regimen, duration, care setting, primary
    target, secondary targets, outcome labels, time to improvement, and free-text notes.
  - The graph now models the CURE-ID treatment-response structure as:
    - `ClinicalContext -> DrugTreatment`
    - `DrugTreatment -> Drug`
    - `DrugTreatment -> TreatmentResponse`
    - `TreatmentResponse -> Finding`
    - `Finding -> Phenotype`
  - JSONL emits `23` `DrugTreatment` nodes.
  - JSONL emits `39` `TreatmentResponse` nodes.
  - After drug and phenotype resolver expansion, this path reconstructs all `40` unique TSV
    drug-to-phenotype `applied_to_treat` concept triples.
  - After drug and condition resolver expansion, each treatment's clinical context plus
    `ClinicalContext -> Condition` reconstructs all `23` TSV drug-to-disease `applied_to_treat`
    concept triples.
  - Regression coverage:
    - `test_rasopathies_treatment_responses_cover_tsv_drug_phenotype_pairs`
    - `test_rasopathies_drug_treatments_cover_tsv_drug_condition_pairs`

- `[x]` Adverse event facts
  - TSV has `7` `has_adverse_events` rows.
  - JSONL contains adverse event data under `report.treatments[].treatment_adverse_events`.
  - JSONL emits `6` source adverse-event labels as `DrugTreatmentAdverseEventEdge` records from
    `DrugTreatment` to raw adverse-event phenotype endpoints.
  - After phenotype resolver expansion, those `6` source adverse-event edges become `7` curated
    drug/adverse-event concept triples.
  - The edge carries:
    - `source_label`
    - `have_adverse_events`
    - `outcomes`
    - `source_adverse_event_index`
  - Regression coverage:
    - `test_rasopathies_adverse_events_cover_tsv_has_adverse_events_triples`

- `[x]` Drug has adverse event JSONL graph view
  - Added `rasopathies_drug_has_adverse_event` to `src/use_cases/cure/cure_rasopathies.yaml`.
  - The view is predicate-specific but not Biolink-specific. It uses local predicate metadata:
    - `id`: `has_adverse_event`
    - `label`: `has adverse event`
  - It traverses:
    - `Drug <- DrugTreatment`
    - `DrugTreatment -> Phenotype` through `DrugTreatmentAdverseEventEdge`
  - It uses `ClinicalContext -> DrugTreatment`, `Patient -> ClinicalContext`, and
    `CaseReport -> Patient` only for aggregate counts and case-report evidence.
  - Each row includes:
    - `drug` with `id`, `xref`, `name`, `url`
    - `phenotype` with `id`, `xref`, `name`
    - `patient_count`
    - `case_report_count`
    - `outcomes`, preserving the adverse-event outcome labels from supporting evidence
    - `evidence[]` objects with adverse-event `source_label`, `have_adverse_events`, `outcomes`,
      selected `DrugTreatment` fields, selected `Patient` fields, and the CURE ID case-report URL.
  - Regression coverage:
    - `test_rasopathies_drug_has_adverse_event_graph_view_shape`

- `[x]` Gene facts
  - TSV has `10` `gene_associated_with_condition` rows and `6` unique genes.
  - JSONL contains genes under `report.gene_sequencing`.
  - JSONL emits `10` `Diagnosis` nodes under clinical_contexts. Each `Diagnosis` preserves
    `how_diagnosis[]` as `diagnosis_methods`.
  - JSONL emits `10` source gene mentions as `Gene` endpoints under `DiagnosisGeneEdge`.
  - JSONL emits `10` `DiagnosisConditionEdge` records.
  - The CURE label resolver normalizes source gene symbols using `Gene.symbol` to the TSV
    `NCBIGene` CURIEs.
  - After gene and condition resolver expansion, the graph reconstructs all `10` TSV
    `gene_associated_with_condition` triples.

- `[x]` Gene associated-with condition JSONL graph view
  - Added `rasopathies_gene_associated_with_condition` to
    `src/use_cases/cure/cure_rasopathies.yaml`.
  - The view is predicate-specific but not Biolink-specific. It uses local predicate metadata:
    - `id`: `gene_associated_with_condition`
    - `label`: `gene associated with condition`
  - It traverses:
    - `Gene <- Diagnosis`
    - `Diagnosis -> Condition`
  - It uses `ClinicalContext -> Diagnosis`, `Patient -> ClinicalContext`, and
    `CaseReport -> Patient` only for aggregate counts and case-report evidence.
  - Each row includes:
    - `gene` with `id`, `xref`, `symbol`
    - `condition` with `id`, `xref`, `name`
    - `patient_count`
    - `case_report_count`
    - `evidence[]` objects with `Diagnosis.diagnosis_methods`, selected `Patient` fields, and the
      CURE ID case-report URL.
  - Regression coverage:
    - `test_rasopathies_gene_associated_with_condition_graph_view_shape`

- `[x]` Variant facts
  - TSV has `10` `has_sequence_variant` rows and `10` `genetically_associated_with` rows.
  - JSONL contains variant strings in `report.nucleotide_change` and `report.protein_change`.
  - JSONL emits `10` case-scoped `GeneVariant` nodes, preserving:
    - `source_gene_symbol`
    - `nucleotide_change`
    - `protein_change`
    - `variant_label`
  - JSONL emits:
    - `10` `DiagnosisGeneVariantEdge` records
    - `10` `GeneGeneVariantEdge` records
  - TSV variant CURIEs are blank for these rows, so first-pass graph modeling preserves source
    variant labels rather than inventing identifiers.
  - After gene and condition resolver expansion, and comparing variants by preserved
    `variant_label`, the graph reconstructs all TSV `has_sequence_variant` and
    `genetically_associated_with` triples.
  - Regression coverage:
    - `test_rasopathies_genetics_cover_tsv_genetic_predicates`

- `[x]` Gene has sequence variant JSONL graph view
  - Added `rasopathies_gene_has_sequence_variant` to `src/use_cases/cure/cure_rasopathies.yaml`.
  - The view is predicate-specific but not Biolink-specific. It uses local predicate metadata:
    - `id`: `has_sequence_variant`
    - `label`: `has sequence variant`
  - It traverses:
    - `Gene -> GeneVariant`
    - `Diagnosis -> GeneVariant` for case-level evidence
  - It uses `ClinicalContext -> Diagnosis`, `Patient -> ClinicalContext`, and
    `CaseReport -> Patient` only for aggregate counts and case-report evidence.
  - Each row includes:
    - `gene` with `id`, `xref`, `symbol`
    - `gene_variant` with `id`, `xref`, `source_gene_symbol`, `nucleotide_change`,
      `protein_change`, and `variant_label`
    - `patient_count`
    - `case_report_count`
    - `evidence[]` objects with `Diagnosis.diagnosis_methods`, selected `Patient` fields, and the
      CURE ID case-report URL.
  - Regression coverage:
    - `test_rasopathies_gene_has_sequence_variant_graph_view_shape`

- `[x]` Sequence variant genetically associated-with condition JSONL graph view
  - Added `rasopathies_sequence_variant_genetically_associated_with_condition` to
    `src/use_cases/cure/cure_rasopathies.yaml`.
  - The view is predicate-specific but not Biolink-specific. It uses local predicate metadata:
    - `id`: `genetically_associated_with`
    - `label`: `genetically associated with`
  - It traverses:
    - `GeneVariant <- Diagnosis`
    - `Diagnosis -> Condition`
  - It uses `ClinicalContext -> Diagnosis`, `Patient -> ClinicalContext`, and
    `CaseReport -> Patient` only for aggregate counts and case-report evidence.
  - Each row includes:
    - `gene_variant` with `id`, `xref`, `source_gene_symbol`, `nucleotide_change`,
      `protein_change`, and `variant_label`
    - `condition` with `id`, `xref`, `name`
    - `patient_count`
    - `case_report_count`
    - `evidence[]` objects with `Diagnosis.diagnosis_methods`, selected `Patient` fields, and the
      CURE ID case-report URL.
  - Regression coverage:
    - `test_rasopathies_sequence_variant_genetically_associated_with_condition_graph_view_shape`

- `[x]` Flat `report.drugs`
  - JSONL has both a flat `report.drugs` list and richer embedded
    `report.treatments[].treatment_drug` objects.
  - For the current rasopathies payload, `report.drugs[]` and
    `report.treatments[].treatment_drug` match exactly by source drug id and name:
    - `23` flat drug entries
    - `23` treatment drug entries
    - `19` unique drug ids in both places
  - The graph treats `report.treatments[]` as the authoritative source because it carries drug
    identity plus regimen, target, outcome, adverse-event, and context fields.

- `[x]` Attachments intentionally excluded
  - JSONL `report.attached_images` contains image attachment metadata only:
    - `id`
    - public image `url`
    - `caption`
  - Current rasopathies payload has `22` image attachments across `8` reports.
  - These are intentionally not modeled as graph nodes or edges.
  - TSV reconstruction does not require attachments.

- `[x]` One-time TSV-like validation query
  - Added a test/query that reconstitutes rows close to the legacy `cureid_data.tsv` shape from graph
    content.
  - It is report-scoped and mention-preserving where the graph has enough detail.
  - This does not need to be a persistent `graph_views` export. It is a parity/QA harness to confirm
    the graph contains everything needed for the Translator-style output CURE ID used to send.
  - The validation reconstructs `237` unique TSV-style association rows from graph traversals.
  - The physical TSV has `240` rows; the difference is the three duplicate disease/phenotype rows
    already documented in the phenotype reconciliation.
  - Expected columns should stay close to:
    - `subject_label_original`
    - `subject_label`
    - `subject_type`
    - `subject_final_label`
    - `subject_final_curie`
    - `subject_missing_final`
    - `predicate_raw`
    - `biolink_predicate`
    - `association_category`
    - `object_label_original`
    - `object_label`
    - `object_type`
    - `object_final_label`
    - `object_final_curie`
    - `object_missing_final`
    - `report_id`
    - `pmid`
    - `link`
    - `outcome`
  - Exact row parity is not required where the graph intentionally collapses duplicate source
    mentions to one resolved concept edge; those differences should be documented in the export
    description or validation notes.
  - Regression coverage:
    - `test_rasopathies_graph_reconstructs_tsv_association_set`

- `[x]` Condition has phenotype JSONL graph view
  - Added `rasopathies_condition_has_phenotype` to `src/use_cases/cure/cure_rasopathies.yaml`.
  - The view is predicate-specific but not Biolink-specific. It uses local predicate metadata:
    - `id`: `has_phenotype`
    - `label`: `has phenotype`
  - It produces one row per resolved `(Condition, Phenotype)` pair.
  - It traverses:
    - `Condition <- ClinicalContext`
    - `ClinicalContext -> Finding`
    - `Finding -> Phenotype`
  - It uses `Patient -> ClinicalContext` and `CaseReport -> Patient` only for aggregate counts and
    case-report evidence.
  - Each row includes:
    - `condition` with `id`, `xref`, `name`
    - `phenotype` with `id`, `xref`, `name`
    - `patient_count`
    - `case_report_count`
    - `evidence[]` objects with `Finding.source_value`, `Finding.source_text`, `Finding.group`,
      selected `Patient` fields, and the CURE ID case-report URL.
  - Regression coverage:
    - `test_rasopathies_condition_has_phenotype_graph_view_shape`

- `[x]` Drug applied-to-treat condition JSONL graph view
  - Added `rasopathies_drug_applied_to_treat_condition` to
    `src/use_cases/cure/cure_rasopathies.yaml`.
  - The view is predicate-specific but not Biolink-specific. It uses local predicate metadata:
    - `id`: `applied_to_treat`
    - `label`: `applied to treat`
  - It traverses only:
    - `Drug <- DrugTreatment`
    - `DrugTreatment <- ClinicalContext`
    - `ClinicalContext -> Condition`
  - It uses `Patient -> ClinicalContext` and `CaseReport -> Patient` only for aggregate counts and
    case-report evidence.
  - Each row includes:
    - `drug` with `id`, `xref`, `name`, `url`
    - `condition` with `id`, `xref`, `name`
    - `patient_count`
    - `case_report_count`
    - `evidence[]` objects with selected `DrugTreatment` fields, selected `Patient` fields, and the
      CURE ID case-report URL.
  - Regression coverage:
    - `test_rasopathies_drug_applied_to_treat_condition_graph_view_shape`

- `[x]` Drug applied-to-treat phenotype JSONL graph view
  - Added `rasopathies_drug_applied_to_treat_phenotype` to
    `src/use_cases/cure/cure_rasopathies.yaml`.
  - The view is predicate-specific but not Biolink-specific. It uses the same local predicate
    metadata as the condition view:
    - `id`: `applied_to_treat`
    - `label`: `applied to treat`
  - It traverses:
    - `Drug <- DrugTreatment`
    - `DrugTreatment -> TreatmentResponse`
    - `TreatmentResponse -> Finding`
    - `Finding -> Phenotype`
  - It includes both primary and secondary treatment-response targets.
  - It uses `ClinicalContext -> DrugTreatment`, `Patient -> ClinicalContext`, and
    `CaseReport -> Patient` only for aggregate counts and case-report evidence.
  - Each row includes:
    - `drug` with `id`, `xref`, `name`, `url`
    - `phenotype` with `id`, `xref`, `name`
    - `patient_count`
    - `case_report_count`
    - `outcomes`, the list of non-empty `TreatmentResponse.outcome` values across the supporting
      evidence, preserving repeated outcome labels when multiple evidence rows report the same
      outcome
    - `evidence[]` objects with selected `DrugTreatment` fields, selected `TreatmentResponse`
      fields, `Finding.source_value`, `Finding.source_text`, selected `Patient` fields, and the
      CURE ID case-report URL.
  - Regression coverage:
    - `test_rasopathies_drug_applied_to_treat_phenotype_graph_view_shape`

## Phenotype Edge Reconciliation Details

The apparent mismatch is explained by resolver expansion and duplicate edge collapse.

Raw local adapter output:

- `125` clinical_context `Finding` nodes and `ClinicalContextFindingEdge` records
- `9` perinatal `Finding` nodes and `PerinatalContextFindingEdge` records
- `134` raw `FindingPhenotypeEdge` records

After applying the CURE label resolver to clinical_context finding phenotype endpoints:

- `141` resolved clinical_context `Finding -> Phenotype` links are produced.
- `4` collapse to duplicate `(ClinicalContext, Phenotype)` pairs when summarized at the report-concept
  level.
- Report-concept exports should collapse these to `137` unique clinical_context phenotype pairs, while
  the graph preserves source-specific `Finding` nodes.

The four duplicate clinical_context pairs are:

- `eea4b243-b0b6-4c80-86ab-87251498a107:clinical_context` -> `HP:0001263`
- `032af03c-721e-47ec-aa6b-eebd100c6b2b:clinical_context` -> `HP:0001382`
- `3d83a833-ced8-4c7d-ad9b-c9863273e0bf:clinical_context` -> `HP:0004927`
- `3d83a833-ced8-4c7d-ad9b-c9863273e0bf:clinical_context` -> `HP:0012418`

The TSV has:

- `140` `has_phenotype_of` rows.
- `137` unique `(report_id, object_final_curie)` pairs.
- `3` duplicate `(report_id, object_final_curie)` extras:
  - `eea4b243-b0b6-4c80-86ab-87251498a107` -> `HP:0001263`
  - `032af03c-721e-47ec-aa6b-eebd100c6b2b` -> `HP:0001382`
  - `3d83a833-ced8-4c7d-ad9b-c9863273e0bf` -> `HP:0012418`

The graph's `ClinicalContext -> Finding -> Phenotype` path exactly matches the TSV's `137` unique
`(report_id, object_final_curie)` pairs when summarized at the report-concept level.

One duplicate resolved graph pair, `3d83a833-ced8-4c7d-ad9b-c9863273e0bf -> HP:0004927`, is not a
duplicate TSV `(report_id, object_final_curie)` row. It arises during resolver expansion from JSONL
source labels. It remains part of the same final set of `137` unique TSV-matching clinical_context
phenotype pairs when the graph is summarized at the clinical_context/phenotype concept level.

## Applied-To-Treat Reconciliation Details

The TSV has `63` `applied_to_treat` rows:

- `40` drug-to-phenotype rows
- `23` drug-to-disease rows

The graph represents these through the CURE-ID case structure rather than as flat association rows.

Drug-to-phenotype reconstruction path:

```text
ClinicalContext -> DrugTreatment -> Drug
DrugTreatment -> TreatmentResponse -> Finding -> Phenotype
```

After `Drug` and `Phenotype` resolver expansion, this path exactly matches all `40` unique TSV
`(report_id, drug CURIE, phenotype CURIE)` triples.

Drug-to-disease reconstruction path:

```text
ClinicalContext -> DrugTreatment -> Drug
ClinicalContext -> Condition
```

After `Drug` and `Condition` resolver expansion, this path exactly matches all `23` unique TSV
`(report_id, drug CURIE, condition CURIE)` triples.

## Adverse Event Reconciliation Details

The TSV has `7` `has_adverse_events` rows. The JSONL has `6` source adverse-event labels; one source
label maps to two curated adverse-event concepts.

The graph represents adverse events as phenotype-like concepts linked directly to the treatment:

```text
ClinicalContext -> DrugTreatment -> Drug
DrugTreatment -> Phenotype
```

The semantic edge is `DrugTreatmentAdverseEventEdge`, which carries source label and outcome
context. After `Drug` and `Phenotype` resolver expansion, this path exactly matches all `7` unique
TSV `(report_id, drug CURIE, adverse-event CURIE, outcome)` tuples.

## Genetics Reconciliation Details

The TSV has three genetics predicates:

- `10` `gene_associated_with_condition` rows
- `10` `has_sequence_variant` rows
- `10` `genetically_associated_with` rows

The graph represents these through diagnosis-scoped genetic evidence:

```text
Patient -> ClinicalContext -> Diagnosis -> Condition
Diagnosis -> Gene
Diagnosis -> GeneVariant
Gene -> GeneVariant
```

`Diagnosis` carries the source `how_diagnosis[]` selections. `Gene` nodes are resolved from source
gene symbols to TSV `NCBIGene` CURIEs. `GeneVariant` nodes are case-scoped because the TSV has no
final variant CURIEs and the JSONL lacks enough transcript context to create canonical HGVS
identifiers safely.

After gene and condition resolver expansion, and comparing variants by preserved `variant_label`,
this path exactly matches all TSV genetics triples.

## Next Implementation Slices

1. Build aggregate query 1: condition to phenotype.
   - Traversal:
     ```text
     Patient -> ClinicalContext -> Condition
     ClinicalContext -> Finding -> Phenotype
     ```
   - Output one row per resolved `(Condition, Phenotype)` pair.
   - Include `patient_count`, `case_report_count`, `patient_ids`, `report_ids`, `finding_ids`, and
     source finding labels.
   - This is the first persistent aggregate view candidate.

2. Build aggregate query 2: drug to treated phenotype.
   - Traversal:
     ```text
     Patient -> ClinicalContext -> DrugTreatment -> Drug
     DrugTreatment -> TreatmentResponse -> Finding -> Phenotype
     ```
   - Output one row per resolved `(Drug, Phenotype)` pair, preserving response outcomes as aggregate
     fields.

3. Build aggregate query 3: drug to treated condition.
   - Traversal:
     ```text
     Patient -> ClinicalContext -> DrugTreatment -> Drug
     ClinicalContext -> Condition
     ```
   - Output one row per resolved `(Drug, Condition)` pair.

4. Build aggregate query 4: drug to adverse-event phenotype.
   - Traversal:
     ```text
     Patient -> ClinicalContext -> DrugTreatment -> Drug
     DrugTreatment -> Phenotype
     ```
   - Output one row per resolved `(Drug, adverse-event Phenotype)` pair, aggregating adverse-event
     outcomes.

5. Build aggregate query 5: gene to condition.
   - Traversal:
     ```text
     Patient -> ClinicalContext -> Diagnosis -> Gene
     Diagnosis -> Condition
     ```
   - Output one row per resolved `(Gene, Condition)` pair.

6. Build aggregate query 6: gene to sequence variant.
   - Traversal:
     ```text
     Patient -> ClinicalContext -> Diagnosis -> Gene
     Diagnosis -> GeneVariant
     Gene -> GeneVariant
     ```
   - Output one row per `(Gene, GeneVariant)` pair. Keep variants source-scoped unless/until CURE ID
     provides canonical variant identifiers.

7. Build aggregate query 7: sequence variant to condition.
   - Traversal:
     ```text
     Patient -> ClinicalContext -> Diagnosis -> GeneVariant
     Diagnosis -> Condition
     ```
   - Output one row per `(GeneVariant, Condition)` pair.

8. Re-run this checklist after each remaining slice.
   - Compare by report-scoped concept pairs where the graph intentionally collapses duplicate rows.
   - Preserve source-level details on treatment/edge payloads where exact TSV row reconstruction would
     otherwise lose information.

9. Add persistent normalized graph view exports after query shape stabilizes.
   - First persistent view: normalized condition-phenotype aggregate export with one row per concept
     pair and distinct patient/case counts.
   - Additional persistent views can follow for treatment, adverse-event, and genetics aggregates if
     the query output proves useful.
   - TSV-like report-scoped reconstruction should remain a one-time validation/test path, not a
     durable graph view.
