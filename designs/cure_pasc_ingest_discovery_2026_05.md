# CURE PASC Design

## Scope

- Source file: `input_files/manual/cure/reports.jsonl`
- Ingest scope: only `form_type == "pasc"`
- Goal: preserve the Long COVID form's mental model in the graph, rather than forcing early normalization into broader house models

## Source Facts

- `reports.jsonl` contains 7,004 reports across CURE form families
- `pasc` contributes 612 reports
- The top-level submission `id` is the stable `CaseReport.id`
- Nested `report.id` is not used as the graph identifier for case reports

## Core Graph

The PASC graph is organized around one case report, one person, one primary Long COVID episode, and optional contextual episodes.

```text
CaseReport
  -> Person
    -> BackgroundContext
    -> Episode (Long COVID, primary)
    -> Episode (Acute COVID, contextual, optional)
    -> Episode (Pregnancy, contextual, optional)
```

## Nodes

### CaseReport

Report-level submission and workflow metadata.

Fields:
- `id`
- `form_type`
- `report_type`
- `status`
- `anonymous`
- `created`
- `updated`
- `percentage_completed`
- `comment_count`
- `outcome_computed`
- `have_adverse_events_old`
- `research_prioritizing`

### Person

Patient demographics and direct patient descriptors only.

Fields:
- `id`
- `sex`
- `gender`
- `gender_same_as_sex`
- `age_group`
- `ethnicity`
- `pregnant`
- `country_treated`
- `race`

### BackgroundContext

Report-time non-episode context. This avoids mixing baseline conditions and background medications into either `Person` or the Long COVID episode.

Fields:
- `immunosuppressant_drugs`

### Episode

Episode is the main clinical organizing node. PASC currently uses:
- one primary Long COVID episode
- an optional acute COVID contextual episode
- an optional pregnancy contextual episode

Common field:
- `role`

Long COVID episode fields:
- `problem_duration`
- `additional_info`
- `drug_additional_details`

Acute COVID episode fields:
- `onset_month`
- `onset_year`
- `care_level`
- `diagnosis_methods`

Pregnancy episode fields:
- `pregnancy_medications`
- `pregnancy_medication_names`
- `treatment_gestational_age`
- `pregnancy_outcome`
- `pregnancy_limited_access_to_treatment`
- `pregnancy_impacted_ability_to_care_for_newborn`

### Condition

Shared condition-like concepts used for:
- primary episode conditions
- prior comorbidities
- post-COVID conditions
- acute complications

This is intentionally source-faithful and not yet ontology-clean.

### Phenotype

Shared symptom/phenotype concepts from `symptoms_severity`.

Fields:
- `id`
- `name`
- `short_name`

### Drug

Shared drug concepts from regimen drugs and other medication-like fields.

Fields:
- `id`
- `name`
- `url`
- `source_id`
- `rxnorm_id`
- `category`
- `fda_approved`

### Exposure

Episode- or context-specific use of one drug.

Used for:
- Long COVID regimen drugs
- acute COVID drugs
- pregnancy medication details
- regular medicines
- immunosuppressant drugs

Important fields include:
- `source_regimen_id`
- `long_drug_name`
- dose / route / frequency / timing
- adverse-event raw fields

### Therapy

Shared non-drug therapy concepts from `alternative_therapies`.

Fields:
- `id`
- `name`
- `slug`

### Treatment

Derived grouping of one or more exposures representing the drug combination assessed for a symptom outcome.

Fields:
- `drug_names`
- `unmatched_drug_names`

### Outcome

Assessment record from `symptoms_outcome`.

Fields:
- `raw_symptom_name`
- `has_unmatched_phenotype`
- `effect`
- `time_to_effect_amount`
- `time_to_effect_units`

### AdverseEvent

Shared adverse-event concept node derived from exposure adverse-event strings.

### VaccinationEvent

Summarized pre-infection vaccination history block for the acute COVID context.

Fields:
- `vaccinated_before_infection`
- `dose_count_before_infection`

### Vaccine

Shared vaccine concept node from `vaccine_received`.

## Edges

### Report / Person / Context

- `CaseReportPersonEdge`: `CaseReport -> Person`
- `PersonBackgroundContextEdge`: `Person -> BackgroundContext`
- `BackgroundContextConditionEdge`: `BackgroundContext -> Condition`
  - `relationship_type = "prior_comorbidity"`
- `BackgroundContextExposureEdge`: `BackgroundContext -> Exposure`
  - `relationship_type = "regular_medicine"`
  - `relationship_type = "immunosuppressant"`

### Episodes

- `PersonEpisodeEdge`: `Person -> Episode`
- `EpisodeEpisodeEdge`: `Episode -> Episode`
  - `relationship_type = "precedes"`
  - `relationship_type = "overlaps"`

Current semantics:
- acute COVID `precedes` Long COVID
- pregnancy `overlaps` Long COVID

### Episode / Condition

- `EpisodeConditionEdge`: `Episode -> Condition`
  - `relationship_type = "primary"`
  - `relationship_type = "complication"`
  - `relationship_type = "comorbidity"`

Current uses:
- Long COVID episode -> Long COVID condition (`primary`)
- acute COVID episode -> Acute COVID-19 (`primary`)
- pregnancy episode -> Pregnancy (`primary`)
- acute COVID episode -> complication conditions (`complication`)
- Long COVID episode -> post-COVID conditions (`comorbidity`)

### Episode / Phenotype / Therapy / Exposure

- `EpisodePhenotypeEdge`: `Episode -> Phenotype`
  - carries `severity`
- `EpisodeTherapyEdge`: `Episode -> Therapy`
- `EpisodeExposureEdge`: `Episode -> Exposure`
- `ExposureDrugEdge`: `Exposure -> Drug`

### Treatment / Outcome

- `ExposureAdverseEventEdge`: `Exposure -> AdverseEvent`
  - carries adverse-event outcomes
- `Exposure -> Treatment` via `TreatmentExposureEdge`
- `EpisodeOutcomeEdge`: `Episode -> Outcome`
- `TreatmentOutcomeEdge`: `Treatment -> Outcome`
- `OutcomePhenotypeEdge`: `Outcome -> Phenotype`

This reflects the PASC symptom-outcome structure:
- exposures are concrete drug uses
- treatment is the assessed drug combination
- outcome is the effect assessment
- phenotype is the symptom being assessed

### Vaccination

- `Episode -> VaccinationEvent` via `VaccinationEventEpisodeEdge`
  - carries `relative_time_value`
  - carries `relative_time_unit`
- `VaccinationEvent -> Vaccine` via `VaccinationEventVaccineEdge`

This preserves the form structure as one summarized vaccination-history block rather than inventing one event per dose.

## Field Placement Decisions

### Long COVID

These live on the primary Long COVID episode or its direct neighbors:
- `symptoms_duration` -> `Episode.problem_duration`
- `symptoms_severity` -> `Episode -> Phenotype` with severity on edge
- `symptoms_outcome` -> `Outcome` plus links to `Treatment` and `Phenotype`
- `alternative_therapies` -> `Episode -> Therapy`
- `drug_additional_details` -> `Episode.drug_additional_details`
- `comorbidities_after_pasc` -> `Episode -> Condition` with `relationship_type = "comorbidity"`

### Acute COVID

These live on the acute contextual episode:
- symptom onset month/year
- diagnosis methods
- care level
- `drugs_acute_covid` as episode exposures
- `complications_acute_covid` as episode-linked complication conditions
- vaccination history as `VaccinationEvent`

### Pregnancy

Pregnancy is modeled as its own contextual episode only when the source says the patient was pregnant during Long COVID.

### Background Context

These live on `BackgroundContext`:
- prior comorbidities
- regular medicines
- immunosuppressants

## Source-Faithfulness Rules

- Preserve raw source text where parsing would be lossy
- Do not force fuzzy matching to hide source inconsistencies
- Keep unmatched treatment drug names explicit on `Treatment`
- Keep unmatched outcome symptom links explicit on `Outcome`
- Do not infer detailed timing intervals that the form does not capture

## Deliberate Omissions

- `attached_images` is not modeled
- No attempt is made yet to normalize `Condition`, `Phenotype`, `Therapy`, or `Drug` to external ontologies from this source alone
- No additional rescue matching is performed beyond simple conservative normalization

## QA Browser Shape

The `cure/CaseReport` page is custom and mirrors the form structure:

- Case Report
  - Details
  - Patient
  - Background Context
- Episodes
  - Long COVID
  - Acute COVID
  - Pregnancy
- Episode relationship strip
  - `precedes`
  - `overlaps`

Long COVID shows:
- details
- post-COVID conditions
- phenotypes
- exposures
- therapies
- outcomes

Acute COVID shows:
- details
- vaccination history
- complications
- exposures

Pregnancy shows:
- details
- exposures when present

## Remaining Work

The meaningful PASC form content is now modeled. The remaining non-modeled source material is limited to upload artifacts rather than core clinical structure.
