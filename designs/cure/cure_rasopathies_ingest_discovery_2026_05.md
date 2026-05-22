# CURE Rasopathies Discovery

## Scope

- Initial discovery source file: `input_files/manual/cure/reports.jsonl`
- Ingest scope: only `form_type == "rasopathies"`
- Goal for first pass: preserve the compact rasopathies case-report structure without forcing it into the PASC episode model

## Source Facts

- The initial `reports.jsonl` discovery file contained `11` rasopathies reports
- All `11` reports are `Approved`
- Report types:
  - `clinician`: `10`
  - `patient`: `1`
- `form_subtype` is blank on all 11 records
- `subtype_ras` is null on all 11 records

## Top-Level Shape

The rasopathies form uses a compact report structure, not the PASC long-COVID episode structure.

Common populated report fields:
- `disease`
- `patient`
- `findings`
- `drugs`
- `treatments`
- `how_diagnosis`
- `gene_sequencing`
- `nucleotide_change`
- `protein_change`
- `premature_birth`
- `fetal_findings`
- `fetal_findings_details`

`extra_fields` currently only contains `previously_approved`.

## Conceptual Model Behind The Form

The rasopathies form is not primarily asking for a disease history. It is asking for a
structured treatment experience for a patient with a rare genetic diagnosis.

The form's narrative center appears to be:

1. identify the disease
2. characterize the patient at a high level
3. enumerate the patient's clinically relevant findings
4. record how the diagnosis was established, especially genetic evidence
5. describe which drug(s) were used
6. evaluate each drug against one or more target findings
7. preserve follow-up detail, response timing, and supporting attachments

That means the key conceptual entities in the form are closer to:

- `Case report`
- `Reporter`
- `Patient`
- `Disease / syndrome identity`
- `Clinical finding`
- `Diagnostic evidence`
- `Gene / variant evidence`
- `Treatment experience`
- `Drug response by target finding`
- `Supporting attachment`

This is materially different from PASC. PASC is organized around illness phase, symptom
timeline, prior conditions, acute episode, vaccination, and follow-up context. Rasopathies is
organized around a diagnosed syndrome plus phenotype findings, then asks whether a drug changed
those findings.

## What The Final Review Says Matters

The `final-review-*` accordion in the backend form spec is especially revealing because it shows
what the form considers the canonical summary of a case. Its sections are:

- Case Characteristics
- Clinical ClinicalContext
- Fetal findings
- Diagnosis
- Medications
- Additional Details

That summary view implies the form designer thinks a complete rasopathies case report is made of:

- who the patient is in broad demographic terms
- what syndrome they have
- what findings they present with
- whether prenatal or fetal findings matter
- what evidence supports the diagnosis
- what drugs were tried
- what target finding each drug was intended to address
- how the patient responded, including secondary effects and time to response
- what extra documentary evidence should travel with the report

## Treatment Logic Encoded In The Form

The medications dialog is the strongest clue to the form's implicit ontology.

It does not just ask "what drug was given?" It asks for:

- drug identity
- regimen and duration
- whether treatment is ongoing
- adverse events
- treatment setting / severity context
- primary treatment target
- primary response
- markers of improvement
- secondary targets and responses
- time to response
- free-text experiential notes

So the treatment concept here is not a simple exposure. It is a mini case study:

- `drug`
- applied to a `target finding`
- with an `observed outcome`
- with optional `secondary target outcomes`
- under a concrete `regimen`
- plus free-text interpretation

One important implication follows from that structure:

The form treats findings as both phenotype descriptors and treatment-response targets. In other
words, findings are not only "things the patient has"; they are also the things a drug is trying
to change.

## Coverage

Report field coverage across 11 rasopathies records:

- `disease`, `patient`, `findings`, `drugs`, `treatments`: `11/11`
- `how_diagnosis`: `10/11`
- `gene_sequencing`: `10/11`
- `premature_birth`: `10/11`
- `fetal_findings`: `10/11`
- `protein_change`: `10/11`
- `nucleotide_change`: `9/11`
- `attached_images`: `8/11`
- `fetal_findings_details`: `5/11`

Patient field coverage:

- `age_group`: `10/11`
- `sex`: `10/11`
- `country_treated`: `10/11`
- `ethnicity`: `4/11`
- `race`: `4/11`

## Observed Value Shapes

### Disease

`disease` is a structured object, not a plain string.

Observed diseases:
- `CFC: Cardiofaciocutaneous Syndrome` (`4`)
- `Noonan Syndrome` (`4`)
- `NSML: Noonan Syndrome with Multiple Lentigines (Formerly LEOPARD)` (`1`)
- `Costello Syndrome` (`1`)
- `SYNGAP1-Related Developmental and Epileptic Encephalopathy` (`1`)

Important note:
- The presence of `SYNGAP1-Related Developmental and Epileptic Encephalopathy` suggests the form family may not be strictly limited to classical rasopathy ontology boundaries. The first-pass ingest should preserve source disease identity rather than over-normalize the family.

### Diagnosis

`how_diagnosis` is a list of selected values, not a scalar.
The graph models this as a clinical_context-scoped `Diagnosis` node so diagnosis methods can stay with
the patient-specific diagnostic event while `Gene`, `GeneVariant`, and `Condition` remain separate
endpoints.

Observed combinations:
- `Gene sequencing | Imaging` (`5`)
- `Doctor suspected in utero | Gene sequencing | Imaging` (`3`)
- `Doctor suspected in utero | Gene sequencing` (`1`)
- one record includes a free-text dysmorphology note alongside the selected diagnosis methods

### Gene / Variant Fields

`gene_sequencing` is a list of gene symbols.

Observed genes:
- `BRAF` (`4`)
- `RAF1` (`2`)
- `RIT1` (`1`)
- `PTPN11` (`1`)
- `SOS1` (`1`)
- `HRAS` (`1`)

Observed variant fields:
- `nucleotide_change` values such as `c.1406G>A`, `c.770C>T`, `c.35G>A`
- `protein_change` values such as `p.Gly469Glu`, `p.D638E`, `p.S257L`
- some records explicitly say `Not reported`

`gene_sequencing_rgd` was not populated in the profiled rows.

### Findings

`findings` is a list of structured values. Records contain between `2` and `12` findings.

Observed content includes:
- canonical checklist-like findings such as `Failure to thrive`, `Short stature`, `Seizures`
- free-text or `Other` entries with semicolon-delimited clinical descriptions
- mixed phenotype and diagnosis-style phrases in the same list

This means findings should likely be preserved source-faithfully on the first pass rather than aggressively parsed into a cleaned phenotype ontology model.

### Treatments / Drugs

`drugs` is a flat list of drug objects.

`treatments` is a richer list of structured treatment records. Records contain:
- `1` treatment in `7` reports
- `2` treatments in `2` reports
- `6` treatments in `2` reports

Each treatment currently appears to include:
- a primary `treatment_drug` object
- initial / current regimen fields
- duration fields
- adverse-event substructure
- primary target and outcome fields
- optional secondary target outcomes
- time-to-improvement
- free-text additional treatment notes

The `drugs` list appears redundant with the drug identities already embedded in `treatments`.

## Initial Modeling Direction

Likely first-pass graph shape:

- `CaseReport`
- `Person`
- `Condition` or source-specific `Disease`
- `Finding` / `Phenotype`-like nodes, preserving source text
- `Gene`
- source-faithful variant attributes kept on the report or a small variant node
- `Treatment`
- `Drug`

Likely avoid in first pass:

- reusing the PASC episode model
- inventing acute/chronic contextual episodes
- heavy ontology cleanup of free-text findings
- deduplicating semantically similar findings across records by aggressive normalization

## Open Questions

- Should `disease` be emitted as a shared `Condition` node or as a small source-specific disease node model?
- Should gene + nucleotide/protein change be represented as a variant node, or kept directly on the case report in v1?
- Should `findings` become shared phenotype nodes, or remain case-scoped finding nodes to avoid over-merging free text?
- Is the flat `drugs` list needed at all if `treatments` already carries richer drug context?

## Proposed First-Pass Plan

1. Build a rasopathies-specific adapter instead of branching the current PASC adapter.
2. Reuse only the clearly generic concepts (`CaseReport`, `Person`, `Drug`) if they still fit.
3. Add a minimal rasopathies model centered on case report, disease, findings, genes/variant fields, and treatments.
4. Keep free-text findings and variant strings source-faithful in v1.

## TSV-Backed Curated Concepts

For rasopathies, the curated file `input_files/manual/cure/cureid_data.tsv` is now treated as the
source of canonical concept nodes.

Current scope:

- emit shared `Condition` nodes from curated disease CURIE/label pairs
- emit shared `Phenotype` nodes from curated phenotype/adverse-event CURIE/label pairs
- use the JSONL adapter only for report-scoped structure and edge details

Implementation consequence:

- the TSV adapter owns canonical concept node names
- the rasopathies JSONL adapter no longer owns `Condition` or `Phenotype` node records
- the rasopathies JSONL adapter emits raw source labels as concept endpoint ids
- the label-to-CURIE resolver normalizes those endpoint ids onto the canonical TSV-backed nodes
- the JSONL adapter emits `ClinicalContext -> Finding -> Phenotype` and
  `PerinatalContext -> Finding -> Phenotype` paths so patient-specific source findings remain
  distinct from harmonized phenotype concepts
- the JSONL adapter emits `ClinicalContext -> DrugTreatment -> Drug` plus
  `DrugTreatment -> TreatmentResponse -> Finding` so drug use, regimen context, and finding-specific
  responses remain case-scoped
- the JSONL adapter emits `DrugTreatment -> Phenotype` through `DrugTreatmentAdverseEventEdge` for
  treatment adverse events
- the JSONL adapter emits `ClinicalContext -> Diagnosis` for diagnosis methods and links that
  diagnosis to `Condition`, `Gene`, and case-scoped `GeneVariant` endpoints
- the JSONL adapter emits `ClinicalContext -> Condition` edges for report-level disease identity
- the JSONL adapter anchors clinical_context through `CaseReport -> Patient -> ClinicalContext`, using
  `PatientClinicalContextEdge`, because the clinical clinical_context belongs to the patient rather than to
  the report container

This prevents case-local source labels from repeatedly overwriting the names of shared
CURIE-backed concept nodes.
5. Validate on all 11 reports before expanding scope.

## Source Refresh 2026-05-22

The active raw report source is now
`input_files/manual/cure/reports_20260518T211409Z.jsonl`.

Refresh profile:

- `13` rasopathies rows, all with top-level `status = Approved`
- `616` PASC rows, all with top-level `status = Approved`
- filename timestamp `20260518T211409Z` is used by the CURE adapters as
  `version_date = 2026-05-18`
- both CURE adapters now filter to `status == Approved` before emitting graph content

The refreshed rasopathies JSONL contains two approved reports that are not represented in the
older curated TSV:

- `1301dd3d-9cc2-4a40-a5b7-bd0cc0083968`
- `16397f81-5bc3-47e3-8135-82e11ca3ad44`

Both are SYNGAP1-related reports. The disease label still resolves through the curated TSV, but
the refreshed rows introduce source labels that the older TSV-backed resolver does not normalize
yet:

- drugs: `Tanganil`, `Sertraline 25 Mg Oral Tablet [Zoloft]`, `Epidiolex`, `Clobazam`
- finding phenotype: `Anxiety`

The short-lived `CureIdLabelResolver` supports a YAML manual label overlay for these refresh gaps.
For this source version, `Syngap` and `SYNGAP1` are manually mapped to `NCBIGene:8831`.

Until CURE ID publishes refreshed normalized IDs for these concepts, the graph keeps those values
without temporary manual mappings as source-label nodes. TSV reconstruction tests therefore treat
`cureid_data.tsv` as a legacy subset that must be covered by the refreshed JSONL graph, rather than
as an exact equality target.

The rasopathies YAML also includes a `rasopathies_translator_version_info` graph view. It emits one
JSONL metadata record for the Translator export scope, including report-source and curated-concept
source versions, approved case ids, and the association view ids included in the export package.

## Populated JSONL Field Inventory

This section is the initial raw "must-account-for" checklist from the `rasopathies` rows in
`input_files/manual/cure/reports.jsonl`. Only paths with at least one non-empty value are listed.

Counts below are record-level or item-level counts observed during recursive profiling of the 11
rasopathies reports.

### Top-level submission fields

- `id` (`11/11`)
- `form_type` (`11/11`)
- `report_type` (`11/11`)
- `anonymous` (`11/11`)
- `created` (`11/11`)
- `updated` (`11/11`)
- `status` (`11/11`)
- `when_reminder` (`11/11`)
- `when_reminder[].value` (`11/11`)
- `report` (`11/11`)

### Report-level metadata

- `report.id` (`11/11`)
- `report.created` (`11/11`)
- `report.updated` (`11/11`)
- `report.status` (`11/11`)
- `report.report_type` (`11/11`)
- `report.anonymous` (`11/11`)
- `report.flagged` (`11/11`)
- `report.reminder` (`11/11`)
- `report.comment_count` (`11/11`)
- `report.comment_latest` (`11/11`)
- `report.extra_fields` (`11/11`)
- `report.extra_fields.previously_approved` (`11/11`)

### Author block

- `report.author` (`11/11`)
- `report.author.id` (`11/11`)
- `report.author.first_name` (`10/11`)
- `report.author.last_name` (`10/11`)
- `report.author.qualification` (`11/11`)
- `report.author.is_staff` (`11/11`)
- `report.author.is_superuser` (`11/11`)

### Disease block

- `report.disease` (`11/11`)
- `report.disease.id` (`11/11`)
- `report.disease.name` (`11/11`)
- `report.disease.url_name` (`11/11`)
- `report.disease.image_url` (`11/11`)

### Patient block

- `report.patient` (`11/11`)
- `report.patient.age_group` (`10/11`)
- `report.patient.sex` (`10/11`)
- `report.patient.country_treated` (`10/11`)
- `report.patient.ethnicity` (`4/11`)
- `report.patient.race` (`4/11`)
- `report.patient.race[].value` (`4 item occurrences`)

### ClinicalContext / findings

- `report.findings` (`11/11`)
- `report.findings[]` (`66 item occurrences`)
- `report.findings[].value` (`66 item occurrences`)
- `report.findings[].group` (`21 item occurrences`)
- `report.findings[].label` (`21 item occurrences`)
- `report.findings[].selected` (`21 item occurrences`)
- `report.findings[].text` (`21 item occurrences`)
- `report.findings[].default` (`7 item occurrences`)

### Diagnosis / genetics

- `report.how_diagnosis` (`10/11`)
- `report.how_diagnosis[]` (`25 item occurrences`)
- `report.how_diagnosis[].value` (`25 item occurrences`)
- `report.gene_sequencing` (`10/11`)
- `report.gene_sequencing[]` (`10 item occurrences`)
- `report.nucleotide_change` (`9/11`)
- `report.protein_change` (`10/11`)

### Pregnancy / fetal context

- `report.premature_birth` (`10/11`)
- `report.fetal_findings` (`10/11`)
- `report.fetal_findings_details` (`5/11`)
- `report.fetal_findings_details[]` (`9 item occurrences`)
- `report.fetal_findings_details[].value` (`9 item occurrences`)

### Flat drug list

- `report.drugs` (`11/11`)
- `report.drugs[]` (`23 item occurrences`)
- `report.drugs[].id` (`23 item occurrences`)
- `report.drugs[].name` (`23 item occurrences`)
- `report.drugs[].url` (`23 item occurrences`)

### Treatment block

- `report.treatments` (`11/11`)
- `report.treatments[]` (`23 item occurrences`)

#### Embedded treatment drug

- `report.treatments[].treatment_drug` (`23 item occurrences`)
- `report.treatments[].treatment_drug.id` (`23 item occurrences`)
- `report.treatments[].treatment_drug.name` (`23 item occurrences`)
- `report.treatments[].treatment_drug.url` (`23 item occurrences`)

#### Initial regimen

- `report.treatments[].treatment_initial_regimen` (`23 item occurrences`)
- `report.treatments[].treatment_initial_regimen.dose_amount` (`15 item occurrences`)
- `report.treatments[].treatment_initial_regimen.unit_of_measurement` (`15 item occurrences`)
- `report.treatments[].treatment_initial_regimen.frequency` (`11 item occurrences`)
- `report.treatments[].treatment_initial_regimen.route` (`11 item occurrences`)

#### Current regimen

- `report.treatments[].treatment_regimen` (`23 item occurrences`)
- `report.treatments[].treatment_regimen.dose_amount` (`15 item occurrences`)
- `report.treatments[].treatment_regimen.unit_of_measurement` (`15 item occurrences`)
- `report.treatments[].treatment_regimen.frequency` (`11 item occurrences`)
- `report.treatments[].treatment_regimen.route` (`11 item occurrences`)
- `report.treatments[].treatment_regimen.dose_change` (`17 item occurrences`)

#### Duration / timing

- `report.treatments[].treatment_duration` (`23 item occurrences`)
- `report.treatments[].treatment_duration.duration_amount` (`12 item occurrences`)
- `report.treatments[].treatment_duration.unit_of_measurement_duration` (`12 item occurrences`)
- `report.treatments[].treatment_duration.treatment_begin` (`3 item occurrences`)
- `report.treatments[].treatment_duration.treatment_begin_month` (`3 item occurrences`)
- `report.treatments[].treatment_duration.treatment_end` (`1 item occurrence`)
- `report.treatments[].treatment_duration.treatment_end_month` (`1 item occurrence`)
- `report.treatments[].treatment_duration.treatment_on_going` (`7 item occurrences`)
- `report.treatments[].treatment_time` (`23 item occurrences`)
- `report.treatments[].treatment_time.time_to_improvement` (`22 item occurrences`)

#### Primary target / primary outcome

- `report.treatments[].treatment_primary_target` (`23 item occurrences`)
- `report.treatments[].treatment_primary_target.primary_drug_target` (`21 item occurrences`)
- `report.treatments[].treatment_primary_target.outcome_primary_target` (`21 item occurrences`)
- `report.treatments[].treatment_primary_target.outcome_primary_target_details` (`12 item occurrences`)

#### Secondary targets / outcomes

- `report.treatments[].secondary_primary_target` (`23 item occurrences`)
- `report.treatments[].secondary_primary_target.has_secondary_drug_target` (`22 item occurrences`)
- `report.treatments[].secondary_primary_target.secondary_drug_target` (`13 item occurrences`)
- `report.treatments[].secondary_primary_target.secondary_drug_target[]` (`18 item occurrences`)
- `report.treatments[].secondary_primary_target.secondary_drug_target[].target` (`18 item occurrences`)
- `report.treatments[].secondary_primary_target.secondary_drug_target[].outcome` (`18 item occurrences`)
- `report.treatments[].secondary_primary_target.secondary_drug_target[].outcome_details` (`9 item occurrences`)

#### Adverse events

- `report.treatments[].treatment_adverse_events` (`23 item occurrences`)
- `report.treatments[].treatment_adverse_events.have_adverse_events` (`21 item occurrences`)
- `report.treatments[].treatment_adverse_events.adverse_events_generic` (`5 item occurrences`)
- `report.treatments[].treatment_adverse_events.adverse_events_generic[]` (`6 item occurrences`)
- `report.treatments[].treatment_adverse_events.adverse_events_generic[].value` (`6 item occurrences`)
- `report.treatments[].treatment_adverse_events.adverse_events_outcome` (`5 item occurrences`)
- `report.treatments[].treatment_adverse_events.adverse_events_outcome[]` (`6 item occurrences`)
- `report.treatments[].treatment_adverse_events.adverse_events_outcome[].value` (`6 item occurrences`)

#### Care setting / additional treatment notes

- `report.treatments[].treatment_additional_details` (`23 item occurrences`)
- `report.treatments[].treatment_additional_details.severity` (`21 item occurrences`)
- `report.treatments[].treatment_additional_details.severity[]` (`29 item occurrences`)
- `report.treatments[].treatment_additional_details.severity[].value` (`29 item occurrences`)
- `report.treatments[].treatment_additional_info` (`23 item occurrences`)
- `report.treatments[].treatment_additional_info.additional_drug_info_non_ID` (`19 item occurrences`)

### Uploaded artifacts

- `report.attached_images` (`8/11`)
- `report.attached_images[]` (`22 item occurrences`)
- `report.attached_images[].id` (`22 item occurrences`)
- `report.attached_images[].url` (`22 item occurrences`)
- `report.attached_images[].caption` (`22 item occurrences`)

## Notes For Later Mapping

- `report.drugs` and `report.treatments[].treatment_drug` appear to overlap strongly, so downstream
  accounting should explicitly decide whether both are independently preserved or one is derived from
  the other.
- `findings`, `how_diagnosis`, `fetal_findings_details`, and many treatment substructures are stored
  as lists of value objects rather than plain strings.
- Some clinically important detail only appears in free-text subfields such as:
  - `report.findings[].text`
  - `report.treatments[].treatment_primary_target.outcome_primary_target_details`
  - `report.treatments[].secondary_primary_target.secondary_drug_target[].outcome_details`
  - `report.treatments[].treatment_additional_info.additional_drug_info_non_ID`
