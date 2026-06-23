# CURE Form To Graph Playbook

## Goal

Provide a repeatable workflow for turning a CURE ID form family into a graph model, validation notes,
QA views, and, when they exist, downstream export documentation.

Use this when adding or revising mappings from CURE ID forms such as Long COVID/PASC, rasopathies,
IACC, rare genetic disorders, or future form families into IFX_ODIN graph nodes, edges, and
`graph_views`.

This playbook complements `playbooks/ingest_playbook.md`. If a new raw source or adapter is being
added, follow the ingest playbook first. Use this playbook for the CURE-specific form semantics:
questions, final review sections, serializer behavior, graph model decisions, QA views, and export
views.

## Core Principle

Model the form's mental model before modeling a downstream export.

CURE forms often encode the intended clinical structure in:

- the page/menu sequence
- question labels and help text
- repeated form groups
- final-review accordions
- serializer and migration cleanup logic
- public filter definitions
- real approved report payloads

The graph should preserve those source semantics first. QA views should then validate that model.
Translator or other downstream exports should only be added when there is an explicit export target.

The form-to-graph page is not a conceptual sketch. Treat it as a lightweight audit surface for the
adapter:

- every displayed payload path should come from the backend form JSON or a real payload
- every displayed graph node/edge field should be verified against the adapter assignments and model
  dataclasses
- every displayed graph edge should come from the QA browser schema for the target graph
- every selected-state label should describe data actually set by that form answer, not merely the
  collection or edge class name

## Expected Inputs

Collect these before changing adapter or graph-view code:

- Backend form JSON:
  - `../project-cure-backend/server/apps/ui_forms/json_data/<form>.json`
  - `../project-cure-backend/server/apps/ui_forms/json_data/<form>-menu.json`
  - `../project-cure-backend/server/apps/ui_forms/json_data/<form>-summary.json`, when present
- Backend routing and form logic:
  - `../project-cure-backend/server/apps/ui_forms/cure_report_logic.py`
  - serializer behavior in `../project-cure-backend/server/apps/api/v2/serializers/report.py`
  - relevant data migrations under `../project-cure-backend/server/apps/core/migrations/`
- Public filtering and browse semantics:
  - `../project-cure-backend/server/apps/api/filter_json/cure_filter_<form>.json`
  - backend viewset filter methods that interpret those fields
- Real approved report payloads:
  - current JSONL source from the registry or local backend reports archive
  - report count by `form_type`, `report_type`, and `status`
  - representative patient-submitted and clinician-submitted examples
- Any curated concept or association files, if they exist for the form:
  - TSV/XLSX concept mappings
  - manual resolver overlays
  - prior Translator-style export files
- Existing IFX_ODIN artifacts:
  - current adapter, models, YAML, and tests for the form family
  - source-specific design docs under `designs/cure/`
  - rasopathies pages under `designs/cure/` as the current visual template

## Discovery Checklist

1. **Identify the form family**
   - Confirm the CURE backend form name, usually `pasc`, `rasopathies`, `iacc-generic`, or another
     JSON file under `ui_forms/json_data`.
   - Confirm report routing rules in `cure_report_logic.py`.
   - Confirm emitted payload selectors: `form_type`, `report_type`, `status`, and disease/category
     conditions.

2. **Read the menu as workflow**
   - List patient and clinician page sequences separately.
   - Mark required pages and optional pages.
   - Note hidden or conditional pages such as pregnancy details.
   - Treat the page sequence as evidence for major graph branches.

3. **Read final review as canonical summary**
   - Extract final-review accordion sections.
   - Record the exact source paths shown to the user, such as
     `report.extra_fields.symptoms_severity`.
   - Use these sections as the first candidate graph modules.

4. **Inventory form fields**
   - Parse the form JSON for `key`, `label`, `description`, `controlType`, `options`,
     conditional display rules, and repeated groups.
   - Normalize backend keys such as `report__extra_fields__symptoms_outcome` to payload paths such
     as `report.extra_fields.symptoms_outcome`.
   - Preserve question labels because they explain source semantics better than field names alone.

5. **Inspect real payloads**
   - Profile approved reports only unless rejected/draft behavior is part of the task.
   - Count populated fields and list shapes.
   - Compare patient and clinician report shapes.
   - Check whether option labels in the form still match payload values after serializer cleanup and
     migrations.

6. **Review serializer and migration behavior**
   - Find code that initializes, renames, canonicalizes, clears, or backfills fields.
   - Check migrations that changed option labels or nested structures.
   - Treat this as source behavior, not implementation noise.

7. **Separate form structure from resolver work**
   - Adapters should emit source-provided identifiers and source labels.
   - Cross-identifier reconciliation belongs in CURE resolvers or resolver configuration.
   - If a graph view needs canonical IDs, verify whether those IDs come from resolver output rather
     than adapter-invented xrefs.

8. **Draft the graph model**
   - Start with the form's repeated clinical objects.
   - Identify stable anchors: `CaseReport`, `Patient`, episodes/clinical contexts, condition,
     treatment/exposure, symptom/phenotype, outcome, adverse event, pregnancy/context.
   - Decide which answers are node fields, which become nodes, and which are edge attributes.
   - Keep the first pass minimal and source-faithful.

9. **Create or update the source design doc**
   - Use `designs/cure/<form>_graph_mapping_<date>.md` or update the existing design doc.
   - Include source files, field inventory, graph modules, mapping decisions, unresolved questions,
     and validation commands.

10. **Develop the visual mapping page**
   - Create or update a page under `designs/cure/`.
   - For a new form, start with a "CURE ID Forms -> <Form> Graph" page showing:
     - form pages/questions on the left
     - the real QA browser graph schema on the right
     - selected field mappings and open questions
   - Match the schema shown by the QA browser route for the graph, for example
     `/db/cure_pasc/schema`, rather than drawing a simplified conceptual schema.
   - List all form questions from the backend form JSON and show where each answer lands:
     - source payload path
     - target graph collection and field
     - edge path that anchors the value in the graph
   - Use the rasopathies pages as the template:
     - `cure_graph_views.html` for the cross-form index page
     - `rasopathies_graph_story.html` for form-to-graph mapping
     - `rasopathies_translator_exports_graph.html` only for forms that have Translator/export paths
   - Use shared page styles from `designs/cure/cure_graph_styles.css` when possible.
   - Link every detail page back to `cure_graph_views.html`.
   - Do not add QA Browser tiles to the cross-form index unless the user explicitly asks for them.

11. **Verify page mappings against adapter code**
   - For each question on the page, identify the exact adapter method that reads the payload path.
   - Confirm whether the answer becomes:
     - a node field
     - a new node
     - an edge field
     - a connector edge with no fields
     - a source payload field that the adapter does not currently emit
   - Check the model dataclass for every displayed `Node.field` and `Edge.field`.
   - Fix the page when it shows a field that only exists conceptually but is not assigned by the
     adapter.
   - Mark non-emitted fields explicitly, for example `Not emitted by CUREAdapter`, rather than
     highlighting a nearby graph node.
   - Remember that a source answer can be copied to more than one graph location. For example, in the
     PASC adapter, `report.regimens[].adverse_events_outcome` is stored on
     `Exposure.adverse_event_outcomes` and copied to `ExposureAdverseEventEdge.outcomes`.

12. **Match the selected-state interaction pattern**
   - Left rail:
     - group questions by backend form section
     - show the exact or lightly cleaned user-facing question text
     - show the backend key or payload field as the small field chip
     - show concise form context such as input type or option list
     - use badges only for meaningful differences such as "clinician form only"
   - Right graph:
     - render the real QA Browser graph schema using Mermaid
     - use top-down orientation unless the established page for that graph uses a different direction
     - highlight selected nodes with the shared orange style
     - highlight selected edges with the shared orange stroke
     - show edge labels only when the selected question sets data on that edge
     - show node field labels only for selected nodes receiving data
     - keep connector-only edges unlabeled
   - Navigation:
     - update `window.location.hash` on selection
     - restore the selected question from the hash on page load
     - scroll the left rail so the selected question remains visible
     - render Mermaid into a host element with an error fallback
   - Mapping panel:
     - show source payload paths
     - show graph node/edge paths
     - show all graph targets, including edge targets when an edge field is set
     - use the panel to preserve detail that would make the graph itself too noisy

13. **Define graph views only after graph paths stabilize**
   - Start with QA views that expose unmatched or ambiguous mappings.
   - Promote persistent exports only after traversal paths are validated against real graph contents
     and an export target is defined.
   - Keep graph views in YAML and let `ArangoOutputAdapter` persist them into
     `metadata_store/graph_views`.

14. **Validate with graph facts, not only form facts**
   - Confirm counts from the raw payload land in expected collections.
   - Trace representative report IDs through the graph.
   - Validate dangling edge cleanup does not remove expected relationships.
   - Compare view output against any historical TSV or public UI expectations as a parity target,
     not as the source of truth.

## Page Verification Checklist

Before calling a form-to-graph page done, run through this checklist:

1. **Question coverage**
   - All relevant backend form questions are represented.
   - Patient and clinician forms have been compared.
   - Differences are shown as small per-question badges, not as a global toggle unless the workflows
     meaningfully diverge.

2. **Payload paths**
   - Payload paths match actual JSON keys after serializer/migration cleanup.
   - Repeated groups use array notation such as `report.regimens[]`.
   - Alternate shapes accepted by the adapter are listed when relevant, for example
     `report.patient.race[]` and `report.patient.races[].value`.

3. **Adapter-backed graph paths**
   - Every graph field shown on the page can be traced to a constructor assignment in the adapter.
   - Every graph field exists on the corresponding model dataclass.
   - Edge attributes are shown only when the adapter sets fields on that edge.
   - Connector edges are highlighted as path context but not labeled.
   - Conditional graph creation is described explicitly, for example "Pregnancy Episode created only
     when value is Yes."

4. **QA schema fidelity**
   - The graph drawing uses the same node/edge classes as the QA Browser schema route.
   - The page does not invent conceptual nodes that are absent from the schema.
   - If a conceptual label is helpful, keep it in the mapping text, not as a fake graph node.

5. **Interaction parity**
   - Hash selection works.
   - Mermaid render fallback works.
   - Left-rail selection scroll works.
   - Selected nodes and edge colors match the rasopathies/PASC shared style.
   - Text fits on desktop and mobile layouts.

6. **Local validation**
   - Parse the inline script.
   - Run `git diff --check` for touched files.
   - Optionally run a small script that extracts `Node.field` / `Edge.field` references from the
     page and verifies them against the model dataclasses.

## Long COVID Starting Points

For Long COVID/PASC, begin with these backend files:

- `../project-cure-backend/server/apps/ui_forms/json_data/pasc.json`
- `../project-cure-backend/server/apps/ui_forms/json_data/pasc-menu.json`
- `../project-cure-backend/server/apps/ui_forms/json_data/pasc-summary.json`
- `../project-cure-backend/server/apps/api/filter_json/cure_filter_long_covid.json`
- `../project-cure-backend/server/apps/api/v2/serializers/report.py`
- migrations touching `symptoms_severity`, `symptoms_outcome`, and PASC report fields

The current IFX_ODIN Long COVID graph already uses an episode-oriented model:

- `CaseReport -> Patient`
- `Patient -> Episode` for primary Long COVID
- optional contextual episodes for acute COVID-19 and pregnancy
- Long COVID symptoms from `report.extra_fields.symptoms_severity`
- Long COVID regimen exposures from `report.regimens`
- treatment/outcome assessment from `report.extra_fields.symptoms_outcome`
- background conditions and background medicines from patient and `extra_fields`

There are no Long COVID graph -> Translator exports in this repo yet. Treat the current work as
`CURE ID PASC form -> Long COVID graph`, plus temporary or durable QA graph views where helpful.

Specific lessons from building the PASC page:

- PASC is Long COVID in the CURE forms; do not treat "PASC" and "Long COVID" as separate form
  families.
- The graph schema should match the QA Browser route for `cure_pasc`.
- The page should show all Long COVID/PASC form questions and where each answer lands.
- Section titles should come from `pasc-menu.json`.
- Question labels should come from `pasc.json`; light cleanup is acceptable for readability, but
  avoid inventing labels that change source semantics.
- Patient and clinician versions mostly align. Prefer per-question badges for one-form-only fields
  over a global patient/clinician toggle.
- Current PASC adapter mappings include several places where the obvious conceptual target is not
  the whole story:
  - adverse event outcomes are stored on `Exposure.adverse_event_outcomes` and copied to
    `ExposureAdverseEventEdge.outcomes`
  - selected adverse-event labels are stored on `Exposure.adverse_events` and emitted as
    `Phenotype.name` through `ExposureAdverseEventEdge`
  - outcome symptom labels are stored on `Outcome.raw_symptom_name`; `OutcomePhenotypeEdge` is only
    emitted when the symptom matches a selected Long COVID phenotype
  - treatment drug combinations are represented by `Treatment.drug_names`,
    `Treatment.unmatched_drug_names`, and `Treatment.has_unmatched_drug_names`
  - pregnancy creates a contextual `Episode` only when `pregnant_during_lc` is `Yes`
  - attachments are retained in the source payload but are not currently emitted by `CUREAdapter`
  - medication entries often set both an `Exposure.long_drug_name` and a connected `Drug`
  - edge labels should show only adapter-set edge fields such as `relationship_type`, `severity`,
    `outcomes`, or vaccination relative-time fields

Key unresolved information to document before adding any future durable exports:

- Which question labels should be carried into the visual mapping page.
- Whether export rows should summarize drug-to-symptom outcomes, symptom prevalence, adverse
  events, or all of these.
- Whether outcomes should be grouped by resolved drug and resolved phenotype, source labels, or both.
- Which patient/context fields are acceptable as evidence fields in public export JSONL.
- Whether pregnancy and acute COVID branches should produce separate exports or only evidence
  context.
- Which unmatched-drug and unmatched-phenotype QA views are still temporary versus user-facing.

## Deliverables

For each form family, produce or update:

- a CURE design doc under `designs/cure/`
- a form-to-graph visual page under `designs/cure/`
- YAML `graph_views` for stable QA views, and export views only where an export target exists
- tests that lock view IDs and essential traversal shape
- validation notes showing real report counts and representative traced cases

## Lessons From Rasopathies

- Final-review sections are a strong guide to the graph modules users expect.
- Persistent exports, where they exist, should summarize graph facts and keep evidence arrays, not
  flatten away the case report context too early.
- Historical TSVs are useful parity checks, but the refreshed JSONL graph may be richer and should not
  be forced into exact TSV cardinality.
- Keep predicate labels local until there is an explicit decision to emit Biolink-shaped records.
- Version metadata belongs in the graph view package so downstream consumers know the report source,
  concept source, and association view set.
- The form-to-graph page should behave like an audit UI:
  - selected question changes title and summary
  - selected graph nodes show field names
  - selected edges are highlighted
  - edge labels appear only for edge fields set by the selected answer
  - selection is linkable through the URL hash
