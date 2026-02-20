# POUNCE Demo UI Plan

## Goal
Build a scientist-facing demo UI on top of `omicsdb_dev2` that shows data can be navigated
**top-down** (Project → Experiment → Data) and **bottom-up** (Metabolite/Gene → Experiments
that measured it → Stats → Pathway context). Scientists need provenance and coverage
transparency to trust the data.

---

## Page Inventory

### Already built
| Route | Description |
|---|---|
| `/demo/genes` | Gene list, biotype facet, symbol search |
| `/demo/metabolites` | Metabolite list, name search |
| `/demo/measured-metabolites` | Measured metabolite list, id-level facet |

### New pages — priority order

| # | Route | Description | Direction |
|---|---|---|---|
| 1 | `/demo/projects` | Project list | Top-down entry |
| 2 | `/demo/projects/{id}` | Project detail + experiments | Top-down |
| 3 | `/demo/experiments/{id}` | Experiment detail + datasets + stats | Top-down |
| 4 | `/demo/metabolites/{id}` | Metabolite detail: measured instances, experiment coverage, pathways | Bottom-up |
| 5 | `/demo/pathways` | Pathway browser | Bottom-up entry |
| 6 | `/demo/pathways/{id}` | Pathway detail: metabolites + experiment coverage | Bottom-up |
| 7 | `/demo/genes/{id}` | Gene detail: measured instances + experiment coverage | Bottom-up |

---

## Page Designs

---

### 1. `/demo/projects` — Project List

**Layout:** facet panel (left) + table (right), same pattern as gene browser

**Left facet:** `project_type` (from `project__project_type` join table)

**Table columns:** Name · Date · Lab Groups · Access · # Experiments · # Biosamples

**Key queries:**
```sql
-- List with counts
SELECT p.id, p.name, p.date, p.access,
       GROUP_CONCAT(DISTINCT pt.value) AS project_types,
       COUNT(DISTINCT pe.experiment_id) AS experiment_count,
       COUNT(DISTINCT pb.biosample_id) AS biosample_count
FROM project p
LEFT JOIN project__project_type pt ON pt.id = p.id
LEFT JOIN project_to_experiment pe ON pe.project_id = p.id
LEFT JOIN project_to_biosample pb ON pb.project_id = p.id
GROUP BY p.id
ORDER BY p.date DESC
```

**⚠ Needs confirmed:** column names of `project__project_type`, `project_to_experiment`,
`project_to_biosample` (likely `id`/`project_id` + `experiment_id`/`biosample_id`)

---

### 2. `/demo/projects/{id}` — Project Detail

**Layout:** two sections — metadata card (top), then experiments table (bottom)

**Metadata card shows:**
- Name, description, date, access level, rare_disease_focus
- Lab groups (from `project__lab_groups`)
- Keywords (from `project__keywords`)
- People/roles (from `project_to_person` + `person`: name, email, role)

**Experiments table (inline):** Name · Type · Platform · Date · # Datasets · Has Stats?
- Links to `/demo/experiments/{id}`

**Biosamples summary:** count + type breakdown, link to generic table browser

**Key queries:**
```sql
-- Experiments for project with dataset/stats counts
SELECT e.id, e.name, e.experiment_type, e.platform_type, e.date,
       COUNT(DISTINCT ed.dataset_id) AS dataset_count,
       COUNT(DISTINCT es.stats_result_id) AS stats_count
FROM experiment e
JOIN project_to_experiment pe ON pe.experiment_id = e.id
LEFT JOIN experiment_to_dataset ed ON ed.experiment_id = e.id
LEFT JOIN experiment_to_stats_result es ON es.experiment_id = e.id
WHERE pe.project_id = :project_id
GROUP BY e.id
ORDER BY e.date DESC

-- People
SELECT p.name, p.email, pp.role
FROM person p
JOIN project_to_person pp ON pp.person_id = p.id
WHERE pp.project_id = :project_id
```

---

### 3. `/demo/experiments/{id}` — Experiment Detail

**Layout:** metadata card (top), datasets table (middle), stats results table (bottom)

**Metadata shows:**
- Name, description, design, experiment_type, platform (name + provider + type)
- public_repo_id + repo_url (link out if present)
- extraction_protocol, acquisition_method
- People (DataGenerator / Informatician)

**Datasets table:**
- data_type · row_count (analytes) · column_count (samples) · Link to parquet stats
- Links to generic QA browser: `/mysql/omicsdb_dev2/table/dataset/row/{id}`

**Stats Results table:**
- name · data_type · row_count · comparison_columns count
- Link to stats result detail (generic row browser or dedicated page TBD)

**Breadcrumb:** Home → Project → Experiment (requires storing project_id in context or back-link)

**Key queries:**
```sql
-- Datasets
SELECT d.id, d.data_type, d.row_count, d.column_count, d.file_reference
FROM dataset d
JOIN experiment_to_dataset ed ON ed.dataset_id = d.id
WHERE ed.experiment_id = :experiment_id

-- Stats results
SELECT sr.id, sr.name, sr.data_type, sr.row_count, sr.column_count
FROM stats_result sr
JOIN experiment_to_stats_result es ON es.stats_result_id = sr.id
WHERE es.experiment_id = :experiment_id

-- People
SELECT p.name, p.email, ep.role
FROM person p
JOIN experiment_to_person ep ON ep.person_id = p.id
WHERE ep.experiment_id = :experiment_id
```

---

### 4. `/demo/metabolites/{id}` — Metabolite Detail (bottom-up anchor)

This is the key bottom-up page. Scientists start here to answer:
*"What data do we have for this metabolite?"*

**Layout:** three sections

**Section A — Reference info:**
- ID (with prefix source: RAMP, ChEBI, HMDB etc), name, type
- Synonyms (from `metabolite__synonyms`)

**Section B — Experiment coverage:**
Table: Experiment Name · Project · data_type · row_count · Has Stats?
- Join path: `metabolite` → `measured_metabolite_to_metabolite` → `measured_metabolite`
  → `measured_metabolite_dataset__data` → `dataset` → `experiment_to_dataset` → `experiment`
  → `project_to_experiment` → `project`

**Section C — Pathways this metabolite belongs to:**
- Table: Pathway Name · Type · Category · Source
- Join: `metabolite_to_pathway` → `pathway`
- Each pathway links to `/demo/pathways/{id}`

**Key queries:**
```sql
-- Experiment coverage
SELECT e.id AS exp_id, e.name AS exp_name, e.experiment_type,
       p.id AS proj_id, p.name AS proj_name,
       d.data_type, d.row_count,
       COUNT(DISTINCT sr.id) AS stats_count
FROM metabolite met
JOIN measured_metabolite_to_metabolite mm2m ON mm2m.metabolite_id = met.id
JOIN measured_metabolite_dataset__data mmd ON mmd.measured_metabolite_id = mm2m.measured_metabolite_id
JOIN dataset d ON d.id = mmd.dataset_id
JOIN experiment_to_dataset ed ON ed.dataset_id = d.id
JOIN experiment e ON e.id = ed.experiment_id
JOIN project_to_experiment pe ON pe.experiment_id = e.id
JOIN project p ON p.id = pe.project_id
LEFT JOIN measured_metabolite_stats_result__data mmsr
    ON mmsr.measured_metabolite_id = mm2m.measured_metabolite_id
LEFT JOIN stats_result sr ON sr.id = mmsr.stats_result_id
WHERE met.id = :metabolite_id
GROUP BY e.id, d.id

-- Pathways
SELECT pw.id, pw.name, pw.type, pw.category, pw.source_id
FROM pathway pw
JOIN metabolite_to_pathway m2p ON m2p.pathway_id = pw.id
WHERE m2p.metabolite_id = :metabolite_id
ORDER BY pw.name
```

**⚠ Needs confirmed:** column names of `measured_metabolite_to_metabolite`,
`measured_metabolite_dataset__data`, `measured_metabolite_stats_result__data`,
`metabolite_to_pathway`

---

### 5. `/demo/pathways` — Pathway Browser

**Layout:** facet panel (left: `type` or `category`) + table (right)

**Table columns:** Name · Type · Category · Source · # Metabolites

**Key query:**
```sql
SELECT pw.id, pw.name, pw.type, pw.category, pw.source_id,
       COUNT(DISTINCT m2p.metabolite_id) AS metabolite_count
FROM pathway pw
LEFT JOIN metabolite_to_pathway m2p ON m2p.pathway_id = pw.id
GROUP BY pw.id
ORDER BY pw.name
```

---

### 6. `/demo/pathways/{id}` — Pathway Detail (the "show everything" page)

This is the highest-level bottom-up view scientists care about most:
*"For this pathway, what metabolomics data do we have?"*

**Layout:** metadata (top), then a metabolite-by-metabolite breakdown

**Metadata:** Name, type, category, source_id (link out)

**Metabolite coverage table:**
One row per metabolite in the pathway, showing experiment coverage inline.

Columns: Metabolite Name · ID · # Experiments with raw data · # Experiments with stats · 
Experiment list (collapsed, expandable)

**Key query:**
```sql
SELECT met.id, met.name,
       COUNT(DISTINCT e.id) AS experiment_count,
       COUNT(DISTINCT sr.id) AS stats_count,
       GROUP_CONCAT(DISTINCT e.name ORDER BY e.name SEPARATOR ' | ') AS experiments
FROM pathway pw
JOIN metabolite_to_pathway m2p ON m2p.pathway_id = pw.id
JOIN metabolite met ON met.id = m2p.metabolite_id
LEFT JOIN measured_metabolite_to_metabolite mm2m ON mm2m.metabolite_id = met.id
LEFT JOIN measured_metabolite_dataset__data mmd ON mmd.measured_metabolite_id = mm2m.measured_metabolite_id
LEFT JOIN experiment_to_dataset ed ON ed.dataset_id = mmd.dataset_id
LEFT JOIN experiment e ON e.id = ed.experiment_id
LEFT JOIN measured_metabolite_stats_result__data mmsr ON mmsr.measured_metabolite_id = mm2m.measured_metabolite_id
LEFT JOIN stats_result sr ON sr.id = mmsr.stats_result_id
WHERE pw.id = :pathway_id
GROUP BY met.id
ORDER BY experiment_count DESC, met.name
```

---

### 7. `/demo/genes/{id}` — Gene Detail (mirrors metabolite detail)

Same pattern as metabolite detail but for gene:
- Reference gene info (symbol, biotype, chromosomal location)
- Experiment coverage via `measured_gene_to_gene` → `measured_gene_dataset__data`
- Stats coverage via `measured_gene_stats_result__data`
- No pathway section (genes don't join to pathways in this schema)

---

## Navigation & UX Principles

1. **Breadcrumbs everywhere** — scientists need to know where they are in the hierarchy
2. **Count badges** — always show "N experiments", "N metabolites", don't hide emptiness
3. **Provenance visible** — show data source/version on detail pages
4. **Links in both directions** — every entity links to its neighbors in both directions
5. **Empty state is honest** — "No experiments found for this metabolite" is useful data
6. **Generic QA browser as escape hatch** — every row has a link to the raw row detail

---

## Open Questions / Things to Confirm

Before building, need column names for these join tables (run `DESCRIBE` on each):

```sql
DESCRIBE project__project_type;
DESCRIBE project_to_experiment;
DESCRIBE project_to_biosample;
DESCRIBE project_to_person;
DESCRIBE experiment_to_dataset;
DESCRIBE experiment_to_stats_result;
DESCRIBE experiment_to_person;
DESCRIBE measured_metabolite_to_metabolite;
DESCRIBE measured_metabolite_dataset__data;
DESCRIBE measured_metabolite_stats_result__data;
DESCRIBE measured_gene_to_gene;
DESCRIBE measured_gene_dataset__data;
DESCRIBE measured_gene_stats_result__data;
DESCRIBE metabolite_to_pathway;
DESCRIBE pathway;
```

Also useful:
```sql
SELECT * FROM project LIMIT 2;
SELECT * FROM pathway LIMIT 3;
```

---

## Build Order (for demo tomorrow)

Given time constraints, suggested priority:

1. **Project list + detail** (#1, #2) — establishes top-down story
2. **Experiment detail** (#3) — completes top-down drill-down  
3. **Metabolite detail** (#4) — bottom-up anchor, highest scientist value
4. **Pathway browser + detail** (#5, #6) — the "wow" use case
5. **Gene detail** (#7) — mirrors metabolite, lower priority if time is tight

