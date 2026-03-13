Update the Pipeline Status Table in `src/use_cases/pharos/TCRD_TODO.md` by reading the actual code and config files.

## What to read

1. `src/use_cases/pharos/pharos.yaml` — all input adapters going into the Pharos ArangoDB graph
2. `src/use_cases/pharos/pharos_aql_post.yaml` — post-processing adapters that modify the graph in-place
3. `src/use_cases/pharos/tcrd.yaml` — adapters that read from Arango and write to TCRD MySQL
4. Source files for each adapter listed in the above yamls — read `get_all()` to determine what Node and Relationship subclasses are yielded

## Table structure

Columns: `Concept | Data Sources (→ graph) | Arango Type | MySQL Tables (graph → TCRD)`

- **Concept** — the node or edge *type* (e.g. `Protein`, `ProteinDiseaseEdge`), not the adapter name or data source name. Multiple adapters contributing to the same type share one row.
- **Data Sources** — one `[x]` entry per source adapter that contributes to this concept. Use the source name (e.g. `UniProt`, `ChEMBL`), not the adapter class name. Checkbox is `[x]` if the adapter exists and is wired in pharos.yaml.
- **Arango Type** — the Python class name(s) yielded (e.g. `Protein`, `ProteinDiseaseEdge`). Just the class name — no arrow notation like `(Protein → Disease)`.
- **MySQL Tables** — `[x] \`table_name\`` for each table the TCRD converter populates. If no converter exists yet, use `[ ] TBD`. Do NOT guess at table names for unfinished items.

## Rules

- One row per concept, not per adapter or per source
- An adapter that yields multiple types (e.g. `ProteinAdapter` yields `Protein`, `Keyword`, `Pathway`, and edges) contributes to multiple rows — one per type
- Do not use the word "side effect" — if an adapter yields a type, it's a legitimate source for that row
- Post-processing adapters (from `pharos_aql_post.yaml`) go below a separator row: `| | *— post-processing (pharos_aql_post.yaml) —* | | |`. Their Concept column is the adapter class name, Arango Type describes what they update, and MySQL says `*(via X)*` since they modify existing nodes/edges rather than creating new ones
- Do not add rows for concepts not yet in the code — those belong in the Planned Data Sources section below the table
- Do not remove rows for concepts that are in the code even if they have no MySQL converter yet

## What to check for MySQL converters

Read `src/use_cases/pharos/tcrd.yaml` to see what adapters are wired. For each adapter, read its `get_all()` to see what model types it yields. Then read `src/output_adapters/sql_converters/tcrd.py` (or equivalent) to see which model types have converter methods and which MySQL tables they write to.

## After updating the table

Do not modify the Code Style TODOs section or the Planned Data Sources section — those are manually maintained.