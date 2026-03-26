# NCATS Ligands Integer Primary Key Migration

## Goal

Migrate `ncats_ligands` in the new Pharos/TCRD schema from a string primary key to an integer primary key, while preserving the current string ligand identifier as a unique business key.

The main motivation is MySQL performance. In `pharos400`, `ncats_ligands.id` and dependent foreign keys are currently `varchar(255)`, while the analogous join path in `pharos319` uses integer keys. The same ligand query that completes in about `0.5s` on `pharos319` took about `163s` on `pharos400`, and the query plan on `pharos400` is materially worse.

This design assumes we will rebuild the database rather than perform a complex in-place migration on a live populated schema.

## Current Findings

### Current schema

In [`src/shared/sqlalchemy_tables/pharos_tables_new.py`](../src/shared/sqlalchemy_tables/pharos_tables_new.py):

- `ncats_ligands.id` is `String(255)` and is the primary key.
- `ncats_ligands.identifier` is also `String(255)`, indexed but not the primary key.
- `ncats_ligand_activity.ncats_ligand_id` is `String(255)` and a foreign key to `ncats_ligands.id`.
- `ncats_dataSource_map.ncats_ligand_id` is `String(255)` and a foreign key to `ncats_ligands.id`.

### Current converter behavior

In [`src/output_adapters/sql_converters/tcrd.py`](../src/output_adapters/sql_converters/tcrd.py):

- `ligand_converter()` writes both `id` and `identifier` from `obj.get("id")`.
- `ligand_edge_converter()` writes `ncats_ligand_id=obj["end_id"]`, so dependent rows currently reference the ligand string ID directly.

### Data profiling

From live `pharos400` inspection:

- `ncats_ligands.id` and `ncats_ligands.identifier` are currently identical for every row inspected.
- Whole-table comparison showed:
  - `total_rows = 417223`
  - `equal_rows = 417223`
  - `different_rows = 0`
  - `id_nulls = 0`
  - `identifier_nulls = 0`
  - `distinct_id_count = 417223`
  - `distinct_identifier_count = 417223`

This means the existing string PK carries no extra payload beyond the current `identifier` column.

### Query-performance findings

The expensive query investigated earlier showed:

- `pharos319` runtime: about `0.50s`
- `pharos400` runtime: about `162.84s`

The target-associated ligand subset itself is fast on both databases:

- `pharos319`: about `0.035s`
- `pharos400`: about `0.067s`

The slowdown starts when the query joins that small ligand subset back to the larger ligand table and facet subquery on string PK/FK columns.

There are also meaningful schema differences:

- `pharos319.ncats_ligands.id` is integer
- `pharos400.ncats_ligands.id` is `varchar(255)`
- `pharos319.ncats_ligand_activity.ncats_ligand_id` is integer
- `pharos400.ncats_ligand_activity.ncats_ligand_id` is `varchar(255)`

## Why This Change Is Safe

This migration is safe from an identifier-preservation standpoint because:

- the current `id` and `identifier` values are identical in `pharos400`
- the ligand business identifier can remain in `identifier`
- dependent tables can reference the new integer PK without losing any source identifier data

The risk is not data loss inside `ncats_ligands`; it is migration correctness for dependent foreign keys and any code that still assumes the SQL ligand PK equals the graph ligand ID.

## Scope

### In scope

- `ncats_ligands`
- `ncats_ligand_activity`
- `ncats_dataSource_map`
- TCRD converter logic for ligand rows and ligand-dependent rows
- validation queries/tests for row counts, FK integrity, and result parity

### Out of scope

- changing graph IDs in Arango
- changing ligand identifiers in the source graph
- rewriting all downstream SQL queries as part of this migration

## Graph Impact

For the Arango graph, string IDs are normal and do not create the same kind of problem seen in MySQL. Arango document keys and IDs are string-based already. Long strings still increase storage and index cost somewhat, but they are not the same risk as large relational joins on `varchar(255)` primary and foreign keys.

This migration should therefore be treated as a Pharos/TCRD MySQL schema and converter change, not a graph ID redesign.

## Proposed Target Schema

### `ncats_ligands`

Current:

- `id`: `String(255)` primary key
- `identifier`: `String(255)` non-PK indexed column

Target:

- `id`: `Integer`, primary key, `autoincrement=False`
- `identifier`: `String(255)`, `NOT NULL`, unique
- retain the existing non-key ligand metadata columns

### `ncats_ligand_activity`

Current:

- `id`: integer primary key
- `ncats_ligand_id`: `String(255)` FK to `ncats_ligands.id`

Target:

- `id`: integer primary key
- `ncats_ligand_id`: integer FK to `ncats_ligands.id`

### `ncats_dataSource_map`

Current:

- `id`: integer primary key
- `ncats_ligand_id`: `String(255)` FK to `ncats_ligands.id`

Target:

- `id`: integer primary key
- `ncats_ligand_id`: integer FK to `ncats_ligands.id`

## Python-Side ID Assignment Pattern

The codebase already has an established pattern for assigning integer IDs in Python rather than letting MySQL allocate them.

In [`src/output_adapters/sql_converters/output_converter_base.py`](../src/output_adapters/sql_converters/output_converter_base.py):

- `preload_id_mappings()` loads existing IDs from the database into memory
- `resolve_id(table, id)` assigns the next integer ID when a lookup key is not already present

This is already used for:

- `protein`
- `target`
- `tissue`
- `ncats_disease`
- `disease_assoc`

This means ligands can follow the same pattern cleanly:

- preload `identifier -> integer id` mappings
- assign new ligand IDs with `resolve_id(...)`
- write dependent foreign keys using the same in-memory mapping

This avoids requiring a database hit during edge conversion and fits the current rebuild-oriented ETL workflow.

## Proposed Converter Behavior

### Ligand rows

`ligand_converter()` should:

- assign `id` using `resolve_id(...)`
- write the graph/source ligand ID into `identifier`
- continue writing ligand metadata fields as before

### Dependent rows

`ligand_edge_converter()` and any datasource mapping converter should:

- resolve the ligand business identifier to the SQL integer PK
- write that integer PK into dependent foreign key columns

This likely requires extending or reusing the preload/mapping mechanism already used for proteins so that ligand identifier to SQL ID resolution is available during conversion.

## Rebuild Strategy

Because we expect to rebuild the database, we do not need a long multi-phase live migration with parallel old/new columns.

Instead, the implementation can update the SQLAlchemy schema and converter logic together, then repopulate the database from scratch.

### Schema changes

- change `ncats_ligands.id` from `String(255)` PK to `Integer` PK with `autoincrement=False`
- make `ncats_ligands.identifier` the preserved ligand business key
- change `ncats_ligand_activity.ncats_ligand_id` from `String(255)` to `Integer`
- change `ncats_dataSource_map.ncats_ligand_id` from `String(255)` to `Integer`
- add a uniqueness constraint or unique index on `ncats_ligands.identifier`

### Converter changes

- `ligand_converter()` should call `resolve_id(...)` for the SQL PK
- ligand-dependent converters should resolve the same integer ID from the ligand business identifier
- preload queries should include ligand IDs so reruns against partially populated databases remain stable

### Rebuild validation

- rebuild and repopulate the working database
- validate row counts and FK integrity
- re-run the known slow ligand query and compare runtime and plan
- confirm that result rows match expected content apart from fields not yet populated in `pharos400`

## Validation Checklist

### Table-level validation

- `count(*)` unchanged in `ncats_ligands`
- `count(*)` unchanged in `ncats_ligand_activity`
- `count(*)` unchanged in `ncats_dataSource_map`
- `count(distinct identifier) = count(*)` for `ncats_ligands`
- no duplicate `identifier` values

### FK validation

- every `ncats_ligand_activity` row resolves to a ligand
- every `ncats_dataSource_map` ligand reference resolves to a ligand
- no dependent rows left with null integer ligand FK after rebuild

### Query validation

Re-run the known slow ligand query and compare:

- runtime
- explain plan
- result row count
- representative output rows

The expectation is that even without query rewrite, integer PK/FK join paths should make the plan more competitive and reduce the optimizer penalty seen in `pharos400`.

## Other Varchar Primary Keys In `pharos_tables_new`

`pharos_tables_new` does include many other varchar-backed primary keys, but most are natural-key ontology or lookup tables rather than high-volume hub tables. Examples include:

- `generif.id`
- `go.go_id`
- `mondo.mondoid`
- `do.doid`
- `uberon.uid`
- `virus.virusTaxid`
- `disease_type.name`
- `phenotype_type.name`
- `input_version.source_key`
- `input_version.file_key`
- ancestry tables keyed by ontology IDs

These should not be treated as equivalent risks to `ncats_ligands.id`. The ligand table stands out because:

- it has a large row count
- it participates in heavy join paths
- it is referenced by dependent fact-style tables

## Recommended Implementation Order

1. Update `pharos_tables_new.py` so ligand PK/FK columns are integer-based.
2. Update the TCRD output converter to assign ligand IDs with `resolve_id(...)`.
3. Add or extend preload logic for ligand identifier to integer ID mappings.
4. Add tests for ligand conversion and dependent FK emission.
5. Rebuild a working database.
6. Validate counts, integrity, and query behavior.
7. Promote the change to the production-oriented Pharos workflow after validation.

## Open Questions

- Should the ligand mapping namespace be `ligand` or `ncats_ligand` inside `resolve_id(...)`?
- Should `identifier` be declared unique in SQLAlchemy explicitly?
- Are there any downstream consumers outside IFX_ODIN that currently expect `ncats_ligand_activity.ncats_ligand_id` to be a string business key?
- Should `ncats_dataSource_map.ncats_ligand_id` keep the historical column name after type migration, or should a new name such as `ligand_id` be introduced?
