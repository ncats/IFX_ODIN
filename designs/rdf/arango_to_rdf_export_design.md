# Arango To RDF Export Design

## Goal

Export a merged IFX graph from an existing ArangoDB staging database into RDF after ETL and Arango-side merge/post-processing are complete.

This is intentionally **not** an `OutputAdapter`. The export should happen from the already-merged Arango state, in the same architectural layer as `src/use_cases/arango_to_mysql.py`.

## Why Export From Arango

- Merge semantics already settle in Arango.
- Re-implementing merge behavior in an RDF writer would duplicate core logic.
- Multi-source builds such as Pharos need RDF that reflects merged nodes, merged edges, and accumulated provenance.
- A post-Arango export can be rerun without rebuilding the source ETL.

## Initial Placement

- Export script/module path: `src/use_cases/arango_to_rdf.py`
- First consumer targets:
  - `gramp`
  - `pharos`
  - other merged staging DBs built by `ArangoOutputAdapter`

## Scope For V1

### Included

- Read node and edge collections from an Arango database.
- Emit file-backed RDF.
- Support full-database export and collection allowlists.
- Preserve merged `sources`, `provenance`, and `entity_resolution`.
- Support scalable streaming export.
- Produce RDF that is easy to load into a local triple store.

### Excluded

- No direct ETL-to-RDF path.
- No OWL/SHACL modeling in v1.
- No reasoning-specific export behavior in v1.
- No attempt to replace IFX IRIs with external ontology IRIs by default.
- No bespoke domain mappings beyond a small curated relation map if needed.

## Output Format

V1 should emit `N-Triples` (`.nt`).

Why:

- line-oriented and streamable
- easy to append and inspect
- simpler escaping rules than Turtle
- straightforward load path into Fuseki/other stores

Possible later additions:

- `Turtle` for human-readable subsets
- `N-Quads` if named-graph partitioning becomes useful

## Data Source

The exporter should read:

- `metadata_store.collection_schemas`
- document collections
- edge collections

It should use collection schema metadata when available, but the export source of truth is the live merged data in Arango.

## Identity Strategy

### Subject IRIs

Use stable IFX IRIs for exported resources.

Recommended pattern:

- Nodes: `{base_resource_uri}{collection}/{urlencoded_id}`
- Edge statement resources: `{base_resource_uri}statement/{edge_collection}/{urlencoded_key_or_hash}`

Example:

- `https://ifx.ncats.nih.gov/resource/Protein/UniProtKB%3AP12345`
- `https://ifx.ncats.nih.gov/resource/Pathway/RAMP_P%3A123`

### Why Not Reuse External IDs As Subjects

- Many merged nodes integrate multiple external identifiers.
- IFX needs one canonical export identity per merged node.
- External IDs can still be emitted as mapping triples.

### External Identifier Representation

For `id` and `xref` values:

- keep the IFX subject IRI as canonical
- emit literal `ifx:id` values
- optionally emit `skos:exactMatch` or `owl:sameAs` later for resolvable external IDs

V1 should keep this simple and not over-assert equivalence.

## Node Mapping

For each Arango document collection:

- emit `rdf:type ifx:{CollectionName}`
- emit scalar fields as datatype properties
- emit repeated values as repeated property triples
- emit `sources`, `provenance`, `entity_resolution` as metadata properties

Example:

```turtle
<https://ifx.ncats.nih.gov/resource/Protein/UniProtKB%3AP12345>
  rdf:type ifx:Protein ;
  ifx:id "UniProtKB:P12345" ;
  ifx:name "TP53" ;
  ifx:sources "UniProt\t2025_01\t2025-01-15\t2025-01-20" .
```

## Edge Mapping

There are two cases.

### 1. Simple Edges

If an edge collection has no meaningful payload beyond endpoints, emit a direct triple:

```turtle
<protein> ifx:hasPathway <pathway> .
```

### 2. Edges With Properties

If an edge has additional payload such as:

- `source`
- `evidence`
- `conf`
- `details`
- `is_reviewed`

then emit:

- a direct triple for the relation itself
- a separate statement resource carrying edge metadata

Example pattern:

```turtle
<protein> ifx:associatedWith <disease> .

<statement>
  rdf:type ifx:ProteinDiseaseEdge ;
  ifx:subject <protein> ;
  ifx:predicate ifx:associatedWith ;
  ifx:object <disease> ;
  ifx:source "JensenLab DISEASES" ;
  ifx:provenance "..." .
```

This avoids requiring RDF-star in v1.

## Predicate Naming

### Node Fields

Default:

- `ifx:{field_name}`

Examples:

- `name` -> `ifx:name`
- `protein_type` -> `ifx:protein_type`

### Edge Predicates

Default:

- derive from edge collection name or model class

Examples:

- `ProteinPathwayEdge` -> `ifx:hasPathway`
- `ProteinDiseaseEdge` -> `ifx:associatedWithDisease`

V1 can use a small explicit mapping table for common edge types and fall back to deterministic naming for the rest.

## Literal Mapping

- `str` -> string literal
- `int` -> `xsd:integer`
- `float` -> `xsd:double`
- `bool` -> `xsd:boolean`
- `date` -> `xsd:date`
- `datetime` -> `xsd:dateTime`

Lists become repeated triples.

Nested dict/object payloads:

- flatten only when the structure is predictable and useful
- otherwise serialize as JSON literal in v1

This is especially relevant for fields like `details` or nested metadata blobs.

## Provenance

V1 should preserve current IFX provenance data with minimal semantic transformation:

- `ifx:sources`
- `ifx:provenance`
- `ifx:entity_resolution`

Possible later improvement:

- add a PROV-O export mode

## CLI Shape

Suggested interface:

```bash
python -m src.use_cases.arango_to_rdf \
  --arango-credentials ./src/use_cases/secrets/ifxdev_arangodb.yaml \
  --arango-db pharos \
  --output-file ./output/pharos.nt
```

Useful options:

- `--collection Protein`
- `--collection ProteinDiseaseEdge`
- `--exclude-collection metadata_store`
- `--base-resource-uri https://ifx.ncats.nih.gov/resource/`
- `--base-ontology-uri https://ifx.ncats.nih.gov/ontology/`
- `--statement-mode reified`

## Implementation Plan

1. Create `src/use_cases/arango_to_rdf.py`.
2. Reuse `ArangoAdapter` for source reads.
3. Read `collection_schemas` from `metadata_store`.
4. Identify document vs edge collections.
5. Implement deterministic IRI builders.
6. Implement literal serializer and escaping.
7. Implement node export.
8. Implement edge export:
   - direct triples for all edges
   - statement resources only when payload fields exist
9. Stream writes to `.nt`.
10. Add an allowlist mode for testing on small collection subsets.

## Validation Plan

### Small

- Export only `Protein`, `Pathway`, `ProteinPathwayEdge` from `gramp`.
- Load into local Fuseki.
- Run sanity SPARQL queries for counts and joins.

### Medium

- Export a limited Pharos subset:
  - `Protein`
  - `Disease`
  - `Pathway`
  - `ProteinDiseaseEdge`
  - `ProteinPathwayEdge`

### Full

- Export an entire merged database and compare:
  - Arango document counts vs RDF class counts
  - Arango edge counts vs RDF relation counts

## Open Questions

1. Should subject IRIs be based on collection + `id`, or collection + `_key`?
   - Recommendation: collection + canonical `id` when present.

2. Should `xref` become literals only, or also external IRI links?
   - Recommendation: literals in v1, optional exact-match links later.

3. Should edge payloads be represented as JSON literals or fully flattened statement properties?
   - Recommendation: flatten predictable scalar fields, JSON-literal complex blobs in v1.

4. Do we want named graphs for source provenance later?
   - Recommendation: no in v1.

## Recommended First Milestone

Implement a generic exporter and validate it against `gramp` first. `gramp` is relatively self-contained and graph-like, so it is the right proving ground before taking on `pharos`.
