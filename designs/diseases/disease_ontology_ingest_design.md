# Disease Ontology (DO) Ingest Design

## Purpose
Ingest Human Disease Ontology (DO) terms and parent-child hierarchy into the target graph and pharos ETLs, complementing the existing Mondo ingest.

## Source Data
- Source page: https://disease-ontology.org/
- File used: `doid.json` (OBO JSON format)
- Direct URL: `https://purl.obolibrary.org/obo/doid.json`
- Download target path: `input_files/auto/disease_ontology/doid.json`

## Prior ETL Reference (pharos319)
The old TCRD pipeline ingested DO into two tables:

| Table | Rows | Description |
|-------|------|-------------|
| `do` | 9,233 | Disease nodes: `doid`, `name`, `def` |
| `do_parent` | 10,880 | Direct parent-child edges: `doid`, `parent_id` |
| `ancestry_do` | 85,072 | Transitive closure (all ancestors) — **not ingested**, computable from graph |

The lower row counts vs. the current file (12,021 nodes, 16,930 edges) reflect an older version of DO in pharos319.

## File Profile
- The JSON contains 16 graphs. Graph 0 (`doid.owl`) is the main DO graph; the rest are imports (CHEBI, CL, HP, UBERON, etc.) and are ignored.
- Non-deprecated DOID CLASS nodes: **12,021**
- `is_a` edges: **16,930** — all endpoints are DOID IDs

## Version Strategy
Two clean version signals in `graphs[0].meta`:
- `meta.version`: `"http://purl.obolibrary.org/obo/doid/releases/2026-02-28/doid.owl"` — release date parseable via existing regex (`/releases/YYYY-MM-DD/`)
- `owl#versionInfo` basicPropertyValue: `"2026-02-28"` — direct fallback

No sidecar version file needed. The Mondo base class version extraction logic handles DO without modification. Version emitted as `v2026-02-28`.

## Scope
### Included
- Disease nodes with canonical DOID IDs (`DOID:NNNNNNN`) where node `type == "CLASS"`
- Disease hierarchy edges where `pred == "is_a"` and both endpoints are DOID IDs
- Disease fields: `id`, `name`, `type`, `definition`, `synonyms`, `comments`

### Excluded
- Non-DOID node IDs as first-class Disease nodes
- Non-`CLASS` nodes (property terms, etc.)
- Deprecated nodes (`meta.deprecated == true`)
- Non-`is_a` relationship predicates
- Transitive ancestry (`ancestry_do`) — computable from graph
- `subsets` — DO subsets are not used the same way as Mondo; excluded for now

## Disease Node Harmonization

DO Disease nodes are normalized by TNN at ingest time (DOID → canonical MONDO ID where a mapping exists). `RecordMerger` then merges `do_description` into the existing MONDO Disease node. Result: one canonical Disease node per disease with fields from both ontologies. DO-only terms (no MONDO mapping) remain DOID-prefixed.

The `Disease` model was updated as part of this work:
- `definition` renamed to `mondo_description` (set by the MONDO adapter)
- `do_description` added (set by `DODiseaseAdapter`)
- `uniprot_description` added (field ready, population deferred)

## Data Model Mapping

### Disease node
Source: `graphs[0].nodes[*]`

- `Disease.id`: normalize `http://purl.obolibrary.org/obo/DOID_0001816` → `DOID:0001816`; TNN may further normalize to a canonical MONDO ID
- `Disease.name`: node `lbl`
- `Disease.type`: node `type`
- `Disease.do_description`: `meta.definition.val`
- `Disease.synonyms`: values from `meta.synonyms[*].val`
- `Disease.comments`: values from `meta.comments[*].val`

### DODiseaseParentEdge (shadow edge)
Source: `graphs[0].edges[*]`

A distinct edge type (`DODiseaseParentEdge`) separate from the canonical `DiseaseParentEdge` (MONDO hierarchy). Used for TCRD `do_parent` table reconstruction; ignored in normal graph traversal.

- Include only `pred == "is_a"` edges where both normalized endpoints are DOID IDs
- `start_node`: child Disease (`sub`)
- `end_node`: parent Disease (`obj`)
- `source`: `"DO"`

## Implemented Components
- `src/input_adapters/disease_ontology/do_adapter.py` — `DODiseaseAdapter`, `DODiseaseParentEdgeAdapter`
- `src/constants.py` — `DataSourceName.DiseaseOntology`
- `src/models/disease.py` — `DODiseaseParentEdge` dataclass; `Disease` field renames/additions
- `src/use_cases/pharos/pharos.yaml` and `target_graph.yaml` — both adapters wired
- `src/output_adapters/sql_converters/tcrd.py` — `do_converter` (Disease → `do`), `do_parent_converter` (DODiseaseParentEdge → `do_parent`)
- `src/shared/sqlalchemy_tables/pharos_tables_new.py` — `DO.doid` and `DOParent.doid`/`parent_id` widened from `VARCHAR(12)` to `VARCHAR(20)`

## Pending (graph → TCRD)
- `src/input_adapters/pharos_arango/tcrd/disease.py` needs a `DODiseaseParentEdge` AQL query so `do_parent_converter` is triggered

## Open Decisions
- Whether to ingest DO xrefs (MESH, NCI, OMIM, etc.) as explicit cross-references
- Whether to link DOID↔MONDO via shared xrefs in post-processing