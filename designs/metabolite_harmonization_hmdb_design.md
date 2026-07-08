# HMDB Metabolite Harmonization Ingest Design

## Scope

First-pass HMDB ingest for the `metabolite_harmonization` graph.

This pass intentionally emits identifier-equivalence graph structure plus source-reported display labels:

- one `Metabolite` node for each HMDB metabolite accession
- one `Metabolite` node for each external metabolite identifier reported by HMDB
- one equivalence edge from the HMDB accession node to each reported equivalent identifier node
- source-owned metabolite nodes may carry source-attributed names and synonyms
- no classes, pathways, status, biology, or chemical properties in this adapter pass
- no resolver use; this graph is the backing evidence graph for a future metabolite resolver, so identifiers must enter unresolved

HMDB is treated as a manual registry source because current HMDB download endpoints are not automation-friendly and the release has been stable for a long time.

## Source Snapshot

Registry source:

- `hmdb:metabolites_xml`

Manual file location:

- `input_files/manual/hmdb/hmdb_metabolites.zip`

Observed payload:

- zip member: `hmdb_metabolites.xml`
- zip member timestamp: `2021-10-23`
- registry version: HMDB `5.0`
- registry version date: `2021-11-17`

The XML is large, so the adapter should stream parse `hmdb_metabolites.xml` directly from the zip with `xml.etree.ElementTree.iterparse` or equivalent streaming parser.

## Identifier Families

Observed HMDB label fields:

- `name`
- `synonyms/synonym`

Observed HMDB identifier fields include:

- `accession`
- `secondary_accessions/accession`
- `chebi_id`
- `kegg_id`
- `chemspider_id`
- `pubchem_compound_id`
- `cas_registry_number`
- `drugbank_id`
- `foodb_id`
- `biocyc_id`
- `bigg_id`
- `metlin_id`

Required equivalence endpoints:

| HMDB XML field | Proposed node ID |
| --- | --- |
| `accession` | `HMDB:<value>` |
| `secondary_accessions/accession` | `HMDB:<value>` |
| `chebi_id` | `CHEBI:<value>` |
| `kegg_id` | `KEGG.COMPOUND:<value>` |
| `pubchem_compound_id` | `PUBCHEM.COMPOUND:<value>` |
| `chemspider_id` | `ChemSpider:<value>` |
| `cas_registry_number` | `CAS:<value>` |
| `drugbank_id` | `DRUGBANK:<value>` |
| `foodb_id` | `FoodDB:<value>` |
| `biocyc_id` | `BioCyc:<value>` |
| `bigg_id` | `BiGG:<value>` |
| `metlin_id` | `METLIN:<value>` |

All observed HMDB xref fields above are first-pass must-ingest scope. The point of this graph is to merge source-reported identifier components before deciding which identifier families are useful downstream.

Implementation may need to add missing `Prefix` enum values for `ChemSpider`, `FoodDB`, `BioCyc`, `BiGG`, and `METLIN`, or avoid `EquivalentId` parsing entirely and keep these as `Metabolite.id` strings. Since these are node IDs, not `xref` values, the adapter should not need `EquivalentId.parse()`.

## Node And Edge Model

Node:

- Add a new harmonization-specific metabolite model; do not reuse the existing RaMP-oriented `src.models.metabolite.Metabolite`.
- Tentative class name: `MetaboliteIdentifier` or `MetaboliteIdentifier`.
- Fields:
  - `id: str`
  - `names: List[MetaboliteName]`
  - `synonyms: List[MetaboliteName]`
- `MetaboliteName` should be a small dataclass, not a graph node:
  - `value: str`
  - `source: str`
  - optional `source_field: str`
- Populate `names` and `synonyms` only when the source record provides them directly for that identifier.
- For HMDB, populate `names=[{value: <name>, source: "HMDB", source_field: "name"}]` and HMDB synonym entries on the primary HMDB accession node.
- For external xref nodes emitted from HMDB, emit stub nodes with only `id` for now; do not copy the HMDB name/synonyms onto external IDs.
- Later sources may add their own source-attributed names/synonyms to the nodes they own, for example RefMet names on `REFMET:<id>` nodes or LipidMaps names on `LIPIDMAPS:<id>` nodes.
- Do not create synonym nodes.
- Leave `sources` and provenance unset in the adapter; the ETL framework should stamp source metadata.

Edge:

- Add a reusable relationship class, `MetaboliteIdentifierMappingEdge`, shared by HMDB, WikiPathways, LipidMaps, and RefMet.
- `start_node: <new harmonization metabolite model>`
- `end_node: <new harmonization metabolite model>`
- Edge should carry source-local evidence in a merge-friendly `details` list.
- For HMDB, each detail entry should include:
  - `source`: `HMDB`
  - `source_field`: HMDB XML field that asserted the relationship
  - `source_id`: primary HMDB accession for the source record

Direction:

- Emit `HMDB primary accession -> external/secondary identifier`.
- Treat the edge semantics as undirected equivalence during resolver/harmonization use, even though graph storage uses directed relationships.

Deduplication:

- Deduplicate nodes by exact node ID in memory.
- Deduplicate edge detail entries by `(start_id, end_id, source, source_field, source_id)`.
- Multiple sources may later assert the same `(start_id, end_id)` pair; those assertions should merge into `details` instead of competing as top-level edge fields.
- Metabolite harmonization adapters must run with normal output merge behavior, not `single_source=True`, because multiple adapters can emit the same identifier nodes and edges.
- Skip self-edges if a secondary accession or xref normalizes to the primary accession.

Batching:

- Emit type-grouped batches:
  - metabolite nodes first
  - equivalence edges second
- Keep a bounded batch size, but avoid re-emitting duplicate nodes across batches by retaining a set of emitted node IDs.

## Normalization Rules

Allowed adapter-side normalization:

- trim whitespace
- uppercase stable prefixes in emitted IDs
- convert HMDB accessions from raw values such as `HMDB0000001` to `HMDB:HMDB0000001`
- convert KEGG compound IDs such as `C01152` to `KEGG.COMPOUND:C01152`
- convert PubChem compound IDs to `PUBCHEM.COMPOUND:<cid>`

Not allowed in this adapter:

- no cross-source reconciliation beyond the exact xrefs HMDB reports
- no PubChem CID to InChIKey merging
- no ChEBI secondary ID expansion
- no chemical structure matching
- no name-based matching

## Configuration

Start in `src/use_cases/working.yaml`.

Do not configure resolvers for this working graph. The backing graph is resolver input, not resolver output.

Adapter kwargs should accept either:

- a registry materialized dataset for `hmdb:metabolites_xml`, preferred when the registry path is wired into the ETL config, or
- an explicit `hmdb_zip_file` path for first local validation.

The first implementation should not be promoted into `src/use_cases/pharos/target_graph.yaml`.

## Validation Plan

Local checks:

- Parse a small record limit in unit tests.
- Assert expected nodes and edges for a fixture containing:
  - primary HMDB accession
  - secondary HMDB accession
  - ChEBI
  - KEGG
  - PubChem
  - ChemSpider
  - CAS
- Assert missing/blank fields are skipped.
- Assert repeated xrefs dedupe deterministically.

Manual validation after implementation:

- User runs the working ETL with no resolver configuration.
- Check graph counts for `Metabolite` nodes and `MetaboliteIdentifierMappingEdge` edges.
- Spot-check representative records:
  - `HMDB:HMDB0000001 -> CHEBI:50599`
  - `HMDB:HMDB0000001 -> PUBCHEM.COMPOUND:92105`
  - `HMDB:HMDB0000001 -> KEGG.COMPOUND:C01152`
  - `HMDB:HMDB0000001 -> HMDB:HMDB0004935`

## Decisions

- Secondary HMDB accessions are emitted as equivalence edges.
- All observed HMDB xref fields are must-ingest first-pass scope.
- Use one reusable `MetaboliteIdentifierMappingEdge` class across metabolite harmonization sources.
- Use an edge `details` list for source-specific assertions.
- Do not run resolvers for this graph.

## Open Questions

None for the HMDB first-pass implementation.
