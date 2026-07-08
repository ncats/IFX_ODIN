# WikiPathways Metabolite Harmonization Ingest Design

## Scope

First-pass WikiPathways ingest for the `metabolite_harmonization` graph.

This pass emits source-reported metabolite identifier equivalence groups from
WikiPathways RDF metabolite data nodes:

- one `MetaboliteIdentifier` node for each source subject identifier
- one `MetaboliteIdentifier` node for each `wp:bdb*` equivalent identifier
- source-reported `rdfs:label` values attached to the source subject identifier
- one `MetaboliteIdentifierMappingEdge` from the source subject identifier to each
  source-reported equivalent identifier

No source-specific corrective curation is applied in this pass. Known bad
assertions, including WP5213 glycine linked to PubChem compound 753, are
ingested as reported by WikiPathways.

## Source Snapshot

Registry source:

- `wikipathways:rdf_wp`

Observed local snapshot:

- `wikipathways:rdf_wp:2026-06-10`
- file: `wikipathways-20260610-rdf-wp.zip`
- contents: per-pathway Turtle files under `wp/*.ttl`

The zip does not contain a prebuilt
`wikipathwayRDFmetaboliteIDDictionary.txt`; the adapter derives equivalent ID
groups directly from `wp:Metabolite` data-node subjects and `wp:bdb*`
predicates.

## Observed Payload

In `wikipathways:rdf_wp:2026-06-10`:

- 2,088 Turtle files
- 16,304 metabolite subjects
- 16,304 metabolite subjects with `rdfs:label`
- 16,196 metabolite subjects with at least one `wp:bdb*` equivalent ID

Common `wp:bdb*` predicates:

| Predicate | Observed count |
| --- | ---: |
| `wp:bdbWikidata` | 17,281 |
| `wp:bdbPubChem` | 16,867 |
| `wp:bdbChEBI` | 16,202 |
| `wp:bdbInChIKey` | 14,718 |
| `wp:bdbChemspider` | 13,820 |
| `wp:bdbHmdb` | 11,728 |
| `wp:bdbKeggCompound` | 11,175 |
| `wp:bdbLipidMaps` | 5,011 |
| `wp:bdbReactome` | 3 |

## Identifier Families

The adapter normalizes recognized `identifiers.org` and RDF PubChem URIs to
plain graph node IDs:

| Source URI family | Node ID |
| --- | --- |
| `hmdb` | `HMDB:<id>` |
| `chebi` | `CHEBI:<id>` |
| `kegg.compound` | `KEGG.COMPOUND:<id>` |
| `chemspider` | `ChemSpider:<id>` |
| `pubchem.compound` | `PUBCHEM.COMPOUND:<id>` |
| RDF PubChem compound `CID...` | `PUBCHEM.COMPOUND:<cid>` |
| `cas` | `CAS:<id>` |
| `drugbank` | `DRUGBANK:<id>` |
| `inchikey` | `InChIKey:<id>` |
| `wikidata` | `Wikidata:<id>` |
| Wikidata entity URI | `Wikidata:<id>` |
| `lipidmaps` | `LIPIDMAPS:<id>` |
| `reactome` | `Reactome:<id>` |
| `knapsack` | `KNApSAcK:<id>` |
| `kegg.glycan` | `KEGG.GLYCAN:<id>` |
| `kegg.drug` | `KEGG.DRUG:<id>` |
| `chembl.compound` | `ChEMBL.COMPOUND:<id>` |
| `pubchem.substance` | `PUBCHEM.SUBSTANCE:<id>` |
| `pharmgkb.drug` | `PharmGKB.DRUG:<id>` |
| `pid.pathway` | `PID.PATHWAY:<id>` |

The last few families are unusual for a metabolite harmonization graph, but are
kept because this first pass intentionally preserves source-reported assertions.

## Node And Edge Mapping

Node:

- class: `MetaboliteIdentifier`
- `id`: normalized identifier string
- `names`: populated only on the source subject identifier node from
  `rdfs:label`, with `source="WikiPathways"` and
  `source_field="rdfs:label"`
- `synonyms`: not populated by this adapter

Edge:

- class: `MetaboliteIdentifierMappingEdge`
- direction: source subject identifier -> equivalent identifier
- details:
  - `source`: `WikiPathways`
  - `source_field`: `subject` for subject self-identification, or the final
    `wp:bdb*` predicate name for xrefs
  - `source_id`: normalized source subject identifier

Self-edges are skipped when the subject URI also appears in the `wp:bdb*`
values.

## Decisions

- Parse Turtle with `rdflib`; do not line-parse RDF.
- Ingest source-reported known bad assertions unchanged.
- Attach labels only to the source subject ID, not to every equivalent ID.
- Do not configure resolvers.
- Deduplicate nodes and edge detail assertions in memory.
- Run with normal output merge behavior, not `single_source=True`, because HMDB,
  WikiPathways, LipidMaps, and RefMet can emit the same identifier nodes and
  edges.
