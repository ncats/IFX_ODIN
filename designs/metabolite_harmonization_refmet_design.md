# RefMet Metabolite Harmonization Ingest Design

## Scope

First-pass RefMet ingest for the `metabolite_harmonization` graph.

This pass emits source-reported identifier equivalence structure plus RefMet
common names. RefMet class fields are present but deferred for a later pass.

## Source Snapshot

Registry source:

- `refmet:metabolites_csv`

Observed local snapshot:

- `refmet:metabolites_csv:sha256-f40f14165725`
- file: `refmet.csv`
- rows: 205,944

The source CSV currently has a leading space in the `refmet_id` header. The
adapter strips whitespace from field names before mapping.

## Observed Fields

Relevant first-pass fields:

- `refmet_id`
- `refmet_name`
- `pubchem_cid`
- `chebi_id`
- `hmdb_id`
- `lipidmaps_id`
- `kegg_id`
- `inchi_key`

Class fields observed but deferred:

- `super_class`
- `main_class`
- `sub_class`

## Mapping

Primary node:

- `refmet_id` -> `REFMET:<refmet_id>`

Names:

- `refmet_name` -> `names` with `source="RefMet"` and
  `source_field="refmet_name"`

Equivalent IDs:

| CSV field | Node ID |
| --- | --- |
| `pubchem_cid` | `PUBCHEM.COMPOUND:<value>` |
| `chebi_id` | `CHEBI:<value>` |
| `hmdb_id` | `HMDB:<value>` |
| `lipidmaps_id` | `LIPIDMAPS:<value>` |
| `kegg_id` | `KEGG.COMPOUND:<value>` |
| `inchi_key` | `InChIKey:<value>` |

Edges use `MetaboliteIdentifierMappingEdge` from the RefMet primary identifier to
each equivalent identifier, with `details.source="RefMet"`.
