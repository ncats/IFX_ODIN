# LipidMaps Metabolite Harmonization Ingest Design

## Scope

First-pass LipidMaps ingest for the `metabolite_harmonization` graph.

This pass emits source-reported identifier equivalence structure plus names and
synonyms. LipidMaps class fields are present and high coverage, but are deferred
for a later pass.

## Source Snapshot

Registry source:

- `lipidmaps:lmsd_sdf`

Observed local snapshot:

- `lipidmaps:lmsd_sdf:2026-06-30`
- file: `LMSD.sdf.zip`
- zip member: `structures.sdf`
- records: 50,515

## Observed Fields

Relevant first-pass tags:

- `LM_ID`
- `NAME`
- `SYSTEMATIC_NAME`
- `ABBREVIATION`
- `SYNONYMS`
- `PUBCHEM_CID`
- `CHEBI_ID`
- `HMDB_ID`
- `SWISSLIPIDS_ID`
- `LIPIDBANK_ID`
- `KEGG_ID`
- `PLANTFA_ID`
- `INCHI_KEY`

Class tags observed but deferred:

- `CATEGORY`
- `MAIN_CLASS`
- `SUB_CLASS`
- `CLASS_LEVEL4`

## Mapping

Primary node:

- `LM_ID` -> `LIPIDMAPS:<LM_ID>`

Names:

- `NAME` -> `names` with `source_field="NAME"`
- `SYSTEMATIC_NAME` -> `names` with `source_field="SYSTEMATIC_NAME"`
- `ABBREVIATION` -> `names` with `source_field="ABBREVIATION"`

Synonyms:

- `SYNONYMS` split on semicolon -> `synonyms` with `source_field="SYNONYMS"`

Equivalent IDs:

| SDF tag | Node ID |
| --- | --- |
| `PUBCHEM_CID` | `PUBCHEM.COMPOUND:<value>` |
| `CHEBI_ID` | `CHEBI:<value>` |
| `HMDB_ID` | `HMDB:<value>` |
| `SWISSLIPIDS_ID` | `SwissLipids:<value>` |
| `LIPIDBANK_ID` | `LipidBank:<value>` |
| `KEGG_ID` | `KEGG.COMPOUND:<value>` |
| `PLANTFA_ID` | `PlantFA:<value>` |
| `INCHI_KEY` | `InChIKey:<value>` |

Edges use `MetaboliteIdentifierMappingEdge` from the LipidMaps primary identifier to
each equivalent identifier, with `details.source="LipidMaps"`.
