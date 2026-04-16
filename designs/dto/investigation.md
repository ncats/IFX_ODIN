# DTO Investigation Notes

Date: 2026-04-15

## Goal

Investigate how Pharos 3.19 handled Drug Target Ontology (DTO) protein classes and decide on an initial IFX_ODIN ingest path.

## High-Level Conclusion

Use `pharos319` as the source of truth for the first DTO pass.

The public DTO GitHub repository appears stale relative to what `pharos319` actually loaded. Legacy Pharos includes explicit dataset provenance showing that the DTO content came from newer Schurer Group handoff files in 2019 and 2020, not just from the older public GitHub release surface.

## Evidence From `pharos319`

Legacy schema tables already present:
- `dto`
- `p2dto`
- `ancestry_dto`
- direct protein columns: `protein.dtoid`, `protein.dtoclass`

Legacy schema refs:
- [pharos_tables_old.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/shared/sqlalchemy_tables/pharos_tables_old.py:604)
- [pharos_tables_old.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/shared/sqlalchemy_tables/pharos_tables_old.py:617)
- [pharos_tables_old.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/shared/sqlalchemy_tables/pharos_tables_old.py:933)

Observed row counts in `pharos319`:
- `dto`: `17,779`
- `p2dto`: `43,006`
- `ancestry_dto`: `43,376`
- proteins with non-null `dtoid`: `9,232`

Representative mapped classes:
- `DTO:05007624` `Enzyme`
- `DTO:05007405` `Transporter`
- `DTO:02300001` `G-protein coupled receptor`
- `DTO:03300101` `Kinase`
- `DTO:01300327` `Ion channel`

## Provenance Found In `pharos319`

`dataset` rows in `pharos319` show two DTO-related loads:

1. `Drug Target Ontology IDs and Classifications`
   - source: `Files DTO2UniProt_DTOv2.csv, Final_ProteomeClassification_Sep232019.csv from Schurer Group`
   - app: `load-DTO_Classifications.py`
   - datetime: `2019-10-17 17:49:08`

2. `Drug Target Ontology`
   - source: `File ../data/UMiami/dto_proteome_classification_only.owl from Schurer Group at UMiami`
   - app: `load-DTO.py`
   - app_version: `3.0.0`
   - datetime: `2020-01-20 15:31:48`

`provenance` confirms `dataset_id=89` for table `dto`.

Interpretation:
- `pharos319` uses DTO content newer than the obvious public GitHub release metadata
- first-pass IFX_ODIN ingest should therefore prefer `pharos319` over the public DTO repo

## Public DTO Repo Status

Public repo:
- [DrugTargetOntology/DTO](https://github.com/DrugTargetOntology/DTO)

Observed issue:
- the repo/release surface looks old
- latest GitHub release shown there is `Drug Target Ontology V1.1` from 2017-12-06

That is older than the 2019/2020 provenance in `pharos319`.

## First-Pass IFX_ODIN Modeling Choice

Initial graph model added:
- `DTOClass`
- `DTOClassParentEdge`
- `ProteinDTOClassEdge`

This mirrors the existing Pharos/TCRD DTO tables directly enough for a first pass:
- `dto` -> `DTOClass`
- `dto.parent_id` -> `DTOClassParentEdge`
- `p2dto` -> `ProteinDTOClassEdge`

For now, the direct `protein.dtoid` / `protein.dtoclass` legacy columns are treated as derived/denormalized legacy fields rather than the primary ingest path.

## Implementation Notes

Added reusable source base:
- [base.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/input_adapters/pharos_mysql/base.py)

Added DTO adapters:
- [dto_adapter.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/input_adapters/pharos_mysql/dto_adapter.py)

Added DTO graph model:
- [dto_class.py](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/models/dto_class.py)

Wired into:
- [working.yaml](/Users/kelleherkj/IdeaProjects/IFX_ODIN/src/use_cases/working.yaml)

`working.yaml` now:
- writes to local ArangoDB
- writes dataset artifacts to local MinIO
- pulls DTO from `pharos_credentials.yaml`

## Recommendation

For the first DTO pass:
- use `pharos319` DTO content directly
- validate graph shape and counts in `working.yaml`
- defer any attempt to replace this with a public DTO OWL ingest until there is a concrete reason to do so

## MySQL Conversion Decision

For the graph-to-MySQL path, keep DTO normalized instead of reproducing the old ancestor-expanded `p2dto` behavior:
- `dto`: ontology terms
- `dto_parent`: direct DTO parent edges
- `p2dto`: direct protein-to-DTO assignments only
- `ancestry_dto`: transitive closure derived in MySQL post-processing

Legacy compatibility fields still belong on `protein`:
- `protein.dtoid`
- `protein.dtoclass`

That keeps the graph and MySQL output aligned:
- direct assignments stay direct
- the tree is reconstructed through `dto_parent` and `ancestry_dto`
- downstream GraphQL/UI code can derive the displayed DTO lineage without needing ancestor-expanded `p2dto`
- the legacy `p2dto.generation` column is intentionally dropped in the new schema because it is always implicit in this normalized model

If revisited later:
- compare `pharos319.dto` against current public DTO OWL files
- decide whether the public ontology has caught up or whether DTO still requires a handoff/source outside the repo

## Local Validation Outcome

Validated end to end with:
- local Arango graph build
- Pharos/target-graph post-processing
- local MySQL export via `working_mysql.yaml`

Observed outcomes:
- `DTOClass`, `DTOClassParentEdge`, and direct `ProteinDTOClassEdge` loaded into the graph
- `dto`, `dto_parent`, `p2dto`, and `ancestry_dto` populated in local MySQL
- `dto.parent_id` was correctly repopulated from `dto_parent`
- `p2dto` contains direct assignments only
- `ProteinDTOClassEdge` no longer carries a legacy `generation` field
- `p2dto.generation` is intentionally removed from the new MySQL schema

This confirms the normalized DTO path works without recreating the legacy ancestor-expanded `p2dto` layout.
