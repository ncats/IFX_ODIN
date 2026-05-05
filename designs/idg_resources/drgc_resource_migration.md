# DRGC Resource Migration To Pharos400

## Goal

Load legacy `pharos319.drgc_resource` rows into `pharos400.drgc_resource` without depending on the live RSS API or adding a graph ingest path.

## Discovery Summary

- Legacy source script: [load-DRGC_Resources.txt](/Users/kelleherkj/IdeaProjects/IFX_ODIN/designs/idg_resources/load-DRGC_Resources.txt)
- Live RSS docs page still resolves, but API requests currently time out and the TLS certificate is expired.
- `drgc_resource` exists in both legacy and new SQLAlchemy schemas with the same core payload:
  - `rssid`
  - `resource_type`
  - `target_id`
  - `json`
- `pharos400.drgc_resource` adds nullable `provenance`.

## Legacy Database Findings

- `pharos319.drgc_resource` row count: `2849`
- Distinct legacy targets: `316`
- Resource types observed:
  - `Genetic Construct`: `2415`
  - `Mouse Image-based Expression`: `114`
  - `NanoBRET`: `95`
  - `Cell`: `76`
  - `Mouse`: `52`
  - `Mouse Phenotype`: `46`
  - `Chemical Tool`: `38`
  - `Expression`: `7`
  - `Antibody`: `6`
- Legacy join path is `drgc_resource.target_id -> t2tc.target_id -> protein.id`.

## Mapping Decision

- Do not copy legacy `target_id` directly into `pharos400`.
- Map each row by legacy protein `uniprot`, then derive the destination `target_id` from the current `pharos400` `target`/`t2tc`/`protein` mapping.
- This avoids relying on raw legacy surrogate keys and stays correct if destination IDs differ from `pharos319`.

## Coverage Findings

- Legacy DRGC target proteins matched in `pharos400` by UniProt: `315 / 316`
- One unmatched legacy UniProt:
  - `Q5JXX5` (`GLRA4`)
- Symbol drift observed for matched proteins, reinforcing the choice to key on UniProt instead of symbol:
  - `O15218`: `GPR182` -> `ACKR5`
  - `Q8IZF7`: `ADGRF2` -> `ADGRF2P`

## First-Pass Implementation Scope

- Add a legacy Pharos MySQL adapter that reads `drgc_resource` plus joined UniProt.
- Emit lightweight DRGC resource records into the MySQL ETL path only.
- Add a `pharos400` converter that looks up destination `target_id` by UniProt and writes `drgc_resource`.
- Start in `src/use_cases/working_mysql.yaml` only.

## Deferred

- No graph model or Arango ingest in this pass.
- No attempt to refresh from RSS directly while the API is unstable.
- No promotion to `src/use_cases/pharos/tcrd.yaml` until working validation is complete.
