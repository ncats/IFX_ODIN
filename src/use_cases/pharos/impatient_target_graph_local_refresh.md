# Impatient Target Graph Local TDL Refresh

This test-clone workflow rebuilds `impatient_target_graph` and exports
`current_tdls.csv` from the local QA Browser route.

## What Feeds TDLs

`TDLInputAdapter` calculates TDLs from:

- MOA drug count from `ProteinLigandEdge` where the ligand is a drug.
- Ligand activity count where `meets_idg_cutoff` is true.
- Experimental function/process GO leaf-term count.
- GeneRIF count.
- JensenLab protein PM score.
- Antibodypedia antibody count.

`TDLOverrideAdapter` then overwrites TDL values from
`target_graph:tdl_updates:<version>`.

## One-Time Setup

Use an environment with the project dependencies:

```bash
cd /Users/mainejl/Documents/Projects/ODIN/test_IFX_ODIN
python -m pip install -r requirements.txt
```

The local YAML files currently expect these local secrets:

- `src/use_cases/secrets/local_arangodb.yaml`.
- `src/use_cases/secrets/pharos_credentials.yaml`.

As of 2026-09-05, `pharos_credentials.yaml` is present. The current local build
does not require the Pharos319 registry snapshot for IDG family because
`impatient_target_graph_local.yaml` uses the Tudor-derived
`target_graph:tdl_updates:2026-09-05` file through `ManualIDGFamilyAdapter`.

Local-only Arango files
were created for the test clone:

- `arango-container/secrets.env` with Arango no-auth local mode.
- `src/use_cases/secrets/local_arangodb.yaml` using
  `url: http://127.0.0.1:8042`.

These files live under secret/ignored paths and should not be committed. Do not
write credential values into this file. A template exists at
`src/use_cases/secrets/local_arangodb.example.yaml`.

If using `arango-container/docker-compose.yml`, the container maps host port
`8042` to container port `8529`. The Arango adapter passes
`credentials.url` directly to `ArangoClient`, so the port must be included in
the URL itself, not only in a separate `port` field.

Start local Arango after creating `arango-container/secrets.env`:

```bash
cd /Users/mainejl/Documents/Projects/ODIN/test_IFX_ODIN/arango-container
docker compose up -d
```

On 2026-09-05 Docker Desktop had to be started first. The compose file also
requires the external Docker network `shared_dashboard_network`; create it once
if compose reports it missing:

```bash
docker network create shared_dashboard_network
```

The expected Arango health check is:

```bash
curl -sS http://127.0.0.1:8042/_api/version
```

The working local container was `local_arangodb`, with host port
`8042 -> 8529`.

## Refresh Inputs

Convert Tudor's 2026-09-04 workbook into the existing manual override format:

```bash
python -m src.use_cases.pharos.tdl_workbook_to_updates \
  /Users/mainejl/Downloads/PharosTDL_UniProt_20260904.xlsx \
  --register
```

The converted workbook currently contains 20,188 manual TDL rows:

- `Tclin`: 761 UniProt target assignments (+30 versus the prior 731 count).
  This includes the isoform assignment `P56856-2` / `CLDN18.2` / `Claudin-18 isoform 2`.
- `Tchem`: 2,011
- `Tbio`: 12,357
- `Tdark`: 5,059

Fetch latest Target Harmonizer IDs and register matching resolver snapshots:

```bash
python -m src.use_cases.pharos.fetch_target_harmonizer_ids \
  --version 2026-09-04 \
  --protein-file /Users/mainejl/Documents/Projects/ODIN/TargetGraph/TargetGraph7/src/data/publicdata/target_data/cleaned/resolved_node_ids/protein_ids.tsv \
  --register-resolvers
```

The explicit `--protein-file` matters for the current CLDN18.2 update:
`P56856-2` must be present as `uniprot_isoform` for
`IFXProtein:L7LS7LZ`, otherwise the TDL override can collapse to base
`P56856` or be skipped. The script validates isoform IDs in `tdl_updates.csv`
against the exported `protein_ids.tsv` before upload.

Stage current IFX Harmonizers evidence into the local registry cache:

```bash
python -m src.use_cases.pharos.stage_target_evidence_for_impatient_graph \
  --target-repo /Users/mainejl/Documents/Projects/ODIN/TargetGraph/TargetGraph7 \
  --registry-cache-dir ./registry_cache
```

Current staged evidence refs include:

- `target_graph:gene_ids:2026-09-05`
- `target_graph:transcript_ids:2026-09-05`
- `target_graph:protein_ids:2026-09-05`
- `drug_graph:drug_nodes:2026-09-05`
- `drug_graph:drug_edges:2026-09-05`
- `go:ontology:2026-08-08`
- `go:goa_human_uniprot:2026-07-28`
- `go:goa_human_go:2026-05-28`
- `target_graph:generif:2026-09-05`
- `jensenlab:protein_counts:2026-08-23`
- `antibodypedia:scraped_results:2026-09-05_12-27`

The current Antibodypedia snapshot is detail-enriched from gene pages. It keeps
Antibodypedia UniProt, Ensembl gene, Ensembl transcript, Ensembl protein,
RefSeq protein, and Entrez IDs as provenance columns while preserving the
`uniprot_id` and `antibodies` columns expected by `AntibodyCountAdapter`.
For CLDN18, this preserves the isoform row `P56856-2` via
`ENSP00000340939`.

## Build And Export

Build the graph:

```bash
PYTHONPYCACHEPREFIX=/private/tmp/ifx_pycache \
python -m src.use_cases.pharos.build_impatient_target_graph_local --yes
```

Start QA Browser:

```bash
PYTHONPYCACHEPREFIX=/private/tmp/ifx_pycache \
python -m src.qa_browser.app \
  --credentials ./src/use_cases/secrets/local_arangodb.yaml \
  --host 127.0.0.1 \
  --port 8050
```

Export the current TDLs:

```bash
curl -L \
  "http://127.0.0.1:8050/db/impatient_target_graph/view/current_tdls" \
  -o current_tdls.csv
```

## 2026-09-05 Local Build Result

The local `impatient_target_graph` build completed successfully after local
scratch-clone fixes:

- `src/use_cases/pharos/impatient_target_graph_local.yaml` uses
  `ManualIDGFamilyAdapter` with `target_graph:tdl_updates:2026-09-05` instead
  of `IDGFamilyAdapter` with `legacy_pharos:pharos319_mysql:pharos319`.
- `src/input_adapters/drug_harmonizer/drug_nodes_and_edges.py` raises
  Python's CSV field-size limit before reading `drug_edges_full.tsv`, because
  some `evidence_details` cells exceed the default 131,072 byte limit.
- `src/input_adapters/drug_harmonizer/drug_nodes_and_edges.py` treats
  `Ligand.isDrug` as explicit approved-drug evidence only
  (`approved_drug` in `drug_scope` or `approved` in `approval_status`). Clinical
  candidates, DrugCentral membership, RxNorm membership, and generic
  `biolink:Drug` category are not sufficient for Pharos Tclin scoring.
- `src/input_adapters/pharos_arango/tdl_input_adapter.py` supports
  `use_drug_count_for_tclin`; `impatient_target_graph_aql_post_local.yaml` sets
  it to `false` for Pharos412 so non-Tclin TDLs are graph-calculated and Tclin
  is assigned only from Tudor's Tclin overlay.
- `src/input_adapters/manual/tdl_override.py` supports `allowed_tdls`;
  `impatient_target_graph_aql_post_local.yaml` sets `allowed_tdls: [Tclin]`.
  Tudor's workbook must not be used as a general TDL override because the
  converted CSV includes non-Tclin rows.

The local Arango database exists at:

```text
http://127.0.0.1:8042/_db/impatient_target_graph
```

Final collection counts:

| Collection | Count |
| --- | ---: |
| Gene | 193,394 |
| GeneProteinEdge | 55,089 |
| GeneTranscriptEdge | 450,423 |
| GoTerm | 38,092 |
| GoTermHasParent | 57,456 |
| IsoformProteinEdge | 103,532 |
| Ligand | 1,345,917 |
| Protein | 166,833 |
| ProteinGoTermEdge | 707,720 |
| ProteinLigandEdge | 3,973,401 |
| Transcript | 533,740 |
| TranscriptProteinEdge | 123,956 |

Observed endpoint pairs match the June graph schema:

- `GeneProteinEdge`: `Gene -> Protein`
- `GeneTranscriptEdge`: `Gene -> Transcript`
- `GoTermHasParent`: `GoTerm -> GoTerm`
- `IsoformProteinEdge`: `Protein -> Protein`
- `ProteinGoTermEdge`: `Protein -> GoTerm`
- `ProteinLigandEdge`: `Protein -> Ligand`
- `TranscriptProteinEdge`: `Transcript -> Protein`

Export test:

```bash
curl -sS -L \
  "http://127.0.0.1:8050/db/impatient_target_graph/view/current_tdls" \
  -o current_tdls.csv
```

returned HTTP 200 and wrote 166,833 data rows plus the header
(`current_tdls.csv`, about 55 MiB).

TDL distribution in the exported file:

- All protein rows: `Tclin` 1,901; `Tchem` 7,760; `Tbio` 58,909;
  `Tdark` 98,263.
- Canonical rows only: `Tclin` 760; `Tchem` 3,292; `Tbio` 11,294;
  `Tdark` 5,216.
- Isoform rows: `Tclin` 1,141; `Tchem` 4,461; `Tbio` 13,491;
  `Tdark` 3,719.
- Alternate-product rows: `Tclin` 0; `Tchem` 7; `Tbio` 34,124;
  `Tdark` 89,328.

CLDN18.2/Tudor isoform check:

- `P56856-2` maps to `IFXProtein:L7LS7LZ`.
- `canonical_isoform_status`: `isoform`.
- `tdl`: `Tclin`.
- `P56856-1` remains the canonical row and is `Tchem`, not `Tclin`.
