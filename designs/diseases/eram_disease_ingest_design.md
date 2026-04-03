# ERAM Disease Association Ingest Design

## Status

Discovery only. No code changes yet.

Current recommendation: do **not** build a fresh ERAM ingest from the public downloadable files unless there is a strong requirement to reconstruct the source from scratch. The public ERAM payload appears stale and partially malformed. If ERAM coverage is needed in current Pharos/TCRD outputs, prefer copying or migrating the legacy `eRAM` rows already present in `pharos319`.

## Goal

Add ERAM rare-disease target associations into the Pharos graph as disease associations, then make them available to the TCRD MySQL export path.

First-pass scope should stay narrow:

- ingest ERAM disease associations only
- preserve source-specific payload needed for `disease` rows
- avoid speculative normalization beyond what the raw payload clearly provides

Given the discovery results below, this goal is now probably better treated as a **legacy-data carry-forward task** rather than a new-source ingest task.

## Discovery Findings

### Repo state

- No ERAM adapter exists in this repo yet.
- No ERAM files are currently present under `input_files/`.
- `src/use_cases/pharos/TCRD_TODO.md` already lists ERAM under planned additional disease associations.

### Historical TCRD / Pharos evidence

Read-only queries against `pharos319` show that ERAM previously populated the `disease` table:

- `dtype='eRAM'`
- row count: 14,660
- distinct diseases: 1,380
- distinct proteins: 5,139

Observed field shape in `pharos319` sample rows:

- `did` is consistently `DOID:*`
- `name` is populated
- `source` is populated, often as pipe-delimited provenance such as:
  - `CTD_human`
  - `UniProtKB-KW`
  - `GHR`
  - `GWASCAT`
  - `ORPHANET`
  - combinations of the above
- `evidence` is `NULL` in sampled rows
- `description` is `NULL` in sampled rows

Top historical `source` values seen in `pharos319`:

- `CTD_human`
- `UniProtKB-KW`
- `GHR`
- `GWASCAT`
- `ORPHANET`
- `UNIPROT`
- `CLINVAR`

Current `pharos400` status:

- `disease` rows with `dtype='eRAM'`: 0

### External source discovery

- The original eRAM publication is:
  - Jia et al., NAR 2018, "eRAM: encyclopedia of Rare disease Annotations for Precision Medicine"
- The paper describes eRAM as an integrated rare-disease resource assembled from multiple upstream sources.
- A current downloadable page is available at:
  - `http://119.3.41.228/eram/download.php`
- The download page advertises:
  - `eRAM Gene.zip` — version `v2.00`
  - `eRAM Integrated Phenotype.txt` — version `v2.00`
  - `eRAM Integrated Symptom.txt` — version `v2.00`
  - `Text Mining Test Set.zip` — version `v2.00`
- `eRAM Gene.zip` HTTP metadata:
  - `Last-Modified: 2019-03-28`
  - `Content-Length: 806420`

### Real payload shape from `eRAM Gene.zip`

The archive contains three files:

- `eRAM Text Mined Gene.txt`
- `eRAM Curated Gene.txt`
- `eRAM Inferring Gene.txt`

#### `eRAM Text Mined Gene.txt`

- Header: `Disease<TAB>Text Mined Gene`
- One tab-delimited row per disease.
- No stable disease identifier column was observed.
- The gene field is a semicolon-delimited list of entries shaped like:
  - `95|ACY1`
  - `538|ATP7A`
- This appears to be `NCBI Gene ID|symbol`.

#### `eRAM Curated Gene.txt`

- Header: `Disease<TAB>Curated Gene`
- One tab-delimited row per disease.
- No stable disease identifier column was observed.
- The gene field is a `#`-delimited list of entries shaped like:
  - `BTD|686|CLINVAR;GHR;ORPHANET;UNIPROT`
  - `GBA|2629|CLINVAR;GHR;UNIPROT;UniProtKB-KW`
- This appears to be:
  - gene symbol
  - NCBI Gene ID
  - semicolon-delimited ERAM integrated provenance sources

#### `eRAM Inferring Gene.txt`

- Header: `Disease<TAB>Inferring Gene`
- The data rows inspected do **not** contain the tab delimiter advertised by the header.
- Example rows look like:
  - `biotinidase deficiencyBTD|CIPHER;CTD_human|686`
  - `rett syndromeAPOE|CIPHER|348#BDNF|CIPHER|627#...`
- This file appears malformed or at least inconsistently encoded.
- The gene entries appear to mix:
  - gene symbol
  - provenance source list such as `CIPHER;CTD_human`
  - NCBI Gene ID
- Because the disease name is concatenated directly to the first gene token, disease/gene boundary recovery will need explicit parsing and validation if this file is included.

## Initial Mapping Hypothesis

If a current ERAM file can be obtained and its payload matches the historical Pharos use case, first-pass graph output should likely be:

- `Disease` nodes
- disease association edges, ideally compatible with current `ProteinDiseaseEdge.details`

Likely detail fields of interest, based on historical MySQL output:

- source-specific provenance string or source list
- disease label
- possibly a subtype such as curated / inferred / text-mined if we decide to preserve file-of-origin

## Likely Normalization Expectations

- Historical `pharos319` output used `DOID:*` in `disease.did`.
- The current downloadable gene files do **not** expose a disease ID column in the rows inspected.
- Historical `pharos319` DOID values therefore almost certainly came from an additional mapping or normalization step outside these files.
- This must be verified against the real raw payload before deciding whether the new ingest should:
  - emit canonical `Disease(id='DOID:...')` nodes directly
  - preserve another source disease identifier and rely on later normalization
  - derive DOID mappings from another ERAM file or a separate lookup resource

## Open Questions

- What is the current authoritative ERAM download source?
- Is ERAM still publicly accessible as raw downloadable tables?
- What exact file contains the disease-to-gene or disease-to-target associations?
- Is there a separate ERAM disease metadata file with disease identifiers that is not exposed on the main gene download page?
- Does the raw payload use genes, proteins, or mixed identifiers?
- Does the raw payload already provide DOID disease identifiers somewhere else, or is disease-name normalization required?
- How should the multi-source ERAM provenance string be represented in `DiseaseAssociationDetail`?
  - current detail model supports `source`, `source_id`, `evidence_terms`, `pmids`, `evidence_codes`
  - it does not yet have a dedicated field for an ERAM-integrated source list
- Should first pass include all three files, or only the well-formed `Curated` and `Text Mined` files while `Inferring` is profiled further?

## Risks

- The current ERAM site or download endpoints may no longer be available.
- Historical `pharos319` rows may reflect a loader-specific transformed export rather than a still-available raw ERAM table.
- If the source is gene-based, the Pharos build may rely on resolver side-lifting into protein-facing disease associations.
- If ERAM provenance is important and more complex than the current detail model allows, a small model extension may be needed.
- The current downloadable files are disease-name keyed rather than disease-ID keyed, so naïve ingest would create unstable disease nodes.
- `eRAM Inferring Gene.txt` currently appears malformed and may require custom repair logic or temporary exclusion.

## Recommendation

Do not spend implementation effort re-parsing the public ERAM download unless a concrete requirement depends on rebuilding from source files.

Preferred path if ERAM is still desired:

1. Treat ERAM as a legacy backfill/migration problem.
2. Reuse the already transformed `eRAM` rows from `pharos319`.
3. Map those legacy rows into the current `pharos400` / graph-derived output model as directly as possible.

Rationale:

- `pharos319` already contains the shape we actually want:
  - protein-linked disease associations
  - `DOID:*` disease identifiers
  - populated source strings
- the public ERAM files are older than the historical Pharos rows and do not expose stable disease IDs in the inspected gene files
- at least one downloadable gene file appears malformed
- rebuilding the old normalization pipeline from these files would be high-effort and brittle relative to the likely value

## Minimal Implementation Plan

If this work is revived despite the recommendation above, the source-ingest path would be:

1. Add ERAM download rules and version capture for the current files.
2. Profile the real files:
   - columns
   - disease ID family
   - target ID family
   - provenance/source fields
   - row counts
3. Decide the first-pass scope:
   - likely start with `Curated` and `Text Mined`
   - defer `Inferring` unless its row structure can be recovered cleanly
4. Define a disease normalization strategy before graph emission.
5. Compare payload shape against historical `pharos319` ERAM rows.
6. Implement a minimal ERAM adapter in `src/use_cases/working.yaml` first.
7. Validate the working graph before any promotion into `src/use_cases/pharos/target_graph.yaml`.

The more pragmatic plan is:

1. Profile legacy `eRAM` rows in `pharos319`.
2. Decide the minimal target schema/output we still want in current Pharos/TCRD.
3. Implement a carry-forward or migration path from `pharos319` instead of re-parsing ERAM downloads.

## Validation Targets Once Files Exist

- Representative ERAM records land in the working graph with the expected disease IDs and association details.
- Resolver behavior correctly maps source targets into Pharos protein-facing associations.
- Working MySQL output can reproduce at least the core historical shape:
  - `dtype='eRAM'`
  - disease name
  - disease identifier
  - source/provenance handling
