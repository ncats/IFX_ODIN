# CTD Gene-Disease Ingest Design

## Goal
Add curated CTD gene-disease associations to the graph in `working.yaml`.

## Source
- File: `CTD_curated_genes_diseases.tsv.gz`
- URL: `https://ctdbase.org/reports/CTD_curated_genes_diseases.tsv.gz`
- Local path: `input_files/auto/ctd/CTD_curated_genes_diseases.tsv.gz`
- Version sidecar: `input_files/auto/ctd/ctd_version.tsv`

Observed file shape:
- columns: `GeneSymbol`, `GeneID`, `DiseaseName`, `DiseaseID`, `DirectEvidence`, `OmimIDs`, `PubMedIDs`
- rows: about 34k
- disease IDs: `MESH:` and `OMIM:`
- evidence values seen:
  - `marker/mechanism`
  - `therapeutic`
  - `marker/mechanism|therapeutic`

## Decisions

### Scope
- Use the curated CTD file, not the aggregate file.
- Develop only in `src/use_cases/working.yaml`.
- Do not recreate old TCRD disease crosswalk behavior.

### Disease handling
- Emit `Disease` nodes from CTD.
- Use CTD `DiseaseID` as the source disease ID.
- Use CTD `DiseaseName` as the disease name.
- Let `TranslatorNodeNormResolver` handle disease normalization.

Reason:
- CTD may contain diseases not already present from other sources.
- Disease equivalence belongs in the resolver layer, not CTD-specific adapter logic.

### Protein handling
- Emit `Protein` start nodes using `NCBIGene:<GeneID>`.

Reason:
- This is the chosen input form for the existing resolver path.

### Edge payload
Each CTD association becomes a `ProteinDiseaseEdge` with one detail record:
- `details[0].source = "CTD"`
- `details[0].evidence`: `DirectEvidence` split on `|`
- `details[0].pmids`: `PubMedIDs` split on `|`
- `details[0].source_id`: original CTD `DiseaseID`

We are not keeping:
- `OmimIDs` on the edge

Reason:
- `ProteinDiseaseEdge` records merge across sources, so source-specific payload belongs in `details`
- We want the original CTD disease identifier preserved on the edge.
- We do not want to add another source-specific equivalence signal that overlaps with resolver behavior.

## Data Mapping

### Disease node
- `Disease.id` = CTD `DiseaseID`
- `Disease.name` = CTD `DiseaseName`

### ProteinDiseaseEdge
- `start_node` = `Protein(id=f"NCBIGene:{GeneID}")`
- `end_node` = `Disease(id=DiseaseID, name=DiseaseName)`
- `details` = list containing one CTD disease association detail record

Notes:
- `pmids` are carried when CTD provides them; some curated rows legitimately have no PubMed IDs

## Emission
Emit in two groups:
1. deduped `Disease` nodes
2. `ProteinDiseaseEdge` edges

Adapter behavior:
- dedupe diseases by `DiseaseID`
- preserve CTD source text cleanly
- avoid CTD-specific disease normalization logic

## Version
Use the sidecar TSV produced during download.

Current approach:
- parse `# Report created:` from the file header
- write `version` and `version_date`

## Validation
- file header matches expected columns
- disease IDs are `MESH:` / `OMIM:`
- resolver accepts `NCBIGene:<GeneID>` inputs at an acceptable rate
- CTD diseases appear in the graph even when they were not already present from other sources
- edge counts are in the expected range for the curated file

## Out of Scope
- aggregate CTD associations
- old TCRD `DOID` / `MONDO` mapping behavior
- graph-to-TCRD conversion changes
- promotion to `target_graph.yaml` before working validation
