# UniProt Disease Ingest Design

## Goal
Add UniProt disease associations to the graph in a source-aware way that merges cleanly with other disease sources.

## Source
- File: `uniprot-human.json.gz`
- Version sidecar: `uniprot_version.tsv`
- Current adapter: `ProteinDiseaseEdgeAdapter`

Current UniProt disease comments provide:
- disease cross-reference IDs such as `OMIM:...`
- disease names
- disease descriptions
- evidence records with ECO codes
- PubMed-backed evidence IDs in many cases

## Decisions

### Scope
- Keep UniProt disease ingest in the graph.
- Use modern UniProt JSON fields directly.
- Do not recreate old `pharos319` numeric evidence codes.

### Disease handling
- Yield `Disease` nodes from UniProt.
- Set `Disease.id` from the UniProt disease cross-reference.
- Set `Disease.name` from UniProt disease name.
- Set `Disease.uniprot_description` from the UniProt disease description.

Reason:
- Description is a disease-level property, not an edge-level property.

### Edge payload
Each UniProt disease association becomes a `ProteinDiseaseEdge` with one detail record:
- `details[0].source = "UniProt"`
- `details[0].source_id` = original UniProt disease cross-reference ID
- `details[0].evidence_codes` = ECO evidence codes from UniProt
- `details[0].pmids` = PubMed IDs from UniProt evidence records

We are not keeping:
- legacy numeric UniProt evidence codes from `pharos319`

Reason:
- ECO is the modern evidence system and is better than the old homegrown numbering.
- `ProteinDiseaseEdge` records merge across sources, so source-specific payload belongs in `details`.

## Data Mapping

### Disease node
- `Disease.id` = UniProt disease cross-reference ID
- `Disease.name` = UniProt disease name
- `Disease.uniprot_description` = UniProt disease description

### ProteinDiseaseEdge
- `start_node` = UniProt protein accession
- `end_node` = `Disease(id=...)`
- `details` = list containing one UniProt disease association detail record

Notes:
- `pmids` are carried when UniProt provides PubMed-backed evidence
- `evidence_codes` are stored as ECO strings for now
- no typed disease-evidence wrapper is needed yet

## Emission
Emit in two groups:
1. deduped `Disease` nodes
2. `ProteinDiseaseEdge` edges

Adapter behavior:
- dedupe diseases by UniProt disease cross-reference ID
- preserve source-scoped detail data on the edge
- allow edge merging with CTD and other sources via `details`

## Validation
- `Disease.uniprot_description` is populated on UniProt disease nodes
- UniProt edge details contain `source = "UniProt"`
- `source_id` is present on UniProt details
- `evidence_codes` contain ECO codes when provided
- `pmids` are populated when present in UniProt evidence records
- merged disease edges retain both UniProt and CTD detail records where overlaps exist

## Out of Scope
- recreating old `pharos319` numeric evidence encodings
- special handling for old TCRD disease table behavior
- introducing a typed disease-evidence object beyond plain ECO strings
