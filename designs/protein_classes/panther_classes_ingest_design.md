# PANTHER Protein Classes Ingest Design

## Goal

Add PANTHER protein classes and evolutionary family/subfamily membership to the Pharos graph ingest, then bridge the protein-class portion into the Pharos MySQL path.

## Discovery Date

- 2026-04-14

## Source Inputs

Current official files inspected during discovery:

- `https://data.pantherdb.org/PANTHER19.0/ontology/Protein_Class_19.0`
- `https://data.pantherdb.org/PANTHER19.0/ontology/Protein_class_relationship`
- `https://data.pantherdb.org/ftp/sequence_classifications/current_release/PANTHER_Sequence_Classification_files/PTHR19.0_human`

Legacy TCRD comparison loader:

- `https://github.com/unmtransinfo/TCRD/blob/master/loaders/load-PANTHERClasses.py`

## Legacy TCRD Behavior

The old loader populated:

- `panther_class`
- `p2pc`

Legacy ingest behavior observed from the loader:

- Loaded class definitions from `Protein_Class_14.0`
- Loaded parent relationships from `Protein_class_relationship`
- Loaded protein-to-class assignments from the human sequence classification file
- Matched proteins by UniProt first, then HGNC fallback
- Stored parent IDs as pipe-delimited `parent_pcids`
- Skipped rows without class assignments

Initial Pharos MySQL comparison during discovery:

- `pharos319.panther_class`: `256` rows
- `pharos319.p2pc`: `22,520` rows
- `pharos400.panther_class`: `0` rows
- `pharos400.p2pc`: `0` rows

This appears to be a real gap in the newer Pharos path rather than an already-migrated target-graph ingest.

## Observed Payload Shape

### `Protein_Class_19.0`

Observed shape:

- 3 metadata comment lines starting with `!`
- 239 non-comment class rows
- 4 tab-delimited columns per data row
  - `pcid`
  - hierarchical numeric code
  - class name
  - description

Examples:

- `PC00000` root class with blank description
- `PC00197` name `transmembrane signal receptor`

Notes:

- The file path is under `PANTHER19.0`, but the file header says `version: 17.0` and `date: 1/11/2022`
- No duplicate class IDs were observed in the current file

### `Protein_class_relationship`

Observed shape:

- 2 metadata comment lines starting with `!`
- 214 non-comment rows
- 5 tab-delimited columns per data row
  - child `pcid`
  - child name
  - parent `pcid`
  - parent name
  - level/order code

Notes:

- Each child class had one parent in the current file
- 214 distinct child IDs were observed

### `PTHR19.0_human`

Observed shape:

- 19,450 rows
- 11 tab-delimited columns on every row
- No header row

Observed columns by position:

1. `species|HGNC|UniProtKB` compound field
2. UniProt accession
3. gene symbol
4. PANTHER family / subfamily ID
5. family / subfamily name
6. protein name
7. molecular function GO bundle
8. biological process GO bundle
9. cellular component GO bundle
10. protein class bundle
11. pathway bundle

Protein class encoding in the current file:

- Class assignments are in column `10` as `name#PCxxxxx`
- Multiple assignments are semicolon-delimited
- Example: `G-protein modulator#PC00022;protein-binding activity modulator#PC00095`

Profile summary:

- 13,987 rows with at least one protein class
- 5,463 rows without any protein class assignment
- 21,948 total protein-to-class links parsed
- 194 distinct class IDs observed in the sequence file
- 6,939 rows had multiple class assignments

Important drift from the legacy loader:

- The old loader parsed class tokens from `row[8]`
- In the current file, protein class IDs are in `row[9]`
- `row[10]` now contains pathway data

## Identifier Findings

Protein identifiers available per sequence row:

- UniProt accession in column 2
- HGNC ID embedded in column 1
- gene symbol in column 3

Initial ingest choice:

- Emit `Protein(id=<UniProt accession>)` and rely on the configured target graph resolver path
- Keep HGNC as fallback parsing material only if resolver coverage shows UniProt misses

Class identifiers:

- Stable source IDs are PANTHER class IDs such as `PC00197`
- IFX constant already exists for `PANTHER.FAMILY` in `src/constants.py`

## Graph Mapping

Implemented graph scope:

- New `PantherFamily` node for evolutionary family / subfamily membership
- New `ProteinPantherFamilyEdge`
- New `PantherFamilyParentEdge`
- New `PantherClass` node
- New `ProteinPantherClassEdge`
- New `PantherClassParentEdge`

Implemented `PantherClass` node fields:

- `id`: `PCxxxxx`
- `name`
- `description`
- `hierarchy_code`

Decision:

- Do not put `parent_pcids` on graph nodes
- Keep parent-child structure represented only as edges in the graph
- If `pharos400` needs `parent_pcids`, derive it downstream during table materialization rather than denormalizing the source graph

### Evolutionary family / subfamily graph

Observed shape from `PTHR19.0_human`:

- Column 4 carries IDs like `PTHR23158:SF54`
- All inspected rows used the `family:subfamily` form
- 7,526 distinct top-level `PTHR...` family IDs were observed
- The file groups proteins into evolutionary family / subfamily membership
- This is distinct from the `PCxxxxx` protein class hierarchy

Implemented modeling:

- Emit one `PantherFamily` node type using `PANTHER.FAMILY`
- Represent both family and subfamily with a `level` field
- Emit parent edges from subfamily to family
- Emit protein membership to the subfamily node only
- Do not infer extra deeper hierarchy beyond family -> subfamily

Name handling decision:

- The current human sequence file provides a stable text label per top-level family
- It does not clearly provide a separate trustworthy subfamily label
- Keep family `name`
- Leave subfamily `name` unset rather than speculating

## Final Inclusion / Exclusion Decisions

Included:

- PANTHER protein class nodes from the ontology file
- parent-child class hierarchy
- human protein-to-class edges from the current human sequence classification file
- PANTHER family / subfamily nodes from the human sequence classification file
- subfamily -> family edges
- protein -> subfamily edges
- `panther_class` / `p2pc` materialization in `working_mysql.yaml`

Excluded:

- GO annotations embedded in the sequence file
- pathway assignments embedded in the sequence file
- hash-prefixed ontology rows such as `#PC...`
- inline ontology comment rows such as `#removed ...`

## Open Questions / Risks

- Versioning is not clean: the ontology URLs live under `PANTHER19.0`, while the ontology file header reports `17.0`
- Current cleaned class set is lower than legacy `pharos319.panther_class`
- Current class hierarchy differs from legacy `pharos319` for at least some nodes, for example `PC00233`

## Resolver Coverage Audit

Resolver coverage was checked against the current `PTHR19.0_human` UniProt IDs.

Results:

- `TargetGraphProteinResolver`: `19,433 / 19,450` matched (`99.91%`)
- `TCRDTargetResolver`: `19,368 / 19,450` matched (`99.58%`)

Interpretation:

- UniProt-only emission is the correct identifier strategy for this source
- The remaining misses are not an identifier-family problem
- The extra drop in `TCRDTargetResolver` appears to reflect Pharos target-universe coverage rather than parser failure
- Representative `TCRDTargetResolver` misses include some olfactory receptors, immunoglobulin variants, keratin-associated proteins, and unnamed / locus-style entries

## Implementation Notes

Implemented pieces:

1. Added download rules and version metadata under `input_files/auto/panther/`.
2. Added `PantherFamily`, `PantherClass`, and related edge models.
3. Added the flat-file graph ingest adapter in `src/input_adapters/panther/panther_classes.py`.
4. Added the Arango -> MySQL bridge adapters in `src/input_adapters/pharos_arango/tcrd/panther.py`.
5. Added `TCRDOutputConverter` mappings for `panther_class` and `p2pc`.
6. Validated in `working.yaml` and `working_mysql.yaml`.
7. Promoted the graph adapter into `src/use_cases/pharos/target_graph.yaml` and `src/use_cases/pharos/pharos.yaml`.

`parent_pcids` decision:

- Graph nodes do not store `parent_pcids`
- `parent_pcids` is derived downstream for MySQL from direct `PantherClassParentEdge` links
- This matches legacy Pharos API expectations more closely than storing full ancestry
- Duplicate parent IDs are intentionally not preserved

## Validation Targets

Minimum validation after implementation:

- node count for `PantherClass`
- edge count for protein-to-class links
- edge count for parent-child links
- spot-check several known proteins from `PTHR19.0_human`
- compare raw distinct class IDs and protein-class link counts to working graph output
- compare working MySQL path if applicable before promotion

## Validation Outcome

Validated graph outcome in rebuilt working graph:

- `PantherClass` is searchable on `name` and `description`
- `PantherClass` has no `#PC...` artifact rows after cleanup
- `PantherFamily` / `PantherClass` collections and edges loaded successfully

Validated MySQL outcome in `pharos400_working`:

- `panther_class`: `210` rows
- `p2pc`: `21,848` rows
- `#PC...` rows in `panther_class`: `0`
- multi-parent `parent_pcids` rows: `0`

Representative MySQL rows:

- `PC00021 -> PC00197`
- `PC00197 -> PC00000`
- `PC00233 -> PC00197`

Observed source cleanup impact:

- The downloaded ontology file contains hash-prefixed rows and inline comment rows
- Those rows are not used in the human protein assignment file
- Excluding them produces a cleaner protein-facing class set for Pharos
