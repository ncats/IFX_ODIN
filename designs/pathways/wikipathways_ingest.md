# WikiPathways Ingest Design

## Background

WikiPathways is a community-curated pathway database. The old TCRD loader
(`loaders/load-WikiPathways.py`, Steve Mathias 2015–2019) downloaded a GMT file
from PathVisio, resolved Entrez Gene IDs against the TCRD MySQL database, and
inserted rows into the `pathway` table with `pwtype = 'WikiPathways'`. This design
replaces that approach using the IFX_ODIN ETL pattern.

## Inputs

Downloaded via `workflows/pharos.Snakefile` into `input_files/auto/wikipathways/`:

- `wikipathways_human.gmt` — current Homo sapiens GMT from `https://data.wikipathways.org/current/gmt/`
  - Filename is versioned (e.g. `wikipathways-20260310-gmt-Homo_sapiens.gmt`); downloaded to a stable alias
- `wikipathways_version.tsv` — derived metadata:
  - `version`: date extracted from GMT filename (YYYY-MM-DD)
  - `version_date`: same

The Snakemake rule scrapes the index page to discover the latest GMT URL, downloads
it, and extracts the date from the filename for the version file.

## File Format (surveyed 2026-03-16)

GMT format: one pathway per line, tab-separated:

```
name%WikiPathways_YYYYMMDD%WPID%Homo sapiens \t url \t entrez_gene_id \t ...
```

- Column 0: `name%WikiPathways_YYYYMMDD%WPID%Homo sapiens`
- Column 1: URL (e.g. `https://www.wikipathways.org/instance/WP100`)
- Columns 2+: Entrez Gene IDs (all numeric, no symbols or mixed formats)

Observed shape (March 2026 release):
- 984 pathways, all matching the expected format
- 41,684 total gene entries, 100% numeric Entrez Gene IDs
- Pathway sizes: 1–509 genes

## Scope

- Pathway nodes for all 984 human pathways
- ProteinPathwayRelationship edges linking proteins to their pathways via Entrez Gene ID resolution
- **No pathway hierarchy** — WikiPathways has no parent-child pathway structure (unlike Reactome)

## Mapping to the data model

### Nodes
- **Pathway** (`src/models/pathway.py`)
  - `id`: WP stable ID (e.g. `WP100`)
  - `source_id`: same as `id`
  - `type`: `WikiPathways`
  - `name`: pathway name (text before first `%` in column 0)
  - `url`: column 1 (e.g. `https://www.wikipathways.org/instance/WP100`)

### Edges
- **ProteinPathwayRelationship** (`src/models/pathway.py`)
  - `start_node`: `Protein(id="NCBIGene:<entrez_id>")` — resolved to canonical UniProt protein by `tcrd_targets` resolver
  - `end_node`: `Pathway(id="WP###")`
  - `source`: `WikiPathways`

## Adapters (`src/input_adapters/wikipathways/wikipathways_pathways.py`)

- **`WikiPathwaysBaseAdapter`** — extends `FlatFileAdapter`
  - `get_datasource_name()` → `DataSourceName.WikiPathways`
  - `get_version()` → reads `wikipathways_version.tsv` for `version` and `version_date`
  - `_iter_parsed_lines()` — yields `(name, wpid, url, genes)` tuples

- **`WikiPathwaysPathwayAdapter`**
  - Input: `wikipathways_human.gmt`
  - Output: `Pathway` nodes

- **`WikiPathwaysProteinPathwayEdgeAdapter`**
  - Input: `wikipathways_human.gmt`
  - Output: `ProteinPathwayRelationship` edges
  - Emits `Protein(id="NCBIGene:<entrez_id>")` as `start_node`; resolver maps to canonical protein
  - Skips non-numeric gene IDs as a safety check (none observed, but defensive)

## Version strategy

WikiPathways does not expose a versioned API endpoint. The date is embedded in the
GMT filename (e.g. `wikipathways-20260310-gmt-Homo_sapiens.gmt`). The Snakemake rule
extracts it and writes to `wikipathways_version.tsv` as both `version` and
`version_date`. The adapter reads this file to populate `DatasourceVersionInfo`.

## Constants

Add `WikiPathways = "WikiPathways"` to `DataSourceName` in `src/constants.py`.

## YAML wiring

Both `src/use_cases/pharos/pharos.yaml` and `src/use_cases/pharos/target_graph.yaml`
should include these adapters. `pharos.yaml` is the production Pharos build;
`target_graph.yaml` is the broader target graph pipeline. New Pharos adapters should
be added to both.

### `pharos.yaml` / `target_graph.yaml`

```yaml
- import: ./src/input_adapters/wikipathways/wikipathways_pathways.py
  class: WikiPathwaysPathwayAdapter
  kwargs:
    file_path: ./input_files/auto/wikipathways/wikipathways_human.gmt
    version_file_path: ./input_files/auto/wikipathways/wikipathways_version.tsv

- import: ./src/input_adapters/wikipathways/wikipathways_pathways.py
  class: WikiPathwaysProteinPathwayEdgeAdapter
  kwargs:
    file_path: ./input_files/auto/wikipathways/wikipathways_human.gmt
    version_file_path: ./input_files/auto/wikipathways/wikipathways_version.tsv
```

## Comparison with old TCRD loader

| Aspect | Old loader | This design |
|--------|-----------|-------------|
| Download URL | `pathvisio.org/data/bots/gmt/current/` | `data.wikipathways.org/current/gmt/` |
| Gene ID source | Entrez Gene IDs | Same |
| Gene→Protein mapping | Direct MySQL lookup against TCRD | `tcrd_targets` resolver via `NCBIGene:` prefix |
| Pathway hierarchy | Not loaded | Not loaded (none exists) |
| WP ID source | Column 1 URL (split on `/`) | Column 0 meta field (regex) |
| Storage | MySQL `pathway` table, `pwtype='WikiPathways'` | ArangoDB graph → TCRD via existing pathway converter |

## Notes

- The file is already human-only (filename contains `Homo_sapiens`); no species filtering needed in the adapter.
- Gene IDs in the file are naked integers (no prefix). Wrap as `EquivalentId(id=gene_id, type=Prefix.NCBIGene)` to produce `NCBIGene:12345` for the resolver.
- No `PathwayParentEdge` — WikiPathways has no hierarchy.
- The TCRD MySQL pathway converter (`src/input_adapters/pharos_arango/tcrd/pathway.py`) already handles `ProteinPathwayRelationship`; WikiPathways edges will flow through it automatically once in the graph.