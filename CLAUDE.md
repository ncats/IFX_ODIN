# CLAUDE.md - IFX_ODIN

## Project Overview

IFX_ODIN (Ontology and Data Integration Network) is a modular ETL platform developed at NCATS for biomedical data integration. It powers translational science platforms including Pharos, RaMP-DB, GSRS, and CURE ID.

**Primary contacts:** Jessica Maine, Keith Kelleher

## Tech Stack

- **Language:** Python 3.9+
- **Workflow:** Snakemake
- **ORM:** SQLAlchemy
- **Data models:** dataclasses, LinkML
- **Databases:** MySQL, ArangoDB, Neo4j/Memgraph
- **APIs:** FastAPI, Strawberry GraphQL

## Project Structure

```
src/
├── constants.py           # DataSourceName, Prefix enums (200+)
├── core/                  # ETL engine, config loading
├── input_adapters/        # Data source adapters (40+ sources)
├── output_adapters/       # Database writers (MySQL, ArangoDB, Neo4j, JSONL)
├── interfaces/            # Abstract base classes
├── models/                # Dataclass models (Node, Relationship, etc.)
├── id_resolvers/          # Entity normalization
├── shared/                # Utilities, SQLAlchemy tables
└── use_cases/             # Build scripts, YAML configs, secrets
workflows/                 # Snakemake files for data download
tests/                     # pytest tests
*-container/               # Docker compose for databases
```

## Build & Run Commands

```bash
# Environment setup
python -m venv .venv
source .venv/bin/activate
export PYTHONPATH=/path/to/IFX_ODIN:$PYTHONPATH
pip install -r requirements.txt

# Run tests
pytest

# Download data files
cd workflows && snakemake -j 4

# Run ETL from YAML config
python -c "
from src.use_cases.build_from_yaml import BuildGraphFromYaml
builder = BuildGraphFromYaml('src/use_cases/pounce_v2.yaml')
builder.truncate_datastore()
builder.do_etl(do_post_processing=True)
"

# Start containers
cd mysql-container && docker compose up -d
cd memgraph-container && docker compose up -d
```

## Architecture

**Adapter Pattern + ETL Pipeline:**

1. **InputAdapters** - Extract from data sources via `get_all()` generator
2. **OutputAdapters** - Write to databases via `store()`
3. **ETL Engine** - Orchestrates pipeline with resolver_map and labeler
4. **Id Resolvers** - Entity normalization with configurable match behavior

```python
# Creating an input adapter
class CustomAdapter(InputAdapter):
    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        yield batch  # Batches of 25,000 by default

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.XXX
```

## Key Patterns

- **Generators** for memory-efficient batch processing
- **YAML configs** drive adapter selection and parameters
- **Provenance tracking** on all entities
- **@facets decorator** marks searchable fields on models
- **RecordMerger** handles field conflicts during data integration

## Database Access

Credentials in `src/use_cases/secrets/local_credentials.yaml`:
- MySQL: localhost:3306
- Memgraph Lab: http://localhost:3000
- Memgraph query: localhost:7687

## Key Files

| File | Purpose |
|------|---------|
| `src/core/etl.py` | Main ETL orchestrator |
| `src/core/config.py` | YAML config parsing |
| `src/interfaces/input_adapter.py` | Base adapter class |
| `src/interfaces/output_adapter.py` | Output writer base |
| `src/models/node.py` | Core Node/Relationship classes |
| `src/use_cases/build_from_yaml.py` | Generic YAML-driven builder |

## Current Work

- POUNCE v2 database with UniProt protein annotations
- Schema migration: v1 models in `models/pounce_v1/`, v2 in `models/pounce/`
- SQLAlchemy table refactoring (Sample -> Biosample)

### POUNCE Input Adapter (`src/input_adapters/pounce_sheets/`)

Parses three Excel workbooks per project (all configured via `pounce_v2.yaml`):

| Workbook | Sheets | Purpose |
|----------|--------|---------|
| **Project** | ProjectMeta, BioSampleMap, BioSampleMeta | Project metadata, biosamples, biospecimens, exposures |
| **Experiment** | ExperimentMeta, RunBioSampleMap/Meta, GeneMap/Meta or MetabMap/Meta, RawData/Meta or PeakData/Meta | Experiment metadata, run-level samples, analytes, raw data matrix |
| **StatsResults** | StatsResultsMeta, StatsReadyData, EffectSize_Map, EffectSize | Statistical analysis results (normalized data, effect sizes) |

Sheet constants are in `constants.py` (`ProjectWorkbook`, `ExperimentWorkbook`, `StatsResultsWorkbook`). Map sheets define column name mappings from NCATS standard names to submitter names. Meta sheets contain the actual data rows.

**Data model graph:**
```
Project -> Experiment -> Dataset -> RunBiosample -> Biosample <- Project
                                -> Gene/Metabolite
Project -> Biosample -> Biospecimen
                     -> Exposure
```

- `Dataset` nodes carry a `_data_frame` field written to parquet by the ArangoOutputAdapter
- Dataset edges to RunBiosample and Gene/Metabolite make the data matrix relationships explicit in the graph
- `_parse_data_matrix()` is generic: handles RawData, PeakData, and StatsReadyData via `parser`, `analyte_id_col`, and `default_data_type` parameters
- Empty data sheets (e.g. PeakData for metabo) are skipped gracefully

**Supported templates:** Transcriptomics (genes, RawData) and Metabolomics (metabolites, PeakData). EffectSize parsing is not yet implemented.