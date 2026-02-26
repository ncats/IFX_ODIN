# POUNCE Validation Tool

## Background

Data submitters fill out POUNCE Excel workbooks and send them to the NCATS team, who manually
load them into ArangoDB via the ETL pipeline. Errors in the submitted sheets are caught late,
requiring back-and-forth with submitters to fix problems.

Each submission consists of one Project workbook plus one or more Experiment workbooks. Each
Experiment workbook is paired with a StatsResults workbook (so there are always equal numbers
of each), giving a minimum of three files and more for multi-experiment submissions.

The goal is a self-service validation tool where submitters upload their sheets and immediately
see what is correct, what is missing, and what needs to be fixed — before anything reaches
the database.

A prior validation checkpoint spreadsheet exists (`pounce_input_validation_checkpoints.xlsx`,
authored by John) and was used as a reference. It reflects an older schema (different sheet
names, different field names) but captures the spirit of what needs to be checked and informed
the categories of validators described here.

---

## Architecture

### Validator framework (`src/core/validator.py`)

`ValidationError` carries: `severity` (error/warning), `entity`, `field`, `message`, and optional
`sheet`, `column`, `row`, `source_file`. The `source_file` field pins each error to the exact
xlsx file it came from, which matters when multiple experiment files are submitted together.

All validators implement:
```python
class Validator(ABC):
    def validate(self, data: ParsedPounceData) -> List[ValidationError]: ...
```

Built-in rule types:

| Class | Purpose |
|---|---|
| `RequiredValidator` | Field must be non-null/non-empty on a data entity |
| `AllowedValuesValidator` | Field must be one of an enumerated set |
| `ConditionalRequiredValidator` | Field required only when another field matches a value |
| `RequiredMapKeyValidator` | A key must be configured in a map sheet's `param_maps` entry |
| `ConditionalRequiredMapKeyValidator` | Map key required only when a condition on another entity is met |
| `ParallelListsValidator` | Two or more pipe-delimited list fields must have the same length |
| `IndexedGroupValidator` | If any field in an indexed group (e.g. `exposure1_*`) is set, all required fields in that group must be set |

### Two-stage validation

Validation is split into two stages that are reported together:

**Stage 1 — Structural issues** (`get_structural_issues()`): detected at parse time by
`PounceParser.parse_all()`. These are problems with the workbook structure itself that can't be
expressed as field-level rules: missing required sheets, sheets that can't be read, column names
in a meta sheet that weren't declared in the corresponding map sheet, data matrix rows referencing
IDs not found in the analyte meta sheet, data matrix column headers not matching RunBioSampleMeta
IDs. Each structural issue carries `source_file` pointing to the specific xlsx that caused it.

**Stage 2 — Content issues** (`get_validators()` → `validator.validate(parsed_data)`): field-level
rules applied to the parsed metadata objects. Configured in `pounce_validators.yaml` and loaded by
`validator_loader.py`.

### `get_validation_data()` returns metadata only

Data matrices (RawData, PeakData, StatsReadyData) can be 100k+ rows and are not needed for
field-level validation. `get_validation_data()` parses only the metadata sheets (Project,
Biosample, Experiment, RunBiosample, GeneMeta, MetabMeta, StatsResultsMeta) and returns a
`ParsedPounceData` container. This keeps validation fast regardless of data volume.

The parse result is cached on the adapter instance (`_cached_validation_result`) so that
`get_validation_data()` and `get_structural_issues()` share one parse call.

### `ParsedPounceData` container

`src/input_adapters/pounce_sheets/parsed_pounce_data.py` holds one parsed object per sheet type:

```
project, people, param_maps,
biosamples, biospecimens, demographics, exposures,
experiments, genes, metabolites,
peak_data_meta, raw_data_meta, run_biosamples,
stats_results
```

`param_maps` is a `Dict[str, Dict[str, str]]` keyed by sheet name (e.g. `"BioSampleMap"`),
holding the submitter-column-name → NCATSDPI-key mapping for each map sheet. The
`RequiredMapKeyValidator` and `IndexedGroupValidator` inspect this directly to check that
required columns were declared.

### Validator config is YAML, loaded by `validator_loader.py`

Validators are defined in `pounce_validators.yaml` with a sheet-centric structure:

```yaml
ProjectMeta:
  required: [project_name, description, ...]
  allowed_values:
    privacy_type: [private, ncats, public]
  parallel_lists:
    - [owner_name, owner_email]

BioSampleMap:
  required: [biosample_id, biosample_type, ...]
  indexed_group:
    - ["exposure{}_names", "exposure{}_type", "exposure{}_category"]

ExperimentMeta:
  required_if:
    metabolite_identification_description:
      when_field: platform_type
      when_values: [metabolomics, lipidomics]
```

`validator_loader.py` reads this YAML and instantiates the appropriate validator classes.
It uses `_SHEET_KEY_MAP`, derived from `sheet_field` metadata on all `Parsed*` dataclasses,
to translate `(sheet_name, ncatsdpi_key)` → `(entity_name, python_field_name)`. This means
adding a new field to a `Parsed*` class automatically makes it available in the YAML config
without touching the loader.

**Map sheet vs Meta sheet distinction:** The loader detects whether a sheet name is a map sheet
(BioSampleMap, GeneMap, MetabMap, RunBioSampleMap, EffectSize_Map). For map sheets it emits
`RequiredMapKeyValidator` / `ConditionalRequiredMapKeyValidator` (checking that the key was
declared in `param_maps`). For meta sheets it emits `RequiredValidator` /
`ConditionalRequiredValidator` (checking that row values are non-empty).

### Cross-sheet reference checks are structural, not content validators

Cross-sheet ID reference checks (RunBiosample → Biosample, RawData rows → GeneMeta IDs, etc.)
run at parse time and emit structural issues rather than going through the validator framework.
This gives each error a `source_file` that points to the specific experiment or stats file,
and keeps the validator framework focused on single-entity field rules.

### `sheet_field` metadata drives auto-mapping

Each field on a `Parsed*` dataclass carries `sheet_field` metadata:
```python
gene_id: Optional[str] = sheet_field(key="gene_id", sheet="GeneMeta")
```
`get_sheet_fields()` introspects any dataclass to return `(field, metadata)` pairs.
`validator_loader.py` builds `_SHEET_KEY_MAP` from all `Parsed*` classes at import time.
Adding a new field means adding one line to the dataclass — the loader and validators
pick it up automatically.

### Validation is separate from ETL

`validate_pounce.py` is a standalone script. It loads a YAML config (same format as
`pounce_v2.yaml` but with all `PounceInputAdapter` entries), calls `get_validation_data()`,
`get_structural_issues()`, and `get_validators()` on each adapter, and prints a grouped
report by workbook → sheet. No database writes occur.

---

## File Map

| File | Purpose |
|---|---|
| `src/core/validator.py` | `ValidationError`, `Validator` ABC, all built-in rule classes |
| `src/input_adapters/pounce_sheets/parsed_classes.py` | Annotated dataclasses for every sheet |
| `src/input_adapters/pounce_sheets/parsed_pounce_data.py` | `ParsedPounceData` container |
| `src/input_adapters/pounce_sheets/sheet_field.py` | `sheet_field()` metadata helper and `get_sheet_fields()` |
| `src/input_adapters/pounce_sheets/pounce_parser.py` | Parses workbooks → `ParsedPounceData` + structural issues |
| `src/input_adapters/pounce_sheets/pounce_input_adapter.py` | `PounceInputAdapter` — ETL + validation entry point |
| `src/input_adapters/pounce_sheets/validator_loader.py` | Loads `pounce_validators.yaml` → `List[Validator]` |
| `src/use_cases/pounce_validators.yaml` | Field-level validation rules |
| `src/use_cases/validate_pounce.py` | Standalone validation script |

---

## Future Considerations

**Web UI.**
A simple web tool where submitters upload their workbooks, validation runs, and results are
displayed with green/yellow/red by sheet. The script logic in `validate_pounce.py` maps
directly — call `get_validation_data()` + `get_structural_issues()` + validators, return JSON.

**Other input adapters.**
TSV/CSV adapters currently have column names scattered as raw strings. The same pattern
(constants class + `sheet_field` metadata + YAML validator config) should be applied to
other adapters, especially frequently-updated ones.

**Direct JSON loading.**
Once the validation tool is trusted, the `ParsedPounceData` output from `get_validation_data()`
can be loaded directly into the database, bypassing the Excel parse step in the ETL entirely.

---

## Mapping Coverage Checks

### Motivation

After ETL, each `MeasuredGene` (from GeneMeta) and `MeasuredMetabolite` (from MetabMeta) gets
an edge to a canonical `Gene` or `Metabolite` node. Those canonical nodes come from other
data sources (Ensembl/HGNC for genes, RaMP-DB for metabolites). A submitted ID that matches
nothing in those reference sets produces a dangling edge — the measured analyte is in the
graph but has no canonical node to connect to.

Submitters need to know this *before* ETL, so they can investigate unmapped IDs and decide
whether to fix them, annotate them differently, or accept the gap.

### What "mapped" means per analyte type

**Metabolites:** During ETL, `pounce_node_builder.py` creates `MeasuredMetaboliteEdge` for
each of the metabolite's IDs (primary `metab_id` plus any pipe-delimited `alternate_metab_id`
values). The `RampMetaboliteIdResolver` then attempts to resolve those IDs to RaMP canonical
IDs using its SQLite lookup. A metabolite is considered **mapped** if any of its IDs (primary
or alternate) resolves in RaMP.

Coverage is reported at the project level — deduplicated by `metab_id` across all experiment
files in the submission. Multi-experiment submissions (e.g. three paired workbooks) flatten
their MetabMeta sheets into one list in `ParsedPounceData.metabolites`; without deduplication
the same ID appearing in two experiments would inflate the total count.

**Genes:** During ETL, `pounce_node_builder.py` formats the `gene_id` as `ensembl:{gene_id}`
and creates a `MeasuredGeneEdge` pointing to `Gene(id=ensembl:{gene_id})`. Whether that Gene
node exists depends on whether it was loaded from Ensembl/HGNC data. A gene resolver is not
yet implemented; when it is, it will follow the same interface as `RampMetaboliteIdResolver`.

### Architecture

`src/input_adapters/pounce_sheets/mapping_coverage.py`:

```python
@dataclass
class AnalyteCoverage:
    analyte_type: str     # "Metabolite" or "Gene"
    sheet: str            # "MetabMeta" or "GeneMeta"
    total: int
    mapped: int
    unmapped_ids: List[str]

    @property
    def mapped_pct(self) -> float: ...

def check_metabolite_coverage(
    metabolites: List[ParsedMetab], resolver: IdResolver
) -> AnalyteCoverage: ...

def check_gene_coverage(
    genes: List[ParsedGene], resolver: Optional[IdResolver] = None
) -> Optional[AnalyteCoverage]: ...  # returns None when no resolver configured
```

Both functions accept an already-instantiated `IdResolver` (the same interface used by the
ETL). They create stub `Metabolite` / `Gene` nodes and call `resolver.resolve_internal()`,
checking which return non-empty match lists. This means they use whatever resolver logic is
configured — no resolver-specific code in `mapping_coverage.py` itself.

Both deduplicate by primary ID before checking, so a `metab_id` or `gene_id` that appears
in more than one experiment file is only counted once.

### Integration with `validate_pounce.py`

`validate_pounce.py` uses `ETL_Config` (instead of the base `Config`), which loads and
instantiates all resolvers declared under `resolvers:` in the YAML as part of its
`__init__`. A type→resolver map is built the same way the ETL does it:

```python
config = ETL_Config(yaml_file)
resolver_map = {t: r for r in config.resolvers.values() for t in r.types}
```

The coverage functions receive `resolver_map.get("Metabolite")` and
`resolver_map.get("Gene")` respectively. If a resolver for that type isn't configured,
`None` is passed and the check is skipped with a "no resolver configured" message.

Both `pounce.yaml` and `test_pounce_validation.yaml` declare a `resolvers:` block — no
new config keys were needed. `validate_pounce.py` currently points at `pounce.yaml` by
default (the test config is available but commented out).

Coverage runs after the structural + content validation output:

```
Mapping Coverage:
  MetabMeta — 48/50 (96.0%) metabolites will resolve to canonical Metabolite nodes
    Unmapped: HMDB0000123, CAS:50-00-0

  GeneMeta — no Gene resolver configured
```

If there are no metabolites (transcriptomics submission) the metabolite line is omitted,
and vice versa for genes.

### Not a `Validator`

Mapping coverage is not expressed as `ValidationError` instances. It is a different kind of
output — a coverage statistic with a list of IDs to investigate, not a binary pass/fail per
field. Merging it into the `ValidationError` framework would require creating one warning per
unmapped ID, which is noisy and loses the summary framing. It runs alongside the validator
framework but reports separately.

### Plugging in the gene resolver

When a gene resolver is built, the only change needed is adding it to the YAML:

```yaml
resolvers:
  - label: ramp_resolver
    ...
  - label: gene_resolver
    import: ./src/id_resolvers/gene_resolver.py
    class: GeneIdResolver
    kwargs:
      types:
        - Gene
      ...
```

`ETL_Config` picks it up automatically, `resolver_map.get("Gene")` returns it, and
`check_gene_coverage` receives it and runs. No changes to `mapping_coverage.py` or
`validate_pounce.py`.