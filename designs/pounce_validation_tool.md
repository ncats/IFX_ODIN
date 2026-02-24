# ok,POUNCE Validation Tool

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

## Current State

- `src/input_adapters/pounce_sheets/pounce_input_adapter.py` parses the submitted workbooks into
  the POUNCE data model (Project, Biosample, Experiment, Dataset, etc.)
- The parser silently skips problems — missing sheets, broken cross-references, empty data —
  logging warnings but not surfacing them to the user
- There is no standalone validation workflow; validation only happens implicitly during ETL

---

## Architecture

### Validator framework (`src/core/`)

`ValidationError` carries: `severity`, `entity`, `field`, `message`, and optional `sheet`,
`column`, `row` hints so error messages can point users to the exact location in their
spreadsheet.

All validators implement:
```python
class Validator(ABC):
    def validate(self, data: Any) -> List[ValidationError]: ...
```

Built-in rule types (`required`, `allowed_values`, etc.) are implementations of this interface.
Complex validators (cross-sheet reference checks, etc.) are custom Python classes.

### Validators are tied to input adapters

The `InputAdapter` base class gets two new default methods:

```python
def get_validators(self) -> list:       return []
def get_validation_data(self):          return None
```

Each adapter overrides these when it supports validation. `validators_config` is passed as a
kwarg so the config path can be set per YAML entry:

```yaml
input_adapters:
  - import: ./src/input_adapters/pounce_sheets/pounce_input_adapter.py
    class: PounceInputAdapter
    kwargs:
      project_file: ...
      validators_config: ./src/input_adapters/pounce_sheets/pounce_validators.py
```

### Validation is a separate workflow from ETL

Validation has a different contract than ETL: no database writes, collects all errors rather
than stopping on the first, and must be fast. A `validate_only` flag on the ETL would
conflate two different concerns. Instead, a standalone script and later a web UI call
`get_validation_data()` + `get_validators()` directly.

### `get_validation_data()` returns metadata only

Data matrices (RawData, PeakData, StatsReadyData) can be 100k+ rows and are not needed for
validation. `get_validation_data()` parses only the metadata sheets — Project, Biosample,
Experiment, RunBiosample — and returns a lightweight container. This keeps validation fast
regardless of data volume.

### Validator config is a Python file (for now)

Validators are configured in a Python file, not YAML, so they can reference the constants
classes directly:

```python
from src.input_adapters.pounce_sheets.constants import ProjectWorkbook

VALIDATORS = [
    RequiredValidator(
        sheet=ProjectWorkbook.ProjectSheet.name,
        column=ProjectWorkbook.ProjectSheet.Key.project_name,
        message="ProjectMeta: 'project_name' is required"
    ),
]
```

If a sheet or field name changes in `constants.py`, the validator breaks loudly (not silently).
IDE autocomplete works. Adding a new rule is one entry in the list.

### Web UI

A simple web tool where submitters upload their workbooks (one Project, one or more paired
Experiment + StatsResults), the validation runs, and results are displayed — parsed data shown
in green/yellow/red depending on validation status.

---

## Build Order

1. `src/core/validator.py` — `ValidationError`, `Validator` ABC, built-in rules
2. Add `get_validators()` / `get_validation_data()` to `InputAdapter` interface
3. Implement both in `PounceInputAdapter` (metadata-only parse, load validators from config)
4. `pounce_validators.py` — first few validators using constants references
5. `src/use_cases/validate_pounce.py` — standalone script
6. Unit tests
7. Web UI (upload → validate → display)

---

## Future Considerations

**Migrate validator config to YAML when the web UI needs it.**
When admins need to edit validators through a UI, export the Python config to YAML as a
one-time migration. From that point YAML is the source of truth. The validator interface
is unchanged — only the loader changes.

**Other input adapters should get constants classes.**
TSV/CSV adapters currently have column names scattered as raw strings. If a source file
changes format, things break silently. The same pattern (constants class + validator config)
should be applied to other adapters, especially frequently-updated ones like target_graph.

**Schema consistency validation.**
If submitters fill out sheets inconsistently (e.g., a field that exists in one project's
BioSampleMap but not another's), we should flag that and suggest they align their column
naming. This is separate from field-level validation and would be a cross-submission check.

**Direct JSON loading.**
Once the validation tool is trusted, the parsed output from `get_validation_data()` can
be loaded directly into the database, bypassing the Excel parse step in the ETL entirely.

**LLM-driven data entry.**
Longer term: the web tool asks submitters questions to fill out the same data model,
replacing Excel workbooks as the input mechanism. The validator layer is unchanged —
same rules, different front door.
