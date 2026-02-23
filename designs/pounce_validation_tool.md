lo# POUNCE Validation Tool

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

## Proposed Architecture

### 1. Validator Configuration (`pounce_validators.yaml`)

A dedicated YAML file defines all validation rules, organized by entity and field.
Simple rules use built-in rule types. Complex rules (cross-sheet checks, etc.) point to a
Python class, following the same `import`/`class` pattern used for input adapters in the ETL
YAML.

```yaml
project:
  date:
    - rule: date_format
      format: "%Y%m%d"
      message: "Date must be in YYYYMMDD format (e.g. 20240215)"
  owner_name:
    - rule: list_length_match
      match_field: owner_email
      message: "Owner names and emails must have the same count"

cross_sheet:
  - import: ./src/validators/pounce/reference_validators.py
    class: BiosampleRunReferenceValidator
    message: "RunBiosample references a biosample_id not found in BioSampleMeta"
```

The ETL YAML references it:
```yaml
# pounce_v2.yaml
validators_config: ./src/validators/pounce_validators.yaml
```

Adding a new validation is either one YAML entry (built-in rule) or one Python class plus one
YAML entry (custom logic). Messages are defined per-rule and easy to update.

### 2. Validator Interface

All validators implement a simple interface:

```python
class Validator:
    def validate(self, parsed_data: ParsedPounceData) -> List[ValidationError]:
        ...
```

Built-in rule types (`required`, `date_format`, `list_length_match`, `allowed_values`, etc.)
are implementations of this interface, instantiated automatically from YAML parameters.

`ValidationError` carries: `severity` (error/warning), `entity`, `field`, `row` (if applicable),
and `message`.

### 3. Standalone Validation Workflow

Validation runs independently of the ETL — no database write, no output adapter. The parser
runs, the validators run against the result, and a structured report is returned:

```json
{
  "errors": [...],
  "warnings": [...],
  "parsed": { "project": {...}, "biosamples": [...], "experiments": [...] }
}
```

The `parsed` block represents the data model as the system understood it, even with errors
present, so submitters can see what did and didn't parse correctly.

### 4. Web UI

A simple web tool where submitters upload their workbooks (one Project, one or more paired
Experiment + StatsResults), the validation runs, and
results are displayed — parsed data shown in green/yellow/red depending on validation status.

Admins can view and update the built-in validation rules through the UI. The UI reads and
writes `pounce_validators.yaml` as its backing store. Custom Python validators remain in
version control.

---

## Future Direction

Once the validation tool is working and trusted, the parsed JSON output from validation can
be loaded directly into the database, bypassing the Excel parsing step in the ETL pipeline.

Further out: the web tool's data entry form could be driven by an LLM that asks submitters
questions to fill out the same data model, replacing the Excel workbooks as the input mechanism
entirely. The data model and validator layer remain unchanged.

---

## Build Order

1. Validator interface + built-in rule implementations
2. Validator loader (reads YAML, instantiates validators)
3. Standalone validation script (wraps existing parser, runs validators, outputs report)
4. Web UI (upload → validate → display)
5. Admin UI for managing built-in validators