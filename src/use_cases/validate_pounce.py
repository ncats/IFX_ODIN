import os
from src.core.config import create_object_from_config, Config
from src.input_adapters.pounce_sheets.pounce_parser import (
    _PROJECT_REQUIRED_SHEETS, _EXPERIMENT_RECOGNIZED_SHEETS, _STATS_RECOGNIZED_SHEETS,
)

yaml_file = "./src/use_cases/test_pounce_validation.yaml"

config = Config(yaml_file)
pounce_configs = [
    c for c in config.config_dict.get("input_adapters", [])
    if c.get("class") == "PounceInputAdapter"
]

for chosen in pounce_configs:
    kwargs = chosen.get("kwargs", {})
    project_file = kwargs.get("project_file", "")
    experiment_files = kwargs.get("experiment_files") or []
    stats_files = kwargs.get("stats_results_files") or []

    print(f"\n{'='*60}")
    print(f"Validating: {os.path.basename(project_file)}")
    print(f"{'='*60}")

    adapter = create_object_from_config(chosen)
    parsed_data = adapter.get_validation_data()
    structural_issues = adapter.get_structural_issues()

    content_issues = []
    for validator in adapter.get_validators():
        content_issues.extend(validator.validate(parsed_data))

    all_issues = structural_issues + content_issues
    errors = [e for e in all_issues if e.severity == "error"]
    warnings = [e for e in all_issues if e.severity == "warning"]

    def _source_label(issue) -> str:
        """Return a display label (basename) for the xlsx file an issue came from."""
        if issue.source_file:
            return os.path.basename(issue.source_file)
        issue_sheet = issue.sheet
        if issue_sheet in _PROJECT_REQUIRED_SHEETS:
            return os.path.basename(project_file) if project_file else "Project workbook"
        if issue_sheet in _EXPERIMENT_RECOGNIZED_SHEETS:
            if len(experiment_files) == 1:
                return os.path.basename(experiment_files[0])
            return "Experiment workbook(s): " + ", ".join(os.path.basename(f) for f in experiment_files)
        if issue_sheet in _STATS_RECOGNIZED_SHEETS:
            if len(stats_files) == 1:
                return os.path.basename(stats_files[0])
            return "Stats results workbook(s): " + ", ".join(os.path.basename(f) for f in stats_files)
        return "(unknown workbook)"

    if all_issues:
        error_count = len(errors)
        warning_count = len(warnings)
        parts = []
        if error_count:
            parts.append(f"{error_count} error(s)")
        if warning_count:
            parts.append(f"{warning_count} warning(s)")
        print(f"\n{' and '.join(parts)} found:")

        # Group: workbook → sheet → issues
        by_workbook: dict = {}
        for item in all_issues:
            wb = _source_label(item)
            sheet = item.sheet or "(no sheet)"
            by_workbook.setdefault(wb, {}).setdefault(sheet, []).append(item)

        for workbook, sheets in by_workbook.items():
            print(f"\n{workbook}:")
            for sheet, group in sheets.items():
                print(f"  {sheet}:")
                for item in group:
                    label = "ERROR" if item.severity == "error" else "WARN "
                    row_info = f" row {item.row}" if item.row is not None else ""
                    print(f"    {label}  {item.field}{row_info}: {item.message}")
    else:
        print("\nValidation passed — no errors or warnings.")

    print(f"\nSummary:")
    print(f"  Project: {parsed_data.project.project_name if parsed_data.project else 'N/A'}")
    print(f"  Biosamples: {len(parsed_data.biosamples)}")
    print(f"  Biospecimens: {len(parsed_data.biospecimens)}")
    print(f"  Experiments: {len(parsed_data.experiments)}")
    print(f"  RunBiosamples: {len(parsed_data.run_biosamples)}")
    print(f"  StatsResults: {len(parsed_data.stats_results)}")
    print(f"  People: {len(parsed_data.people)}")
