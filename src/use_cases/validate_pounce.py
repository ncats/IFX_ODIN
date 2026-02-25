import os
from src.core.config import ETL_Config, create_object_from_config
from src.input_adapters.pounce_sheets.mapping_coverage import (
    check_metabolite_coverage, check_gene_coverage,
)
from src.input_adapters.pounce_sheets.pounce_parser import (
    _PROJECT_REQUIRED_SHEETS, _EXPERIMENT_RECOGNIZED_SHEETS, _STATS_RECOGNIZED_SHEETS,
)

yaml_file = "./src/use_cases/test_pounce_validation.yaml"

config = ETL_Config(yaml_file)
resolver_map = {t: r for r in config.resolvers.values() for t in r.types}

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

    has_metabolites = bool(parsed_data.metabolites)
    has_genes = bool(parsed_data.genes)

    if has_metabolites or has_genes:
        print(f"\nMapping Coverage:")

        if has_metabolites:
            metab_resolver = resolver_map.get("Metabolite")
            if metab_resolver:
                cov = check_metabolite_coverage(parsed_data.metabolites, metab_resolver)
                print(f"  MetabMeta — {cov.mapped}/{cov.total} ({cov.mapped_pct:.1f}%)"
                      f" metabolites will resolve to canonical Metabolite nodes")
                if cov.unmapped_ids:
                    print(f"    Unmapped: {', '.join(cov.unmapped_ids)}")
            else:
                print(f"  MetabMeta — no Metabolite resolver configured")

        if has_genes:
            gene_resolver = resolver_map.get("Gene")
            cov = check_gene_coverage(parsed_data.genes, gene_resolver)
            if cov:
                print(f"  GeneMeta — {cov.mapped}/{cov.total} ({cov.mapped_pct:.1f}%)"
                      f" genes will resolve to canonical Gene nodes")
                if cov.unmapped_ids:
                    print(f"    Unmapped: {', '.join(cov.unmapped_ids)}")
            else:
                print(f"  GeneMeta — no Gene resolver configured")