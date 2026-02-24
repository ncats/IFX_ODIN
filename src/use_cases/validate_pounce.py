from src.core.config import create_object_from_config, Config

yaml_file = "./src/use_cases/pounce.yaml"

config = Config(yaml_file)
pounce_configs = [
    c for c in config.config_dict.get("input_adapters", [])
    if c.get("class") == "PounceInputAdapter"
]

print(pounce_configs)

chosen = pounce_configs[0]
kwargs = chosen.get("kwargs", {})
print(f"Validating instance {chosen}: {kwargs.get('project_file', '?')}")

adapter = create_object_from_config(chosen)
parsed_data = adapter.get_validation_data()

validators = adapter.get_validators()
all_errors = []
for validator in validators:
    all_errors.extend(validator.validate(parsed_data))

errors = [e for e in all_errors if e.severity == "error"]
warnings = [e for e in all_errors if e.severity == "warning"]

if errors:
    print(f"\n{len(errors)} error(s) found:\n")
    for err in errors:
        location = f"  [{err.sheet}]" if err.sheet else ""
        row_info = f" row {err.row}" if err.row is not None else ""
        print(f"  ERROR {err.entity}.{err.field}{location}{row_info}: {err.message}")

if warnings:
    print(f"\n{len(warnings)} warning(s):\n")
    for w in warnings:
        location = f"  [{w.sheet}]" if w.sheet else ""
        row_info = f" row {w.row}" if w.row is not None else ""
        print(f"  WARN  {w.entity}.{w.field}{location}{row_info}: {w.message}")

if not errors and not warnings:
    print("\nValidation passed — no errors or warnings.")

# Summary
print(f"\nSummary:")
print(f"  Project: {parsed_data.project.project_name if parsed_data.project else 'N/A'}")
print(f"  Biosamples: {len(parsed_data.biosamples)}")
print(f"  Biospecimens: {len(parsed_data.biospecimens)}")
print(f"  Experiments: {len(parsed_data.experiments)}")
print(f"  RunBiosamples: {len(parsed_data.run_biosamples)}")
print(f"  StatsResults: {len(parsed_data.stats_results)}")
print(f"  People: {len(parsed_data.people)}")
