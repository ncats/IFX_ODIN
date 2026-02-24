from typing import List

import yaml

from src.core.config import create_object_from_config
from src.core.validator import AllowedValuesValidator, RequiredMapKeyValidator, RequiredValidator, Validator
from src.input_adapters.pounce_sheets.constants import ExperimentWorkbook, ProjectWorkbook
from src.input_adapters.pounce_sheets.parsed_classes import (
    ParsedBiosample, ParsedBiospecimen, ParsedExperiment, ParsedExposure,
    ParsedProject, ParsedRunBiosample, ParsedStatsResultsMeta,
)
from src.input_adapters.pounce_sheets.sheet_field import get_sheet_fields

# Map (sheet_name, ncatsdpi_key) -> (entity_name, python_field_name)
# Derived from sheet_field metadata on all parsed dataclasses.
_ENTITY_CLASSES = [
    ("project", ParsedProject),
    ("biosamples", ParsedBiosample),
    ("biospecimens", ParsedBiospecimen),
    ("exposures", ParsedExposure),
    ("experiments", ParsedExperiment),
    ("run_biosamples", ParsedRunBiosample),
    ("stats_results", ParsedStatsResultsMeta),
]
_SHEET_KEY_MAP = {
    (meta["sheet"], meta["key"]): (entity_name, f.name)
    for entity_name, cls in _ENTITY_CLASSES
    for f, meta in get_sheet_fields(cls)
}

# Map sheets whose YAML entries represent configuration checks (key present in
# param_maps[sheet]) rather than content checks (row values non-null).
# Derived from constants so it stays in sync automatically.
_MAP_SHEETS: set = {
    ProjectWorkbook.BiosampleMapSheet.name,
    ExperimentWorkbook.RunSampleMapSheet.name,
}

_DEFAULT_REQUIRED_MESSAGE = "Sheet {sheet} is missing required field: {field}"
_DEFAULT_MAP_REQUIRED_MESSAGE = "Map sheet {sheet} is missing configuration for: {field}"


def load_validators(yaml_file_or_dict) -> List[Validator]:
    if not isinstance(yaml_file_or_dict, dict):
        with open(yaml_file_or_dict) as f:
            yaml_file_or_dict = yaml.safe_load(f)
    config = yaml_file_or_dict

    required_message = config.get("required_message", _DEFAULT_REQUIRED_MESSAGE)

    validators = []
    for top_key, top_value in config.items():
        if top_key == "required_message":
            continue

        if top_key == "cross_entity":
            for entry in top_value:
                if "import" in entry:
                    validators.append(create_object_from_config(entry))
            continue

        # Sheet-based format: top_key is a sheet name, value has "required" / "allowed_values"
        if isinstance(top_value, dict) and ("required" in top_value or "allowed_values" in top_value):
            sheet = top_key

            for ncatsdpi_key in top_value.get("required", []):
                if sheet in _MAP_SHEETS:
                    # Map sheet: check that the key is configured (column mapped)
                    validators.append(RequiredMapKeyValidator(
                        entity="param_maps",
                        field=ncatsdpi_key,
                        message=_DEFAULT_MAP_REQUIRED_MESSAGE.format(sheet=sheet, field=ncatsdpi_key),
                        sheet=sheet,
                        column=ncatsdpi_key,
                    ))
                else:
                    if (sheet, ncatsdpi_key) not in _SHEET_KEY_MAP:
                        known_sheets = sorted({s for s, _ in _SHEET_KEY_MAP})
                        raise KeyError(
                            f"No parsed field for sheet='{sheet}', key='{ncatsdpi_key}'. "
                            f"Known data sheets: {known_sheets}. "
                            f"Note: use the *Meta sheet name (e.g. BioSampleMeta), not the Map sheet."
                        )
                    entity, field = _SHEET_KEY_MAP[(sheet, ncatsdpi_key)]
                    validators.append(RequiredValidator(
                        entity=entity,
                        field=field,
                        message=required_message.format(sheet=sheet, field=ncatsdpi_key),
                        sheet=sheet,
                        column=ncatsdpi_key,
                    ))

            for ncatsdpi_key, values in top_value.get("allowed_values", {}).items():
                if (sheet, ncatsdpi_key) not in _SHEET_KEY_MAP:
                    known_sheets = sorted({s for s, _ in _SHEET_KEY_MAP})
                    raise KeyError(
                        f"No parsed field for sheet='{sheet}', key='{ncatsdpi_key}'. "
                        f"Known data sheets: {known_sheets}. "
                        f"Note: use the *Meta sheet name (e.g. BioSampleMeta), not the Map sheet."
                    )
                entity, field = _SHEET_KEY_MAP[(sheet, ncatsdpi_key)]
                validators.append(AllowedValuesValidator(
                    entity=entity,
                    field=field,
                    message=f"Field '{ncatsdpi_key}' in {sheet} must be one of: {', '.join(values)}",
                    values=values,
                    sheet=sheet,
                    column=ncatsdpi_key,
                ))

    return validators
