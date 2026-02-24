"""Validators for POUNCE submissions.

All required-field and allowed-value rules are declared explicitly here using
constants references. Cross-entity validators are custom ``Validator`` subclasses.
"""

from typing import Any, List

from src.core.validator import AllowedValuesValidator, RequiredValidator, ValidationError, Validator
from src.input_adapters.pounce_sheets.constants import ExperimentWorkbook, ProjectWorkbook

_PROJ = ProjectWorkbook.ProjectSheet
_BS_MAP = ProjectWorkbook.BiosampleMapSheet
_BS_META = ProjectWorkbook.BiosampleMetaSheet
_EXP = ExperimentWorkbook.ExperimentSheet
_RUN_MAP = ExperimentWorkbook.RunSampleMapSheet
_RUN_META = ExperimentWorkbook.RunSampleMetaSheet


def generate_validators() -> List[Validator]:
    """Return all validators for a POUNCE submission."""
    return [
        # --- Project ---
        RequiredValidator(
            entity="project", field="project_id",
            message="Required field 'project_id' is missing",
            sheet=_PROJ.name, column=_PROJ.Key.project_id,
        ),
        RequiredValidator(
            entity="project", field="project_name",
            message="Required field 'project_name' is missing",
            sheet=_PROJ.name, column=_PROJ.Key.project_name,
        ),
        RequiredValidator(
            entity="project", field="date",
            message="Required field 'date' is missing",
            sheet=_PROJ.name, column=_PROJ.Key.date,
        ),
        RequiredValidator(
            entity="project", field="owner_names",
            message="Required field 'owner_names' is missing",
            sheet=_PROJ.name, column=_PROJ.Key.owner_name,
        ),
        AllowedValuesValidator(
            entity="project", field="privacy_type",
            message="Field 'privacy_type' must be one of: private, ncats, public",
            values=["private", "ncats", "public"],
            sheet=_PROJ.name, column=_PROJ.Key.privacy_type,
        ),

        # --- Biosamples ---
        RequiredValidator(
            entity="biosamples", field="biosample_id",
            message="Required field 'biosample_id' is missing",
            sheet=_BS_META.name, column=_BS_MAP.Key.biosample_id,
        ),
        RequiredValidator(
            entity="biosamples", field="biosample_type",
            message="Required field 'biosample_type' is missing",
            sheet=_BS_META.name, column=_BS_MAP.Key.biosample_type,
        ),

        # --- Biospecimens ---
        RequiredValidator(
            entity="biospecimens", field="biospecimen_id",
            message="Required field 'biospecimen_id' is missing",
            sheet=_BS_META.name, column=_BS_MAP.Key.biospecimen_id,
        ),
        RequiredValidator(
            entity="biospecimens", field="biospecimen_type",
            message="Required field 'biospecimen_type' is missing",
            sheet=_BS_META.name, column=_BS_MAP.Key.biospecimen_type,
        ),
        RequiredValidator(
            entity="biospecimens", field="organism_names",
            message="Required field 'organism_names' is missing",
            sheet=_BS_META.name, column=_BS_MAP.Key.organism_names,
        ),

        # --- Exposures ---
        RequiredValidator(
            entity="exposures", field="names",
            message="Required field 'names' is missing",
            sheet=_BS_META.name, column=_BS_MAP.Key.exposure_names,
        ),
        RequiredValidator(
            entity="exposures", field="type",
            message="Required field 'type' is missing",
            sheet=_BS_META.name, column=_BS_MAP.Key.exposure_type,
        ),

        # --- Experiments ---
        RequiredValidator(
            entity="experiments", field="experiment_name",
            message="Required field 'experiment_name' is missing",
            sheet=_EXP.name, column=_EXP.Key.experiment_name,
        ),
        RequiredValidator(
            entity="experiments", field="experiment_type",
            message="Required field 'experiment_type' is missing",
            sheet=_EXP.name, column=_EXP.Key.experiment_type,
        ),

        # --- RunBiosamples ---
        RequiredValidator(
            entity="run_biosamples", field="run_biosample_id",
            message="Required field 'run_biosample_id' is missing",
            sheet=_RUN_META.name, column=_RUN_MAP.Key.run_biosample_id,
        ),
        RequiredValidator(
            entity="run_biosamples", field="biosample_id",
            message="Required field 'biosample_id' is missing",
            sheet=_RUN_META.name, column=_RUN_MAP.Key.biosample_id,
        ),

        # --- Cross-entity ---
        RunBiosampleReferenceValidator(),
    ]


# ---------------------------------------------------------------------------
# Cross-entity validators
# ---------------------------------------------------------------------------

class RunBiosampleReferenceValidator(Validator):
    """Verify that every RunBiosample.biosample_id references a known biosample."""

    def __init__(self):
        super().__init__(
            entity="run_biosamples",
            field="biosample_id",
            message="RunBiosample references an unknown biosample_id",
            sheet=_RUN_META.name,
            column=_RUN_MAP.Key.biosample_id,
        )

    def validate(self, data: Any) -> List[ValidationError]:
        biosample_ids = set()
        for bs in getattr(data, "biosamples", []) or []:
            if bs.biosample_id:
                biosample_ids.add(str(bs.biosample_id))

        errors = []
        for i, rb in enumerate(getattr(data, "run_biosamples", []) or []):
            if rb.biosample_id and str(rb.biosample_id) not in biosample_ids:
                errors.append(ValidationError(
                    severity="error",
                    entity=self.entity,
                    field=self.field,
                    message=f"RunBiosample references unknown biosample_id: {rb.biosample_id}",
                    sheet=self.sheet,
                    column=self.column,
                    row=i,
                ))
        return errors
