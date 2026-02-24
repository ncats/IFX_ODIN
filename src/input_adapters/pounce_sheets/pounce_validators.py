"""Validators for POUNCE submissions.

Cross-entity validators are custom ``Validator`` subclasses.
"""

from typing import Any, List

from src.core.validator import ValidationError, Validator
from src.input_adapters.pounce_sheets.constants import ExperimentWorkbook

_RUN_MAP = ExperimentWorkbook.RunSampleMapSheet
_RUN_META = ExperimentWorkbook.RunSampleMetaSheet


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
