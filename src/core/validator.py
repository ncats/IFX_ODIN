from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, List, Optional


@dataclass
class ValidationError:
    severity: str        # "error" | "warning"
    entity: str          # e.g. "project", "biosamples"
    field: str           # e.g. "name", "project_type"
    message: str
    sheet: Optional[str] = None        # e.g. "ProjectMeta" — tells user where to look
    column: Optional[str] = None       # e.g. "project_name"
    row: Optional[int] = None          # set for list entities
    source_file: Optional[str] = None  # path to the xlsx file that produced this issue


class Validator(ABC):
    def __init__(self, entity: str, field: str, message: str,
                 sheet: Optional[str] = None, column: Optional[str] = None,
                 severity: str = "error"):
        self.entity = entity
        self.field = field
        self.message = message
        self.sheet = sheet
        self.column = column
        self.severity = severity

    def _make_error(self, row: Optional[int] = None) -> ValidationError:
        return ValidationError(
            severity=self.severity,
            entity=self.entity,
            field=self.field,
            message=self.message,
            sheet=self.sheet,
            column=self.column,
            row=row,
        )

    @abstractmethod
    def validate(self, data: Any) -> List[ValidationError]:
        raise NotImplementedError


class RequiredValidator(Validator):
    def validate(self, data: Any) -> List[ValidationError]:
        entity_obj = getattr(data, self.entity, None)
        if entity_obj is None:
            return [self._make_error()]
        if isinstance(entity_obj, list):
            errors = []
            for i, item in enumerate(entity_obj):
                v = getattr(item, self.field, None)
                if v is None or v == "" or v == []:
                    errors.append(self._make_error(row=i))
            return errors
        value = getattr(entity_obj, self.field, None)
        if value is None or value == "" or value == []:
            return [self._make_error()]
        return []


class RequiredMapKeyValidator(Validator):
    """Checks that a key is configured in data.param_maps[sheet].

    Validates that the submitter has mapped the given NCATSDPI key to a column
    name in their map sheet — i.e., the configuration is present, regardless of
    whether the data rows have values.
    """

    def validate(self, data: Any) -> List[ValidationError]:
        param_maps = getattr(data, "param_maps", None) or {}
        param_map = param_maps.get(self.sheet, {})
        val = param_map.get(self.field)
        if not val or val in ("NA", "N/A", ""):
            return [self._make_error()]
        return []


class AllowedValuesValidator(Validator):
    def __init__(self, entity: str, field: str, message: str, values: List[str],
                 sheet: Optional[str] = None, column: Optional[str] = None):
        super().__init__(entity, field, message, sheet, column)
        self.values = values

    def _value_allowed(self, value: Any) -> bool:
        if isinstance(value, list):
            return all(v in self.values for v in value if v is not None)
        return value in self.values

    def validate(self, data: Any) -> List[ValidationError]:
        entity_obj = getattr(data, self.entity, None)
        if entity_obj is None:
            return []
        if isinstance(entity_obj, list):
            errors = []
            for i, item in enumerate(entity_obj):
                value = getattr(item, self.field, None)
                if value is not None and not self._value_allowed(value):
                    errors.append(self._make_error(row=i))
            return errors
        value = getattr(entity_obj, self.field, None)
        if value is None:
            return []
        if not self._value_allowed(value):
            return [self._make_error()]
        return []
