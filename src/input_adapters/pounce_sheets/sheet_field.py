"""Metadata-rich field descriptors for POUNCE Excel sheet parsing.

``sheet_field()`` wraps ``dataclasses.field()`` and stores Excel-specific metadata
(sheet name, NCATSDPI key, parse type) so that parsers can be auto-generated
from the dataclass definition alone.
"""

from dataclasses import field, fields
from typing import Any, Optional

SHEET_FIELD_META_KEY = "sheet_field"


def sheet_field(
    key: str,
    sheet: Optional[str] = None,
    parse: str = "string",
    indexed: bool = False,
    default: Any = None,
    default_factory: Any = None,
):
    """Create a dataclass field annotated with Excel sheet metadata.

    Parameters
    ----------
    key:
        NCATSDPI key in the Excel sheet (e.g. ``"project_name"``).
        For indexed fields use ``{}`` as a placeholder (e.g. ``"exposure{}_names"``).
    sheet:
        Sheet name this field is read from (e.g. ``"ProjectMeta"``).
    parse:
        Parse strategy: ``"string"`` | ``"string_list"`` | ``"date"`` |
        ``"int"`` | ``"float"`` | ``"bool"`` | ``"category"``.
    indexed:
        ``True`` for templates like ``exposure{}_names`` that expand to
        ``exposure1_names``, ``exposure2_names``, etc.
    default:
        Default value for the dataclass field.
    default_factory:
        Factory callable for mutable defaults (mutually exclusive with *default*).
    """
    metadata = {
        SHEET_FIELD_META_KEY: {
            "key": key,
            "sheet": sheet,
            "parse": parse,
            "indexed": indexed,
        }
    }
    if default_factory is not None:
        return field(default_factory=default_factory, metadata=metadata)
    return field(default=default, metadata=metadata)


def get_sheet_fields(cls) -> list:
    """Return a list of ``(field, sheet_field_meta)`` tuples for *cls*.

    Only fields created with :func:`sheet_field` are included.
    """
    result = []
    for f in fields(cls):
        meta = f.metadata.get(SHEET_FIELD_META_KEY)
        if meta is not None:
            result.append((f, meta))
    return result
