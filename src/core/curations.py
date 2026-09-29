"""Typed, reusable curation streams shared by ODIN builds and applications."""

from __future__ import annotations

import hashlib
import json
import math
import re
import types
from dataclasses import dataclass, field, fields
from typing import Any, Iterable, Optional, Union, get_args, get_origin, get_type_hints


FORMAT_VERSION = 2
PUBLISHED_PREFIX = "curations/v2"

METABOLITE_EQUIVALENCE_EDGES = "metabolite_equivalence_edges"
METABOLITE_ANNOTATIONS = "metabolite_annotations"
METABOLITE_EXPECTED_CLIQUES = "metabolite_expected_cliques"
METABOLITE_RECORD_SUPPRESSIONS = "metabolite_record_suppressions"
RECORD_PROPERTIES = "record_properties"


@dataclass(frozen=True)
class CuratablePropertyDefinition:
    model_type: str
    property_name: str
    value_type: type
    nullable: bool

    @property
    def value_type_name(self) -> str:
        return {
            bool: "boolean",
            str: "string",
            int: "integer",
            float: "number",
        }[self.value_type]

    def metadata(self) -> dict:
        return {
            "model_type": self.model_type,
            "name": self.property_name,
            "label": self.property_name.replace("_", " ").strip().title(),
            "value_type": self.value_type_name,
            "nullable": self.nullable,
        }


@dataclass(frozen=True)
class CurationTypeDefinition:
    id: str
    actions: frozenset[str]
    model_types: tuple[str, ...] = ()
    edge_types: tuple[str, ...] = ()


_FRAMEWORK_PROPERTY_DENYLIST = frozenset({
    "id", "xref", "provenance", "sources", "start_node", "end_node",
    "_id", "_key", "_rev", "_from", "_to", "start_id", "end_id",
    "creation", "updates", "resolved_ids", "entity_resolution",
    "_curation_original",
})
_MODEL_PROPERTY_DENYLIST = {
    "MetaboliteIdentifier": frozenset({"prefix"}),
}
_SUPPORTED_PROPERTY_TYPES = frozenset({bool, str, int, float})


def _curatable_model_class(model_type: str):
    if model_type == "MetaboliteIdentifier":
        from src.models.metabolite_harmonization import MetaboliteIdentifier
        return MetaboliteIdentifier
    return None


def _unwrap_optional(type_hint) -> tuple[Any, bool]:
    origin = get_origin(type_hint)
    if origin in (Union, types.UnionType):
        arguments = get_args(type_hint)
        non_none = tuple(argument for argument in arguments if argument is not type(None))
        if len(non_none) == 1 and len(non_none) != len(arguments):
            return non_none[0], True
    return type_hint, False


def curatable_property_definitions(model_type: str) -> tuple[CuratablePropertyDefinition, ...]:
    model_class = _curatable_model_class(model_type)
    if model_class is None:
        raise ValueError(f"Unsupported curation target model: {model_type!r}")
    type_hints = get_type_hints(model_class)
    denied = _FRAMEWORK_PROPERTY_DENYLIST | _MODEL_PROPERTY_DENYLIST.get(model_type, frozenset())
    definitions = []
    for model_field in fields(model_class):
        if model_field.name.startswith("_") or model_field.name in denied:
            continue
        value_type, nullable = _unwrap_optional(type_hints.get(model_field.name, model_field.type))
        if value_type not in _SUPPORTED_PROPERTY_TYPES:
            continue
        definitions.append(CuratablePropertyDefinition(
            model_type=model_type,
            property_name=model_field.name,
            value_type=value_type,
            nullable=nullable,
        ))
    return tuple(definitions)


def curatable_property_definition(
    model_type: str,
    property_name: str,
) -> Optional[CuratablePropertyDefinition]:
    return next((
        definition
        for definition in curatable_property_definitions(model_type)
        if definition.property_name == property_name
    ), None)


CURATION_TYPES = {
    RECORD_PROPERTIES: CurationTypeDefinition(
        id=RECORD_PROPERTIES,
        actions=frozenset({"set_properties"}),
    ),
    METABOLITE_EQUIVALENCE_EDGES: CurationTypeDefinition(
        id=METABOLITE_EQUIVALENCE_EDGES,
        actions=frozenset({"remove_edge", "retain_edge"}),
        edge_types=("MetaboliteIdentifierMappingEdge",),
    ),
    METABOLITE_ANNOTATIONS: CurationTypeDefinition(
        id=METABOLITE_ANNOTATIONS,
        actions=frozenset({"set_properties", "set_property", "unset_property"}),
        model_types=("MetaboliteIdentifier",),
    ),
    METABOLITE_EXPECTED_CLIQUES: CurationTypeDefinition(
        id=METABOLITE_EXPECTED_CLIQUES,
        actions=frozenset({"assert_same_clique", "retire_assertion"}),
    ),
    METABOLITE_RECORD_SUPPRESSIONS: CurationTypeDefinition(
        id=METABOLITE_RECORD_SUPPRESSIONS,
        actions=frozenset({"suppress_record", "restore_record"}),
        model_types=("MetaboliteIdentifier",),
    ),
}


def validate_curation_type(curation_type: str) -> CurationTypeDefinition:
    definition = CURATION_TYPES.get(str(curation_type or "").strip())
    if definition is None:
        raise ValueError(f"Unsupported curation type: {curation_type!r}")
    return definition


def validate_operation(curation_type: str, operation: dict) -> dict:
    definition = validate_curation_type(curation_type)
    if not isinstance(operation, dict):
        raise ValueError("Curation operation must be an object")
    action = str(operation.get("action") or "").strip()
    if action not in definition.actions:
        raise ValueError(f"Action {action!r} is not supported by {curation_type}")

    if curation_type == RECORD_PROPERTIES:
        _validate_record_property_operation(operation)
    elif action in {"set_properties", "set_property", "unset_property"}:
        target = operation.get("target")
        if not isinstance(target, dict) or target.get("kind") != "node":
            raise ValueError("Property curations currently require a node target")
        model_type = str(target.get("model_type") or "").strip()
        target_id = str(target.get("id") or "").strip()
        if model_type not in definition.model_types:
            raise ValueError(f"Model type {model_type!r} is not registered for {curation_type}")
        if not target_id:
            raise ValueError("Property curation target id is required")
        if action == "set_properties":
            values = operation.get("values", {})
            remove_overrides = operation.get("remove_overrides", [])
            if not isinstance(values, dict):
                raise ValueError("set_properties values must be an object")
            if not isinstance(remove_overrides, list) or any(
                not isinstance(item, str) or not item.strip() for item in remove_overrides
            ):
                raise ValueError("set_properties remove_overrides must be a list of property names")
            if len(remove_overrides) != len(set(remove_overrides)):
                raise ValueError("set_properties remove_overrides must not contain duplicates")
            overlap = set(values) & set(remove_overrides)
            if overlap:
                raise ValueError(
                    "set_properties values and remove_overrides must be disjoint: "
                    + ", ".join(sorted(overlap))
                )
            if not values and not remove_overrides:
                raise ValueError("set_properties requires at least one property decision")
            decisions = [(name, "set", value) for name, value in values.items()]
            decisions.extend((name, "remove_override", None) for name in remove_overrides)
        else:
            property_name = str(operation.get("property") or "").strip()
            if action == "unset_property" and "value" in operation:
                raise ValueError("unset_property must not include a value")
            decisions = [(property_name, "set" if action == "set_property" else "remove_override", operation.get("value"))]
        for property_name, mode, value in decisions:
            property_definition = curatable_property_definition(model_type, property_name)
            if property_definition is None:
                raise ValueError(
                    f"Property {model_type}.{property_name} is denied or has no supported scalar editor"
                )
            if mode == "set":
                _validate_property_value(property_definition, value)
    if action in {"remove_edge", "retain_edge"}:
        edge_type = str(operation.get("edge_type") or "").strip()
        left = str(operation.get("start_id") or "").strip()
        right = str(operation.get("end_id") or "").strip()
        if edge_type not in definition.edge_types:
            raise ValueError(f"Edge type {edge_type!r} is not registered for {curation_type}")
        if not left or not right or left == right:
            raise ValueError("Edge curation requires two different endpoint ids")
        if operation.get("symmetric") is not True:
            raise ValueError(f"{edge_type} curation requires symmetric=true")
    if action in {"suppress_record", "restore_record"}:
        target = operation.get("target")
        if not isinstance(target, dict) or target.get("kind") != "node":
            raise ValueError("Record suppression curations require a node target")
        model_type = str(target.get("model_type") or "").strip()
        target_id = str(target.get("id") or "").strip()
        if model_type not in definition.model_types:
            raise ValueError(f"Model type {model_type!r} is not registered for {curation_type}")
        if not target_id:
            raise ValueError("Record suppression target id is required")
        note = str(operation.get("note") or "").strip()
        if action == "suppress_record" and not note:
            raise ValueError("Suppressing a record requires a rationale")
    return operation


def _validate_record_property_operation(operation: dict) -> None:
    target = operation.get("target")
    if not isinstance(target, dict) or target.get("kind") != "node":
        raise ValueError("Record property curations require a node target")
    for field_name, label in (
        ("curation_set", "curation set"),
        ("model_type", "target model type"),
        ("id", "target id"),
    ):
        if not str(target.get(field_name) or "").strip():
            raise ValueError(f"Record property curation {label} is required")
    note = str(operation.get("note") or "").strip()
    if not note:
        raise ValueError("Record property curation requires a rationale")
    decisions = operation.get("decisions")
    if not isinstance(decisions, list) or not decisions:
        raise ValueError("Record property curation requires at least one field decision")
    canonical_paths = []
    for decision in decisions:
        if not isinstance(decision, dict):
            raise ValueError("Record property decisions must be objects")
        path = decision.get("path")
        _validate_record_property_path(path)
        canonical_path = canonical_json(path)
        if canonical_path in canonical_paths:
            raise ValueError("Record property decisions must not repeat a field path")
        canonical_paths.append(canonical_path)
        mode = str(decision.get("mode") or "").strip()
        if mode not in {"set", "remove_override"}:
            raise ValueError("Record property decision mode must be set or remove_override")
        if mode == "set" and "value" not in decision:
            raise ValueError("Setting a record property requires a value")
        if mode == "remove_override" and "value" in decision:
            raise ValueError("Removing a record property override must not include a value")
        if mode == "set" and "observed_value" not in decision:
            raise ValueError("Setting a record property requires the observed loaded value")
        if "observed_exists" in decision and type(decision["observed_exists"]) is not bool:
            raise ValueError("Record property observed_exists must be true or false")


def _validate_record_property_path(path: Any) -> None:
    from src.core.record_property_curations import is_protected_curation_field

    if not isinstance(path, list) or not path:
        raise ValueError("Record property decision path must be a non-empty list")
    expect_field = True
    for token in path:
        if expect_field:
            if not isinstance(token, str) or not token.strip():
                raise ValueError("Record property field path tokens must be non-empty strings")
            field_name = token.strip()
            if is_protected_curation_field(field_name):
                raise ValueError(f"Field {field_name!r} is managed by the graph build")
            expect_field = False
            continue
        if isinstance(token, str):
            field_name = token.strip()
            if not field_name:
                raise ValueError("Record property field path tokens must be non-empty strings")
            if is_protected_curation_field(field_name):
                raise ValueError(f"Field {field_name!r} is managed by the graph build")
            continue
        if not isinstance(token, dict) or set(token) != {"match"}:
            raise ValueError("Nested list paths require an exact-match selector")
        match = token.get("match")
        if not isinstance(match, dict) or not match:
            raise ValueError("Nested list selectors require at least one match field")
        if any(
            not isinstance(key, str) or not key.strip() or key.startswith("_")
            for key in match
        ):
            raise ValueError("Nested list selector fields must be named public fields")
        expect_field = True
    if expect_field:
        raise ValueError("Record property path cannot end with a list selector")


def _validate_property_value(definition: CuratablePropertyDefinition, value: Any) -> None:
    if value is None:
        if not definition.nullable:
            raise ValueError(
                f"Property {definition.model_type}.{definition.property_name} does not allow null"
            )
        return
    expected = definition.value_type
    if expected is float:
        valid = type(value) in {int, float} and math.isfinite(float(value))
    else:
        valid = type(value) is expected
    if not valid:
        raise ValueError(
            f"Property {definition.model_type}.{definition.property_name} requires "
            f"{definition.value_type_name}{' or null' if definition.nullable else ''}"
        )


def operation_subject(curation_type: str, operation: dict) -> tuple[str, ...]:
    """Return the stable subject whose latest operation wins."""
    validate_operation(curation_type, operation)
    action = operation["action"]
    if action in {"set_property", "unset_property"}:
        target = operation["target"]
        return (
            "property",
            target["kind"],
            target["model_type"],
            target["id"],
            operation["property"],
        )
    if action == "set_properties":
        raise ValueError("set_properties has multiple subjects; use operation_subjects")
    if action in {"remove_edge", "retain_edge"}:
        edge_type = operation["edge_type"]
        left, right = sorted((operation["start_id"], operation["end_id"]))
        return ("edge", edge_type, left, right)
    if action in {"suppress_record", "restore_record"}:
        target = operation["target"]
        return ("record_harmonization", target["model_type"], target["id"])
    assertion_id = str(operation.get("assertion_id") or "").strip()
    if not assertion_id:
        raise ValueError("Assertion curation requires assertion_id")
    return ("assertion", assertion_id)


def operation_subjects(curation_type: str, operation: dict) -> tuple[tuple[str, ...], ...]:
    validate_operation(curation_type, operation)
    if curation_type == RECORD_PROPERTIES:
        target = operation["target"]
        return tuple(
            (
                "record_property",
                target["curation_set"],
                target["kind"],
                target["model_type"],
                target["id"],
                canonical_json(decision["path"]),
            )
            for decision in operation["decisions"]
        )
    if operation["action"] != "set_properties":
        return (operation_subject(curation_type, operation),)
    target = operation["target"]
    property_names = [*(operation.get("values") or {}).keys(), *(operation.get("remove_overrides") or [])]
    return tuple(
        ("property", target["kind"], target["model_type"], target["id"], property_name)
        for property_name in property_names
    )


@dataclass(frozen=True)
class ResolvedPropertyDecision:
    curation_type: str
    target: dict
    property_name: str
    mode: str
    value: Any
    batch_id: str
    published_at: str
    published_by: Optional[dict]
    source_operation: dict

    @property
    def subject(self) -> tuple[str, ...]:
        return (
            "property", self.target["kind"], self.target["model_type"],
            self.target["id"], self.property_name,
        )


@dataclass(frozen=True)
class ResolvedRecordPropertyDecision:
    curation_type: str
    target: dict
    path: list[Any]
    mode: str
    value: Any
    observed_value: Any
    observed_exists: bool
    batch_id: str
    published_at: str
    published_by: Optional[dict]
    source_operation: dict

    @property
    def subject(self) -> tuple[str, ...]:
        return (
            "record_property",
            self.target["curation_set"],
            self.target["kind"],
            self.target["model_type"],
            self.target["id"],
            canonical_json(self.path),
        )


def property_decisions_from_operation(
    curation_type: str,
    operation: dict,
    *,
    batch_id: str,
    published_at: str,
    published_by: Optional[dict],
) -> tuple[ResolvedPropertyDecision, ...]:
    validate_operation(curation_type, operation)
    if curation_type == RECORD_PROPERTIES:
        return ()
    action = operation["action"]
    if action not in {"set_properties", "set_property", "unset_property"}:
        return ()
    if action == "set_properties":
        raw_decisions = [
            (property_name, "set", value)
            for property_name, value in (operation.get("values") or {}).items()
        ]
        raw_decisions.extend(
            (property_name, "remove_override", None)
            for property_name in operation.get("remove_overrides") or []
        )
    else:
        raw_decisions = [(
            operation["property"],
            "set" if action == "set_property" else "remove_override",
            operation.get("value"),
        )]
    return tuple(
        ResolvedPropertyDecision(
            curation_type=curation_type,
            target=dict(operation["target"]),
            property_name=property_name,
            mode=mode,
            value=value,
            batch_id=batch_id,
            published_at=published_at,
            published_by=published_by,
            source_operation=dict(operation),
        )
        for property_name, mode, value in raw_decisions
    )


def record_property_decisions_from_operation(
    curation_type: str,
    operation: dict,
    *,
    batch_id: str,
    published_at: str,
    published_by: Optional[dict],
) -> tuple[ResolvedRecordPropertyDecision, ...]:
    validate_operation(curation_type, operation)
    if curation_type != RECORD_PROPERTIES:
        return ()
    return tuple(
        ResolvedRecordPropertyDecision(
            curation_type=curation_type,
            target=dict(operation["target"]),
            path=list(decision["path"]),
            mode=decision["mode"],
            value=decision.get("value"),
            observed_value=decision.get("observed_value"),
            observed_exists=decision.get("observed_exists", True),
            batch_id=batch_id,
            published_at=published_at,
            published_by=published_by,
            source_operation=dict(operation),
        )
        for decision in operation["decisions"]
    )


def canonical_json(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def payload_sha256(payload: Any) -> str:
    return hashlib.sha256(canonical_json(payload).encode("utf-8")).hexdigest()


def batch_key(curation_type: str, batch_id: str) -> str:
    validate_curation_type(curation_type)
    if not re.fullmatch(r"[A-Za-z0-9._-]+", str(batch_id or "")):
        raise ValueError("Invalid curation batch id")
    return f"{PUBLISHED_PREFIX}/{curation_type}/batches/{batch_id}.json"


def manifest_key(curation_type: str) -> str:
    validate_curation_type(curation_type)
    return f"{PUBLISHED_PREFIX}/{curation_type}/manifest.json"


@dataclass(frozen=True)
class ResolvedCurationOperation:
    curation_type: str
    operation: dict
    batch_id: str
    published_at: str
    published_by: Optional[dict]


@dataclass
class CurationSnapshot:
    curation_type: str
    manifest_revision: int
    manifest_hash: str
    batch_ids: list[str]
    batch_hashes: list[str]
    operations: list[ResolvedCurationOperation]
    fingerprint: str
    source_uri: str
    _active_by_subject: dict[tuple[str, ...], ResolvedCurationOperation] = field(
        default_factory=dict,
        repr=False,
    )
    _active_property_decisions: dict[tuple[str, ...], ResolvedPropertyDecision] = field(
        default_factory=dict,
        repr=False,
    )
    _active_record_property_decisions: dict[
        tuple[str, ...], ResolvedRecordPropertyDecision
    ] = field(default_factory=dict, repr=False)

    @property
    def active_operations(self) -> list[ResolvedCurationOperation]:
        return list(self._active_by_subject.values())

    @property
    def active_property_decisions(self) -> list[ResolvedPropertyDecision]:
        return list(self._active_property_decisions.values())

    @property
    def active_record_property_decisions(self) -> list[ResolvedRecordPropertyDecision]:
        return list(self._active_record_property_decisions.values())

    def record_property_decisions_for_set(
        self, curation_set: str
    ) -> list[ResolvedRecordPropertyDecision]:
        return [
            decision
            for decision in self.active_record_property_decisions
            if decision.target.get("curation_set") == curation_set
        ]

    def metadata(self) -> dict:
        return {
            "curation_type": self.curation_type,
            "manifest_revision": self.manifest_revision,
            "manifest_hash": self.manifest_hash,
            "batch_ids": self.batch_ids,
            "batch_hashes": self.batch_hashes,
            "resolved_operation_fingerprint": self.fingerprint,
            "active_operation_count": (
                len(self._active_by_subject)
                + len(self._active_property_decisions)
                + len(self._active_record_property_decisions)
            ),
            "source_uri": self.source_uri,
        }


def _read_json(storage, key: str) -> tuple[dict, str]:
    raw = storage.read_text(key)
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Invalid curation JSON at s3://{storage.bucket}/{key}: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"Curation JSON must be an object at s3://{storage.bucket}/{key}")
    return payload, raw


def _is_missing_object_error(exc: Exception) -> bool:
    error_code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
    return isinstance(exc, KeyError) or error_code in {"404", "NoSuchKey", "NotFound"}


def resolve_curation_type(storage, curation_type: str, *, allow_missing: bool = False) -> CurationSnapshot:
    validate_curation_type(curation_type)
    key = manifest_key(curation_type)
    try:
        manifest, _raw_manifest = _read_json(storage, key)
    except Exception as exc:
        if not _is_missing_object_error(exc):
            raise
        if not allow_missing:
            raise ValueError(f"Missing curation manifest: s3://{storage.bucket}/{key}")
        manifest = {
            "format_version": FORMAT_VERSION,
            "curation_type": curation_type,
            "revision": 0,
            "batches": [],
        }
    if manifest.get("format_version") != FORMAT_VERSION:
        raise ValueError(f"Unsupported curation manifest version in {key}")
    if manifest.get("curation_type") != curation_type:
        raise ValueError(f"Curation manifest type mismatch in {key}")

    operations: list[ResolvedCurationOperation] = []
    batch_ids: list[str] = []
    batch_hashes: list[str] = []
    active_by_subject: dict[tuple[str, ...], ResolvedCurationOperation] = {}
    active_property_decisions: dict[tuple[str, ...], ResolvedPropertyDecision] = {}
    active_record_property_decisions: dict[
        tuple[str, ...], ResolvedRecordPropertyDecision
    ] = {}
    for entry in manifest.get("batches") or []:
        batch_id = str(entry.get("batch_id") or "").strip()
        object_key = str(entry.get("object_key") or batch_key(curation_type, batch_id))
        batch, _raw_batch = _read_json(storage, object_key)
        actual_hash = payload_sha256(batch)
        expected_hash = str(entry.get("sha256") or "")
        if not expected_hash or actual_hash != expected_hash:
            raise ValueError(f"Curation batch hash mismatch: s3://{storage.bucket}/{object_key}")
        if batch.get("format_version") != FORMAT_VERSION or batch.get("curation_type") != curation_type:
            raise ValueError(f"Curation batch contract mismatch: s3://{storage.bucket}/{object_key}")
        if batch.get("curation_batch_id") != batch_id:
            raise ValueError(f"Curation batch id mismatch: s3://{storage.bucket}/{object_key}")
        batch_ids.append(batch_id)
        batch_hashes.append(actual_hash)
        for operation in batch.get("operations") or []:
            validate_operation(curation_type, operation)
            resolved = ResolvedCurationOperation(
                curation_type=curation_type,
                operation=dict(operation),
                batch_id=batch_id,
                published_at=str(batch.get("published_at") or batch.get("created_at") or ""),
                published_by=batch.get("created_by"),
            )
            operations.append(resolved)
            property_decisions = property_decisions_from_operation(
                curation_type,
                operation,
                batch_id=batch_id,
                published_at=resolved.published_at,
                published_by=resolved.published_by,
            )
            if property_decisions:
                for decision in property_decisions:
                    active_property_decisions[decision.subject] = decision
            elif curation_type == RECORD_PROPERTIES:
                for decision in record_property_decisions_from_operation(
                    curation_type,
                    operation,
                    batch_id=batch_id,
                    published_at=resolved.published_at,
                    published_by=resolved.published_by,
                ):
                    active_record_property_decisions[decision.subject] = decision
            else:
                active_by_subject[operation_subject(curation_type, operation)] = resolved

    fingerprint_payload = [
        {
            "subject": list(subject),
            "batch_id": resolved.batch_id,
            "operation": resolved.operation,
        }
        for subject, resolved in sorted(active_by_subject.items())
    ]
    fingerprint_payload.extend({
        "subject": list(subject),
        "batch_id": decision.batch_id,
        "decision": {
            "mode": decision.mode,
            "value": decision.value,
        },
    } for subject, decision in sorted(active_property_decisions.items()))
    fingerprint_payload.extend({
        "subject": list(subject),
        "batch_id": decision.batch_id,
        "decision": {
            "mode": decision.mode,
            "value": decision.value,
            "observed_value": decision.observed_value,
            "observed_exists": decision.observed_exists,
        },
    } for subject, decision in sorted(active_record_property_decisions.items()))
    fingerprint_payload.sort(key=lambda item: item["subject"])
    return CurationSnapshot(
        curation_type=curation_type,
        manifest_revision=int(manifest.get("revision") or 0),
        manifest_hash=payload_sha256(manifest),
        batch_ids=batch_ids,
        batch_hashes=batch_hashes,
        operations=operations,
        fingerprint=payload_sha256(fingerprint_payload),
        source_uri=f"s3://{storage.bucket}/{key}",
        _active_by_subject=active_by_subject,
        _active_property_decisions=active_property_decisions,
        _active_record_property_decisions=active_record_property_decisions,
    )


def resolve_curation_types(
    storage,
    curation_types: Iterable[str],
    *,
    allow_missing: bool = False,
) -> dict[str, CurationSnapshot]:
    return {
        curation_type: resolve_curation_type(storage, curation_type, allow_missing=allow_missing)
        for curation_type in dict.fromkeys(curation_types)
    }
