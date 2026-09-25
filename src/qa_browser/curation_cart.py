"""Persistent, per-curator draft carts for QA Browser graph curations."""

from __future__ import annotations

import hashlib
import json
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from src.core.curations import (
    FORMAT_VERSION,
    batch_key,
    manifest_key,
    operation_subjects,
    payload_sha256,
    validate_curation_type,
    validate_operation,
)

DRAFT_PREFIX = "curation-drafts/v2"
_cart_lock = threading.Lock()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_required(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} is required")
    return text


def _safe_curation_type(curation_type: str) -> str:
    clean_type = _clean_required(curation_type, "curation type")
    validate_curation_type(clean_type)
    return clean_type


def curator_key(curator_id: str) -> str:
    normalized = _clean_required(curator_id, "curator identity").casefold()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]


def draft_key(curation_type: str, curator_id: str) -> str:
    return f"{DRAFT_PREFIX}/{_safe_curation_type(curation_type)}/{curator_key(curator_id)}.json"


def _read_optional_json(storage, key: str) -> Optional[dict]:
    try:
        return json.loads(storage.read_text(key))
    except KeyError:
        return None
    except Exception as exc:
        error_code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if error_code in {"404", "NoSuchKey", "NotFound"}:
            return None
        raise


def _write_immutable_json(storage, key: str, payload: dict) -> bool:
    """Create an immutable object and report whether this publisher won the race."""
    text = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if not hasattr(storage, "read_text_with_etag"):
        if _read_optional_json(storage, key) is not None:
            return False
        storage.write_text(key, text, content_type="application/json")
        return True
    try:
        storage.write_text(
            key,
            text,
            content_type="application/json",
            if_none_match="*",
        )
        return True
    except Exception as exc:
        if _is_precondition_failure(exc):
            return False
        raise


def _read_optional_json_with_etag(storage, key: str) -> tuple[Optional[dict], Optional[str]]:
    if not hasattr(storage, "read_text_with_etag"):
        return _read_optional_json(storage, key), None
    try:
        raw, etag = storage.read_text_with_etag(key)
        return json.loads(raw), etag
    except Exception as exc:
        error_code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
        if isinstance(exc, KeyError) or error_code in {"404", "NoSuchKey", "NotFound"}:
            return None, None
        raise


def _is_precondition_failure(exc: Exception) -> bool:
    error_code = str(getattr(exc, "response", {}).get("Error", {}).get("Code", ""))
    return error_code in {"409", "412", "ConditionalRequestConflict", "PreconditionFailed"}


def _publish_manifest_entry(storage, curation_type: str, entry: dict) -> dict:
    key = manifest_key(curation_type)
    for _attempt in range(5):
        manifest, etag = _read_optional_json_with_etag(storage, key)
        manifest = manifest or {
            "format_version": FORMAT_VERSION,
            "curation_type": curation_type,
            "revision": 0,
            "batches": [],
        }
        if (
            manifest.get("format_version") != FORMAT_VERSION
            or manifest.get("curation_type") != curation_type
        ):
            raise ValueError(f"Unsupported curation manifest at s3://{storage.bucket}/{key}")
        if any(item.get("batch_id") == entry["batch_id"] for item in manifest.get("batches") or []):
            return manifest
        manifest.setdefault("batches", []).append(entry)
        manifest["revision"] = int(manifest.get("revision") or 0) + 1
        manifest["updated_at"] = _utc_now()
        text = json.dumps(manifest, indent=2, sort_keys=True) + "\n"
        try:
            if hasattr(storage, "read_text_with_etag"):
                storage.write_text(
                    key,
                    text,
                    content_type="application/json",
                    if_match=etag,
                    if_none_match="*" if etag is None else None,
                )
            else:
                storage.write_text(key, text, content_type="application/json")
            return manifest
        except Exception as exc:
            if not _is_precondition_failure(exc):
                raise
    raise RuntimeError(f"Curation manifest changed repeatedly while publishing {entry['batch_id']}")


def empty_cart(curation_type: str, curator_id: str, curator_name: str) -> dict:
    return {
        "format_version": FORMAT_VERSION,
        "draft_id": str(uuid.uuid4()),
        "curation_type": _safe_curation_type(curation_type),
        "curator": {
            "id": _clean_required(curator_id, "curator identity"),
            "name": _clean_required(curator_name, "curator name"),
        },
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "operations": [],
    }


def load_cart(storage, curation_type: str, curator_id: str, curator_name: str) -> dict:
    key = draft_key(curation_type, curator_id)
    cart, _etag = _load_cart_state(storage, curation_type, curator_id, curator_name)
    return _present_cart(storage, key, cart)


def _load_cart_state(
    storage,
    curation_type: str,
    curator_id: str,
    curator_name: str,
) -> tuple[dict, Optional[str]]:
    key = draft_key(curation_type, curator_id)
    cart, etag = _read_optional_json_with_etag(storage, key)
    cart = cart or empty_cart(curation_type, curator_id, curator_name)
    if (
        cart.get("format_version") != FORMAT_VERSION
        or cart.get("curation_type") != curation_type
    ):
        raise ValueError(f"Unsupported curation cart at s3://{storage.bucket}/{key}")
    return cart, etag


def _present_cart(storage, key: str, cart: dict) -> dict:
    cart["operation_count"] = len(cart.get("operations") or [])
    cart["storage_uri"] = f"s3://{storage.bucket}/{key}"
    return cart


def _write_draft(storage, key: str, cart: dict, etag: Optional[str]) -> None:
    text = json.dumps(cart, indent=2, sort_keys=True) + "\n"
    if hasattr(storage, "read_text_with_etag"):
        storage.write_text(
            key,
            text,
            content_type="application/json",
            if_match=etag,
            if_none_match="*" if etag is None else None,
        )
    else:
        storage.write_text(key, text, content_type="application/json")


def _delete_draft(storage, key: str, etag: Optional[str]) -> None:
    if hasattr(storage, "read_text_with_etag"):
        storage.delete_file(key, if_match=etag)
    else:
        storage.delete_file(key)


def add_cart_operation(
    storage,
    curation_type: str,
    curator_id: str,
    curator_name: str,
    operation: dict,
    replace_target: bool = False,
) -> dict:
    with _cart_lock:
        validate_operation(curation_type, operation)
        incoming_payload = dict(operation)
        incoming_payload.pop("operation_id", None)
        incoming_payload.pop("added_at", None)
        incoming_payload.pop("added_by", None)
        key = draft_key(curation_type, curator_id)
        for _attempt in range(5):
            cart, etag = _load_cart_state(
                storage, curation_type, curator_id, curator_name
            )
            operations = cart.setdefault("operations", [])
            operation_payload = incoming_payload
            if operation_payload.get("action") == "set_properties":
                target = operation_payload["target"]
                same_target = [
                    item for item in operations
                    if item.get("action") in {"set_properties", "set_property", "unset_property"}
                    and item.get("target") == target
                ]
                if same_target and not replace_target:
                    merged_values = {}
                    merged_remove_overrides = []
                    for item in same_target:
                        if item.get("action") == "set_properties":
                            merged_values.update(item.get("values") or {})
                            merged_remove_overrides.extend(item.get("remove_overrides") or [])
                        elif item.get("action") == "set_property":
                            merged_values[item["property"]] = item.get("value")
                        else:
                            merged_remove_overrides.append(item["property"])
                    for property_name, value in (operation_payload.get("values") or {}).items():
                        merged_values[property_name] = value
                        merged_remove_overrides = [
                            item for item in merged_remove_overrides if item != property_name
                        ]
                    for property_name in operation_payload.get("remove_overrides") or []:
                        merged_values.pop(property_name, None)
                        if property_name not in merged_remove_overrides:
                            merged_remove_overrides.append(property_name)
                    operation_payload = {
                        **operation_payload,
                        "values": merged_values,
                        "remove_overrides": merged_remove_overrides,
                    }
                operations[:] = [item for item in operations if item not in same_target]
            validate_operation(curation_type, operation_payload)
            operation_id = hashlib.sha256(
                json.dumps(operation_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
            ).hexdigest()[:24]
            if not any(item.get("operation_id") == operation_id for item in operations):
                subjects = set(operation_subjects(curation_type, operation_payload))
                operations[:] = [
                    item
                    for item in operations
                    if subjects.isdisjoint(operation_subjects(curation_type, item))
                ]
                operations.append({
                    **operation_payload,
                    "operation_id": operation_id,
                    "added_at": _utc_now(),
                    "added_by": {"id": curator_id, "name": curator_name},
                })
            cart["curator"] = {"id": curator_id, "name": curator_name}
            cart["updated_at"] = _utc_now()
            try:
                _write_draft(storage, key, cart, etag)
                return _present_cart(storage, key, cart)
            except Exception as exc:
                if not _is_precondition_failure(exc):
                    raise
        raise RuntimeError("Curation draft changed repeatedly while adding an operation")


def remove_cart_operation(
    storage,
    curation_type: str,
    curator_id: str,
    curator_name: str,
    operation_id: str,
) -> dict:
    with _cart_lock:
        key = draft_key(curation_type, curator_id)
        for _attempt in range(5):
            cart, etag = _load_cart_state(
                storage, curation_type, curator_id, curator_name
            )
            cart["operations"] = [
                operation
                for operation in cart.get("operations") or []
                if operation.get("operation_id") != operation_id
            ]
            cart["updated_at"] = _utc_now()
            try:
                if cart["operations"]:
                    _write_draft(storage, key, cart, etag)
                elif etag is not None or not hasattr(storage, "read_text_with_etag"):
                    _delete_draft(storage, key, etag)
                return _present_cart(storage, key, cart)
            except Exception as exc:
                if not _is_precondition_failure(exc):
                    raise
        raise RuntimeError("Curation draft changed repeatedly while removing an operation")


def _remove_published_operations_from_draft(
    storage,
    curation_type: str,
    curator_id: str,
    curator_name: str,
    published_operation_ids: set[str],
) -> None:
    key = draft_key(curation_type, curator_id)
    for _attempt in range(5):
        cart, etag = _load_cart_state(storage, curation_type, curator_id, curator_name)
        remaining = [
            operation
            for operation in cart.get("operations") or []
            if operation.get("operation_id") not in published_operation_ids
        ]
        try:
            if remaining:
                now = _utc_now()
                cart["draft_id"] = str(uuid.uuid4())
                cart["created_at"] = now
                cart["updated_at"] = now
                cart["operations"] = remaining
                _write_draft(storage, key, cart, etag)
            elif etag is not None or not hasattr(storage, "read_text_with_etag"):
                _delete_draft(storage, key, etag)
            return
        except Exception as exc:
            if not _is_precondition_failure(exc):
                raise
    raise RuntimeError("Curation draft changed repeatedly while completing publication")


def publish_cart(
    storage,
    curation_type: str,
    curator_id: str,
    curator_name: str,
    batch_name: str,
    description: str = "",
) -> dict:
    with _cart_lock:
        cart, _etag = _load_cart_state(storage, curation_type, curator_id, curator_name)
        operations = cart.get("operations") or []
        if not operations:
            raise ValueError("The curation cart is empty")
        clean_batch_name = _clean_required(batch_name, "batch name")
        published_at = _utc_now()
        batch_id = f"qa-browser-{cart['draft_id']}"
        batch = {
            "format_version": FORMAT_VERSION,
            "curation_batch_id": batch_id,
            "curation_type": curation_type,
            "name": clean_batch_name,
            "description": str(description or "").strip(),
            "created_at": published_at,
            "published_at": published_at,
            "created_by": {
                "id": curator_id,
                "name": curator_name,
            },
            "source": {
                "type": "qa_browser_curation_cart",
                "draft_id": cart["draft_id"],
                "draft_created_at": cart["created_at"],
                "draft_updated_at": cart["updated_at"],
            },
            "operations": operations,
        }
        for operation in operations:
            validate_operation(curation_type, operation)
        published_key = batch_key(curation_type, batch_id)
        if not _write_immutable_json(storage, published_key, batch):
            batch = _read_optional_json(storage, published_key)
            if batch is None:
                raise RuntimeError(f"Published curation batch disappeared: {published_key}")
        batch_hash = payload_sha256(batch)
        active_manifest_key = manifest_key(curation_type)
        manifest = _publish_manifest_entry(storage, curation_type, {
            "batch_id": batch_id,
            "object_key": published_key,
            "sha256": batch_hash,
            "published_at": batch["published_at"],
        })
        _remove_published_operations_from_draft(
            storage,
            curation_type,
            curator_id,
            curator_name,
            {operation.get("operation_id") for operation in operations},
        )
        return {
            "batch": batch,
            "storage_uri": f"s3://{storage.bucket}/{published_key}",
            "manifest_uri": f"s3://{storage.bucket}/{active_manifest_key}",
            "manifest_revision": manifest["revision"],
            "operation_count": len(operations),
        }
