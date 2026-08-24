"""Persistent, per-curator draft carts for QA Browser graph curations."""

from __future__ import annotations

import hashlib
import json
import re
import threading
import uuid
from datetime import datetime, timezone
from typing import Any, Optional


FORMAT_VERSION = 1
DRAFT_PREFIX = "curation-drafts/v1"
PUBLISHED_PREFIX = "curations/v1"
_cart_lock = threading.Lock()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _clean_required(value: Any, label: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"{label} is required")
    return text


def _safe_graph_name(graph: str) -> str:
    clean_graph = _clean_required(graph, "graph")
    if not re.fullmatch(r"[a-z0-9_]+", clean_graph):
        raise ValueError("graph must contain only lowercase letters, numbers, and underscores")
    return clean_graph


def curator_key(curator_id: str) -> str:
    normalized = _clean_required(curator_id, "curator identity").casefold()
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:24]


def draft_key(graph: str, curator_id: str) -> str:
    return f"{DRAFT_PREFIX}/{_safe_graph_name(graph)}/{curator_key(curator_id)}.json"


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


def _write_json(storage, key: str, payload: dict) -> None:
    storage.write_text(
        key,
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        content_type="application/json",
    )


def _edge_decision_subject(operation: dict) -> Optional[tuple[str, str, str]]:
    if operation.get("action") not in {"remove_edge", "retain_edge"}:
        return None
    edge_type = str(operation.get("edge_type") or "").strip()
    start_id = str(operation.get("start_id") or "").strip()
    end_id = str(operation.get("end_id") or "").strip()
    if not edge_type or not start_id or not end_id:
        return None
    left, right = sorted((start_id, end_id))
    return edge_type, left, right


def empty_cart(graph: str, curator_id: str, curator_name: str) -> dict:
    return {
        "format_version": FORMAT_VERSION,
        "draft_id": str(uuid.uuid4()),
        "graph": _safe_graph_name(graph),
        "curator": {
            "id": _clean_required(curator_id, "curator identity"),
            "name": _clean_required(curator_name, "curator name"),
        },
        "created_at": _utc_now(),
        "updated_at": _utc_now(),
        "operations": [],
    }


def load_cart(storage, graph: str, curator_id: str, curator_name: str) -> dict:
    key = draft_key(graph, curator_id)
    cart = _read_optional_json(storage, key) or empty_cart(graph, curator_id, curator_name)
    if cart.get("format_version") != FORMAT_VERSION or cart.get("graph") != graph:
        raise ValueError(f"Unsupported curation cart at s3://{storage.bucket}/{key}")
    cart["operation_count"] = len(cart.get("operations") or [])
    cart["storage_uri"] = f"s3://{storage.bucket}/{key}"
    return cart


def add_cart_operation(
    storage,
    graph: str,
    curator_id: str,
    curator_name: str,
    operation: dict,
) -> dict:
    with _cart_lock:
        cart = load_cart(storage, graph, curator_id, curator_name)
        operation_payload = dict(operation)
        operation_payload.pop("operation_id", None)
        operation_payload.pop("added_at", None)
        operation_payload.pop("added_by", None)
        identity_payload = {
            key: operation_payload.get(key)
            for key in ("action", "edge_type", "start_id", "end_id", "symmetric")
            if key in operation_payload
        }
        operation_id = hashlib.sha256(
            json.dumps(identity_payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:24]
        operations = cart.setdefault("operations", [])
        if not any(item.get("operation_id") == operation_id for item in operations):
            decision_subject = _edge_decision_subject(operation_payload)
            if decision_subject is not None:
                operations[:] = [
                    item
                    for item in operations
                    if _edge_decision_subject(item) != decision_subject
                ]
            operations.append({
                **operation_payload,
                "operation_id": operation_id,
                "added_at": _utc_now(),
                "added_by": {
                    "id": curator_id,
                    "name": curator_name,
                },
            })
        cart["curator"] = {"id": curator_id, "name": curator_name}
        cart["updated_at"] = _utc_now()
        cart["operation_count"] = len(operations)
        key = draft_key(graph, curator_id)
        cart.pop("storage_uri", None)
        _write_json(storage, key, cart)
        cart["storage_uri"] = f"s3://{storage.bucket}/{key}"
        return cart


def remove_cart_operation(storage, graph: str, curator_id: str, curator_name: str, operation_id: str) -> dict:
    with _cart_lock:
        cart = load_cart(storage, graph, curator_id, curator_name)
        cart["operations"] = [
            operation
            for operation in cart.get("operations") or []
            if operation.get("operation_id") != operation_id
        ]
        cart["updated_at"] = _utc_now()
        cart["operation_count"] = len(cart["operations"])
        key = draft_key(graph, curator_id)
        cart.pop("storage_uri", None)
        if cart["operations"]:
            _write_json(storage, key, cart)
            cart["storage_uri"] = f"s3://{storage.bucket}/{key}"
        else:
            storage.delete_file(key)
            cart["storage_uri"] = f"s3://{storage.bucket}/{key}"
        return cart


def publish_cart(
    storage,
    graph: str,
    curator_id: str,
    curator_name: str,
    batch_name: str,
    description: str = "",
) -> dict:
    with _cart_lock:
        cart = load_cart(storage, graph, curator_id, curator_name)
        operations = cart.get("operations") or []
        if not operations:
            raise ValueError("The curation cart is empty")
        clean_batch_name = _clean_required(batch_name, "batch name")
        published_at = _utc_now()
        batch_id = f"qa-browser-{cart['draft_id']}"
        batch = {
            "format_version": FORMAT_VERSION,
            "curation_batch_id": batch_id,
            "graph": graph,
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
        published_key = f"{PUBLISHED_PREFIX}/{graph}/{batch_id}.json"
        existing_batch = _read_optional_json(storage, published_key)
        if existing_batch is None:
            _write_json(storage, published_key, batch)
        else:
            batch = existing_batch
        storage.delete_file(draft_key(graph, curator_id))
        return {
            "batch": batch,
            "storage_uri": f"s3://{storage.bucket}/{published_key}",
            "operation_count": len(operations),
        }
