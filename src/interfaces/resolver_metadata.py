import hashlib
import json
from copy import deepcopy

from src.models.registry_dataset import RegistryDatasetMetadata


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _metadata_safe(value):
    if isinstance(value, RegistryDatasetMetadata):
        metadata = value.to_metadata()
        _strip_local_dirs(metadata)
        return metadata
    if isinstance(value, dict):
        return {key: _metadata_safe(entry) for key, entry in value.items()}
    if isinstance(value, list):
        return [_metadata_safe(entry) for entry in value]
    return value


def _strip_local_dirs(value) -> None:
    if isinstance(value, dict):
        value.pop("local_dir", None)
        for entry in value.values():
            _strip_local_dirs(entry)
    elif isinstance(value, list):
        for entry in value:
            _strip_local_dirs(entry)


def resolver_fingerprint(resolver_config: dict) -> dict:
    normalized = _metadata_safe(deepcopy(resolver_config))
    label = normalized.pop("label", None)
    config_hash = hashlib.sha256(_canonical_json(normalized).encode("utf-8")).hexdigest()
    kwargs = normalized.get("kwargs", {})
    return {
        "label": label,
        "import": normalized.get("import"),
        "class": normalized.get("class"),
        "kwargs": kwargs,
        "dataset_inputs": _dataset_inputs(kwargs),
        "fingerprint": config_hash,
    }


def resolver_fingerprints_by_type(resolver_configs: list[dict] | None) -> dict:
    fingerprints = {}
    for resolver_config in resolver_configs or []:
        metadata = resolver_fingerprint(resolver_config)
        node_types = _resolver_types_from_metadata(metadata)
        for node_type in node_types:
            if node_type in fingerprints:
                raise ValueError(f"Multiple resolver configs declare type {node_type}")
            fingerprints[node_type] = metadata
    return fingerprints


def _resolver_types_from_metadata(metadata: dict) -> list[str]:
    return list((metadata.get("kwargs") or {}).get("types") or [])


def _dataset_inputs(value) -> list[dict]:
    inputs = []
    if isinstance(value, dict):
        snapshot_id = value.get("snapshot_id")
        kind = value.get("kind")
        if isinstance(snapshot_id, str) and isinstance(kind, str):
            inputs.append({"kind": kind, "snapshot_id": snapshot_id})
        else:
            for entry in value.values():
                inputs.extend(_dataset_inputs(entry))
    elif isinstance(value, list):
        for entry in value:
            inputs.extend(_dataset_inputs(entry))
    return sorted(inputs, key=lambda item: (item["kind"], item["snapshot_id"]))


def resolver_fingerprint_summary(fingerprints_by_type: dict | None) -> dict:
    return {
        node_type: {
            "label": metadata.get("label"),
            "import": metadata.get("import"),
            "class": metadata.get("class"),
            "dataset_inputs": metadata.get("dataset_inputs") or [],
            "fingerprint": metadata.get("fingerprint"),
        }
        for node_type, metadata in (fingerprints_by_type or {}).items()
    }
