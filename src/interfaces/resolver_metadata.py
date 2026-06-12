import hashlib
import json
from copy import deepcopy

from src.registry.fetchers import MaterializedDataset


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _metadata_safe(value):
    if isinstance(value, MaterializedDataset):
        return value.to_metadata()
    if isinstance(value, dict):
        return {key: _metadata_safe(entry) for key, entry in value.items()}
    if isinstance(value, list):
        return [_metadata_safe(entry) for entry in value]
    return value


def resolver_fingerprint(resolver_config: dict) -> dict:
    normalized = _metadata_safe(deepcopy(resolver_config))
    label = normalized.pop("label", None)
    config_hash = hashlib.sha256(_canonical_json(normalized).encode("utf-8")).hexdigest()
    return {
        "label": label,
        "import": normalized.get("import"),
        "class": normalized.get("class"),
        "kwargs": normalized.get("kwargs", {}),
        "fingerprint": config_hash,
    }


def resolver_fingerprints_by_type(resolver_configs: list[dict] | None) -> dict:
    fingerprints = {}
    for resolver_config in resolver_configs or []:
        metadata = resolver_fingerprint(resolver_config)
        for node_type in metadata.get("kwargs", {}).get("types", []) or []:
            if node_type in fingerprints:
                raise ValueError(f"Multiple resolver configs declare type {node_type}")
            fingerprints[node_type] = metadata
    return fingerprints


def resolver_fingerprint_summary(fingerprints_by_type: dict | None) -> dict:
    return {
        node_type: {
            "label": metadata.get("label"),
            "import": metadata.get("import"),
            "class": metadata.get("class"),
            "fingerprint": metadata.get("fingerprint"),
        }
        for node_type, metadata in (fingerprints_by_type or {}).items()
    }
