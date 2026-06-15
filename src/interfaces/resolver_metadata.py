import hashlib
import json
from copy import deepcopy

from src.registry.fetchers import MaterializedDataset


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _metadata_safe(value):
    if isinstance(value, MaterializedDataset):
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
    resolver_snapshot = kwargs.get("resolver_snapshot") if isinstance(kwargs, dict) else None
    return {
        "label": label,
        "import": normalized.get("import"),
        "class": normalized.get("class"),
        "kwargs": kwargs,
        "resolver_snapshot": resolver_snapshot,
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


def resolver_fingerprint_summary(fingerprints_by_type: dict | None) -> dict:
    return {
        node_type: {
            "label": metadata.get("label"),
            "import": metadata.get("import"),
            "class": metadata.get("class"),
            "resolver_snapshot": (metadata.get("resolver_snapshot") or {}).get("snapshot_id")
            if isinstance(metadata.get("resolver_snapshot"), dict) else None,
            "fingerprint": metadata.get("fingerprint"),
        }
        for node_type, metadata in (fingerprints_by_type or {}).items()
    }
