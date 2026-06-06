import hashlib
import json
from copy import deepcopy


def _canonical_json(value) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def resolver_fingerprint(resolver_config: dict) -> dict:
    normalized = deepcopy(resolver_config)
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
