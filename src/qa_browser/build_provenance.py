"""Read-only projections of exact dataset inputs recorded on an ODIN graph."""

from typing import Dict, List, Optional


def extract_build_inputs(etl_metadata: Optional[dict]) -> List[dict]:
    if not etl_metadata:
        return []

    inputs_by_identity: Dict[tuple[str, str], dict] = {}

    def add_input(dataset: dict, usage: Optional[str] = None) -> None:
        snapshot_id = dataset.get("snapshot_id")
        kind = dataset.get("kind") or "source_snapshot"
        if not isinstance(snapshot_id, str) or not snapshot_id or not isinstance(kind, str):
            return
        entry = inputs_by_identity.setdefault(
            (kind, snapshot_id),
            {
                "kind": dataset.get("kind"),
                "source": dataset.get("source"),
                "dataset": dataset.get("dataset"),
                "version": dataset.get("version"),
                "version_date": dataset.get("version_date"),
                "download_date": dataset.get("download_date"),
                "snapshot_id": snapshot_id,
                "manifest_uri": dataset.get("manifest_uri"),
                "usages": [],
            },
        )
        usages = set(entry["usages"])
        usages.update(dataset.get("usages") or [])
        if usage:
            usages.add(usage)
        entry["usages"] = sorted(usages)

    def visit(value, usage: Optional[str] = None) -> None:
        if isinstance(value, dict):
            if value.get("source") and value.get("dataset"):
                add_input(value, usage)
            for nested in value.values():
                visit(nested, usage)
        elif isinstance(value, list):
            for nested in value:
                visit(nested, usage)

    for dataset in etl_metadata.get("registry_datasets") or []:
        if isinstance(dataset, dict):
            add_input(dataset)

    resolver_metadata = (etl_metadata.get("resolver_metadata") or {}).get("by_type") or {}
    for node_type, metadata in resolver_metadata.items():
        if isinstance(metadata, dict):
            usage = f"resolver:{metadata.get('label') or node_type}"
            visit(metadata.get("kwargs") or {}, usage)

    return sorted(
        inputs_by_identity.values(),
        key=lambda item: (
            item.get("kind") or "",
            item.get("source") or "",
            item.get("dataset") or "",
            item.get("version") or "",
        ),
    )
