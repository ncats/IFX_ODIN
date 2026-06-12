import time
from typing import Callable, Dict, List, Optional, Tuple


RegistryEntry = Dict[str, object]
RegistryUsage = Dict[str, List[str]]


def registry_entry_id(entry: dict) -> Optional[str]:
    return entry.get("registry_id") or entry.get("snapshot_id") or entry.get("registration_id")


def graph_badge_hue(graph_name: str) -> int:
    return 15 + (sum((index + 1) * ord(ch) for index, ch in enumerate(graph_name)) % 320)


def graph_usage_filters(usage_by_registry_id: RegistryUsage) -> List[dict]:
    return [
        {
            "name": graph_name,
            "hue": graph_badge_hue(graph_name),
        }
        for graph_name in sorted({graph for graphs in usage_by_registry_id.values() for graph in graphs})
    ]


def graph_usage_styles(filters: List[dict]) -> Dict[str, str]:
    return {
        graph["name"]: f"--graph-hue: {graph['hue']};"
        for graph in filters
    }


def with_graph_usages(entry: dict, usage_by_registry_id: RegistryUsage) -> dict:
    entry_id = registry_entry_id(entry)
    return {
        **entry,
        "registry_id": entry_id,
        "graph_usages": usage_by_registry_id.get(entry_id, []) if entry_id else [],
    }


def group_by_source_dataset(entries: List[dict], item_key: str) -> List[dict]:
    grouped_sources = {}
    for entry in entries:
        source = entry.get("source") or ""
        dataset = entry.get("dataset") or ""
        source_group = grouped_sources.setdefault(source, {
            "source": entry.get("source"),
            "datasets": {},
        })
        dataset_group = source_group["datasets"].setdefault(dataset, {
            "dataset": entry.get("dataset"),
            item_key: [],
        })
        dataset_group[item_key].append(entry)

    return [
        {
            "source": source_group["source"],
            "datasets": list(source_group["datasets"].values()),
        }
        for source_group in grouped_sources.values()
    ]


def extract_registry_datasets(etl_meta: Optional[dict]) -> List[dict]:
    if not etl_meta:
        return []

    datasets_by_registry_id: Dict[str, dict] = {}

    def add_dataset(dataset: dict, usage: Optional[str] = None):
        entry_id = registry_entry_id(dataset)
        if not entry_id:
            return
        entry = datasets_by_registry_id.setdefault(entry_id, {
            "registry_id": entry_id,
            "source": dataset.get("source"),
            "dataset": dataset.get("dataset"),
            "version": dataset.get("version"),
            "version_date": dataset.get("version_date"),
            "download_date": dataset.get("download_date"),
            "snapshot_id": dataset.get("snapshot_id") or entry_id,
            "registration_id": dataset.get("registration_id"),
            "manifest_uri": dataset.get("manifest_uri"),
            "usages": [],
        })
        usages = set(entry.get("usages") or [])
        usages.update(dataset.get("usages") or [])
        if usage:
            usages.add(usage)
        entry["usages"] = sorted(usages)

    def visit(value, usage: Optional[str] = None):
        if isinstance(value, dict):
            if registry_entry_id(value) and value.get("source") and value.get("dataset"):
                add_dataset(value, usage=usage)
            for nested in value.values():
                visit(nested, usage=usage)
        elif isinstance(value, list):
            for nested in value:
                visit(nested, usage=usage)

    for dataset in etl_meta.get("registry_datasets") or []:
        if isinstance(dataset, dict):
            add_dataset(dataset)

    resolver_metadata = (etl_meta.get("resolver_metadata") or {}).get("by_type") or {}
    for node_type, metadata in resolver_metadata.items():
        usage = f"resolver:{metadata.get('label') or node_type}"
        visit(metadata.get("kwargs") or {}, usage=usage)

    return sorted(
        datasets_by_registry_id.values(),
        key=lambda item: (item.get("source") or "", item.get("dataset") or "", item.get("version") or ""),
    )


def load_graph_registry_usage_cached(
    *,
    credentials: dict,
    cache: dict,
    ttl_seconds: int,
    get_sys_db: Callable[[], object],
    get_db: Callable[[str], object],
) -> Tuple[RegistryUsage, Optional[str]]:
    now = time.time()
    cached_usage = cache.get("usage_by_registry_id")
    cached_error = cache.get("error")
    loaded_at = cache.get("loaded_at") or 0.0
    if cached_usage is not None and now - loaded_at < ttl_seconds:
        return cached_usage, cached_error

    usage_by_registry_id: RegistryUsage = {}
    if not credentials:
        return usage_by_registry_id, None

    try:
        sys_db = get_sys_db()
        db_names = [db for db in sys_db.databases() if not db.startswith("_")]
    except Exception as exc:
        error = str(exc)
        cache.update({
            "loaded_at": now,
            "usage_by_registry_id": usage_by_registry_id,
            "error": error,
        })
        return usage_by_registry_id, error

    for db_name in db_names:
        try:
            db = get_db(db_name)
            if not db.has_collection("metadata_store"):
                continue
            cursor = db.aql.execute('RETURN DOCUMENT("metadata_store", "etl_metadata").value')
            results = list(cursor)
            if not results or not results[0]:
                continue
            for dataset in extract_registry_datasets(results[0]):
                entry_id = registry_entry_id(dataset)
                if not entry_id:
                    continue
                graph_names = usage_by_registry_id.setdefault(entry_id, [])
                if db_name not in graph_names:
                    graph_names.append(db_name)
        except Exception:
            continue

    usage_by_registry_id = {
        entry_id: sorted(graph_names)
        for entry_id, graph_names in sorted(usage_by_registry_id.items())
    }
    cache.update({
        "loaded_at": now,
        "usage_by_registry_id": usage_by_registry_id,
        "error": None,
    })
    return usage_by_registry_id, None
