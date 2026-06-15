import time
from typing import Callable, Dict, List, Optional, Tuple


RegistryEntry = Dict[str, object]
RegistryUsage = Dict[str, Dict[str, List[str]]]


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
        for graph_name in sorted({
            graph
            for graphs in usage_by_registry_id.values()
            for graph in _graph_usage_keys(graphs)
        })
    ]


def graph_usage_styles(filters: List[dict]) -> Dict[str, str]:
    return {
        graph["name"]: f"--graph-hue: {graph['hue']};"
        for graph in filters
    }


def with_graph_usages(entry: dict, usage_by_registry_id: RegistryUsage) -> dict:
    entry_id = registry_entry_id(entry)
    graph_usage = _normalize_graph_usage(usage_by_registry_id.get(entry_id, {}) if entry_id else {})
    graph_usage_details = [
        {
            "graph": graph_name,
            "categories": categories,
            "category_label": _usage_category_label(categories),
            "category_class": "-".join(categories),
        }
        for graph_name, categories in sorted(graph_usage.items())
    ]
    return {
        **entry,
        "registry_id": entry_id,
        "graph_usages": [detail["graph"] for detail in graph_usage_details],
        "graph_usage_details": graph_usage_details,
    }


def _graph_usage_keys(graph_usage) -> List[str]:
    if isinstance(graph_usage, dict):
        return list(graph_usage.keys())
    if isinstance(graph_usage, list):
        return list(graph_usage)
    return []


def _normalize_graph_usage(graph_usage) -> Dict[str, List[str]]:
    if isinstance(graph_usage, dict):
        return {
            graph_name: list(categories or [])
            for graph_name, categories in graph_usage.items()
        }
    if isinstance(graph_usage, list):
        return {
            graph_name: []
            for graph_name in graph_usage
        }
    return {}


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
            resolver_usages = [
                usage for usage in dataset.get("usages") or []
                if str(usage).startswith("resolver:")
            ]
            for usage in resolver_usages:
                visit(dataset.get("resolver_inputs") or {}, usage=usage)

    resolver_metadata = (etl_meta.get("resolver_metadata") or {}).get("by_type") or {}
    for node_type, metadata in resolver_metadata.items():
        usage = f"resolver:{metadata.get('label') or node_type}"
        resolver_snapshot = metadata.get("resolver_snapshot")
        if isinstance(resolver_snapshot, dict) and registry_entry_id(resolver_snapshot):
            add_dataset(resolver_snapshot, usage=usage)
            visit(resolver_snapshot.get("resolver_inputs") or {}, usage=usage)
        else:
            visit(metadata.get("kwargs") or {}, usage=usage)

    return sorted(
        datasets_by_registry_id.values(),
        key=lambda item: (item.get("source") or "", item.get("dataset") or "", item.get("version") or ""),
    )


def _compact_registry_dependency(dataset: Optional[dict], *, include_derived_from: bool = True) -> Optional[dict]:
    if not isinstance(dataset, dict):
        return None
    entry_id = registry_entry_id(dataset)
    if not entry_id:
        return None
    dependency = {
        "registry_id": entry_id,
        "source": dataset.get("source"),
        "dataset": dataset.get("dataset"),
        "version": dataset.get("version"),
        "snapshot_id": dataset.get("snapshot_id") or entry_id,
        "registration_id": dataset.get("registration_id"),
        "manifest_uri": dataset.get("manifest_uri"),
        "kind": dataset.get("kind"),
    }
    if include_derived_from:
        dependency["derived_from"] = [
            upstream
            for upstream in (
                _compact_registry_dependency(entry, include_derived_from=False)
                for entry in dataset.get("derived_from") or []
            )
            if upstream
        ]
    return dependency


def _usage_name(usage: str, prefix: str) -> Optional[str]:
    usage = str(usage or "")
    marker = f"{prefix}:"
    if not usage.startswith(marker):
        return None
    return usage[len(marker):] or prefix


def extract_registry_graph(etl_meta: Optional[dict], graph_name: str) -> Optional[dict]:
    if not etl_meta:
        return None

    adapters: Dict[str, dict] = {}
    resolvers: Dict[str, dict] = {}

    for dataset in etl_meta.get("registry_datasets") or []:
        if not isinstance(dataset, dict):
            continue
        dependency = _compact_registry_dependency(dataset)
        if not dependency:
            continue
        for usage in dataset.get("usages") or []:
            adapter_name = _usage_name(usage, "adapter")
            if adapter_name:
                adapter = adapters.setdefault(adapter_name, {
                    "name": adapter_name,
                    "datasets": [],
                })
                adapter["datasets"].append(dependency)

            resolver_name = _usage_name(usage, "resolver")
            if resolver_name:
                resolver = resolvers.setdefault(resolver_name, {
                    "name": resolver_name,
                    "class": None,
                    "types": [],
                    "snapshot": None,
                    "inputs": [],
                })
                if dataset.get("kind") == "resolver_snapshot" or dataset.get("resolver_inputs"):
                    resolver["snapshot"] = dependency
                    resolver["inputs"].extend(
                        input_dependency
                        for input_dependency in (
                            _compact_registry_dependency(input_dataset)
                            for input_dataset in (dataset.get("resolver_inputs") or {}).values()
                        )
                        if input_dependency
                    )

    resolver_metadata = (etl_meta.get("resolver_metadata") or {}).get("by_type") or {}
    for node_type, metadata in resolver_metadata.items():
        if not isinstance(metadata, dict):
            continue
        resolver_name = metadata.get("label") or node_type
        resolver = resolvers.setdefault(resolver_name, {
            "name": resolver_name,
            "class": None,
            "types": [],
            "snapshot": None,
            "inputs": [],
        })
        resolver["class"] = resolver["class"] or metadata.get("class")
        if node_type not in resolver["types"]:
            resolver["types"].append(node_type)
        resolver_snapshot = metadata.get("resolver_snapshot")
        if not isinstance(resolver_snapshot, dict):
            resolver_snapshot = (metadata.get("kwargs") or {}).get("resolver_snapshot")
        snapshot_dependency = _compact_registry_dependency(resolver_snapshot)
        if snapshot_dependency:
            resolver["snapshot"] = snapshot_dependency
        if isinstance(resolver_snapshot, dict):
            resolver["inputs"].extend(
                input_dependency
                for input_dependency in (
                    _compact_registry_dependency(input_dataset)
                    for input_dataset in (resolver_snapshot.get("resolver_inputs") or {}).values()
                )
                if input_dependency
            )

    for resolver in resolvers.values():
        unique_inputs = {}
        for dependency in resolver.get("inputs") or []:
            unique_inputs[dependency["registry_id"]] = dependency
        resolver["inputs"] = sorted(
            unique_inputs.values(),
            key=lambda item: (item.get("source") or "", item.get("dataset") or "", item.get("version") or ""),
        )
        resolver["types"] = sorted(resolver.get("types") or [])

    data_sources = {}
    dependency_nodes = []

    def lineage_node_id(kind: str, value: str) -> str:
        return f"{kind}:{value}"

    def add_data_source(dependency: dict):
        data_sources[dependency["registry_id"]] = dependency
        for upstream in dependency.get("derived_from") or []:
            data_sources[upstream["registry_id"]] = upstream

    for adapter in sorted(adapters.values(), key=lambda item: item["name"]):
        dependency_ids = []
        for dependency in adapter.get("datasets") or []:
            add_data_source(dependency)
            dependency_ids.append(dependency["registry_id"])
        dependency_nodes.append({
            "kind": "adapter",
            "name": adapter["name"],
            "class": adapter["name"],
            "types": [],
            "snapshot": None,
            "dependencies": sorted(set(dependency_ids)),
        })

    for resolver in sorted(resolvers.values(), key=lambda item: item["name"]):
        dependency_ids = []
        for dependency in resolver.get("inputs") or []:
            add_data_source(dependency)
            dependency_ids.append(dependency["registry_id"])
        dependency_nodes.append({
            "kind": "resolver",
            "name": resolver["name"],
            "class": resolver.get("class"),
            "types": list(resolver.get("types") or []),
            "snapshot": resolver.get("snapshot"),
            "dependencies": sorted(set(dependency_ids)),
        })

    dependency_nodes = sorted(
        dependency_nodes,
        key=lambda item: (0 if item.get("kind") == "resolver" else 1, item.get("name") or ""),
    )
    process_rows = {
        lineage_node_id(node["kind"], node["name"]): index + 1
        for index, node in enumerate(dependency_nodes)
    }

    lineage_nodes_by_id = {}
    lineage_edges = []

    def add_lineage_node(node: dict):
        existing = lineage_nodes_by_id.get(node["id"])
        if existing is None:
            lineage_nodes_by_id[node["id"]] = node
        else:
            existing["level"] = max(existing.get("level", 0), node.get("level", 0))
            existing["row"] = min(existing.get("row", node.get("row", 1)), node.get("row", 1))
        return lineage_nodes_by_id[node["id"]]

    def add_lineage_edge(from_id: str, to_id: str, label: str):
        edge = {"from": from_id, "to": to_id, "label": label}
        if edge not in lineage_edges:
            lineage_edges.append(edge)

    def add_data_lineage(dependency: dict, target_row: int) -> int:
        node_id = lineage_node_id("registry", dependency["registry_id"])
        upstream_levels = []
        for upstream in dependency.get("derived_from") or []:
            upstream_level = add_data_lineage(upstream, target_row)
            upstream_levels.append(upstream_level)
            add_lineage_edge(lineage_node_id("registry", upstream["registry_id"]), node_id, "derived")
        level = max(upstream_levels) + 1 if upstream_levels else 0
        add_lineage_node({
            "id": node_id,
            "kind": "derived" if dependency.get("derived_from") else "data",
            "label": dependency.get("snapshot_id") or dependency["registry_id"],
            "subtitle": "derived artifact" if dependency.get("derived_from") else dependency.get("kind") or "data source",
            "level": level,
            "row": target_row,
            "href": None,
        })
        return level

    process_levels = []
    for node in dependency_nodes:
        process_node_id = lineage_node_id(node["kind"], node["name"])
        process_row = process_rows[process_node_id]
        dependency_levels = []
        for dependency_id in node.get("dependencies") or []:
            dependency = data_sources.get(dependency_id)
            if not dependency:
                continue
            dependency_level = add_data_lineage(dependency, process_row)
            dependency_levels.append(dependency_level)
            add_lineage_edge(lineage_node_id("registry", dependency_id), process_node_id, "uses")
        level = max(dependency_levels) + 1 if dependency_levels else 1
        process_levels.append(level)
        snapshot = node.get("snapshot") or {}
        add_lineage_node({
            "id": process_node_id,
            "kind": node["kind"],
            "label": node["name"],
            "subtitle": node.get("class") or node["kind"],
            "level": level,
            "row": process_row,
            "href": (
                f"/registry/resolvers/{snapshot.get('source')}/{snapshot.get('dataset')}?version={snapshot.get('version')}"
                if node["kind"] == "resolver" and snapshot.get("source") and snapshot.get("dataset") and snapshot.get("version")
                else None
            ),
            "types": node.get("types") or [],
            "snapshot_id": snapshot.get("snapshot_id"),
        })

    graph_level = max(process_levels) + 1 if process_levels else 1
    graph_node_id = lineage_node_id("graph", graph_name)
    add_lineage_node({
        "id": graph_node_id,
        "kind": "graph",
        "label": graph_name,
        "subtitle": "Arango graph",
        "level": graph_level,
        "row": max(1, ((len(dependency_nodes) + 1) // 2)),
        "href": f"/db/{graph_name}",
    })
    for node in dependency_nodes:
        add_lineage_edge(lineage_node_id(node["kind"], node["name"]), graph_node_id, "builds")

    lineage_nodes = sorted(
        lineage_nodes_by_id.values(),
        key=lambda item: (item.get("level", 0), item.get("row", 0), item.get("kind") or "", item.get("label") or ""),
    )

    return {
        "name": graph_name,
        "source_yaml": etl_meta.get("source_yaml") or (etl_meta.get("resolver_metadata") or {}).get("source_yaml"),
        "run_date": etl_meta.get("run_date"),
        "runner": etl_meta.get("runner"),
        "hostname": etl_meta.get("hostname"),
        "git_commit": (etl_meta.get("git_info") or {}).get("commit"),
        "adapters": sorted(adapters.values(), key=lambda item: item["name"]),
        "resolvers": sorted(resolvers.values(), key=lambda item: item["name"]),
        "data_sources": sorted(
            data_sources.values(),
            key=lambda item: (item.get("source") or "", item.get("dataset") or "", item.get("version") or ""),
        ),
        "dependency_nodes": sorted(
            dependency_nodes,
            key=lambda item: (0 if item.get("kind") == "resolver" else 1, item.get("name") or ""),
        ),
        "lineage_nodes": lineage_nodes,
        "lineage_edges": lineage_edges,
        "lineage_level_count": (max((node.get("level", 0) for node in lineage_nodes), default=0) + 1),
    }


def load_registry_graphs_cached(
    *,
    credentials: dict,
    cache: dict,
    ttl_seconds: int,
    get_sys_db: Callable[[], object],
    get_db: Callable[[str], object],
) -> Tuple[List[dict], Optional[str]]:
    now = time.time()
    cached_graphs = cache.get("graphs")
    cached_error = cache.get("error")
    loaded_at = cache.get("loaded_at") or 0.0
    if cached_graphs is not None and now - loaded_at < ttl_seconds:
        return cached_graphs, cached_error

    graphs = []
    if not credentials:
        return graphs, None

    try:
        sys_db = get_sys_db()
        db_names = [db for db in sys_db.databases() if not db.startswith("_")]
    except Exception as exc:
        error = str(exc)
        cache.update({
            "loaded_at": now,
            "graphs": graphs,
            "error": error,
        })
        return graphs, error

    for db_name in db_names:
        try:
            db = get_db(db_name)
            if not db.has_collection("metadata_store"):
                continue
            cursor = db.aql.execute('RETURN DOCUMENT("metadata_store", "etl_metadata").value')
            results = list(cursor)
            if not results or not results[0]:
                continue
            graph = extract_registry_graph(results[0], db_name)
            if graph:
                graphs.append(graph)
        except Exception:
            continue

    graphs = sorted(graphs, key=lambda graph: graph["name"])
    cache.update({
        "loaded_at": now,
        "graphs": graphs,
        "error": None,
    })
    return graphs, None


def _usage_categories(usages: List[str]) -> List[str]:
    categories = set()
    for usage in usages or []:
        if str(usage).startswith("adapter:"):
            categories.add("adapter")
        elif str(usage).startswith("resolver:"):
            categories.add("resolver")
    return sorted(categories)


def _usage_category_label(categories: List[str]) -> str:
    if categories == ["adapter"]:
        return "adapter"
    if categories == ["resolver"]:
        return "resolver"
    if categories == ["adapter", "resolver"]:
        return "adapter + resolver"
    return "graph"


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
                graph_usage = usage_by_registry_id.setdefault(entry_id, {})
                categories = set(graph_usage.get(db_name) or [])
                categories.update(_usage_categories(dataset.get("usages") or []))
                graph_usage[db_name] = sorted(categories)
        except Exception:
            continue

    usage_by_registry_id = {
        entry_id: {
            graph_name: sorted(categories)
            for graph_name, categories in sorted(graph_usage.items())
        }
        for entry_id, graph_usage in sorted(usage_by_registry_id.items())
    }
    cache.update({
        "loaded_at": now,
        "usage_by_registry_id": usage_by_registry_id,
        "error": None,
    })
    return usage_by_registry_id, None
