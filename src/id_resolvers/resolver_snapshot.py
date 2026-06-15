from src.registry.fetchers import MaterializedDataset


def resolver_definition(resolver_snapshot: MaterializedDataset) -> dict:
    return resolver_snapshot.manifest.get("definition") or {}


def resolver_options(resolver_snapshot: MaterializedDataset) -> dict:
    return dict(resolver_definition(resolver_snapshot).get("options") or {})


def resolver_input(resolver_snapshot: MaterializedDataset, input_name: str) -> MaterializedDataset:
    try:
        return resolver_snapshot.resolver_inputs[input_name]
    except KeyError as exc:
        raise KeyError(
            f"Resolver snapshot {resolver_snapshot.snapshot_id} does not define input {input_name!r}"
        ) from exc
