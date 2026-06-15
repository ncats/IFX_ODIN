from src.core.config import ETL_Config
from src.core.etl import ETL
from src.interfaces.resolver_metadata import resolver_fingerprints_by_type
from src.registry.fetchers import MaterializedDataset


def preload_core_model_modules():
    # Preload the model modules involved in ETL inheritance/dataclass composition
    # so YAML-driven adapter imports do not control their first-import order.
    import src.models.node
    import src.models.protein
    import src.models.disease
    import src.models.gwas_trait
    import src.models.panther_class
    import src.models.dto_class
    import src.models.external_link


class BuildGraphFromYaml:
    configuration: ETL_Config
    etl: ETL
    yaml_file: str

    def __init__(self, yaml_file: str):
        self.load_yaml(yaml_file)

    def load_yaml(self, yaml_file: str):
        preload_core_model_modules()
        self.yaml_file = yaml_file
        self.configuration = ETL_Config(yaml_file)
        output_adapters = self.configuration.create_output_adapters()
        graph_views = self.configuration.config_dict.get("graph_views", [])
        resolver_fingerprints = resolver_fingerprints_by_type(self.configuration.config_dict.get("resolvers"))
        registry_datasets = _registry_datasets_from_config(self.configuration.config_dict)
        for output_adapter in output_adapters:
            if hasattr(output_adapter, "set_graph_views_metadata"):
                output_adapter.set_graph_views_metadata(graph_views=graph_views, source_yaml=yaml_file)
            if hasattr(output_adapter, "set_resolver_metadata"):
                output_adapter.set_resolver_metadata(
                    resolver_fingerprints_by_type=resolver_fingerprints,
                    source_yaml=yaml_file,
                )
            if hasattr(output_adapter, "set_registry_dataset_metadata"):
                output_adapter.set_registry_dataset_metadata(registry_datasets)
        input_adapters = self.configuration.create_input_adapters()
        resolver_map = {}
        for resolver in self.configuration.resolvers.values():
            for type in resolver.types:
                if type in resolver_map:
                    raise Exception(f"It only makes sense to have one resolver for each type. Resolver for {type} already exists", resolver_map[type], resolver)
                resolver_map[type] = resolver

        self.etl = ETL(input_adapters=input_adapters, output_adapters=output_adapters, resolver_map=resolver_map)

    def prepare_datastore(self, truncate_tables: bool = True):
        self.etl.create_or_truncate_datastores(truncate_tables=truncate_tables)

    def do_etl(self, do_post_processing = True, clean_edges: bool = True, resume: bool = False):
        self.etl.do_etl(do_post_processing, clean_edges, resume=resume, run_id=self.yaml_file)


def _registry_datasets_from_config(config_node) -> list[dict]:
    datasets_by_snapshot = {}

    def visit(value, usage: str):
        if isinstance(value, MaterializedDataset):
            metadata = value.to_metadata()
            existing = datasets_by_snapshot.setdefault(metadata["snapshot_id"], metadata)
            usages = set(existing.get("usages") or [])
            usages.add(usage)
            existing["usages"] = sorted(usages)
            return
        if isinstance(value, dict):
            for entry in value.values():
                visit(entry, usage)
            return
        if isinstance(value, list):
            for entry in value:
                visit(entry, usage)

    for entry in config_node.get("input_adapters", []) or []:
        label = f"adapter:{entry.get('class')}" if isinstance(entry, dict) else "adapter"
        visit(entry, label)
    for entry in config_node.get("resolvers", []) or []:
        label = f"resolver:{entry.get('label') or entry.get('class')}" if isinstance(entry, dict) else "resolver"
        visit(entry, label)

    return sorted(datasets_by_snapshot.values(), key=lambda item: item["snapshot_id"])
