from src.core.config import ETL_Config, Dashboard_Config
from src.core.etl import ETL
from src.interfaces.data_api_adapter import APIAdapter


def preload_core_model_modules():
    # Preload the model modules involved in ETL inheritance/dataclass composition
    # so YAML-driven adapter imports do not control their first-import order.
    import src.models.node
    import src.models.protein
    import src.models.disease
    import src.models.gwas_trait
    import src.models.panther_class


class HostDashboardFromYaml:
    configuration: Dashboard_Config
    api_adapter: APIAdapter

    def __init__(self, yaml_file: str):
        self.load_yaml(yaml_file)

    def load_yaml(self, yaml_file: str):
        self.configuration = Dashboard_Config(yaml_file)
        self.api_adapter = self.configuration.create_object_list('api_adapter')[0]


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
        for output_adapter in output_adapters:
            if hasattr(output_adapter, "set_graph_views_metadata"):
                output_adapter.set_graph_views_metadata(graph_views=graph_views, source_yaml=yaml_file)
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
