from src.core.config import ETL_Config, Dashboard_Config
from src.core.etl import ETL
from src.interfaces.data_api_adapter import APIAdapter


class HostDashboardFromYaml:
    configuration: Dashboard_Config
    api_adapter: APIAdapter

    def __init__(self, yaml_file: str):
        self.load_yaml(yaml_file)

    def load_yaml(self, yaml_file: str):
        self.configuration = Dashboard_Config(yaml_file)
        self.api_adapter = self.configuration.create_object_list('api_adapter')[0]
        self.api_adapter.labeler = self.configuration.create_labeler()


class BuildGraphFromYaml:
    configuration: ETL_Config
    etl: ETL

    def __init__(self, yaml_file: str):
        self.load_yaml(yaml_file)

    def load_yaml(self, yaml_file: str):
        self.configuration = ETL_Config(yaml_file)
        output_adapters = self.configuration.create_output_adapters()
        input_adapters = self.configuration.create_input_adapters()
        resolver_map = {}
        for resolver in self.configuration.resolvers.values():
            for type in resolver.types:
                if type in resolver_map:
                    raise Exception(f"It only makes sense to have one resolver for each type. Resolver for {type} already exists", resolver_map[type], resolver)
                resolver_map[type] = resolver

        self.etl = ETL(input_adapters=input_adapters, output_adapters=output_adapters, resolver_map=resolver_map)
        labeler = self.configuration.create_labeler()
        if labeler is not None:
            self.etl.set_labeler(labeler)

    def truncate_datastore(self):
        self.etl.create_or_truncate_datastores()

    def do_etl(self, do_post_processing = True):
        self.etl.do_etl(do_post_processing)


