from src.core.config import Config
from src.core.etl import ETL


class BuildGraphFromYaml:
    configuration: Config
    etl: ETL

    def __init__(self, yaml_file: str):
        self.load_yaml(yaml_file)

    def load_yaml(self, yaml_file: str):
        self.configuration = Config(yaml_file)
        output_adapters = self.configuration.create_output_adapters()
        input_adapters = self.configuration.create_adapters()
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

    def do_etl(self):
        self.etl.do_etl(testing = self.configuration.is_testing())


