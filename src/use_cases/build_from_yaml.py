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
        node_adapters = self.configuration.create_node_adapters()
        edge_adapters = self.configuration.create_edge_adapters()
        self.etl = ETL(input_adapters=[*node_adapters, *edge_adapters], output_adapters=output_adapters)
        labeler = self.configuration.create_labeler()
        if labeler is not None:
            self.etl.set_labeler(labeler)

    def truncate_datastore(self):
        self.etl.create_or_truncate_datastores()

    def do_etl(self):
        self.etl.do_etl(testing = self.configuration.is_testing())


