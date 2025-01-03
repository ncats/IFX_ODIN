from src.input_adapters.test_pounce_data_adapter2 import PounceTestAdapter
from src.use_cases.build_from_yaml import BuildGraphFromYaml

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce.yaml")
builder.truncate_datastore()
builder.etl.input_adapters.append(PounceTestAdapter())
builder.do_etl()

