from src.use_cases.build_from_yaml import BuildGraphFromYaml

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce.yaml")
builder.truncate_datastore()
builder.do_etl()