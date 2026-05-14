from src.use_cases.build_from_yaml import BuildGraphFromYaml

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/cure/cure_rasopathies.yaml")
builder.prepare_datastore()
builder.do_etl()
