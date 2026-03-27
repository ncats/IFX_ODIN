from src.use_cases.build_from_yaml import BuildGraphFromYaml

etl_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/working.yaml")
etl_builder.prepare_datastore()
etl_builder.do_etl()
