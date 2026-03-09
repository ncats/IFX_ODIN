from src.use_cases.build_from_yaml import BuildGraphFromYaml

ccle_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/ccle.yaml")
ccle_builder.truncate_datastore()
ccle_builder.do_etl()
