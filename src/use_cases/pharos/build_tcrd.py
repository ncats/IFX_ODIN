from src.use_cases.build_from_yaml import BuildGraphFromYaml

etl_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pharos/tcrd.yaml")
etl_builder.prepare_datastore(truncate_tables = False)
etl_builder.do_etl()