from src.use_cases.build_from_yaml import BuildGraphFromYaml

ccle_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_v2.yaml")
ccle_builder.truncate_datastore()
ccle_builder.do_etl(do_post_processing=False)
