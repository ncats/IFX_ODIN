from src.use_cases.build_from_yaml import BuildGraphFromYaml

etl_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/target_graph.yaml")
etl_builder.truncate_datastore()
etl_builder.do_etl()

post_etl_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/target_graph_aql_post.yaml")
post_etl_builder.do_etl()
