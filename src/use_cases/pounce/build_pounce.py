from src.use_cases.build_from_yaml import BuildGraphFromYaml

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce/pounce.yaml")
builder.truncate_datastore()
builder.do_etl(clean_edges=False)

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce/pounce_aql_post.yaml")
builder.do_etl()
