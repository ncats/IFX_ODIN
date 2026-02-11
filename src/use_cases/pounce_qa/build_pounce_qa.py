from src.use_cases.build_from_yaml import BuildGraphFromYaml

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_qa/pounce.yaml")
builder.truncate_datastore()
builder.do_etl()

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_qa/pounce_1.yaml")
builder.truncate_datastore()
builder.do_etl()

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_qa/pounce_2.yaml")
builder.truncate_datastore()
builder.do_etl()

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_qa/pounce_3.yaml")
builder.truncate_datastore()
builder.do_etl()

builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_qa/pounce_4.yaml")
builder.truncate_datastore()
builder.do_etl()
