from src.use_cases.build_from_yaml import BuildGraphFromYaml
#
# ccle_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_ccle.yaml")
# ccle_builder.truncate_datastore()
# ccle_builder.do_etl()

ncats_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pounce_ncats.yaml")
ncats_builder.truncate_datastore()
ncats_builder.do_etl()