from src.use_cases.build_from_yaml import BuildGraphFromYaml


if __name__ == "__main__":
    builder = BuildGraphFromYaml("src/use_cases/working.yaml")
    builder.prepare_datastore()
    builder.do_etl()
