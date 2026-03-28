import sys

from src.use_cases.build_from_yaml import BuildGraphFromYaml


DEFAULT_YAMLS = [
    "./src/use_cases/working_pharos_compare.yaml",
    "./src/use_cases/working_target_graph_compare.yaml",
]


def run_build(yaml_file: str) -> None:
    builder = BuildGraphFromYaml(yaml_file=yaml_file)
    builder.prepare_datastore()
    builder.do_etl(clean_edges=False)


def main(argv: list[str]) -> None:
    yaml_files = argv[1:] or DEFAULT_YAMLS
    for yaml_file in yaml_files:
        run_build(yaml_file)


if __name__ == "__main__":
    main(sys.argv)
