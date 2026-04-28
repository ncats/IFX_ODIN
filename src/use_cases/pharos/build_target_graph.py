from src.use_cases.build_cli import run_arango_build_cli


def main():
    run_arango_build_cli(
        build_name="target_graph",
        database_name="target_graph",
        primary_yaml="./src/use_cases/pharos/target_graph.yaml",
        post_yaml="./src/use_cases/pharos/target_graph_aql_post.yaml",
    )


if __name__ == "__main__":
    main()
