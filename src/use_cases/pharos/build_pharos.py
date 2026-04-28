from src.use_cases.build_cli import run_arango_build_cli


def main():
    run_arango_build_cli(
        build_name="pharos",
        database_name="pharos",
        primary_yaml="./src/use_cases/pharos/pharos.yaml",
        post_yaml="./src/use_cases/pharos/pharos_aql_post.yaml",
    )


if __name__ == "__main__":
    main()
