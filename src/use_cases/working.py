from src.use_cases.build_cli import run_arango_build_cli


if __name__ == "__main__":
    run_arango_build_cli(
        build_name="working",
        database_name="metabolite_harmonization",
        primary_yaml="src/use_cases/working.yaml",
    )
