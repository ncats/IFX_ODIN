from src.use_cases.build_cli import run_arango_build_cli


if __name__ == "__main__":
    run_arango_build_cli(
        build_name="working",
        database_name="metabolite_harmonization",
        primary_yaml="src/use_cases/working.yaml",
    )
# from src.core.data_registry import DataRegistry
#
# registry = DataRegistry.from_registry_credentials('src/use_cases/secrets/aws_ifx_registry.yaml')
# registry.build_derived_artifact('chebi','endogenous_human_metabolites', dest='/private/tmp/ifx-registry-cache')