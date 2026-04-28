import argparse

from src.output_adapters.arango_output_adapter import ArangoOutputAdapter
from src.use_cases.build_from_yaml import BuildGraphFromYaml


def add_common_build_args(parser: argparse.ArgumentParser, build_name: str) -> argparse.ArgumentParser:
    parser.add_argument(
        "--resume",
        action="store_true",
        help=f"Resume a prior {build_name} build without truncating the datastore and skip completed adapters.",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip the confirmation prompt before truncating the datastore.",
    )
    return parser


def confirm_truncate(database_name: str) -> bool:
    prompt = (
        f"Are you sure you want to wipe out '{database_name}' before rebuilding it? "
        "[y/N]: "
    )
    response = input(prompt).strip().lower()
    return response in {"y", "yes"}


def _arango_database_exists(builder: BuildGraphFromYaml) -> bool | None:
    for output_adapter in builder.etl.output_adapters:
        if not isinstance(output_adapter, ArangoOutputAdapter):
            continue
        sys_db = output_adapter.client.db(
            "_system",
            username=output_adapter.credentials.user,
            password=output_adapter.credentials.password,
        )
        return sys_db.has_database(output_adapter.database_name)
    return None


def prepare_primary_builder(builder: BuildGraphFromYaml, *, resume: bool, yes: bool, database_name: str) -> bool:
    if resume:
        exists = _arango_database_exists(builder)
        if exists is False:
            print(
                f"Cannot resume '{database_name}' because the database does not exist yet. "
                "Run without --resume for a fresh build."
            )
            return False
        return True

    if not yes and not confirm_truncate(database_name):
        print("Build cancelled.")
        return False

    builder.prepare_datastore()
    return True


def parse_common_build_args(build_name: str) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=f"Build or resume the {build_name} Arango graph.")
    add_common_build_args(parser, build_name)
    return parser.parse_args()


def run_arango_build_cli(
    *,
    build_name: str,
    database_name: str,
    primary_yaml: str,
    post_yaml: str | None = None,
):
    args = parse_common_build_args(build_name)

    primary_builder = BuildGraphFromYaml(yaml_file=primary_yaml)
    if not prepare_primary_builder(
        primary_builder,
        resume=args.resume,
        yes=args.yes,
        database_name=database_name,
    ):
        return
    primary_builder.do_etl(clean_edges=not bool(post_yaml), resume=args.resume)

    if post_yaml:
        post_builder = BuildGraphFromYaml(yaml_file=post_yaml)
        post_builder.do_etl(resume=args.resume)
