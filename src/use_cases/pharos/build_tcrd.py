import argparse

from src.use_cases.build_cli import add_common_build_args, confirm_truncate
from src.use_cases.build_from_yaml import BuildGraphFromYaml


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build or resume the TCRD MySQL database.")
    add_common_build_args(parser, "TCRD")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    etl_builder = BuildGraphFromYaml(yaml_file="./src/use_cases/pharos/tcrd.yaml")
    if not args.resume:
        if not args.yes and not confirm_truncate("pharos400"):
            print("Build cancelled.")
            return
        etl_builder.prepare_datastore()

    etl_builder.do_etl(resume=args.resume)


if __name__ == "__main__":
    main()
