from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.pubmed_mirror.config import MirrorConfig
from scripts.pubmed_mirror.service import PubMedMirrorService


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Maintain a local PubMed mirror in MySQL.")
    parser.add_argument(
        "command",
        choices=["init", "rebuild", "update", "status"],
        help="Command to run.",
    )
    parser.add_argument(
        "--credentials-yaml",
        default="src/use_cases/secrets/pharos_write_credentials.yaml",
        help="Path to MySQL credentials YAML.",
    )
    parser.add_argument(
        "--schema-name",
        default="ifx_pubmed",
        help="MySQL schema name for the PubMed mirror.",
    )
    parser.add_argument(
        "--data-dir",
        default="input_files/pubmed",
        help="Local directory used for downloaded PubMed archives.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        help="Batch size for MySQL upserts and deletes.",
    )
    parser.add_argument(
        "--limit-archives",
        type=int,
        default=None,
        help="Optional limit on the number of archives processed, useful for testing.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = MirrorConfig(
        credentials_yaml=Path(args.credentials_yaml),
        schema_name=args.schema_name,
        data_dir=Path(args.data_dir),
        batch_size=args.batch_size,
        limit_archives=args.limit_archives,
    )
    service = PubMedMirrorService(config)

    if args.command == "init":
        service.init()
    elif args.command == "rebuild":
        service.rebuild()
    elif args.command == "update":
        service.update()
    elif args.command == "status":
        current_status = service.status()
        for key, value in current_status.items():
            print(f"{key}: {value}")
    else:
        raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
