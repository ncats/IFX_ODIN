import argparse
from pathlib import Path
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.core.data_registry import DataRegistry


def main() -> None:
    parser = argparse.ArgumentParser(description="Test read-only access to the IFX registry S3 bucket.")
    parser.add_argument(
        "credentials",
        nargs="?",
        default="src/use_cases/secrets/aws_ifx_registry.yaml",
        help="Path to AWS assume-role registry credentials YAML.",
    )
    parser.add_argument("--prefix", default="", help="Optional S3 key prefix to list.")
    parser.add_argument("--max-keys", type=int, default=10, help="Maximum number of keys to print.")
    args = parser.parse_args()

    registry = DataRegistry.from_registry_credentials(Path(args.credentials), connect_timeout=5, read_timeout=20)
    storage = registry.storage
    storage.ensure_bucket()
    keys = storage.list_keys(args.prefix)

    print(f"Connected to s3://{storage.bucket}")
    print(f"Listed {len(keys)} key(s) under prefix {args.prefix!r}")
    for key in keys[: args.max_keys]:
        print(key)


if __name__ == "__main__":
    main()
