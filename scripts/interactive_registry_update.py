import argparse
from pathlib import Path
from typing import Optional

from src.core.data_registry import DataRegistry


def status_label(status: dict) -> str:
    if status.get("error"):
        return "error"
    if not status.get("registered_versions"):
        return "missing"
    if status.get("is_latest_registered") is True:
        return "current"
    if status.get("is_latest_registered") is False:
        return "update available"
    return "unknown"


def format_age(days: Optional[int]) -> str:
    return "" if days is None else f"{days}d old"


def format_status_line(status: dict) -> str:
    latest_registered = status.get("latest_registered_version") or "-"
    latest = status.get("latest_version") or "-"
    age = format_age(status.get("days_since_last_update"))
    label = status_label(status)
    parts = [
        f"{status['source']}:{status['dataset']}",
        label,
        f"registered={latest_registered}",
        f"latest={latest}",
    ]
    if age:
        parts.append(age)
    if status.get("version_strategy"):
        parts.append(f"strategy={status['version_strategy']}")
    if status.get("error"):
        parts.append(f"error={status['error']}")
    return " | ".join(parts)


def prompt_yes_no(prompt: str) -> bool:
    while True:
        answer = input(f"{prompt} [y/n] ").strip().lower()
        if answer in {"y", "yes"}:
            return True
        if answer in {"n", "no", ""}:
            return False
        print("Please answer y or n.")


def should_prompt_for_update(status: dict) -> bool:
    return status_label(status) in {"missing", "update available"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Interactively refresh out-of-sync IFX Registry source snapshots."
    )
    parser.add_argument(
        "--credentials",
        type=Path,
        default=Path("src/use_cases/secrets/ifxdev_minio.yaml"),
        help="Path to MinIO credentials YAML.",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path("/tmp/ifx-registry-cache"),
        help="Local cache/work directory for fetched registry files.",
    )
    parser.add_argument(
        "--check-timeout",
        type=int,
        default=30,
        help="Timeout in seconds for latest-version checks.",
    )
    parser.add_argument(
        "--update-timeout",
        type=int,
        default=60,
        help="Timeout in seconds for update fetch requests.",
    )
    parser.add_argument(
        "--internal-minio-url",
        action="store_true",
        help="Use the internal MinIO URL from the credentials file.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    registry = DataRegistry.from_minio_credentials(
        args.credentials,
        use_internal_url=args.internal_minio_url,
    )
    statuses = registry.check_all_latest_registered(timeout=args.check_timeout)
    if not statuses:
        print("No registered source datasets are configured for latest-version checks.")
        return

    updated = []
    skipped = []
    failed = []
    selected = []

    for status in statuses:
        print(format_status_line(status))
        if not should_prompt_for_update(status):
            continue

        source = status["source"]
        dataset = status["dataset"]
        if prompt_yes_no(f"Update {source}:{dataset}?"):
            selected.append(status)
        else:
            skipped.append(f"{source}:{dataset}")

    if selected:
        print()
        print(f"Updating {len(selected)} selected source dataset(s).")

    for status in selected:
        source = status["source"]
        dataset = status["dataset"]
        try:
            manifest_path = registry.refresh_dataset(
                source,
                dataset,
                dest=args.cache_dir,
                timeout=args.update_timeout,
            )
            updated.append(f"{source}:{dataset} -> {manifest_path}")
        except Exception as exc:
            failed.append(f"{source}:{dataset}: {exc}")
            print(f"Failed updating {source}:{dataset}: {exc}")

    print()
    print(f"Updated: {len(updated)}")
    for item in updated:
        print(f"  {item}")
    print(f"Skipped: {len(skipped)}")
    for item in skipped:
        print(f"  {item}")
    print(f"Failed: {len(failed)}")
    for item in failed:
        print(f"  {item}")


if __name__ == "__main__":
    main()
