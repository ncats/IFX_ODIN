import argparse
from pathlib import Path
import sys
import tempfile

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.core.data_registry import DataRegistry
from src.registry.storage import DEFAULT_REGISTRY_CACHE_DIR


DEFAULT_PREFIXES = ("sources/", "derived/", "external/", "resolvers/")
DEFAULT_LOCAL_ROOTS = (str(DEFAULT_REGISTRY_CACHE_DIR),)
SIZE_UNITS = ("B", "KB", "MB", "GB", "TB")


def object_size(storage, key: str) -> int | None:
    try:
        response = storage.client().head_object(Bucket=storage.bucket, Key=key)
        return response.get("ContentLength")
    except Exception:
        return None


def format_size(size_bytes: int | None) -> str:
    if size_bytes is None:
        return "unknown"
    size = float(size_bytes)
    for unit in SIZE_UNITS:
        if size < 1024 or unit == SIZE_UNITS[-1]:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024


def find_local_file(key: str, source_size: int | None, local_roots: list[str]) -> Path | None:
    candidates = [key]
    parts = key.split("/", 1)
    if parts[0] in {"sources", "derived", "external", "resolvers"} and len(parts) == 2:
        candidates.append(parts[1])
    for root in local_roots:
        for relative_path in candidates:
            candidate = Path(root) / relative_path
            if candidate.exists() and (source_size is None or candidate.stat().st_size == source_size):
                return candidate
    return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync IFX registry objects from MinIO to AWS S3.")
    parser.add_argument("--source-credentials", default="src/use_cases/secrets/ifxdev_minio.yaml")
    parser.add_argument("--target-credentials", default="src/use_cases/secrets/aws_ifx_registry.yaml")
    parser.add_argument("--prefix", action="append", default=[], help="Prefix to sync; repeatable.")
    parser.add_argument(
        "--local-root",
        action="append",
        default=None,
        help="Local directory to check before downloading from MinIO; repeatable. Defaults to /var/tmp/ifx-registry-cache.",
    )
    parser.add_argument("--execute", action="store_true", help="Actually copy objects. Default is dry-run.")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite target objects even when present.")
    args = parser.parse_args()

    source = DataRegistry.from_minio_credentials(args.source_credentials).storage
    target = DataRegistry.from_registry_credentials(args.target_credentials).storage
    prefixes = tuple(args.prefix or DEFAULT_PREFIXES)
    local_roots = list(args.local_root or DEFAULT_LOCAL_ROOTS)

    source_keys = []
    for prefix in prefixes:
        source_keys.extend(source.list_keys(prefix))
    source_keys = sorted(set(source_keys))

    target_keys = set()
    for prefix in prefixes:
        target_keys.update(target.list_keys(prefix))

    to_copy = []
    skipped = 0
    sizes_by_key = {}
    local_keys = set()
    local_bytes = 0
    download_bytes = 0
    unknown_download_count = 0
    for key in source_keys:
        if args.overwrite or key not in target_keys:
            size = object_size(source, key)
            sizes_by_key[key] = size
            local_path = find_local_file(key, size, local_roots)
            if local_path is not None:
                local_keys.add(key)
                if size is not None:
                    local_bytes += size
            elif size is None:
                unknown_download_count += 1
            else:
                download_bytes += size
            to_copy.append(key)
        else:
            skipped += 1

    mode = "EXECUTE" if args.execute else "DRY-RUN"
    print(f"{mode}: source={source.bucket} target={target.bucket}")
    print(f"prefixes: {', '.join(prefixes)}")
    print(f"source objects: {len(source_keys)}")
    print(f"already present: {skipped}")
    print(f"to copy: {len(to_copy)}")
    total_known_bytes = local_bytes + download_bytes
    unknown_count = sum(1 for key in to_copy if sizes_by_key.get(key) is None)
    print(f"known upload volume: {format_size(total_known_bytes)}")
    if unknown_count:
        print(f"unknown-size uploads: {unknown_count} object(s)")
    print(f"will upload from local roots: {len(local_keys)} object(s), {format_size(local_bytes)}")
    print(f"will download from MinIO: {len(to_copy) - len(local_keys)} object(s), {format_size(download_bytes)}")
    if unknown_download_count:
        print(f"unknown-size MinIO downloads: {unknown_download_count} object(s)")
    if local_roots:
        print(f"local roots: {', '.join(local_roots)}")

    if not args.execute:
        for key in to_copy[:25]:
            print(key)
        if len(to_copy) > 25:
            print(f"... {len(to_copy) - 25} more")
        return

    with tempfile.TemporaryDirectory(prefix="ifx-registry-sync-") as tmp_dir:
        tmp_root = Path(tmp_dir)
        for index, key in enumerate(to_copy, start=1):
            source_size = sizes_by_key.get(key)
            local_path = find_local_file(key, source_size, local_roots)
            source_label = "local" if local_path is not None else "minio"
            if local_path is None:
                local_path = tmp_root / key
                source.download_file(key, local_path)
            target.upload_file(local_path, key)
            target_size = object_size(target, key)
            local_size = local_path.stat().st_size
            if target_size is not None and target_size != local_size:
                raise RuntimeError(f"Size mismatch after copy for {key}: local={local_size} target={target_size}")
            print(f"[{index}/{len(to_copy)}] {source_label} {key}")


if __name__ == "__main__":
    main()
