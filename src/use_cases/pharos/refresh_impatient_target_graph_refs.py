#!/usr/bin/env python3
"""Refresh registry refs in impatient_target_graph YAML files."""

import argparse
import re
from pathlib import Path

from src.core.data_registry import DataRegistry, registry_version_sort_key


DEFAULT_REGISTRY_CREDENTIALS = "./src/use_cases/secrets/aws_ifx_registry.yaml"
DEFAULT_PRIMARY_YAML = "src/use_cases/pharos/impatient_target_graph.yaml"
DEFAULT_POST_YAML = "src/use_cases/pharos/impatient_target_graph_aql_post.yaml"

PRIMARY_DATASETS = [
    ("target_graph", "gene_ids"),
    ("target_graph", "protein_ids"),
    ("target_graph", "transcript_ids"),
    ("target_graph", "uniprot_mapping"),
    ("target_graph", "tdl_updates"),
    ("drug_graph", "drug_nodes"),
    ("drug_graph", "drug_edges"),
    ("go", "ontology"),
    ("go", "goa_human_uniprot"),
    ("go", "goa_human_go"),
    ("jensenlab", "protein_counts"),
    ("ncbi", "publications"),
]
OPTIONAL_PRIMARY_DATASETS = [
    ("antibodypedia", "scraped_results"),
]
CORE_EVIDENCE_DATASETS = [
    ("go", "ontology"),
    ("go", "goa_human_uniprot"),
    ("go", "goa_human_go"),
    ("jensenlab", "protein_counts"),
    ("ncbi", "publications"),
]
POST_DATASETS = [
    ("target_graph", "tdl_updates"),
]
TARGET_RESOLVERS = [
    "tg_genes",
    "tg_transcripts",
    "tg_proteins",
]
RESOLVERS = [
    ("translator", "translator_nn"),
    ("target_graph", "tg_genes"),
    ("target_graph", "tg_transcripts"),
    ("target_graph", "tg_proteins"),
]


def latest_data_ref(registry: DataRegistry, source: str, dataset: str) -> str:
    versions = registry.list_versions(source, dataset)
    if not versions:
        raise LookupError(f"No registered versions found for {source}:{dataset}")
    return f"{source}:{dataset}:{versions[-1]}"


def latest_resolver_ref(registry: DataRegistry, source: str, resolver: str) -> str:
    entries = [
        entry
        for entry in registry.list_resolver_snapshots()
        if entry.get("source") == source and entry.get("resolver") == resolver and entry.get("version")
    ]
    if not entries:
        raise LookupError(f"No registered resolver snapshots found for {source}:{resolver}")
    entries.sort(key=lambda entry: registry_version_sort_key(entry["version"]))
    return f"{source}:{resolver}:{entries[-1]['version']}"


def replace_refs(path: Path, replacements: dict[tuple[str, str], str]) -> tuple[list[str], str]:
    original = path.read_text(encoding="utf-8")
    updated = original
    changes = []
    for (source, dataset), new_ref in replacements.items():
        pattern = rf"{re.escape(source)}:{re.escape(dataset)}:[A-Za-z0-9_.-]+"
        updated, count = re.subn(pattern, new_ref, updated)
        if count:
            changes.append(f"{path}: {source}:{dataset} -> {new_ref} ({count} replacement(s))")
    return changes, updated


def build_registry(args) -> tuple[DataRegistry, bool]:
    credentials_path = Path(args.registry_credentials)
    use_local = args.no_upload or not credentials_path.exists()
    if use_local:
        if not args.no_upload and not credentials_path.exists():
            print(f"Registry credentials not found at {credentials_path}; using local registry cache.")
        return DataRegistry.local(cache_dir=Path(args.registry_cache_dir)), False
    return DataRegistry.from_registry_credentials(credentials_path), True


def action_word(remote: bool) -> str:
    return "Published" if remote else "Cached"


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Update impatient_target_graph YAML files to the latest registered "
            "source and resolver snapshots."
        ),
    )
    parser.add_argument("--registry-credentials", default=DEFAULT_REGISTRY_CREDENTIALS)
    parser.add_argument("--registry-cache-dir", default="./registry_cache")
    parser.add_argument("--primary-yaml", default=DEFAULT_PRIMARY_YAML)
    parser.add_argument("--post-yaml", default=DEFAULT_POST_YAML)
    parser.add_argument("--write", action="store_true", help="Write changes. Default is dry-run.")
    parser.add_argument(
        "--no-upload",
        action="store_true",
        help="Use only the local registry cache, even when registry credentials are present.",
    )
    parser.add_argument(
        "--register-target-resolvers",
        action="store_true",
        help="Register tg_genes/tg_transcripts/tg_proteins before updating resolver_snapshot refs.",
    )
    parser.add_argument(
        "--register-chembl-external",
        action="store_true",
        help="Register chembl:activity_database from registry_sources.yaml before updating refs.",
    )
    parser.add_argument(
        "--register-tdl-updates",
        action="store_true",
        help="Publish input_files/manual/target_graph/tdl_updates.csv before updating refs.",
    )
    parser.add_argument(
        "--register-core-evidence",
        action="store_true",
        help="Refresh GO/GOA, JensenLab protein_counts, and NCBI publications before updating refs.",
    )
    args = parser.parse_args()

    registry, remote_registry = build_registry(args)
    cache_dir = Path(args.registry_cache_dir)

    if args.register_chembl_external:
        path = registry.register_external_source("chembl", "activity_database", dest=cache_dir, upload=remote_registry)
        if not remote_registry:
            registry.upload_external_registration(path)
        print(f"{action_word(remote_registry)} chembl:activity_database -> {path}")

    if args.register_tdl_updates:
        path = (
            registry.refresh_dataset("target_graph", "tdl_updates", dest=cache_dir)
            if remote_registry
            else registry.fetch_dataset("target_graph", "tdl_updates", dest=cache_dir)
        )
        if not remote_registry:
            registry.upload_snapshot(path)
        print(f"{action_word(remote_registry)} target_graph:tdl_updates -> {path}")

    if args.register_core_evidence:
        for source, dataset in CORE_EVIDENCE_DATASETS:
            path = (
                registry.refresh_dataset(source, dataset, dest=cache_dir)
                if remote_registry
                else registry.fetch_dataset(source, dataset, dest=cache_dir)
            )
            if not remote_registry:
                registry.upload_snapshot(path)
            print(f"{action_word(remote_registry)} {source}:{dataset} -> {path}")

    if args.register_target_resolvers:
        for resolver in TARGET_RESOLVERS:
            path = registry.register_resolver_snapshot(
                "target_graph",
                resolver,
                dest=cache_dir,
                upload=remote_registry,
            )
            if not remote_registry:
                registry.upload_resolver_snapshot(path)
            print(f"{action_word(remote_registry)} target_graph:{resolver} -> {path}")

    registry.refresh_catalog()
    primary_replacements = {
        (source, dataset): latest_data_ref(registry, source, dataset)
        for source, dataset in PRIMARY_DATASETS
    }
    for source, dataset in OPTIONAL_PRIMARY_DATASETS:
        try:
            primary_replacements[(source, dataset)] = latest_data_ref(registry, source, dataset)
        except LookupError as exc:
            print(f"Warning: optional registry ref not updated: {exc}")
    primary_replacements.update({
        (source, resolver): latest_resolver_ref(registry, source, resolver)
        for source, resolver in RESOLVERS
    })
    post_replacements = {
        (source, dataset): latest_data_ref(registry, source, dataset)
        for source, dataset in POST_DATASETS
    }
    post_replacements.update({
        (source, resolver): latest_resolver_ref(registry, source, resolver)
        for source, resolver in [("target_graph", "tg_proteins")]
    })

    planned = []
    for path, replacements in [
        (Path(args.primary_yaml), primary_replacements),
        (Path(args.post_yaml), post_replacements),
    ]:
        changes, updated = replace_refs(path, replacements)
        planned.extend(changes)
        if args.write and updated != path.read_text(encoding="utf-8"):
            path.write_text(updated, encoding="utf-8")

    if planned:
        print("Registry ref updates:")
        for item in planned:
            print(f"  {item}")
    else:
        print("No matching registry refs changed.")

    if not args.write:
        print("\nDry-run only. Add --write to update the YAML files.")
    else:
        print("\nYAML files updated.")
        print("Next:")
        print("  python -m src.use_cases.pharos.build_impatient_target_graph --yes")
        print("  python -m src.qa_browser.app --host 127.0.0.1 --port 8050")
        print("  curl -L \"http://127.0.0.1:8050/db/impatient_target_graph/view/current_tdls\" -o current_tdls.csv")


if __name__ == "__main__":
    main()
