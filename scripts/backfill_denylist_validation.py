#!/usr/bin/env python3
"""Backfill validation.denylist_still_merged onto already-materialized harmonization stages.

Stage docs are content-addressed by their rules/parameters/graph fingerprint, so
`_ensure_harmonization_stage` returns the existing doc as-is once a stage is
"complete" and never recomputes it. That means stages materialized before the
denylist-still-merged validation existed won't get it just by revisiting the QA
browser. This script patches it in directly, using each stage's already
materialized HarmonizedMetabolite / member-edge collections to rebuild its
cliques -- no rule re-application or full pipeline rematerialization required.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import yaml

import src.qa_browser.app as qa_app


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--credentials", "-c",
        default="./src/use_cases/secrets/local_arangodb.yaml",
        help="Path to ArangoDB credentials YAML file",
    )
    parser.add_argument(
        "--registry-credentials", "-s",
        default="./src/use_cases/secrets/aws_ifx_registry.yaml",
        help="Path to registry object-storage credentials YAML file",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Recompute denylist validation even for stages that already have it stored",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would change without writing anything",
    )
    args = parser.parse_args()

    cred_path = Path(args.credentials)
    if cred_path.exists():
        with open(cred_path) as f:
            qa_app._credentials = yaml.safe_load(f)
        print(f"Loaded ArangoDB credentials from {cred_path}")
    else:
        print(f"Warning: {cred_path} not found, using defaults")

    registry_cred_path = Path(args.registry_credentials)
    if not registry_cred_path.exists():
        raise FileNotFoundError(registry_cred_path)
    with registry_cred_path.open() as handle:
        qa_app._registry_storage_credentials = yaml.safe_load(handle)

    db = qa_app.get_db("metabolite_harmonization")
    stage_collection = db.collection(qa_app._HARMONIZATION_STAGE_COLLECTION)

    stages = list(db.aql.execute(
        f"FOR s IN {qa_app._HARMONIZATION_STAGE_COLLECTION} FILTER s.status == 'complete' RETURN s"
    ))
    print(f"Found {len(stages)} complete stage(s).")

    updated = 0
    skipped_existing = 0
    for stage in stages:
        stage_key = stage["_key"]
        existing = (stage.get("validation") or {}).get("denylist_still_merged")
        if isinstance(existing, dict) and not args.force:
            skipped_existing += 1
            continue

        rule_ids = stage.get("rule_ids") or []
        rule_enabled = "ignore_ramp_mapping_denylist" in rule_ids
        denylist_pairs = (
            qa_app._load_metabolite_edge_removal_curations()["pairs"]
            if rule_enabled
            else set()
        )

        member_sets = qa_app._load_stage_harmonized_member_sets(stage_key)
        groups = [row["member_ids"] for row in member_sets]

        validation = qa_app._build_harmonization_stage_denylist_validation(
            groups, denylist_pairs, rule_enabled,
        )

        label = stage.get("name") or stage_key
        if rule_enabled:
            status = f"{validation['warning_count']} still-merged pair(s) of {validation['denylist_pair_count']} on the denylist"
        else:
            status = "rule not enabled"
        print(f"  {stage_key} ({label}): {status}")

        if not args.dry_run:
            stage_collection.update({
                "_key": stage_key,
                "validation": {"denylist_still_merged": validation},
            })
        updated += 1

    verb = "Would update" if args.dry_run else "Updated"
    print(f"\n{verb} {updated} stage(s); skipped {skipped_existing} that already had it (use --force to recompute).")


if __name__ == "__main__":
    main()
