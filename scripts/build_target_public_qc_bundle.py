#!/usr/bin/env python3
"""Build read-only target QC artifacts for the QA Browser bundle.

The local TargetGraph QC registry and review intake files are internal working
artifacts. This script converts them into a compact public bundle:

- aggregate reviewed/open QC counts;
- a filtered TSV of rows that still need a decision;
- no writable intake file and no reviewer notes.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.qa_browser.target_id_graph import (
    REVIEW_GROUPS,
    TRIAGE_CATEGORIES,
    classify_review_group,
    classify_triage_category,
)


PUBLIC_REVIEW_COLUMNS = [
    "registry_id",
    "entity_type",
    "standard_name",
    "row_id",
    "source",
    "namespace",
    "divergence_type",
    "source_value",
    "consensus_value",
    "other_sources",
    "n_sources_agree",
    "n_sources_total",
    "auto_decision",
    "auto_confidence",
    "auto_rationale",
    "scenario_id",
    "status",
    "finding_summary",
    "triage_category",
    "review_group",
    "review_group_label",
    "review_group_description",
    "public_status_note",
]


def _read_reviewed_registry_ids(review_file: Path) -> set[str]:
    reviewed: set[str] = set()
    if not review_file.exists():
        return reviewed
    with open(review_file, newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh, delimiter="\t")
        for row in reader:
            raw = row.get("Registry ID") or row.get("registry_id") or ""
            for reg_id in raw.split("|"):
                reg_id = reg_id.strip()
                if reg_id:
                    reviewed.add(reg_id)
    return reviewed


def _iter_registry_rows(qc_dir: Path):
    for entity_type, filename in [
        ("gene", "gene_divergence_registry.tsv"),
        ("protein", "protein_divergence_registry.tsv"),
        ("transcript", "transcript_divergence_registry.tsv"),
    ]:
        path = qc_dir / filename
        if not path.exists():
            continue
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh, delimiter="\t")
            for row in reader:
                yield entity_type, row


def build_public_bundle(qc_dir: Path, out_dir: Path) -> dict[str, Any]:
    review_file = qc_dir / "review" / "app_completed" / "target_app_review_decisions.tsv"
    reviewed_ids = _read_reviewed_registry_ids(review_file)

    group_by_key = {group["key"]: group for group in REVIEW_GROUPS}
    divergence_by_entity: Counter[str] = Counter()
    divergence_type_counts: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    auto_decision_counts: Counter[str] = Counter()
    source_counts: Counter[str] = Counter()
    namespace_counts: Counter[str] = Counter()
    triage_counts: Counter[str] = Counter()
    review_group_counts: Counter[str] = Counter()
    review_group_open_counts: Counter[str] = Counter()
    review_group_resolved_counts: Counter[str] = Counter()
    triage_open_counts: Counter[str] = Counter()
    triage_resolved_counts: Counter[str] = Counter()
    open_conflict_auto_decisions: Counter[str] = Counter()
    confidence_values: list[float] = []

    open_rows = 0
    total_rows = 0
    out_dir.mkdir(parents=True, exist_ok=True)
    rows_path = out_dir / "target_review_public.tsv"
    with open(rows_path, "w", newline="", encoding="utf-8") as out_fh:
        writer = csv.DictWriter(out_fh, delimiter="\t", fieldnames=PUBLIC_REVIEW_COLUMNS)
        writer.writeheader()

        for entity_type, row in _iter_registry_rows(qc_dir):
            total_rows += 1
            registry_id = row.get("registry_id", "")
            status = "resolved" if registry_id in reviewed_ids else (row.get("status") or "open")
            row["status"] = status
            triage = classify_triage_category(row)
            group_key = classify_review_group(entity_type, row)
            group = group_by_key.get(group_key, {})
            div_type = row.get("divergence_type") or ""
            auto_decision = row.get("auto_decision") or ""
            source = row.get("source") or ""
            namespace = row.get("namespace") or ""

            divergence_by_entity[entity_type] += 1
            divergence_type_counts[div_type] += 1
            status_counts[status] += 1
            triage_counts[triage] += 1
            review_group_counts[group_key] += 1
            if auto_decision:
                auto_decision_counts[auto_decision] += 1
            if source:
                source_counts[source] += 1
            if namespace:
                namespace_counts[namespace] += 1
            try:
                confidence_values.append(float(row.get("auto_confidence") or ""))
            except ValueError:
                pass

            if status == "resolved":
                triage_resolved_counts[triage] += 1
                review_group_resolved_counts[group_key] += 1
                continue

            open_rows += 1
            triage_open_counts[triage] += 1
            review_group_open_counts[group_key] += 1
            if triage == "source_conflict" and auto_decision:
                open_conflict_auto_decisions[auto_decision] += 1
            writer.writerow({
                **{col: row.get(col, "") for col in PUBLIC_REVIEW_COLUMNS},
                "entity_type": entity_type,
                "status": status,
                "triage_category": triage,
                "review_group": group_key,
                "review_group_label": group.get("label", group_key),
                "review_group_description": group.get("description", ""),
                "public_status_note": "Still needs ODIN expert review before being treated as fully QC-resolved.",
            })

    confidence_average = round(sum(confidence_values) / len(confidence_values), 3) if confidence_values else 0
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "review_mode": "public_read_only",
        "source_qc_dir": str(qc_dir),
        "source_review_file": str(review_file),
        "open_review_rows_file": rows_path.name,
        "divergence": {
            "total_divergences": total_rows,
            "divergences_by_entity": dict(divergence_by_entity.most_common()),
            "divergence_type_counts": dict(divergence_type_counts.most_common()),
            "status_counts": dict(status_counts.most_common()),
            "auto_decision_counts": dict(auto_decision_counts.most_common()),
            "source_divergence_counts": dict(source_counts.most_common()),
            "namespace_divergence_counts": dict(namespace_counts.most_common()),
            "triage_category_counts": dict(triage_counts.most_common()),
            "triage_category_open_counts": dict(triage_open_counts.most_common()),
            "triage_category_resolved_counts": dict(triage_resolved_counts.most_common()),
            "review_group_counts": dict(review_group_counts.most_common()),
            "review_group_open_counts": dict(review_group_open_counts.most_common()),
            "review_group_resolved_counts": dict(review_group_resolved_counts.most_common()),
            "open_conflicts": open_rows,
            "open_conflict_auto_decisions": dict(open_conflict_auto_decisions.most_common()),
            "confidence_average": confidence_average,
            "review_groups": REVIEW_GROUPS,
            "triage_categories": TRIAGE_CATEGORIES,
            "review_mode": "public",
            "review_can_write": False,
        },
    }
    with open(out_dir / "target_qc_public_summary.json", "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, sort_keys=True)
        fh.write("\n")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qc-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    summary = build_public_bundle(args.qc_dir, args.out_dir)
    divergence = summary["divergence"]
    print(f"Wrote public target QC bundle to {args.out_dir}")
    print(f"  Total QC findings: {divergence['total_divergences']:,}")
    print(f"  Reviewed/resolved: {divergence['status_counts'].get('resolved', 0):,}")
    print(f"  Still open: {divergence['status_counts'].get('open', 0):,}")


if __name__ == "__main__":
    main()
