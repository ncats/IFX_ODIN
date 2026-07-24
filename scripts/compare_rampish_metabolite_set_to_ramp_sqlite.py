#!/usr/bin/env python3
"""Compare a materialized harmonization stage to RaMP SQLite compound IDs."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


DEFAULT_SQLITE = "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v4.0.0.sqlite"
DEFAULT_CREDENTIALS = "src/use_cases/secrets/ifxdev_arangodb.yaml"
DEFAULT_DATABASE = "metabolite_harmonization"
DEFAULT_PIPELINE_NAME = "RaMP-ish Metabolite Set"

PREFIX_MAP = {
    "CAS": "CAS",
    "LIPIDMAPS": "LIPIDMAPS",
    "chebi": "CHEBI",
    "chemspider": "ChemSpider",
    "hmdb": "HMDB",
    "kegg": "KEGG.COMPOUND",
    "kegg_glycan": "KEGG.GLYCAN",
    "lipidbank": "LipidBank",
    "plantfa": "PlantFA",
    "polymer": "RHEA.POLYMER",
    "pubchem": "PUBCHEM.COMPOUND",
    "refmet": "REFMET",
    "rhea-comp": "RHEA.COMP",
    "swisslipids": "SwissLipids",
    "wikidata": "Wikidata",
}


def normalize_ramp_source_id(id_type: str | None, source_id: str | None) -> str | None:
    if not id_type or not source_id:
        return None
    graph_prefix = PREFIX_MAP.get(id_type)
    if not graph_prefix:
        return source_id if ":" in source_id else None
    if ":" not in source_id:
        return f"{graph_prefix}:{source_id}"
    return f"{graph_prefix}:{source_id.split(':', 1)[1]}"


def prefix(identifier: str) -> str:
    return identifier.split(":", 1)[0] if ":" in identifier else "<none>"


def load_ramp_sqlite(sqlite_path: str):
    conn = sqlite3.connect(sqlite_path)
    ramp_id_to_ids: dict[str, set[str]] = defaultdict(set)
    id_to_ramp_ids: dict[str, set[str]] = defaultdict(set)
    id_evidence: dict[str, set[tuple[str, str]]] = defaultdict(set)
    row_count = 0
    unmapped = Counter()
    for id_type, source_id, ramp_id, data_source in conn.execute(
        """
        SELECT IDtype, sourceId, rampId, dataSource
        FROM source
        WHERE rampId LIKE 'RAMP_C%'
        """
    ):
        row_count += 1
        normalized_id = normalize_ramp_source_id(id_type, source_id)
        if normalized_id is None:
            unmapped[id_type] += 1
            continue
        ramp_id_to_ids[ramp_id].add(normalized_id)
        id_to_ramp_ids[normalized_id].add(ramp_id)
        id_evidence[normalized_id].add((id_type, data_source))
    conn.close()
    return row_count, ramp_id_to_ids, id_to_ramp_ids, id_evidence, unmapped


def get_stage_key(db: ArangoAdapter, pipeline_name: str, stage_key: str | None) -> tuple[str, dict | None]:
    if stage_key:
        return stage_key, None
    rows = db.runQuery(
        """
        FOR p IN HarmonizationPipeline
          FILTER LOWER(p.name) == LOWER(@pipeline_name)
          SORT p.updated_at DESC
          LIMIT 1
          RETURN KEEP(p, "_key", "name", "latest_stage_key", "latest_run_key", "rule_ids", "updated_at")
        """,
        {"pipeline_name": pipeline_name},
    )
    if not rows:
        raise ValueError(f"No HarmonizationPipeline named {pipeline_name!r}")
    if not rows[0].get("latest_stage_key"):
        raise ValueError(f"HarmonizationPipeline {pipeline_name!r} has no latest_stage_key")
    return rows[0]["latest_stage_key"], rows[0]


def load_graph_stage(db: ArangoAdapter, stage_key: str):
    stage_rows = db.runQuery(
        """
        LET s = DOCUMENT("HarmonizationStage", @stage_key)
        RETURN KEEP(s, "_key", "name", "status", "summary", "rule_ids", "stage_index", "created_at")
        """,
        {"stage_key": stage_key},
    )
    stage = stage_rows[0] if stage_rows else {}
    active_ids = set(db.runQuery(
        """
        FOR c IN HarmonizationStageActiveIdentifierChunk
          FILTER c.stage_key == @stage_key
          FOR id IN c.identifier_ids
            RETURN id
        """,
        {"stage_key": stage_key},
    ))
    member_rows = db.runQuery(
        """
        FOR e IN HarmonizedMetaboliteMemberEdge
          FILTER e.stage_key == @stage_key
          RETURN {id: e.member_id, clique: SPLIT(e._from, "/")[1]}
        """,
        {"stage_key": stage_key},
    )
    group_by_id = {row["id"]: row["clique"] for row in member_rows}
    for identifier in active_ids:
        group_by_id.setdefault(identifier, f"singleton:{identifier}")
    return stage, active_ids, member_rows, group_by_id


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default=DEFAULT_SQLITE)
    parser.add_argument("--credentials", default=DEFAULT_CREDENTIALS)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--pipeline-name", default=DEFAULT_PIPELINE_NAME)
    parser.add_argument("--stage-key")
    parser.add_argument("--sample-size", type=int, default=10)
    args = parser.parse_args()

    row_count, ramp_id_to_ids, id_to_ramp_ids, _id_evidence, unmapped = load_ramp_sqlite(args.sqlite)
    ramp_ids = set(id_to_ramp_ids)

    creds = DBCredentials.from_yaml(yaml.safe_load(open(args.credentials)))
    db = ArangoAdapter(creds, args.database)
    stage_key, pipeline = get_stage_key(db, args.pipeline_name, args.stage_key)
    stage, graph_active_ids, member_rows, graph_group_by_id = load_graph_stage(db, stage_key)

    shared_ids = graph_active_ids & ramp_ids
    only_ramp = ramp_ids - graph_active_ids
    only_graph = graph_active_ids - ramp_ids

    graph_common_groups: dict[str, set[str]] = defaultdict(set)
    ramp_common_groups: dict[str, set[str]] = defaultdict(set)
    for identifier in shared_ids:
        graph_common_groups[graph_group_by_id[identifier]].add(identifier)
        for ramp_id in id_to_ramp_ids[identifier]:
            ramp_common_groups[ramp_id].add(identifier)

    graph_sets = Counter(frozenset(ids) for ids in graph_common_groups.values())
    ramp_sets = Counter(frozenset(ids) for ids in ramp_common_groups.values())
    exact_matching_groups = sum(min(count, graph_sets[group]) for group, count in ramp_sets.items())

    ramp_splits = []
    for ramp_id, identifiers in ramp_common_groups.items():
        graph_groups: dict[str, list[str]] = defaultdict(list)
        for identifier in identifiers:
            graph_groups[graph_group_by_id[identifier]].append(identifier)
        if len(graph_groups) > 1:
            ramp_splits.append({
                "rampId": ramp_id,
                "shared_ids": len(identifiers),
                "graph_groups": len(graph_groups),
                "graph_group_sizes": sorted((len(ids) for ids in graph_groups.values()), reverse=True),
                "sample_ids": sorted(identifiers)[:args.sample_size],
            })
    ramp_splits.sort(key=lambda row: (-row["graph_groups"], -row["shared_ids"], row["rampId"]))

    graph_merges = []
    for graph_group, identifiers in graph_common_groups.items():
        ramp_groups: dict[str, list[str]] = defaultdict(list)
        for identifier in identifiers:
            for ramp_id in id_to_ramp_ids[identifier]:
                ramp_groups[ramp_id].append(identifier)
        if len(ramp_groups) > 1:
            graph_merges.append({
                "graph_group": graph_group,
                "shared_ids": len(identifiers),
                "ramp_ids": len(ramp_groups),
                "ramp_group_sizes": sorted((len(set(ids)) for ids in ramp_groups.values()), reverse=True),
                "sample_ids": sorted(identifiers)[:args.sample_size],
                "sample_rampIds": sorted(ramp_groups)[:args.sample_size],
            })
    graph_merges.sort(key=lambda row: (-row["ramp_ids"], -row["shared_ids"], row["graph_group"]))

    result = {
        "sqlite": args.sqlite,
        "database": args.database,
        "pipeline": pipeline,
        "stage": stage,
        "ramp": {
            "source_rows": row_count,
            "normalized_source_ids": len(ramp_ids),
            "compound_analytes": len(ramp_id_to_ids),
            "ids_with_multiple_ramp_ids": sum(1 for ramp_group in id_to_ramp_ids.values() if len(ramp_group) > 1),
            "unmapped_idtypes": dict(unmapped),
        },
        "graph_stage": {
            "active_identifier_ids": len(graph_active_ids),
            "non_singleton_member_edges": len(member_rows),
            "groups_represented_by_active_ids": len(set(graph_group_by_id.values())),
        },
        "coverage": {
            "shared_ids": len(shared_ids),
            "ramp_ids_missing_from_graph_stage": len(only_ramp),
            "graph_stage_ids_absent_from_ramp_source": len(only_graph),
            "missing_ramp_by_prefix": Counter(prefix(identifier) for identifier in only_ramp).most_common(30),
            "extra_graph_by_prefix": Counter(prefix(identifier) for identifier in only_graph).most_common(30),
        },
        "shared_partition": {
            "ramp_groups_represented": len(ramp_common_groups),
            "graph_groups_represented": len(graph_common_groups),
            "exact_matching_groups_on_shared_ids": exact_matching_groups,
            "ramp_groups_split_in_graph": len(ramp_splits),
            "graph_groups_merging_multiple_ramp_ids": len(graph_merges),
            "top_ramp_groups_split_in_graph": ramp_splits[:args.sample_size],
            "top_graph_groups_merging_ramp_ids": graph_merges[:args.sample_size],
        },
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
