#!/usr/bin/env python3
"""Compare HarmonizedMetabolite cliques (final pipeline stage) to RaMP SQLite compound groupings.

Graph cliques come from HarmonizedMetabolite nodes + HarmonizedMetaboliteMemberEdge for a given
stage_key, plus singletons from HarmonizationStageActiveIdentifierChunk.

RaMP cliques are sets of normalized source IDs sharing the same RAMP_C id in the source table.
"""

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
DEFAULT_STAGE_KEY = "stage-06-e23ebdd2bf1ef4c0"

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


def prefix_of(identifier: str) -> str:
    return identifier.split(":", 1)[0] if ":" in identifier else "<none>"


def load_ramp_sqlite(sqlite_path: str):
    """Load RaMP source table for RAMP_C ids; return clique mappings both directions."""
    conn = sqlite3.connect(sqlite_path)
    ramp_id_to_ids: dict[str, set[str]] = defaultdict(set)
    id_to_ramp_ids: dict[str, set[str]] = defaultdict(set)
    unmapped: Counter = Counter()
    row_count = 0
    for id_type, source_id, ramp_id in conn.execute(
        "SELECT IDtype, sourceId, rampId FROM source WHERE rampId LIKE 'RAMP_C%'"
    ):
        row_count += 1
        normalized = normalize_ramp_source_id(id_type, source_id)
        if normalized is None:
            unmapped[id_type] += 1
            continue
        ramp_id_to_ids[ramp_id].add(normalized)
        id_to_ramp_ids[normalized].add(ramp_id)
    conn.close()
    return row_count, ramp_id_to_ids, id_to_ramp_ids, unmapped


def load_graph_cliques(db: ArangoAdapter, stage_key: str):
    """
    Return:
      clique_key_to_members: dict[str, set[str]]  (clique _key -> set of member IDs)
      id_to_clique_key: dict[str, str]             (member ID -> clique _key, singletons use "singleton:{id}")
      active_ids: set[str]                          (all active IDs at this stage)
      clique_meta: dict[str, dict]                  (clique _key -> {size, representative_id, rank_by_size})
    """
    print(f"Loading HarmonizedMetabolite nodes for stage {stage_key}...", file=sys.stderr)
    meta_rows = db.runQuery(
        """
        FOR h IN HarmonizedMetabolite
          FILTER h.stage_key == @stage_key
          RETURN KEEP(h, "_key", "size", "rank_by_size", "representative_id", "name")
        """,
        {"stage_key": stage_key},
    )
    clique_meta = {row["_key"]: row for row in meta_rows}

    print(f"  {len(clique_meta):,} clique nodes found", file=sys.stderr)

    print("Loading member edges...", file=sys.stderr)
    member_rows = db.runQuery(
        """
        FOR e IN HarmonizedMetaboliteMemberEdge
          FILTER e.stage_key == @stage_key
          RETURN {clique_key: SPLIT(e._from, "/")[1], member_id: e.member_id}
        """,
        {"stage_key": stage_key},
    )
    clique_key_to_members: dict[str, set[str]] = defaultdict(set)
    id_to_clique_key: dict[str, str] = {}
    for row in member_rows:
        clique_key = row["clique_key"]
        member_id = row["member_id"]
        clique_key_to_members[clique_key].add(member_id)
        id_to_clique_key[member_id] = clique_key

    print(f"  {len(member_rows):,} member edges, {len(id_to_clique_key):,} unique clique members", file=sys.stderr)

    print("Loading active identifier chunks...", file=sys.stderr)
    active_ids: set[str] = set(db.runQuery(
        """
        FOR c IN HarmonizationStageActiveIdentifierChunk
          FILTER c.stage_key == @stage_key
          FOR id IN c.identifier_ids
            RETURN id
        """,
        {"stage_key": stage_key},
    ))
    singletons = active_ids - set(id_to_clique_key)
    for sid in singletons:
        id_to_clique_key[sid] = f"singleton:{sid}"

    print(f"  {len(active_ids):,} active IDs, {len(singletons):,} singletons", file=sys.stderr)

    return clique_key_to_members, id_to_clique_key, active_ids, clique_meta


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sqlite", default=DEFAULT_SQLITE)
    parser.add_argument("--credentials", default=DEFAULT_CREDENTIALS)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--stage-key", default=DEFAULT_STAGE_KEY)
    parser.add_argument("--sample-size", type=int, default=10)
    args = parser.parse_args()

    print("Loading RaMP SQLite...", file=sys.stderr)
    row_count, ramp_id_to_ids, id_to_ramp_ids, unmapped = load_ramp_sqlite(args.sqlite)
    ramp_ids = set(id_to_ramp_ids)
    print(f"  {row_count:,} rows, {len(ramp_ids):,} distinct normalized IDs, "
          f"{len(ramp_id_to_ids):,} RAMP_C cliques", file=sys.stderr)

    creds = DBCredentials.from_yaml(yaml.safe_load(open(args.credentials)))
    db = ArangoAdapter(creds, args.database)

    clique_key_to_members, id_to_clique_key, active_ids, clique_meta = load_graph_cliques(db, args.stage_key)

    # Intersection
    shared_ids = active_ids & ramp_ids
    only_ramp = ramp_ids - active_ids
    only_graph = active_ids - ramp_ids

    # Build per-clique views restricted to shared IDs
    graph_common: dict[str, set[str]] = defaultdict(set)  # clique_key -> shared IDs in it
    ramp_common: dict[str, set[str]] = defaultdict(set)   # ramp_id -> shared IDs in it
    for identifier in shared_ids:
        clique_key = id_to_clique_key[identifier]
        graph_common[clique_key].add(identifier)
        for ramp_id in id_to_ramp_ids[identifier]:
            ramp_common[ramp_id].add(identifier)

    # Exact match count (partition-level)
    graph_sets = Counter(frozenset(ids) for ids in graph_common.values())
    ramp_sets = Counter(frozenset(ids) for ids in ramp_common.values())
    exact_matches = sum(min(count, graph_sets[group]) for group, count in ramp_sets.items())

    # RaMP cliques split across multiple graph groups
    ramp_splits = []
    for ramp_id, identifiers in ramp_common.items():
        groups: dict[str, list[str]] = defaultdict(list)
        for identifier in identifiers:
            groups[id_to_clique_key[identifier]].append(identifier)
        if len(groups) > 1:
            ramp_splits.append({
                "ramp_id": ramp_id,
                "shared_ids": len(identifiers),
                "graph_groups": len(groups),
                "graph_group_sizes": sorted((len(ids) for ids in groups.values()), reverse=True),
                "sample_ids": sorted(identifiers)[:args.sample_size],
                "graph_group_keys": sorted(groups)[:args.sample_size],
            })
    ramp_splits.sort(key=lambda r: (-r["graph_groups"], -r["shared_ids"], r["ramp_id"]))

    # Graph cliques merging multiple RaMP ids
    graph_merges = []
    for clique_key, identifiers in graph_common.items():
        ramp_groups: dict[str, list[str]] = defaultdict(list)
        for identifier in identifiers:
            for ramp_id in id_to_ramp_ids[identifier]:
                ramp_groups[ramp_id].append(identifier)
        if len(ramp_groups) > 1:
            meta = clique_meta.get(clique_key, {})
            graph_merges.append({
                "clique_key": clique_key,
                "clique_size": meta.get("size"),
                "representative_id": meta.get("representative_id"),
                "shared_ids": len(identifiers),
                "ramp_ids": len(ramp_groups),
                "ramp_group_sizes": sorted((len(set(ids)) for ids in ramp_groups.values()), reverse=True),
                "sample_ids": sorted(identifiers)[:args.sample_size],
                "sample_ramp_ids": sorted(ramp_groups)[:args.sample_size],
            })
    graph_merges.sort(key=lambda r: (-r["ramp_ids"], -r["shared_ids"], r["clique_key"]))

    # Graph clique size distribution
    size_dist = Counter(clique_meta[k]["size"] for k in clique_meta if "size" in clique_meta[k])

    result = {
        "sqlite": args.sqlite,
        "database": args.database,
        "stage_key": args.stage_key,
        "ramp": {
            "source_rows": row_count,
            "normalized_source_ids": len(ramp_ids),
            "compound_cliques": len(ramp_id_to_ids),
            "ids_with_multiple_ramp_ids": sum(1 for v in id_to_ramp_ids.values() if len(v) > 1),
            "unmapped_idtypes": dict(unmapped),
        },
        "graph": {
            "active_ids": len(active_ids),
            "non_singleton_cliques": len(clique_meta),
            "clique_member_edges": sum(len(m) for m in clique_key_to_members.values()),
            "singleton_ids": sum(1 for v in id_to_clique_key.values() if v.startswith("singleton:")),
            "total_groups": len(set(id_to_clique_key.values())),
            "size_distribution": [
                {"size": size, "cliques": count}
                for size, count in sorted(size_dist.items())
                if size <= 20 or size in (50, 100, 200, 500)
            ],
        },
        "coverage": {
            "shared_ids": len(shared_ids),
            "ramp_ids_missing_from_graph": len(only_ramp),
            "graph_ids_absent_from_ramp": len(only_graph),
            "missing_ramp_by_prefix": Counter(prefix_of(i) for i in only_ramp).most_common(30),
            "extra_graph_by_prefix": Counter(prefix_of(i) for i in only_graph).most_common(30),
        },
        "shared_partition": {
            "ramp_cliques_with_shared_ids": len(ramp_common),
            "graph_cliques_with_shared_ids": len(graph_common),
            "exact_matching_cliques_on_shared_ids": exact_matches,
            "ramp_cliques_split_in_graph": len(ramp_splits),
            "graph_cliques_merging_multiple_ramp_ids": len(graph_merges),
            "top_ramp_cliques_split_in_graph": ramp_splits[:args.sample_size],
            "top_graph_cliques_merging_ramp_ids": graph_merges[:args.sample_size],
        },
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
