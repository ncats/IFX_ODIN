#!/usr/bin/env python3
"""Compare RaMP compound source IDs with metabolite_harmonization identifiers."""

from __future__ import annotations

import argparse
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable, Optional

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials


DEFAULT_SQLITE = "/Users/kelleherkj/IdeaProjects/ramp-backend-ncats/schema/RaMP_SQLite_v4.0.0.sqlite"
DEFAULT_CREDENTIALS = "src/use_cases/secrets/ifxdev_arangodb.yaml"
DEFAULT_DATABASE = "metabolite_harmonization"


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


def normalize_ramp_source_id(id_type: Optional[str], source_id: str) -> Optional[str]:
    if not id_type or not source_id:
        return None
    graph_prefix = PREFIX_MAP.get(id_type)
    if not graph_prefix:
        if ":" in source_id:
            return source_id
        return None
    if ":" not in source_id:
        return f"{graph_prefix}:{source_id}"
    _, local_id = source_id.split(":", 1)
    return f"{graph_prefix}:{local_id}"


def load_ramp_compound_ids(sqlite_path: str):
    conn = sqlite3.connect(sqlite_path)
    rows = conn.execute(
        """
        SELECT IDtype, sourceId, dataSource
        FROM source
        WHERE rampId LIKE 'RAMP_C%'
        """
    )
    normalized_ids = set()
    raw_counts = Counter()
    normalized_by_type: dict[str, set[str]] = defaultdict(set)
    evidence_by_id: dict[str, set[tuple[str, str]]] = defaultdict(set)
    unmapped_counts = Counter()
    for id_type, source_id, data_source in rows:
        raw_counts[id_type] += 1
        normalized_id = normalize_ramp_source_id(id_type, source_id)
        if normalized_id is None:
            unmapped_counts[id_type] += 1
            continue
        normalized_ids.add(normalized_id)
        normalized_by_type[id_type].add(normalized_id)
        evidence_by_id[normalized_id].add((id_type, data_source))
    conn.close()
    return normalized_ids, raw_counts, normalized_by_type, unmapped_counts, evidence_by_id


def load_ramp_reaction_metabolite_ids(sqlite_path: str) -> set[str]:
    conn = sqlite3.connect(sqlite_path)
    rows = conn.execute(
        """
        SELECT s.IDtype, m.met_source_id
        FROM reaction2met m
        JOIN source s ON s.sourceId = m.met_source_id
        WHERE s.rampId LIKE 'RAMP_C%'
        """
    )
    normalized_ids = {
        normalized_id
        for id_type, source_id in rows
        if (normalized_id := normalize_ramp_source_id(id_type, source_id)) is not None
    }
    conn.close()
    return normalized_ids


def load_graph_ids(credentials_path: str, database_name: str):
    creds = DBCredentials.from_yaml(yaml.safe_load(open(credentials_path)))
    adapter = ArangoAdapter(creds, database_name)
    rows = adapter.runQuery(
        """
        FOR d IN MetaboliteIdentifier
          RETURN {id: d.id, prefix: d.prefix, sources: d.sources}
        """
    )
    ids = {row["id"] for row in rows}
    prefix_counts = Counter(row.get("prefix") for row in rows)
    prefix_by_id = {row["id"]: row.get("prefix") for row in rows}
    source_names_by_id = {
        row["id"]: {
            source.split("\t", 1)[0]
            for source in (row.get("sources") or [])
            if source
        }
        for row in rows
    }
    return ids, prefix_counts, prefix_by_id, source_names_by_id


def count_documents(adapter: ArangoAdapter, collection: str, ids: set[str], chunk_size: int = 5000) -> int:
    total = 0
    ordered_ids = sorted(ids)
    for idx in range(0, len(ordered_ids), chunk_size):
        chunk = ordered_ids[idx:idx + chunk_size]
        total += adapter.runQuery(
            """
            FOR id IN @ids
              FILTER DOCUMENT(@collection, id) != null
              COLLECT WITH COUNT INTO n
              RETURN n
            """,
            {"ids": chunk, "collection": collection},
        )[0]
    return total


def sample(values: Iterable[str], limit: int) -> list[str]:
    return sorted(values)[:limit]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default=DEFAULT_SQLITE)
    parser.add_argument("--credentials", default=DEFAULT_CREDENTIALS)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--sample-size", type=int, default=10)
    args = parser.parse_args()

    (
        ramp_ids,
        ramp_rows_by_type,
        ramp_ids_by_type,
        unmapped_counts,
        ramp_evidence_by_id,
    ) = load_ramp_compound_ids(args.sqlite)
    ramp_reaction_metabolite_ids = load_ramp_reaction_metabolite_ids(args.sqlite)
    graph_ids, graph_prefix_counts, graph_prefix_by_id, graph_sources_by_id = load_graph_ids(
        args.credentials,
        args.database,
    )

    print(f"RaMP SQLite: {args.sqlite}")
    print(f"Graph database: {args.database}")
    print()
    print(f"RaMP compound source IDs, normalized distinct: {len(ramp_ids):,}")
    print(f"Graph MetaboliteIdentifier IDs: {len(graph_ids):,}")
    print(f"RaMP IDs present in graph: {len(ramp_ids & graph_ids):,}")
    print(f"RaMP IDs missing from graph: {len(ramp_ids - graph_ids):,}")
    print(f"Graph IDs absent from RaMP compound source table: {len(graph_ids - ramp_ids):,}")
    if unmapped_counts:
        print("Unmapped RaMP ID types:", dict(sorted(unmapped_counts.items())))
    print()

    print("By RaMP IDtype")
    print("IDtype\tRaMP rows\tRaMP distinct\tpresent in graph\tmissing from graph")
    for id_type, row_count in ramp_rows_by_type.most_common():
        distinct_ids = ramp_ids_by_type.get(id_type, set())
        present = len(distinct_ids & graph_ids)
        missing = len(distinct_ids - graph_ids)
        print(f"{id_type}\t{row_count:,}\t{len(distinct_ids):,}\t{present:,}\t{missing:,}")
    print()

    missing = ramp_ids - graph_ids
    missing_with_reaction_evidence = missing & ramp_reaction_metabolite_ids
    missing_source_table_only = missing - ramp_reaction_metabolite_ids
    print(
        "Missing RaMP IDs with reaction2met evidence: "
        f"{len(missing_with_reaction_evidence):,}"
    )
    print(
        "Missing RaMP IDs present only in RaMP source table: "
        f"{len(missing_source_table_only):,}"
    )
    print()

    print("RaMP IDs missing from graph, by RaMP IDtype/dataSource")
    print("IDtype\tdataSource\tdistinct missing IDs")
    missing_evidence_counts = Counter(
        evidence
        for missing_id in missing
        for evidence in ramp_evidence_by_id.get(missing_id, set())
    )
    for (id_type, data_source), count in missing_evidence_counts.most_common():
        print(f"{id_type}\t{data_source}\t{count:,}")
    print()

    print("Graph prefix distribution")
    print("prefix\tgraph IDs")
    for prefix, count in graph_prefix_counts.most_common():
        print(f"{prefix}\t{count:,}")
    print()

    extra = graph_ids - ramp_ids
    print("Graph IDs absent from RaMP source table, by graph prefix")
    print("prefix\tdistinct graph-only IDs")
    extra_prefix_counts = Counter(graph_prefix_by_id.get(extra_id) for extra_id in extra)
    for prefix, count in extra_prefix_counts.most_common():
        print(f"{prefix}\t{count:,}")
    print()

    print("Graph IDs absent from RaMP source table, by graph source/prefix")
    print("source\tprefix\tdistinct graph-only IDs")
    extra_source_prefix_counts = Counter()
    for extra_id in extra:
        sources = graph_sources_by_id.get(extra_id) or {"<no source>"}
        prefix = graph_prefix_by_id.get(extra_id)
        for source in sources:
            extra_source_prefix_counts[(source, prefix)] += 1
    for (source, prefix), count in extra_source_prefix_counts.most_common(60):
        print(f"{source}\t{prefix}\t{count:,}")
    print()

    missing_chebi = {missing_id for missing_id in missing if missing_id.startswith("CHEBI:")}
    if missing_chebi:
        creds = DBCredentials.from_yaml(yaml.safe_load(open(args.credentials)))
        adapter = ArangoAdapter(creds, args.database)
        print(
            "Missing RaMP CHEBI IDs present as ChemicalEntity: "
            f"{count_documents(adapter, 'ChemicalEntity', missing_chebi):,} / {len(missing_chebi):,}"
        )
        print()

    print(f"Sample RaMP IDs missing from graph ({min(args.sample_size, len(missing))})")
    for value in sample(missing, args.sample_size):
        evidence = ", ".join(f"{id_type}/{data_source}" for id_type, data_source in sorted(ramp_evidence_by_id[value]))
        print(f"{value}\t{evidence}")
    print()
    print(f"Sample graph IDs absent from RaMP source table ({min(args.sample_size, len(extra))})")
    for value in sample(extra, args.sample_size):
        sources = ", ".join(sorted(graph_sources_by_id.get(value) or []))
        print(f"{value}\t{graph_prefix_by_id.get(value)}\t{sources}")


if __name__ == "__main__":
    main()
