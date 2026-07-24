#!/usr/bin/env python3
"""Profile harmonized graph IDs that are absent from RaMP SQLite source."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.compare_rampish_metabolite_set_to_ramp_sqlite import (  # noqa: E402
    DEFAULT_CREDENTIALS,
    DEFAULT_DATABASE,
    DEFAULT_SQLITE,
    load_ramp_sqlite,
)
from src.shared.arango_adapter import ArangoAdapter  # noqa: E402
from src.shared.db_credentials import DBCredentials  # noqa: E402


DEFAULT_STAGE_KEY = "stage-06-5d8f2367871fc699"


def source_name(value) -> str:
    return str(value).strip().split("\t", 1)[0]


def prefix(identifier: str) -> str:
    return identifier.split(":", 1)[0] if ":" in identifier else "<none>"


def load_stage_active_ids(db: ArangoAdapter, stage_key: str) -> set[str]:
    return set(db.runQuery(
        """
        FOR c IN HarmonizationStageActiveIdentifierChunk
          FILTER c.stage_key == @stage_key
          FOR id IN c.identifier_ids
            RETURN id
        """,
        {"stage_key": stage_key},
    ))


def load_graph_only_node_profiles(db: ArangoAdapter, ids: set[str], chunk_size: int = 5000) -> list[dict]:
    rows = []
    ordered = sorted(ids)
    for index in range(0, len(ordered), chunk_size):
        chunk = ordered[index:index + chunk_size]
        rows.extend(db.runQuery(
            """
            FOR id IN @ids
              LET d = DOCUMENT("MetaboliteIdentifier", id)
              FILTER d != null
              LET support = UNIQUE(FLATTEN([
                (FOR item IN d.sources || []
                  RETURN IS_OBJECT(item)
                    ? (item.name != null ? item.name : (item.source != null ? item.source : item.id))
                    : SPLIT(TO_STRING(item), "\t")[0]),
                (FOR item IN d.names || [] RETURN item.source),
                (FOR item IN d.synonyms || [] RETURN item.source),
                (FOR item IN d.chem_props || [] RETURN item.source)
              ]))
              LET clean_support = UNIQUE(
                FOR s IN support
                  FILTER s != null AND TRIM(TO_STRING(s)) != ""
                  RETURN TRIM(TO_STRING(s))
              )
              LET path_count = LENGTH(FOR e IN MetabolitePathwayEdge FILTER e._from == d._id RETURN 1)
              LET rhea_count = LENGTH(FOR e IN RheaMetaboliteReactionEdge FILTER e._from == d._id RETURN 1)
              LET protein_count = LENGTH(FOR e IN HmdbMetaboliteProteinAssociationEdge FILTER e._from == d._id RETURN 1)
              LET class_count = LENGTH(FOR e IN MetaboliteClassificationEdge FILTER e._from == d._id RETURN 1)
              LET ontology_count = LENGTH(FOR e IN HmdbMetaboliteOntologyEdge FILTER e._from == d._id RETURN 1)
              RETURN {
                id: d.id,
                prefix: d.prefix,
                support: clean_support,
                path_count: path_count,
                rhea_count: rhea_count,
                protein_count: protein_count,
                class_count: class_count,
                ontology_count: ontology_count,
                name_count: LENGTH(d.names || []),
                synonym_count: LENGTH(d.synonyms || []),
                chem_prop_count: LENGTH(d.chem_props || [])
              }
            """,
            {"ids": chunk},
        ))
    return rows


def load_ramp_source_ids_by_prefix(sqlite_path: str) -> Counter:
    from scripts.compare_rampish_metabolite_set_to_ramp_sqlite import normalize_ramp_source_id

    conn = sqlite3.connect(sqlite_path)
    counts = Counter()
    for id_type, source_id in conn.execute(
        """
        SELECT IDtype, sourceId
        FROM source
        WHERE rampId LIKE 'RAMP_C%'
        """
    ):
        normalized = normalize_ramp_source_id(id_type, source_id)
        if normalized:
            counts[prefix(normalized)] += 1
    conn.close()
    return counts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--sqlite", default=DEFAULT_SQLITE)
    parser.add_argument("--credentials", default=DEFAULT_CREDENTIALS)
    parser.add_argument("--database", default=DEFAULT_DATABASE)
    parser.add_argument("--stage-key", default=DEFAULT_STAGE_KEY)
    parser.add_argument("--sample-size", type=int, default=8)
    args = parser.parse_args()

    _row_count, _ramp_id_to_ids, id_to_ramp_ids, _evidence, _unmapped = load_ramp_sqlite(args.sqlite)
    ramp_ids = set(id_to_ramp_ids)
    ramp_prefix_rows = load_ramp_source_ids_by_prefix(args.sqlite)

    creds = DBCredentials.from_yaml(yaml.safe_load(open(args.credentials)))
    db = ArangoAdapter(creds, args.database)
    active_ids = load_stage_active_ids(db, args.stage_key)
    graph_only_ids = active_ids - ramp_ids
    profiles = load_graph_only_node_profiles(db, graph_only_ids)

    prefix_counts = Counter(row["prefix"] or prefix(row["id"]) for row in profiles)
    source_counts = Counter()
    source_prefix_counts = Counter()
    evidence_counts = Counter()
    evidence_prefix_counts = Counter()
    support_signature_counts = Counter()
    sample_by_prefix: dict[str, list[str]] = defaultdict(list)

    for row in profiles:
        row_prefix = row["prefix"] or prefix(row["id"])
        support = sorted({source_name(source) for source in row.get("support") or []})
        support_signature_counts[tuple(support)] += 1
        if not support:
            source_counts["<no support>"] += 1
            source_prefix_counts[("<no support>", row_prefix)] += 1
        for source in support:
            source_counts[source] += 1
            source_prefix_counts[(source, row_prefix)] += 1
        evidence_labels = []
        for label, field in [
            ("pathway", "path_count"),
            ("rhea_reaction", "rhea_count"),
            ("hmdb_protein", "protein_count"),
            ("classification", "class_count"),
            ("hmdb_ontology", "ontology_count"),
        ]:
            if row.get(field, 0) > 0:
                evidence_labels.append(label)
        if not evidence_labels:
            evidence_counts["<no checked evidence>"] += 1
            evidence_prefix_counts[("<no checked evidence>", row_prefix)] += 1
        for label in evidence_labels:
            evidence_counts[label] += 1
            evidence_prefix_counts[(label, row_prefix)] += 1
        if len(sample_by_prefix[row_prefix]) < args.sample_size:
            sample_by_prefix[row_prefix].append(row["id"])

    result = {
        "sqlite": args.sqlite,
        "database": args.database,
        "stage_key": args.stage_key,
        "ramp_distinct_source_ids": len(ramp_ids),
        "stage_active_ids": len(active_ids),
        "graph_only_active_ids": len(graph_only_ids),
        "graph_only_by_prefix": prefix_counts.most_common(40),
        "ramp_source_rows_by_prefix": ramp_prefix_rows.most_common(40),
        "graph_only_by_support_source": source_counts.most_common(40),
        "graph_only_by_support_source_and_prefix": [
            {"source": source, "prefix": pref, "count": count}
            for (source, pref), count in source_prefix_counts.most_common(80)
        ],
        "graph_only_by_checked_evidence": evidence_counts.most_common(),
        "graph_only_by_checked_evidence_and_prefix": [
            {"evidence": evidence, "prefix": pref, "count": count}
            for (evidence, pref), count in evidence_prefix_counts.most_common(80)
        ],
        "top_support_signatures": [
            {"support": list(signature), "count": count}
            for signature, count in support_signature_counts.most_common(40)
        ],
        "samples_by_prefix": dict(sorted(sample_by_prefix.items())),
    }
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
