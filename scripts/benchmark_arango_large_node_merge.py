#!/usr/bin/env python3
import argparse
import json
import math
import random
import statistics
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Sequence

import yaml
from arango.exceptions import DocumentInsertError, DocumentReplaceError

from src.shared.arango_adapter import ArangoAdapter
from src.shared.db_credentials import DBCredentials
from src.shared.record_merger import FieldConflictBehavior, RecordMerger


@dataclass
class CohortSpec:
    name: str
    min_publications: int | None
    max_publications: int | None
    sort_mode: str


@dataclass
class BenchmarkResult:
    cohort: str
    strategy: str
    batch_size: int
    docs: int
    elapsed_seconds: float
    docs_per_second: float
    read_payload_bytes: int
    write_payload_bytes: int
    success: bool = True
    error: str | None = None

    def to_dict(self) -> dict:
        return {
            "cohort": self.cohort,
            "strategy": self.strategy,
            "batch_size": self.batch_size,
            "docs": self.docs,
            "elapsed_seconds": round(self.elapsed_seconds, 3),
            "docs_per_second": round(self.docs_per_second, 1),
            "read_payload_mb": round(self.read_payload_bytes / (1024 * 1024), 3),
            "write_payload_mb": round(self.write_payload_bytes / (1024 * 1024), 3),
            "success": self.success,
            "error": self.error,
        }


def load_credentials(path: str) -> DBCredentials:
    with open(path) as handle:
        return DBCredentials.from_yaml(yaml.safe_load(handle))


def safe_json_size_bytes(docs: Sequence[dict]) -> int:
    return len(json.dumps(list(docs), sort_keys=True, default=str).encode("utf-8"))


def batched(items: Sequence, batch_size: int) -> List[Sequence]:
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]


def import_docs_with_backoff(collection, docs: Sequence[dict]) -> None:
    if not docs:
        return
    try:
        collection.import_bulk(list(docs), on_duplicate="replace", details=False)
    except DocumentInsertError:
        if len(docs) == 1:
            raise
        midpoint = len(docs) // 2
        import_docs_with_backoff(collection, docs[:midpoint])
        import_docs_with_backoff(collection, docs[midpoint:])


def replace_docs_with_backoff(collection, docs: Sequence[dict]) -> None:
    if not docs:
        return
    try:
        collection.replace_many(list(docs), check_rev=False)
    except (DocumentInsertError, DocumentReplaceError):
        if len(docs) == 1:
            raise
        midpoint = len(docs) // 2
        replace_docs_with_backoff(collection, docs[:midpoint])
        replace_docs_with_backoff(collection, docs[midpoint:])


def build_cohort_specs(small_max: int, large_min: int) -> dict[str, CohortSpec]:
    return {
        "small": CohortSpec("small", None, small_max, "asc"),
        "medium": CohortSpec("medium", small_max, large_min, "asc"),
        "large": CohortSpec("large", large_min, None, "desc"),
    }


def cohort_filter_clause(spec: CohortSpec) -> str:
    clauses = []
    if spec.min_publications is not None:
        clauses.append("pub_count >= @min_publications")
    if spec.max_publications is not None:
        clauses.append("pub_count < @max_publications")
    return " AND ".join(clauses) if clauses else "true"


def cohort_bind_vars(spec: CohortSpec, **extra) -> dict:
    bind_vars = dict(extra)
    if spec.min_publications is not None:
        bind_vars["min_publications"] = spec.min_publications
    if spec.max_publications is not None:
        bind_vars["max_publications"] = spec.max_publications
    return bind_vars


def select_cohort_key_info(adapter: ArangoAdapter, spec: CohortSpec, limit: int) -> List[dict]:
    query = f"""
    FOR doc IN `Protein`
      LET pub_count = LENGTH(doc.publications || [])
      FILTER {cohort_filter_clause(spec)}
      SORT {"pub_count DESC," if spec.sort_mode == "desc" else "pub_count ASC,"} doc._key
      LIMIT @limit
      RETURN {{_key: doc._key, pub_count: pub_count}}
    """
    return adapter.runQuery(query, cohort_bind_vars(spec, limit=limit))


def count_cohort(adapter: ArangoAdapter, spec: CohortSpec) -> int:
    query = f"""
    FOR doc IN `Protein`
      LET pub_count = LENGTH(doc.publications || [])
      FILTER {cohort_filter_clause(spec)}
      COLLECT WITH COUNT INTO n
      RETURN n
    """
    rows = adapter.runQuery(query, cohort_bind_vars(spec))
    return rows[0] if rows else 0


def proportional_allocation(total: int, counts: dict[str, int]) -> dict[str, int]:
    available = {name: count for name, count in counts.items() if count > 0}
    if not available or total <= 0:
        return {name: 0 for name in counts}

    total_available = sum(available.values())
    raw = {name: (total * count) / total_available for name, count in available.items()}
    allocation = {name: min(counts[name], int(math.floor(value))) for name, value in raw.items()}
    assigned = sum(allocation.values())
    remainders = sorted(
        ((raw[name] - allocation[name], name) for name in available),
        reverse=True,
    )
    for _, name in remainders:
        if assigned >= total:
            break
        if allocation[name] < counts[name]:
            allocation[name] += 1
            assigned += 1

    for name in counts:
        allocation.setdefault(name, 0)
    return allocation


def build_selection_plan(
    source_adapter: ArangoAdapter,
    specs: dict[str, CohortSpec],
    per_cohort_size: int,
    mixed_size: int,
) -> tuple[dict[str, list[dict]], dict[str, int], dict[str, int]]:
    source_counts = {name: count_cohort(source_adapter, spec) for name, spec in specs.items()}
    mixed_allocation = proportional_allocation(mixed_size, source_counts)
    requested_sizes = {
        name: min(source_counts[name], max(per_cohort_size, mixed_allocation.get(name, 0)))
        for name in specs
    }
    selected = {
        name: select_cohort_key_info(source_adapter, specs[name], requested_sizes[name])
        for name in specs
    }
    return selected, source_counts, mixed_allocation


def clone_selected_docs(creds: DBCredentials, source_db: str, target_db: str, selected: dict[str, list[dict]]) -> dict:
    target_adapter = ArangoAdapter(creds, target_db)
    sys_db = target_adapter.client.db("_system", username=creds.user, password=creds.password)
    if sys_db.has_database(target_db):
        sys_db.delete_database(target_db)
    sys_db.create_database(target_db)

    source_db_handle = ArangoAdapter(creds, source_db).get_db()
    source_collection = source_db_handle.collection("Protein")
    target_collection = target_adapter.get_db().create_collection("Protein")

    unique_keys = []
    seen = set()
    for cohort_rows in selected.values():
        for row in cohort_rows:
            key = row["_key"]
            if key in seen:
                continue
            seen.add(key)
            unique_keys.append(key)

    copied = 0
    started = time.perf_counter()
    print(f"clone_selected_keys count={len(unique_keys)}", flush=True)
    for key_batch in batched(unique_keys, 10):
        docs = [doc for doc in source_collection.get_many(list(key_batch)) if doc is not None]
        import_docs_with_backoff(target_collection, docs)
        copied += len(docs)
        print(f"clone_progress copied={copied}", flush=True)

    return {
        "selected_keys": len(unique_keys),
        "copied_count": copied,
        "elapsed_seconds": time.perf_counter() - started,
    }


def fetch_docs_for_keys(creds: DBCredentials, db_name: str, key_info: Sequence[dict]) -> List[dict]:
    collection = ArangoAdapter(creds, db_name).get_db().collection("Protein")
    docs = [doc for doc in collection.get_many([row["_key"] for row in key_info]) if doc is not None]
    pub_count_by_key = {row["_key"]: row["pub_count"] for row in key_info}
    for doc in docs:
        doc["_pub_count"] = pub_count_by_key.get(doc["_key"], 0)
    return docs


def summarize_samples(samples: Sequence[dict]) -> dict:
    pub_counts = [doc.get("_pub_count", len(doc.get("publications") or [])) for doc in samples]
    sizes = [safe_json_size_bytes([doc]) for doc in samples]
    history_counts = [len(doc.get("pm_score_by_year") or []) for doc in samples]
    return {
        "samples": len(samples),
        "publication_count_min": min(pub_counts) if pub_counts else 0,
        "publication_count_median": int(statistics.median(pub_counts)) if pub_counts else 0,
        "publication_count_max": max(pub_counts) if pub_counts else 0,
        "history_count_median": int(statistics.median(history_counts)) if history_counts else 0,
        "doc_size_kb_median": round(statistics.median(sizes) / 1024, 1) if sizes else 0,
        "doc_size_kb_max": round(max(sizes) / 1024, 1) if sizes else 0,
    }


def build_patch_docs(existing_docs: Sequence[dict], seed: int) -> List[dict]:
    rng = random.Random(seed)
    patches = []
    for idx, doc in enumerate(existing_docs):
        baseline_score = doc.get("pm_score") or []
        old_score = float(baseline_score[0]) if baseline_score else 0.0
        delta = round(0.001 * ((idx % 17) + 1), 6)
        new_score = round(old_score + delta, 6)
        history = list(doc.get("pm_score_by_year") or [])
        history.append({"year": 2027 + (idx % 3), "score": delta})
        history = sorted(history, key=lambda row: (row.get("year") or 0, row.get("score") or 0))
        patches.append({
            "_key": doc["_key"],
            "id": doc["id"],
            "_pub_count": doc.get("_pub_count", len(doc.get("publications") or [])),
            "provenance": f"benchmark\tbulk_patch\tseed={seed}",
            "entity_resolution": f"Benchmark\tbulk_patch\t{doc['id']}",
            "pm_score": [new_score],
            "pm_score_by_year": history,
            "novelty": [round(rng.random(), 6)],
        })
    return patches


def current_strategy(adapter: ArangoAdapter, collection, patch_docs: Sequence[dict], heavy_publication_threshold: int) -> tuple[int, int]:
    merger = RecordMerger(field_conflict_behavior=FieldConflictBehavior.KeepLast)
    existing_docs = collection.get_many([doc["_key"] for doc in patch_docs])
    existing_record_map = {doc["id"]: doc for doc in existing_docs}
    merged_docs = merger.merge_records([dict(doc) for doc in patch_docs], existing_record_map, nodes_or_edges="nodes")
    payload = [{**doc, "_key": doc["_key"]} for doc in merged_docs]
    collection.insert_many(payload, overwrite=True)
    return safe_json_size_bytes(existing_docs), safe_json_size_bytes(payload)


def update_many_strategy(adapter: ArangoAdapter, collection, patch_docs: Sequence[dict], heavy_publication_threshold: int) -> tuple[int, int]:
    payload = [{
        "_key": doc["_key"],
        "pm_score": doc["pm_score"],
        "pm_score_by_year": doc["pm_score_by_year"],
        "novelty": doc["novelty"],
    } for doc in patch_docs]
    collection.update_many(payload, merge=True, keep_none=False, check_rev=False)
    return 0, safe_json_size_bytes(payload)


def hybrid_strategy(adapter: ArangoAdapter, collection, patch_docs: Sequence[dict], heavy_publication_threshold: int) -> tuple[int, int]:
    light_docs = [doc for doc in patch_docs if doc.get("_pub_count", 0) < heavy_publication_threshold]
    heavy_docs = [doc for doc in patch_docs if doc.get("_pub_count", 0) >= heavy_publication_threshold]
    read_bytes = 0
    write_bytes = 0
    if light_docs:
        batch_read_bytes, batch_write_bytes = current_strategy(adapter, collection, light_docs, heavy_publication_threshold)
        read_bytes += batch_read_bytes
        write_bytes += batch_write_bytes
    if heavy_docs:
        batch_read_bytes, batch_write_bytes = update_many_strategy(adapter, collection, heavy_docs, heavy_publication_threshold)
        read_bytes += batch_read_bytes
        write_bytes += batch_write_bytes
    return read_bytes, write_bytes


STRATEGIES: Dict[str, Callable] = {
    "current": current_strategy,
    "update_many": update_many_strategy,
    "hybrid": hybrid_strategy,
}


def run_strategy(
    creds: DBCredentials,
    db_name: str,
    cohort_name: str,
    strategy_name: str,
    patch_docs: Sequence[dict],
    batch_size: int,
    heavy_publication_threshold: int,
) -> BenchmarkResult:
    adapter = ArangoAdapter(creds, db_name)
    collection = adapter.get_db().collection("Protein")
    strategy = STRATEGIES[strategy_name]
    read_bytes = 0
    write_bytes = 0
    started = time.perf_counter()
    try:
        for batch in batched(list(patch_docs), batch_size):
            batch_read_bytes, batch_write_bytes = strategy(adapter, collection, batch, heavy_publication_threshold)
            read_bytes += batch_read_bytes
            write_bytes += batch_write_bytes
    except Exception as exc:
        elapsed = time.perf_counter() - started
        return BenchmarkResult(
            cohort=cohort_name,
            strategy=strategy_name,
            batch_size=batch_size,
            docs=len(patch_docs),
            elapsed_seconds=elapsed,
            docs_per_second=0.0,
            read_payload_bytes=read_bytes,
            write_payload_bytes=write_bytes,
            success=False,
            error=f"{type(exc).__name__}: {exc}",
        )
    elapsed = time.perf_counter() - started
    return BenchmarkResult(
        cohort=cohort_name,
        strategy=strategy_name,
        batch_size=batch_size,
        docs=len(patch_docs),
        elapsed_seconds=elapsed,
        docs_per_second=len(patch_docs) / elapsed if elapsed else math.inf,
        read_payload_bytes=read_bytes,
        write_payload_bytes=write_bytes,
    )


def restore_baseline(creds: DBCredentials, db_name: str, baseline_docs: Sequence[dict]) -> None:
    collection = ArangoAdapter(creds, db_name).get_db().collection("Protein")
    cleaned_docs = [
        {k: v for k, v in doc.items() if k != "_pub_count"}
        for doc in baseline_docs
    ]
    for batch in batched(cleaned_docs, 25):
        replace_docs_with_backoff(collection, batch)


def take_mixed_sample(selected: dict[str, list[dict]], mixed_allocation: dict[str, int]) -> List[dict]:
    mixed = []
    for cohort_name in ("small", "medium", "large"):
        mixed.extend(selected[cohort_name][:mixed_allocation.get(cohort_name, 0)])
    return mixed


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Arango large-node merge strategies across representative Protein cohorts.")
    parser.add_argument("--credentials", default="src/use_cases/secrets/ifxdev_arangodb.yaml")
    parser.add_argument("--source-db", default="pharos")
    parser.add_argument("--target-db", default="test_pharos_merge_bench")
    parser.add_argument("--per-cohort-size", type=int, default=100)
    parser.add_argument("--mixed-size", type=int, default=300)
    parser.add_argument("--small-max-publications", type=int, default=10)
    parser.add_argument("--large-min-publications", type=int, default=1000)
    parser.add_argument("--heavy-publication-threshold", type=int, default=1000)
    parser.add_argument("--batch-sizes", nargs="+", type=int, default=[300, 1000])
    parser.add_argument("--strategies", nargs="+", choices=sorted(STRATEGIES.keys()), default=["current", "update_many", "hybrid"])
    parser.add_argument("--seed", type=int, default=1337)
    args = parser.parse_args()

    creds = load_credentials(args.credentials)
    source_adapter = ArangoAdapter(creds, args.source_db)
    specs = build_cohort_specs(args.small_max_publications, args.large_min_publications)

    selected, source_counts, mixed_allocation = build_selection_plan(
        source_adapter=source_adapter,
        specs=specs,
        per_cohort_size=args.per_cohort_size,
        mixed_size=args.mixed_size,
    )

    print("cohort_source_counts", json.dumps(source_counts, indent=2))
    print("mixed_allocation", json.dumps(mixed_allocation, indent=2))
    print("cohort_selected_counts", json.dumps({name: len(rows) for name, rows in selected.items()}, indent=2))

    clone_result = clone_selected_docs(creds, args.source_db, args.target_db, selected)
    print("clone_result", json.dumps({
        "source_db": args.source_db,
        "target_db": args.target_db,
        **clone_result,
    }, indent=2))

    cohorts = dict(selected)
    cohorts["mixed"] = take_mixed_sample(selected, mixed_allocation)

    cohort_docs = {name: fetch_docs_for_keys(creds, args.target_db, rows) for name, rows in cohorts.items()}
    cohort_summaries = {name: summarize_samples(docs) for name, docs in cohort_docs.items()}
    print("cohort_summaries", json.dumps(cohort_summaries, indent=2))

    results = []
    for cohort_name, docs in cohort_docs.items():
        patch_docs = build_patch_docs(docs, seed=args.seed)
        baseline_docs = fetch_docs_for_keys(creds, args.target_db, cohorts[cohort_name])
        for strategy_name in args.strategies:
            for batch_size in args.batch_sizes:
                restore_baseline(creds, args.target_db, baseline_docs)
                result = run_strategy(
                    creds=creds,
                    db_name=args.target_db,
                    cohort_name=cohort_name,
                    strategy_name=strategy_name,
                    patch_docs=patch_docs,
                    batch_size=batch_size,
                    heavy_publication_threshold=args.heavy_publication_threshold,
                )
                results.append(result)
                print("benchmark_result", json.dumps(result.to_dict(), indent=2))

    print("benchmark_summary", json.dumps([result.to_dict() for result in results], indent=2))


if __name__ == "__main__":
    main()
