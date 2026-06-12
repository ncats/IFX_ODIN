from collections import defaultdict
from datetime import date
from pathlib import Path
import re
from typing import Dict, List, Optional, Set, Tuple

import pyarrow as pa
import pyarrow.parquet as pq

from src.registry.fetchers import ArtifactFile, DerivedArtifact, DerivedArtifactBuilder, ResolvedDependency


_UNIPROT_ACCESSION_RE = re.compile(
    r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9])(?:-\d+)?$"
)
_PROGRESS_BATCH_INTERVAL = 1000


def _require_dependency(
    dependencies: List[ResolvedDependency],
    *,
    source: str,
    dataset: str,
) -> ResolvedDependency:
    matches = [
        dependency
        for dependency in dependencies
        if dependency.source == source and dependency.dataset == dataset
    ]
    if not matches:
        raise LookupError(f"Missing derived artifact dependency {source}/{dataset}")
    if len(matches) > 1:
        raise ValueError(f"Multiple derived artifact dependencies match {source}/{dataset}")
    return matches[0]


class SurechemblPatentFamilyMentionsBuilder(DerivedArtifactBuilder):
    source = "surechembl"
    dataset = "patent_family_mentions"

    def build(
        self,
        *,
        config: dict,
        dependencies: List[ResolvedDependency],
        dest: Path,
        version: str,
    ) -> DerivedArtifact:
        dependency = _require_dependency(
            dependencies,
            source="surechembl",
            dataset="patent_discovery",
        )
        output_path = dest / (config.get("output") or {}).get("file_name", "protein_patent_family_mentions.parquet")
        stats = build_patent_family_mentions(
            patents_file=dependency.file("patents.parquet"),
            biomedical_entities_file=dependency.file("biomedical_entities.parquet"),
            biomedical_locations_file=dependency.file("biomedical_locations.parquet"),
            output_path=output_path,
            min_publication_year=int(config.get("min_publication_year", 1950)),
            max_publication_year=config.get("max_publication_year"),
            max_location_batches=config.get("max_location_batches"),
        )
        return DerivedArtifact(
            source=self.source,
            dataset=self.dataset,
            version=version,
            version_date=dependency.version,
            derived_from=[
                {
                    "snapshot_id": dependency.snapshot_id,
                    "manifest_uri": dependency.manifest_uri,
                }
            ],
            transform=config.get("transform") or {
                "name": "surechembl_patent_family_mentions",
                "version": 1,
            },
            files=[
                ArtifactFile(
                    output_path,
                    "application/vnd.apache.parquet",
                )
            ],
            stats=stats,
        )


def build_patent_family_mentions(
    *,
    patents_file: Path,
    biomedical_entities_file: Path,
    biomedical_locations_file: Path,
    output_path: Path,
    min_publication_year: int = 1950,
    max_publication_year: Optional[int] = None,
    max_location_batches: Optional[int] = None,
) -> Dict:
    max_year = max_publication_year or date.today().year
    print(f"Building SureChEMBL patent family mentions -> {output_path}", flush=True)
    print(f"Loading SureChEMBL protein entity targets from {biomedical_entities_file}", flush=True)
    entity_targets = _load_entity_targets(biomedical_entities_file)
    print(f"Loaded {len(entity_targets):,} protein entity targets", flush=True)
    print(f"Scanning SureChEMBL locations for relevant patent ids from {biomedical_locations_file}", flush=True)
    relevant_patent_ids = _collect_relevant_patent_ids(
        biomedical_locations_file,
        entity_targets,
        max_location_batches=max_location_batches,
    )
    print(f"Collected {len(relevant_patent_ids):,} relevant patent ids", flush=True)
    print(f"Loading SureChEMBL patent metadata from {patents_file}", flush=True)
    patent_metadata = _load_patent_metadata(
        patents_file,
        relevant_patent_ids,
        min_publication_year=min_publication_year,
        max_publication_year=max_year,
    )
    print(f"Loaded metadata for {len(patent_metadata):,} relevant patent ids", flush=True)
    mention_map: Dict[str, set[int]] = defaultdict(set)
    source_map: Dict[str, set[str]] = defaultdict(set)

    print("Building target-to-patent-family mention sets", flush=True)
    for batch_idx, batch in _iter_location_batches(
        biomedical_locations_file,
        max_location_batches=max_location_batches,
        with_index=True,
    ):
        patent_ids = batch.column("patent_id").to_pylist()
        entity_ids = batch.column("entity_id").to_pylist()
        for patent_id, entity_id in zip(patent_ids, entity_ids):
            normalized = entity_targets.get(int(entity_id))
            if normalized is None:
                continue
            target_id, source_type = normalized
            family_and_year = patent_metadata.get(int(patent_id))
            if family_and_year is None:
                continue
            family_id, year = family_and_year
            mention_map[target_id].add(_packed_family_key(year, family_id))
            source_map[target_id].add(source_type)
        if batch_idx % _PROGRESS_BATCH_INTERVAL == 0:
            print(
                f"Processed {batch_idx:,} location batches; "
                f"{len(mention_map):,} targets with patent-family mentions",
                flush=True,
            )

    print(f"Preparing {len(mention_map):,} output rows", flush=True)
    rows = []
    for protein_id in sorted(mention_map):
        rows.append({
            "protein_id": protein_id,
            "patent_family_mentions": [
                _format_family_key(value)
                for value in sorted(mention_map[protein_id])
            ],
            "patent_identifier_sources": sorted(source_map.get(protein_id) or []),
        })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Writing SureChEMBL derived parquet to {output_path}", flush=True)
    table = pa.Table.from_pylist(
        rows,
        schema=pa.schema([
            ("protein_id", pa.string()),
            ("patent_family_mentions", pa.list_(pa.string())),
            ("patent_identifier_sources", pa.list_(pa.string())),
        ]),
    )
    pq.write_table(table, output_path)
    print(f"Wrote {len(rows):,} SureChEMBL patent-family rows", flush=True)

    return {
        "row_count": len(rows),
        "entity_target_count": len(entity_targets),
        "relevant_patent_count": len(relevant_patent_ids),
        "patent_metadata_count": len(patent_metadata),
        "min_publication_year": min_publication_year,
        "max_publication_year": max_year,
    }


def _normalize_resolved_form(resolved_form: Optional[str]) -> Optional[Tuple[str, str]]:
    if resolved_form is None:
        return None
    normalized = resolved_form.strip()
    if not normalized:
        return None
    if normalized.startswith("HGNC:"):
        return normalized, "HGNC"
    if _UNIPROT_ACCESSION_RE.match(normalized):
        return f"UniProtKB:{normalized}", "UniProtKB"
    return None


def _load_entity_targets(path: Path) -> Dict[int, Tuple[str, str]]:
    entity_targets: Dict[int, Tuple[str, str]] = {}
    parquet_file = pq.ParquetFile(path)
    for batch_idx, batch in enumerate(
        parquet_file.iter_batches(columns=["id", "entity_type_id", "resolved_form"]),
        start=1,
    ):
        ids = batch.column("id").to_pylist()
        entity_type_ids = batch.column("entity_type_id").to_pylist()
        resolved_forms = batch.column("resolved_form").to_pylist()
        for entity_id, entity_type_id, resolved_form in zip(ids, entity_type_ids, resolved_forms):
            if entity_type_id != 1:
                continue
            normalized = _normalize_resolved_form(resolved_form)
            if normalized is None:
                continue
            entity_targets[int(entity_id)] = normalized
        if batch_idx % _PROGRESS_BATCH_INTERVAL == 0:
            print(
                f"Processed {batch_idx:,} entity batches; "
                f"{len(entity_targets):,} protein entity targets",
                flush=True,
            )
    return entity_targets


def _iter_location_batches(
    path: Path,
    *,
    max_location_batches: Optional[int] = None,
    with_index: bool = False,
):
    parquet_file = pq.ParquetFile(path)
    for batch_idx, batch in enumerate(parquet_file.iter_batches(columns=["patent_id", "entity_id"]), start=1):
        if max_location_batches is not None and batch_idx > max_location_batches:
            break
        yield (batch_idx, batch) if with_index else batch


def _collect_relevant_patent_ids(
    path: Path,
    entity_targets: Dict[int, Tuple[str, str]],
    *,
    max_location_batches: Optional[int] = None,
) -> Set[int]:
    patent_ids: Set[int] = set()
    for batch_idx, batch in _iter_location_batches(
        path,
        max_location_batches=max_location_batches,
        with_index=True,
    ):
        batch_patent_ids = batch.column("patent_id").to_pylist()
        batch_entity_ids = batch.column("entity_id").to_pylist()
        for patent_id, entity_id in zip(batch_patent_ids, batch_entity_ids):
            if int(entity_id) in entity_targets:
                patent_ids.add(int(patent_id))
        if batch_idx % _PROGRESS_BATCH_INTERVAL == 0:
            print(
                f"Scanned {batch_idx:,} location batches; "
                f"{len(patent_ids):,} relevant patent ids",
                flush=True,
            )
    return patent_ids


def _load_patent_metadata(
    path: Path,
    relevant_patent_ids: Set[int],
    *,
    min_publication_year: int,
    max_publication_year: int,
) -> Dict[int, Tuple[int, int]]:
    if not relevant_patent_ids:
        return {}
    parquet_file = pq.ParquetFile(path)
    patent_metadata: Dict[int, Tuple[int, int]] = {}
    for batch_idx, batch in enumerate(
        parquet_file.iter_batches(columns=["id", "publication_date", "family_id"]),
        start=1,
    ):
        patent_ids = batch.column("id").to_pylist()
        publication_dates = batch.column("publication_date").to_pylist()
        family_ids = batch.column("family_id").to_pylist()
        for patent_id, publication_date, family_id in zip(patent_ids, publication_dates, family_ids):
            patent_id = int(patent_id)
            if patent_id not in relevant_patent_ids:
                continue
            family_id = int(family_id) if family_id is not None else -1
            if family_id <= 0:
                continue
            year = publication_date.year if publication_date is not None else None
            if year is None or year < min_publication_year or year > max_publication_year:
                continue
            patent_metadata[patent_id] = (family_id, year)
        if batch_idx % _PROGRESS_BATCH_INTERVAL == 0:
            print(
                f"Processed {batch_idx:,} patent metadata batches; "
                f"{len(patent_metadata):,} relevant patent metadata rows",
                flush=True,
            )
    return patent_metadata


def _packed_family_key(year: int, family_id: int) -> int:
    return (int(year) << 32) | int(family_id)


def _format_family_key(value: int) -> str:
    year = int(value >> 32)
    family_id = int(value & 0xFFFFFFFF)
    return f"{year}:{family_id}"
