from __future__ import annotations

import csv
import re
from collections import defaultdict
from datetime import date
from typing import Dict, Generator, List, Optional, Set, Tuple

import pyarrow.parquet as pq

from src.constants import DataSourceName
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo, parse_to_date
from src.models.protein import Protein


_UNIPROT_ACCESSION_RE = re.compile(
    r"^(?:[OPQ][0-9][A-Z0-9]{3}[0-9]|[A-NR-Z][0-9][A-Z][A-Z0-9]{2}[0-9])(?:-\d+)?$"
)


class SureChEMBLPatentFamilyAdapter(InputAdapter):
    batch_size = 2_000

    def __init__(
        self,
        patents_file_path: str,
        biomedical_entities_file_path: str,
        biomedical_locations_file_path: str,
        version_file_path: str,
        min_publication_year: int = 1950,
        max_publication_year: Optional[int] = None,
        max_location_batches: Optional[int] = None,
    ):
        self.patents_file_path = patents_file_path
        self.biomedical_entities_file_path = biomedical_entities_file_path
        self.biomedical_locations_file_path = biomedical_locations_file_path
        self.version_file_path = version_file_path
        self.min_publication_year = min_publication_year
        self.max_publication_year = max_publication_year or date.today().year
        self.max_location_batches = max_location_batches
        self.version_info = self._load_version_info()
        self.entity_targets = self._load_entity_targets()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.SureChEMBL

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _load_version_info(self) -> DatasourceVersionInfo:
        with open(self.version_file_path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle, delimiter="\t")
            row = next(reader)
        version_date = row.get("version_date") or None
        download_date = row.get("download_date") or None
        return DatasourceVersionInfo(
            version=row.get("version") or None,
            version_date=parse_to_date(version_date),
            download_date=parse_to_date(download_date),
        )

    @staticmethod
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

    def _load_entity_targets(self) -> Dict[int, Tuple[str, str]]:
        entity_targets: Dict[int, Tuple[str, str]] = {}
        parquet_file = pq.ParquetFile(self.biomedical_entities_file_path)
        for batch in parquet_file.iter_batches(columns=["id", "entity_type_id", "resolved_form"]):
            ids = batch.column("id").to_pylist()
            entity_type_ids = batch.column("entity_type_id").to_pylist()
            resolved_forms = batch.column("resolved_form").to_pylist()
            for entity_id, entity_type_id, resolved_form in zip(ids, entity_type_ids, resolved_forms):
                if entity_type_id != 1:
                    continue
                normalized = self._normalize_resolved_form(resolved_form)
                if normalized is None:
                    continue
                entity_targets[int(entity_id)] = normalized
        return entity_targets

    def _iter_location_batches(self):
        parquet_file = pq.ParquetFile(self.biomedical_locations_file_path)
        for batch_idx, batch in enumerate(parquet_file.iter_batches(columns=["patent_id", "entity_id"]), start=1):
            if self.max_location_batches is not None and batch_idx > self.max_location_batches:
                break
            yield batch

    def _collect_relevant_patent_ids(self) -> Set[int]:
        patent_ids: Set[int] = set()
        for batch in self._iter_location_batches():
            batch_patent_ids = batch.column("patent_id").to_pylist()
            batch_entity_ids = batch.column("entity_id").to_pylist()
            for patent_id, entity_id in zip(batch_patent_ids, batch_entity_ids):
                if int(entity_id) in self.entity_targets:
                    patent_ids.add(int(patent_id))
        return patent_ids

    def _load_patent_metadata(self, relevant_patent_ids: Set[int]) -> Dict[int, Tuple[int, int]]:
        if not relevant_patent_ids:
            return {}
        parquet_file = pq.ParquetFile(self.patents_file_path)
        patent_metadata: Dict[int, Tuple[int, int]] = {}

        for batch in parquet_file.iter_batches(columns=["id", "publication_date", "family_id"]):
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
                if year is None or year < self.min_publication_year or year > self.max_publication_year:
                    continue
                patent_metadata[patent_id] = (family_id, year)

        return patent_metadata

    @staticmethod
    def _packed_family_key(year: int, family_id: int) -> int:
        return (int(year) << 32) | int(family_id)

    @staticmethod
    def _format_family_key(value: int) -> str:
        year = int(value >> 32)
        family_id = int(value & 0xFFFFFFFF)
        return f"{year}:{family_id}"

    def _flush_buffer(self, buffer_map: Dict[str, set[int]], source_map: Dict[str, set[str]]) -> List[Protein]:
        proteins: List[Protein] = []
        for protein_id in sorted(buffer_map.keys()):
            mention_keys = sorted(buffer_map[protein_id])
            proteins.append(
                Protein(
                    id=protein_id,
                    patent_family_mentions=[self._format_family_key(value) for value in mention_keys],
                    patent_identifier_sources=sorted(source_map.get(protein_id) or []),
                )
            )
        return proteins

    def _yield_chunked_proteins(
        self,
        buffer_map: Dict[str, set[int]],
        source_map: Dict[str, set[str]],
    ) -> Generator[List[Protein], None, None]:
        proteins = self._flush_buffer(buffer_map, source_map)
        for idx in range(0, len(proteins), self.batch_size):
            yield proteins[idx: idx + self.batch_size]

    def get_all(self) -> Generator[List[Protein], None, None]:
        relevant_patent_ids = self._collect_relevant_patent_ids()
        patent_metadata = self._load_patent_metadata(relevant_patent_ids)

        buffer_map: Dict[str, set[int]] = defaultdict(set)
        source_map: Dict[str, set[str]] = defaultdict(set)

        for batch in self._iter_location_batches():
            patent_ids = batch.column("patent_id").to_pylist()
            entity_ids = batch.column("entity_id").to_pylist()
            for patent_id, entity_id in zip(patent_ids, entity_ids):
                normalized = self.entity_targets.get(int(entity_id))
                if normalized is None:
                    continue
                target_id, source_type = normalized
                patent_id = int(patent_id)
                family_and_year = patent_metadata.get(patent_id)
                if family_and_year is None:
                    continue
                family_id, year = family_and_year

                buffer_map[target_id].add(self._packed_family_key(year, family_id))
                source_map[target_id].add(source_type)

        if buffer_map:
            yield from self._yield_chunked_proteins(buffer_map, source_map)
