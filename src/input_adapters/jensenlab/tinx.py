import csv
import os
import re
from collections import defaultdict
from datetime import date, datetime
from typing import Dict, Generator, Iterable, List, Optional, Set, Tuple, Union

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.disease import Disease, DiseaseAssociationDetail, TINXImportanceEdge
from src.models.node import EquivalentId, Node, Relationship
from src.models.protein import Protein


class TINXAdapter(InputAdapter):
    def __init__(
        self,
        protein_mentions_file_path: str,
        disease_mentions_file_path: str,
        version_file_path: Optional[str] = None,
        max_proteins: Optional[int] = None,
        max_diseases: Optional[int] = None,
        max_pairs: Optional[int] = None,
    ):
        self.protein_mentions_file_path = protein_mentions_file_path
        self.disease_mentions_file_path = disease_mentions_file_path
        self.version_file_path = version_file_path
        self.max_proteins = max_proteins
        self.max_diseases = max_diseases
        self.max_pairs = max_pairs

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.TINX

    def get_version(self) -> DatasourceVersionInfo:
        version = None
        version_date = None
        if self.version_file_path and os.path.exists(self.version_file_path):
            with open(self.version_file_path, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                row = next(reader, None)
                if row:
                    version = row.get("version") or None
                    raw_version_date = row.get("version_date") or None
                    if raw_version_date:
                        try:
                            version_date = date.fromisoformat(raw_version_date)
                        except ValueError:
                            version_date = None

        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=self._download_date(),
        )

    def get_all(self) -> Generator[List[Union[Node, Relationship]], None, None]:
        protein_pmids, pmid_to_protein_count, pmid_to_proteins = self._load_protein_mentions()
        disease_pmids, pmid_to_disease_count = self._load_disease_mentions()

        proteins = [
            Protein(
                id=EquivalentId(id=protein_id, type=Prefix.ENSEMBL).id_str(),
                novelty=[novelty],
            )
            for protein_id, pmids in protein_pmids.items()
            for novelty in [self._compute_novelty(pmids, pmid_to_protein_count)]
            if novelty is not None
        ]

        diseases = [
            Disease(
                id=doid,
                novelty=[novelty],
            )
            for doid, pmids in disease_pmids.items()
            for novelty in [self._compute_novelty(pmids, pmid_to_disease_count)]
            if novelty is not None
        ]

        yield proteins
        yield diseases
        batch: List[TINXImportanceEdge] = []
        pair_count = 0
        for doid, disease_pmid_set in disease_pmids.items():
            protein_scores: Dict[str, float] = defaultdict(float)
            for pmid in disease_pmid_set:
                proteins_for_pmid = pmid_to_proteins.get(pmid)
                protein_count = pmid_to_protein_count.get(pmid, 0)
                disease_count = pmid_to_disease_count.get(pmid, 0)
                if not proteins_for_pmid or protein_count <= 0 or disease_count <= 0:
                    continue
                increment = 1.0 / (protein_count * disease_count)
                for protein_id in proteins_for_pmid:
                    protein_scores[protein_id] += increment

            for protein_id, importance in protein_scores.items():
                if importance <= 0:
                    continue
                batch.append(
                    TINXImportanceEdge(
                        start_node=Protein(id=EquivalentId(id=protein_id, type=Prefix.ENSEMBL).id_str()),
                        end_node=Disease(id=doid),
                        details=[
                            DiseaseAssociationDetail(
                                source="TIN-X",
                                source_id=doid,
                                doid=doid,
                                importance=[importance],
                            )
                        ],
                    )
                )
                pair_count += 1
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
                if self.max_pairs is not None and pair_count >= self.max_pairs:
                    break
            if self.max_pairs is not None and pair_count >= self.max_pairs:
                break

        if batch:
            yield batch

    def _load_protein_mentions(self) -> Tuple[Dict[str, Set[str]], Dict[str, int], Dict[str, Set[str]]]:
        protein_pmids: Dict[str, Set[str]] = {}
        pmid_to_protein_count: Dict[str, int] = defaultdict(int)
        pmid_to_proteins: Dict[str, Set[str]] = defaultdict(set)
        loaded_proteins = 0
        with open(self.protein_mentions_file_path, "r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                row = raw_line.rstrip("\n").split("\t", 1)
                if len(row) < 2:
                    continue
                protein_id = row[0].strip()
                if not protein_id.startswith("ENSP"):
                    continue
                pmids = self._parse_pmid_field(row[1])
                if not pmids:
                    continue
                if protein_id in protein_pmids:
                    continue
                protein_pmids[protein_id] = pmids
                for pmid in pmids:
                    pmid_to_protein_count[pmid] += 1
                    pmid_to_proteins[pmid].add(protein_id)
                loaded_proteins += 1
                if self.max_proteins is not None and loaded_proteins >= self.max_proteins:
                    break
        return protein_pmids, pmid_to_protein_count, pmid_to_proteins

    def _load_disease_mentions(self) -> Tuple[Dict[str, Set[str]], Dict[str, int]]:
        disease_pmids: Dict[str, Set[str]] = {}
        pmid_to_disease_count: Dict[str, int] = defaultdict(int)
        loaded_diseases = 0

        with open(self.disease_mentions_file_path, "r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                row = raw_line.rstrip("\n").split("\t", 1)
                if len(row) < 2:
                    continue
                doid = self._normalize_doid(row[0].strip())
                if doid is None:
                    continue
                pmids = self._parse_pmid_field(row[1])
                if not pmids:
                    continue
                if doid in disease_pmids:
                    continue
                disease_pmids[doid] = pmids
                for pmid in pmids:
                    pmid_to_disease_count[pmid] += 1
                loaded_diseases += 1
                if self.max_diseases is not None and loaded_diseases >= self.max_diseases:
                    break
        return disease_pmids, pmid_to_disease_count

    def _download_date(self) -> Optional[date]:
        timestamps = []
        for file_path in (
            self.protein_mentions_file_path,
            self.disease_mentions_file_path,
        ):
            if os.path.exists(file_path):
                timestamps.append(os.path.getmtime(file_path))
        if not timestamps:
            return None
        return datetime.fromtimestamp(max(timestamps)).date()

    @staticmethod
    def _parse_pmid_field(raw_pmids: str) -> Set[str]:
        return {pmid for pmid in raw_pmids.strip().split() if pmid}

    @staticmethod
    def _compute_novelty(entity_pmids: Iterable[str], pmid_counts: Dict[str, int]) -> Optional[float]:
        denominator = 0.0
        for pmid in entity_pmids:
            count = pmid_counts.get(pmid, 0)
            if count > 0:
                denominator += 1.0 / count
        if denominator == 0.0:
            return None
        return 1.0 / denominator

    @staticmethod
    def _normalize_doid(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        if value.startswith("DOID:"):
            return value
        match = re.search(r"/obo/DOID_(\d+)$", value)
        if match:
            return f"DOID:{match.group(1)}"
        return None
