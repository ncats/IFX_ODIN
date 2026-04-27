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
    progress_every = 1000

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
        print("TIN-X: building protein PMID maps")
        pmid_to_protein_count, pmid_to_proteins = self._build_protein_pmid_maps()
        print(
            f"TIN-X: built protein PMID maps for {len(pmid_to_proteins)} pmids "
            f"across {sum(pmid_to_protein_count.values())} protein-pmid mentions"
        )

        print("TIN-X: building disease PMID counts")
        pmid_to_disease_count = self._build_disease_pmid_counts()
        print(
            f"TIN-X: built disease PMID counts for {len(pmid_to_disease_count)} pmids "
            f"across {sum(pmid_to_disease_count.values())} disease-pmid mentions"
        )

        print("TIN-X: emitting protein novelty")
        yield from self._yield_protein_novelty(pmid_to_protein_count)
        print("TIN-X: emitting disease novelty")
        yield from self._yield_disease_novelty(pmid_to_disease_count)
        print("TIN-X: emitting protein-disease importance edges")

        batch: List[TINXImportanceEdge] = []
        pair_count = 0
        processed_diseases = 0
        for doid, disease_pmid_set in self._iter_disease_mentions():
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
                    print(
                        f"TIN-X: yielding {len(batch)} importance edges "
                        f"after {processed_diseases + 1} diseases and {pair_count} total pairs"
                    )
                    yield batch
                    batch = []
                if self.max_pairs is not None and pair_count >= self.max_pairs:
                    break
            processed_diseases += 1
            if processed_diseases % self.progress_every == 0:
                print(
                    f"TIN-X: processed {processed_diseases} diseases and emitted "
                    f"{pair_count} importance pairs"
                )
            if self.max_pairs is not None and pair_count >= self.max_pairs:
                break

        if batch:
            print(
                f"TIN-X: final importance batch {len(batch)} edges "
                f"after {processed_diseases} diseases and {pair_count} total pairs"
            )
            yield batch

    def _build_protein_pmid_maps(self) -> Tuple[Dict[str, int], Dict[str, List[str]]]:
        pmid_to_protein_count: Dict[str, int] = defaultdict(int)
        pmid_to_proteins: Dict[str, List[str]] = defaultdict(list)
        for loaded_proteins, (protein_id, pmids) in enumerate(self._iter_protein_mentions(), start=1):
            for pmid in pmids:
                pmid_to_protein_count[pmid] += 1
                pmid_to_proteins[pmid].append(protein_id)
            if loaded_proteins % self.progress_every == 0:
                print(
                    f"TIN-X: indexed {loaded_proteins} proteins into "
                    f"{len(pmid_to_proteins)} unique pmids"
                )
        return pmid_to_protein_count, pmid_to_proteins

    def _build_disease_pmid_counts(self) -> Dict[str, int]:
        pmid_to_disease_count: Dict[str, int] = defaultdict(int)
        for loaded_diseases, (_, pmids) in enumerate(self._iter_disease_mentions(), start=1):
            for pmid in pmids:
                pmid_to_disease_count[pmid] += 1
            if loaded_diseases % self.progress_every == 0:
                print(
                    f"TIN-X: indexed {loaded_diseases} diseases into "
                    f"{len(pmid_to_disease_count)} unique pmids"
                )
        return pmid_to_disease_count

    def _yield_protein_novelty(self, pmid_to_protein_count: Dict[str, int]) -> Generator[List[Protein], None, None]:
        batch: List[Protein] = []
        emitted = 0
        for protein_id, pmids in self._iter_protein_mentions():
            novelty = self._compute_novelty(pmids, pmid_to_protein_count)
            if novelty is None:
                continue
            batch.append(
                Protein(
                    id=EquivalentId(id=protein_id, type=Prefix.ENSEMBL).id_str(),
                    novelty=[novelty],
                )
            )
            emitted += 1
            if len(batch) >= self.batch_size:
                print(f"TIN-X: yielding {len(batch)} protein novelty nodes ({emitted} total)")
                yield batch
                batch = []
        if batch:
            print(f"TIN-X: final protein novelty batch {len(batch)} ({emitted} total)")
            yield batch

    def _yield_disease_novelty(self, pmid_to_disease_count: Dict[str, int]) -> Generator[List[Disease], None, None]:
        batch: List[Disease] = []
        emitted = 0
        for doid, pmids in self._iter_disease_mentions():
            novelty = self._compute_novelty(pmids, pmid_to_disease_count)
            if novelty is None:
                continue
            batch.append(
                Disease(
                    id=doid,
                    novelty=[novelty],
                )
            )
            emitted += 1
            if len(batch) >= self.batch_size:
                print(f"TIN-X: yielding {len(batch)} disease novelty nodes ({emitted} total)")
                yield batch
                batch = []
        if batch:
            print(f"TIN-X: final disease novelty batch {len(batch)} ({emitted} total)")
            yield batch

    def _iter_protein_mentions(self) -> Generator[Tuple[str, Set[str]], None, None]:
        seen_proteins: Set[str] = set()
        with open(self.protein_mentions_file_path, "r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                row = raw_line.rstrip("\n").split("\t", 1)
                if len(row) < 2:
                    continue
                protein_id = row[0].strip()
                if not protein_id.startswith("ENSP") or protein_id in seen_proteins:
                    continue
                pmids = self._parse_pmid_field(row[1])
                if not pmids:
                    continue
                seen_proteins.add(protein_id)
                yield protein_id, pmids
                if self.max_proteins is not None and len(seen_proteins) >= self.max_proteins:
                    break

    def _iter_disease_mentions(self) -> Generator[Tuple[str, Set[str]], None, None]:
        seen_diseases: Set[str] = set()
        with open(self.disease_mentions_file_path, "r", encoding="utf-8", errors="replace") as handle:
            for raw_line in handle:
                row = raw_line.rstrip("\n").split("\t", 1)
                if len(row) < 2:
                    continue
                doid = self._normalize_doid(row[0].strip())
                if doid is None or doid in seen_diseases:
                    continue
                pmids = self._parse_pmid_field(row[1])
                if not pmids:
                    continue
                seen_diseases.add(doid)
                yield doid, pmids
                if self.max_diseases is not None and len(seen_diseases) >= self.max_diseases:
                    break

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
