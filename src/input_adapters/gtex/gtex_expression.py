import csv
import gzip
import math
import os
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime
from statistics import median
from typing import Dict, Generator, List, Optional, Union

from src.constants import DataSourceName, Prefix
from src.models.gene import Gene
from src.models.node import EquivalentId
from src.input_adapters.shared.expression_adapter_base import ExpressionAdapterBase
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.expression import ExpressionDetail, GeneTissueExpressionEdge
from src.models.tissue import Tissue


@dataclass
class SubjectInfo:
    sex: str
    dthhrdy: Optional[float]


@dataclass
class SampleInfo:
    sample_id: str
    subject_id: str
    tissue: str
    uberon_id: Optional[str]
    smatsscr: Optional[float]
    sex: str


class GTExExpressionAdapter(InputAdapter):
    def __init__(
        self,
        matrix_file_path: str,
        sample_attributes_file_path: str,
        subject_phenotypes_file_path: str,
        version_file_path: str,
        max_genes: Optional[int] = None,
        max_samples: Optional[int] = None,
    ):
        self.matrix_file_path = matrix_file_path
        self.sample_attributes_file_path = sample_attributes_file_path
        self.subject_phenotypes_file_path = subject_phenotypes_file_path
        self.version_file_path = version_file_path
        self.max_genes = max_genes
        self.max_samples = max_samples
        self.version_info = self._load_version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.GTEx

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Union[Tissue, Gene, GeneTissueExpressionEdge]], None, None]:
        subjects = self._load_subjects()
        samples = self._load_samples(subjects)
        yield self._build_tissue_nodes(samples)
        for relationship_batch in self._iter_expression_relationship_batches(samples):
            yield relationship_batch

    def _build_tissue_nodes(self, samples: Dict[str, SampleInfo]) -> List[Tissue]:
        seen: Dict[str, Tissue] = {}
        for sample in samples.values():
            tissue_id = sample.uberon_id if sample.uberon_id else sample.tissue
            if tissue_id not in seen:
                seen[tissue_id] = Tissue(id=tissue_id, name=sample.tissue)
        return list(seen.values())

    def _load_version_info(self) -> DatasourceVersionInfo:
        with open(self.version_file_path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            row = next(reader)

        download_date = self._min_file_date(
            [
                self.matrix_file_path,
                self.sample_attributes_file_path,
                self.subject_phenotypes_file_path,
                self.version_file_path,
            ]
        )
        return DatasourceVersionInfo(
            version=row["version"],
            version_date=date.fromisoformat(row["version_date"]),
            download_date=download_date,
        )

    @staticmethod
    def _min_file_date(file_paths: List[str]) -> Optional[date]:
        timestamps = []
        for file_path in file_paths:
            if not os.path.exists(file_path):
                continue
            timestamps.append(datetime.fromtimestamp(os.path.getmtime(file_path)).date())
        if not timestamps:
            return None
        return min(timestamps)

    def _load_subjects(self) -> Dict[str, SubjectInfo]:
        subject_map: Dict[str, SubjectInfo] = {}
        with open(self.subject_phenotypes_file_path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                subject_id = row.get("SUBJID")
                sex = row.get("SEX")
                if subject_id is None or sex is None:
                    continue
                sex_clean = sex.strip()
                if sex_clean not in {"1", "2"}:
                    continue
                dthhrdy = self._safe_float(row.get("DTHHRDY"))
                subject_map[subject_id] = SubjectInfo(sex=sex_clean, dthhrdy=dthhrdy)
        return subject_map

    def _load_samples(self, subjects: Dict[str, SubjectInfo]) -> Dict[str, SampleInfo]:
        sample_map: Dict[str, SampleInfo] = {}
        with open(self.sample_attributes_file_path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            for row in reader:
                sample_id = row.get("SAMPID")
                tissue = row.get("SMTSD") or row.get("SMTS")
                uberon_id = row.get("SMUBRID")
                if not sample_id or not tissue:
                    continue

                subject_id = self._subject_from_sample_id(sample_id)
                subject = subjects.get(subject_id)
                if subject is None:
                    continue

                # Exclude subjects with a death-hardy score > 2 (legacy filter)
                if subject.dthhrdy is not None and subject.dthhrdy > 2:
                    continue

                smatsscr = self._safe_float(row.get("SMATSSCR"))
                # Exclude samples with moderate (2) or severe (3) autolysis (legacy filter)
                if smatsscr is not None and smatsscr >= 2:
                    continue

                sample_map[sample_id] = SampleInfo(
                    sample_id=sample_id,
                    subject_id=subject_id,
                    tissue=tissue,
                    uberon_id=uberon_id,
                    smatsscr=smatsscr,
                    sex=subject.sex,
                )
        return sample_map

    @staticmethod
    def _subject_from_sample_id(sample_id: str) -> str:
        parts = sample_id.split("-")
        if len(parts) < 2:
            return sample_id
        return f"{parts[0]}-{parts[1]}"

    @staticmethod
    def _safe_float(value: Optional[str]) -> Optional[float]:
        if value is None:
            return None
        value = value.strip()
        if value == "" or value.lower() == "na":
            return None
        try:
            return float(value)
        except ValueError:
            return None

    def _iter_expression_relationship_batches(
        self,
        samples: Dict[str, SampleInfo],
    ) -> Generator[List[Union[Gene, GeneTissueExpressionEdge]], None, None]:
        with gzip.open(self.matrix_file_path, "rt") as f:
            _ = f.readline()
            _ = f.readline()
            reader = csv.reader(f, delimiter="\t")
            header = next(reader)
            sample_ids = header[2:]
            if self.max_samples is not None:
                sample_ids = sample_ids[: self.max_samples]

            batch: List[Union[Gene, GeneTissueExpressionEdge]] = []
            gene_count = 0

            for row in reader:
                if len(row) < 2:
                    continue
                if self.max_genes is not None and gene_count >= self.max_genes:
                    break
                ensembl_id = EquivalentId(id=row[0].split(".")[0], type=Prefix.ENSEMBL).id_str()
                values = row[2:]
                if self.max_samples is not None:
                    values = values[: self.max_samples]
                tissue_values = self._aggregate_gene_by_tissue(sample_ids, values, samples)
                gene, relationships = self._build_records_for_gene(ensembl_id, tissue_values)
                gene_count += 1
                batch.append(gene)
                for relationship in relationships:
                    batch.append(relationship)
                    if len(batch) >= self.batch_size:
                        yield batch
                        batch = []

            if batch:
                yield batch

    def _aggregate_gene_by_tissue(
        self,
        sample_ids: List[str],
        values: List[str],
        samples: Dict[str, SampleInfo],
    ) -> Dict[str, Dict[str, Optional[float]]]:
        per_tissue_male: Dict[str, List[float]] = defaultdict(list)
        per_tissue_female: Dict[str, List[float]] = defaultdict(list)
        per_tissue_uberon: Dict[str, Optional[str]] = {}

        for idx, sample_id in enumerate(sample_ids):
            if idx >= len(values):
                break
            sample = samples.get(sample_id)
            if sample is None:
                continue
            tpm = self._safe_float(values[idx])
            if tpm is None:
                continue

            tissue = sample.tissue
            per_tissue_uberon[tissue] = sample.uberon_id
            if sample.sex == "1":
                per_tissue_male[tissue].append(tpm)
            elif sample.sex == "2":
                per_tissue_female[tissue].append(tpm)

        all_tissues = per_tissue_male.keys() | per_tissue_female.keys()
        tissue_values: Dict[str, Dict[str, Optional[float]]] = {}
        for tissue in all_tissues:
            male_values = per_tissue_male.get(tissue, [])
            female_values = per_tissue_female.get(tissue, [])
            all_values = male_values + female_values
            tissue_values[tissue] = {
                "uberon_id": per_tissue_uberon.get(tissue),
                "tpm": median(all_values),
                "tpm_male": median(male_values) if male_values else None,
                "tpm_female": median(female_values) if female_values else None,
            }
        return tissue_values

    def _build_records_for_gene(
        self,
        ensembl_id: str,
        tissue_values: Dict[str, Dict[str, Optional[float]]],
    ) -> tuple[Gene, List[GeneTissueExpressionEdge]]:
        all_rank_map = self._normalized_rank({k: v["tpm"] for k, v in tissue_values.items()})
        male_rank_map = self._normalized_rank({k: v["tpm_male"] for k, v in tissue_values.items()})
        female_rank_map = self._normalized_rank({k: v["tpm_female"] for k, v in tissue_values.items()})

        overall_tpm = [v["tpm"] for v in tissue_values.values() if v["tpm"] is not None]
        male_tpm = [v["tpm_male"] for v in tissue_values.values() if v["tpm_male"] is not None]
        female_tpm = [v["tpm_female"] for v in tissue_values.values() if v["tpm_female"] is not None]

        gene = Gene(
            id=ensembl_id,
            calculated_properties={
                "gtex_tau": ExpressionAdapterBase._compute_tau(overall_tpm),
                "gtex_tau_male": ExpressionAdapterBase._compute_tau(male_tpm) if male_tpm else 0.0,
                "gtex_tau_female": ExpressionAdapterBase._compute_tau(female_tpm) if female_tpm else 0.0,
            },
        )
        records: List[GeneTissueExpressionEdge] = []

        for tissue, values in tissue_values.items():
            uberon_id = values.get("uberon_id")
            tissue_id = uberon_id if uberon_id else tissue

            details = [
                ExpressionDetail(
                    source="GTEx",
                    tissue=tissue,
                    source_id=ensembl_id,
                    source_tissue_id=uberon_id,
                    sex=None,
                    number_value=values.get("tpm"),
                    source_rank=all_rank_map.get(tissue),
                ),
            ]
            if values.get("tpm_male") is not None:
                details.append(ExpressionDetail(
                    source="GTEx",
                    tissue=tissue,
                    source_id=ensembl_id,
                    source_tissue_id=uberon_id,
                    sex="male",
                    number_value=values.get("tpm_male"),
                    source_rank=male_rank_map.get(tissue),
                ))
            if values.get("tpm_female") is not None:
                details.append(ExpressionDetail(
                    source="GTEx",
                    tissue=tissue,
                    source_id=ensembl_id,
                    source_tissue_id=uberon_id,
                    sex="female",
                    number_value=values.get("tpm_female"),
                    source_rank=female_rank_map.get(tissue),
                ))

            records.append(
                GeneTissueExpressionEdge(
                    start_node=gene,
                    end_node=Tissue(id=tissue_id),
                    details=details,
                )
            )
        return gene, records

    @staticmethod
    def _normalized_rank(values: Dict[str, Optional[float]]) -> Dict[str, Optional[float]]:
        """Equivalent to the legacy calculateRanks: average-method rank / n, then min-max normalised to [0, 1].
        Lowest TPM → 0.0, highest TPM → 1.0. Ties get the average of their ranks."""
        present = {k: v for k, v in values.items() if v is not None and not math.isnan(v)}
        if not present:
            return {k: None for k in values}

        # All-zero case: return zeros (matches original behaviour)
        if max(present.values()) == 0:
            return {k: (0.0 if k in present else None) for k in values}

        # Average-method rankdata: sort, group ties, assign mean rank (1-based)
        sorted_vals = sorted(present.values())
        n = len(sorted_vals)
        avg_rank: Dict[float, float] = {}
        i = 0
        while i < n:
            j = i
            while j < n - 1 and sorted_vals[j + 1] == sorted_vals[j]:
                j += 1
            mean = (i + 1 + j + 1) / 2  # average of 1-based ranks for this tie group
            for idx in range(i, j + 1):
                avg_rank[sorted_vals[idx]] = mean
            i = j + 1

        raw = {k: avg_rank[v] / n for k, v in present.items()}

        min_r = min(raw.values())
        max_r = max(raw.values())
        r_range = max_r - min_r

        if r_range == 0:
            return {k: (raw[k] if k in raw else None) for k in values}

        return {k: ((raw[k] - min_r) / r_range if k in raw else None) for k in values}
