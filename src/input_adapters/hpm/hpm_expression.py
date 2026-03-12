import csv
import os
from datetime import date, datetime
from typing import Dict, Generator, List, Optional, Union

from src.constants import DataSourceName, Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.expression import ExpressionDetail, ProteinTissueExpressionEdge
from src.models.node import EquivalentId
from src.models.protein import Protein
from src.models.tissue import Tissue


class HPMExpressionAdapter(InputAdapter):
    def __init__(self, data_file_path: str, uberon_map_file_path: str, version_file_path: str):
        self.data_file_path = data_file_path
        self.uberon_map_file_path = uberon_map_file_path
        self.version_file_path = version_file_path
        self.version_info = self._load_version_info()

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.HPM

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[Union[Tissue, Protein, ProteinTissueExpressionEdge]], None, None]:
        uberon_map = self._load_uberon_map()
        tissue_columns = self._load_tissue_columns()
        yield self._build_tissue_nodes(tissue_columns, uberon_map)
        yield from self._iter_expression_batches(tissue_columns, uberon_map)

    def _load_version_info(self) -> DatasourceVersionInfo:
        with open(self.version_file_path, "r") as f:
            reader = csv.DictReader(f, delimiter="\t")
            row = next(reader)

        download_date = None
        if os.path.exists(self.data_file_path):
            download_date = datetime.fromtimestamp(os.path.getmtime(self.data_file_path)).date()

        version_date = None
        raw_date = row.get("version_date")
        if raw_date:
            try:
                version_date = date.fromisoformat(raw_date.strip())
            except ValueError:
                pass

        return DatasourceVersionInfo(
            version=row.get("version"),
            version_date=version_date,
            download_date=download_date,
        )

    def _load_uberon_map(self) -> Dict[str, Optional[str]]:
        uberon_map: Dict[str, Optional[str]] = {}
        with open(self.uberon_map_file_path, "r") as f:
            reader = csv.reader(f, delimiter="\t")
            for row in reader:
                if len(row) >= 2:
                    tissue_name = row[0].strip().lower()
                    uberon_id = row[1].strip() if row[1].strip() else None
                    uberon_map[tissue_name] = uberon_id
        return uberon_map

    @staticmethod
    def _tissue_id(tissue_name: str, uberon_id: Optional[str]) -> str:
        if uberon_id:
            return uberon_id
        return EquivalentId(id=tissue_name, type=Prefix.Name).id_str()

    def _load_tissue_columns(self) -> List[str]:
        with open(self.data_file_path, "r") as f:
            reader = csv.DictReader(f)
            return [col for col in reader.fieldnames if col not in ("Accession", "RefSeq Accession")]

    def _build_tissue_nodes(self, tissue_columns: List[str], uberon_map: Dict[str, Optional[str]]) -> List[Tissue]:
        return [
            Tissue(id=self._tissue_id(col, uberon_map.get(col.lower())), name=col)
            for col in tissue_columns
        ]

    def _iter_expression_batches(
        self, tissue_columns: List[str], uberon_map: Dict[str, Optional[str]]
    ) -> Generator[List[Union[Protein, ProteinTissueExpressionEdge]], None, None]:
        batch: List[Union[Protein, ProteinTissueExpressionEdge]] = []

        with open(self.data_file_path, "r") as f:
            reader = csv.DictReader(f)

            for row in reader:
                raw_refseq = row.get("RefSeq Accession", "").strip()
                if not raw_refseq:
                    continue

                refseq_id = raw_refseq.split(".")[0]
                protein_id = EquivalentId(id=refseq_id, type=Prefix.RefSeq).id_str()

                values = []
                for col in tissue_columns:
                    try:
                        values.append(float(row[col]))
                    except (ValueError, KeyError):
                        values.append(0.0)

                tau = self._compute_tau(values)
                rank_map = self._normalized_rank(dict(zip(tissue_columns, values)))

                protein = Protein(id=protein_id, calculated_properties={"hpm_tau": tau})
                edges = []
                for col, value in zip(tissue_columns, values):
                    uberon_id = uberon_map.get(col.lower())
                    tissue_id = self._tissue_id(col, uberon_id)
                    detail = ExpressionDetail(
                        source="HPM Protein",
                        tissue=col,
                        uberon_id=uberon_id,
                        number_value=value,
                        expressed=(value > 0),
                        source_rank=rank_map.get(col),
                    )
                    edges.append(
                        ProteinTissueExpressionEdge(
                            start_node=protein,
                            end_node=Tissue(id=tissue_id),
                            details=[detail],
                        )
                    )

                batch.append(protein)
                batch.extend(edges)
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []

        if batch:
            yield batch

    @staticmethod
    def _compute_tau(values: List[float]) -> float:
        n = len(values)
        if n <= 1:
            return 0.0
        max_val = max(values)
        if max_val == 0.0:
            return 0.0
        return sum(1 - (v / max_val) for v in values) / (n - 1)

    @staticmethod
    def _normalized_rank(values: Dict[str, float]) -> Dict[str, Optional[float]]:
        """Average-method rank, min-max normalized to [0.0, 1.0]. Lowest → 0.0, highest → 1.0."""
        if not values:
            return {}

        if max(values.values()) == 0:
            return {k: 0.0 for k in values}

        sorted_vals = sorted(values.values())
        n = len(sorted_vals)
        avg_rank: Dict[float, float] = {}
        i = 0
        while i < n:
            j = i
            while j < n - 1 and sorted_vals[j + 1] == sorted_vals[j]:
                j += 1
            mean = (i + 1 + j + 1) / 2
            for idx in range(i, j + 1):
                avg_rank[sorted_vals[idx]] = mean
            i = j + 1

        raw = {k: avg_rank[v] / n for k, v in values.items()}
        min_r = min(raw.values())
        max_r = max(raw.values())
        r_range = max_r - min_r

        if r_range == 0:
            return {k: raw[k] for k in values}

        return {k: (raw[k] - min_r) / r_range for k in values}
