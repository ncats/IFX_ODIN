import csv
import os
from datetime import date, datetime
from typing import Dict, List, Optional

from src.constants import Prefix
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.node import EquivalentId


class ExpressionAdapterBase(InputAdapter):
    """Shared base for expression adapters that use a TSV version file and UBERON map.

    Provides: version loading, UBERON map loading, tissue ID resolution,
    tau (tissue specificity) calculation, and normalized rank.
    """

    def __init__(self, data_file_path: str, version_file_path: str, uberon_map_file_path: str):
        self.data_file_path = data_file_path
        self.version_file_path = version_file_path
        self.uberon_map_file_path = uberon_map_file_path
        self.version_info = self._load_version_info()

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

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
                    key = row[0].strip().lower()
                    value = row[1].strip() if row[1].strip() else None
                    uberon_map[key] = value
        return uberon_map

    @staticmethod
    def _tissue_id(tissue_name: str, uberon_map: Dict[str, Optional[str]]) -> str:
        uberon_id = uberon_map.get(tissue_name.lower())
        if uberon_id:
            return uberon_id
        return EquivalentId(id=tissue_name, type=Prefix.Name).id_str()

    @staticmethod
    def _compute_tau(values: List[float]) -> float:
        """Tau tissue specificity score (Yanai et al. 2005)."""
        n = len(values)
        if n <= 1:
            return 0.0
        max_val = max(values)
        if max_val == 0.0:
            return 0.0
        return sum(1 - (v / max_val) for v in values) / (n - 1)

    @staticmethod
    def _normalized_rank(values: Dict[str, float]) -> Dict[str, float]:
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
            return raw
        return {k: (raw[k] - min_r) / r_range for k in values}