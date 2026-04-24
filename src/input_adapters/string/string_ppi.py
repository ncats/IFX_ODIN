import csv
import gzip
from datetime import date
from pathlib import Path
from typing import Generator, List, Optional

from src.constants import DataSourceName, Prefix
from src.input_adapters.flat_file_adapter import FlatFileAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ppi import PPIEdge
from src.models.protein import Protein


class StringPPIAdapter(FlatFileAdapter):
    version_info: DatasourceVersionInfo

    def __init__(
        self,
        file_path: str,
        version_file_path: Optional[str] = None,
        score_cutoff: int = 400,
        max_rows: Optional[int] = None,
    ):
        FlatFileAdapter.__init__(self, file_path=file_path)
        self.score_cutoff = int(score_cutoff)
        self.max_rows = max_rows
        self.version_info = self._load_version_info(version_file_path)

    def _load_version_info(self, version_file_path: Optional[str]) -> DatasourceVersionInfo:
        version = None
        version_date = None
        download_date = self.download_date
        if version_file_path:
            with open(version_file_path, "r", encoding="utf-8") as handle:
                reader = csv.DictReader(handle, delimiter="\t")
                first_row = next(reader, None)
                if first_row:
                    version = first_row.get("version") or None
                    version_date_str = first_row.get("version_date") or None
                    download_date_str = first_row.get("download_date") or None
                    version_date = date.fromisoformat(version_date_str) if version_date_str else None
                    download_date = date.fromisoformat(download_date_str) if download_date_str else download_date
        return DatasourceVersionInfo(
            version=version,
            version_date=version_date,
            download_date=download_date,
        )

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.STRING

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def _open_input(self):
        path = Path(self.file_path)
        if path.suffix == ".gz":
            return gzip.open(path, "rt", encoding="utf-8")
        return open(path, "r", encoding="utf-8")

    @staticmethod
    def _strip_taxon_prefix(protein_id: str) -> str:
        if protein_id.startswith("9606."):
            return protein_id.split(".", 1)[1]
        return protein_id

    def get_all(self) -> Generator[List[PPIEdge], None, None]:
        batch: List[PPIEdge] = []
        kept_rows = 0
        with self._open_input() as handle:
            header = handle.readline().strip().split()
            for line in handle:
                if self.max_rows is not None and kept_rows >= self.max_rows:
                    break
                parts = line.strip().split()
                if not parts:
                    continue
                row = dict(zip(header, parts))
                score = int(row["combined_score"])
                if score < self.score_cutoff:
                    continue

                protein1 = self._strip_taxon_prefix(row["protein1"])
                protein2 = self._strip_taxon_prefix(row["protein2"])
                if protein1 == protein2:
                    continue
                protein1, protein2 = sorted((protein1, protein2))

                edge = PPIEdge(
                    start_node=Protein(id=f"{Prefix.ENSEMBL}:{protein1}"),
                    end_node=Protein(id=f"{Prefix.ENSEMBL}:{protein2}"),
                    score=[score],
                )
                batch.append(edge)
                kept_rows += 1
                if len(batch) >= self.batch_size:
                    yield batch
                    batch = []
        yield batch
